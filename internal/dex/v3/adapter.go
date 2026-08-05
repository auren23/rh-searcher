// Package v3 Uniswap V3-compatible 池适配器。
// 多个 V3 DEX 只需配置不同 Factory/Router/Quoter/initCodeHash。
package v3

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/auren23/rh-searcher/internal/dex"
)

// Pool 内存中的 V3 池状态。
type Pool struct {
	Address      common.Address
	Exchange     string
	Token0       common.Address
	Token1       common.Address
	Fee          uint32
	Tick         int
	Liquidity    *big.Int
	SqrtPriceX96 *big.Int
	// ticks 未实现的完整 tick 位图；MVP 只做单区间报价
	ObservedBlock uint64
}

func (p *Pool) Pool() dex.Pool {
	return dex.Pool{
		ID: p.Address.Hex(), Protocol: "v3", Exchange: p.Exchange,
		Token0: p.Token0, Token1: p.Token1, Fee: p.Fee,
		Liquidity: p.Liquidity, SqrtPriceX96: p.SqrtPriceX96, Tick: p.Tick,
	}
}

// Factory 事件与函数签名（完整 ABI 只需用到的事件/方法）。
const (
	eventPoolCreated = "PoolCreated(address indexed token0, address indexed token1, uint24 indexed fee, int24 tickSpacing, address pool)"
	eventSwap        = "Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"
	eventMint        = "Mint(address sender, address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)"
	eventBurn        = "Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1)"
)

// 事件签名 hash 不含 indexed 关键字，见下方 *Topic() 函数。

// Adapter V3 适配器。
type Adapter struct {
	cli          *ethclient.Client
	exchange     string
	factory      common.Address
	router       common.Address
	routerKind   string // swaprouter | universal
	initCodeHash common.Hash
	factoryBlock uint64
	events       map[common.Hash]abi.Event
}

func NewAdapter(cli *ethclient.Client, exchange string, factory, router common.Address, routerKind string, initCodeHash common.Hash, factoryBlock uint64) (*Adapter, error) {
	a := &Adapter{
		cli: cli, exchange: exchange, factory: factory, router: router,
		routerKind: routerKind, initCodeHash: initCodeHash, factoryBlock: factoryBlock,
		events: make(map[common.Hash]abi.Event),
	}
	fullABI := fmt.Sprintf(`[{"anonymous":false,"inputs":[{"indexed":true,"name":"token0","type":"address"},{"indexed":true,"name":"token1","type":"address"},{"indexed":true,"name":"fee","type":"uint24"},{"indexed":false,"name":"tickSpacing","type":"int24"},{"indexed":false,"name":"pool","type":"address"}],"name":"PoolCreated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount0","type":"int256"},{"indexed":false,"name":"amount1","type":"int256"},{"indexed":false,"name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"name":"liquidity","type":"uint128"},{"indexed":false,"name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"tickLower","type":"int24"},{"indexed":true,"name":"tickUpper","type":"int24"},{"indexed":false,"name":"amount","type":"uint128"},{"indexed":false,"name":"amount0","type":"uint256"},{"indexed":false,"name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"tickLower","type":"int24"},{"indexed":true,"name":"tickUpper","type":"int24"},{"indexed":false,"name":"amount","type":"uint128"},{"indexed":false,"name":"amount0","type":"uint256"},{"indexed":false,"name":"amount1","type":"uint256"}],"name":"Burn","type":"event"}]`)
	parsed, err := abi.JSON(strings.NewReader(fullABI))
	if err != nil {
		return nil, fmt.Errorf("parse v3 abi: %w", err)
	}
	for _, ev := range parsed.Events {
		a.events[ev.ID] = ev
	}
	return a, nil
}

func (a *Adapter) Protocol() string { return "v3" }

// PoolCreatedTopic 工厂创建池事件 topic0。
func PoolCreatedTopic() common.Hash {
	return crypto.Keccak256Hash([]byte("PoolCreated(address,address,uint24,int24,address)"))
}

// SwapTopic Swap 事件 topic0。
func SwapTopic() common.Hash {
	return crypto.Keccak256Hash([]byte("Swap(address,address,int256,int256,uint160,uint128,int24)"))
}

// DiscoverPools 扫描工厂 PoolCreated 日志，构造池并读取初始 slot0/liquidity。
func (a *Adapter) DiscoverPools(ctx context.Context, fromBlock uint64, toBlock uint64) ([]*Pool, error) {
	q := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: []common.Address{a.factory},
	}
	logs, err := a.cli.FilterLogs(ctx, q)
	if err != nil {
		if strings.Contains(err.Error(), "429") {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(3 * time.Second): // ponytail: 公共 RPC 限速退避
			}
			return a.DiscoverPools(ctx, fromBlock, toBlock) // 重试同批
		}
		return nil, err
	}
	ev := a.events[PoolCreatedTopic()]
	if ev.Name == "" {
		return nil, fmt.Errorf("PoolCreated event not found in abi")
	}
	out := []*Pool{}
	for _, l := range logs {
		if len(l.Topics) < 4 {
			continue
		}
		// indexed: token0, token1, fee；data: tickSpacing, pool
		v, err := ev.Inputs.Unpack(l.Data)
		if err != nil || len(v) < 2 {
			continue
		}
		poolAddr := v[1].(common.Address)
		p := &Pool{
			Address: poolAddr, Exchange: a.exchange,
			Token0:        common.BytesToAddress(l.Topics[1][12:]),
			Token1:        common.BytesToAddress(l.Topics[2][12:]),
			Fee:           uint32(new(big.Int).SetBytes(l.Topics[3][29:]).Uint64()),
			ObservedBlock: l.BlockNumber,
		}
		// 惰性状态：slot0/liquidity 首次事件时加载，避免全量发现的 RPC 开销
		out = append(out, p)
	}
	return out, nil
}

// loadSlot0 读取池的 slot0（sqrtPriceX96/tick）与 liquidity。
func (a *Adapter) loadSlot0(ctx context.Context, p *Pool) error {
	data := crypto.Keccak256([]byte("slot0()"))[:4]
	res, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: data}, nil)
	if err != nil {
		return err
	}
	if len(res) >= 32+32 {
		p.SqrtPriceX96 = new(big.Int).SetBytes(res[0:32])
		// tick 是 int24，编码在第二个 32 字节槽的末尾
		raw := new(big.Int).SetBytes(res[61:64])
		t := raw.Int64()
		if t >= 1<<23 {
			t -= 1 << 24
		}
		p.Tick = int(t)
	}
	liqData := crypto.Keccak256([]byte("liquidity()"))[:4]
	liqRes, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: liqData}, nil)
	if err == nil && len(liqRes) >= 32 {
		p.Liquidity = new(big.Int).SetBytes(liqRes[:32])
	}
	return nil
}

// ApplyLog 应用 Swap/Mint/Burn 到池状态。
func (a *Adapter) ApplyLog(p *Pool, log types.Log) (*Pool, error) {
	if log.Address != p.Address {
		return p, fmt.Errorf("log from wrong pool")
	}
	ev, ok := a.events[log.Topics[0]]
	if !ok {
		return p, nil
	}
	v, err := ev.Inputs.Unpack(log.Data)
	if err != nil {
		return p, fmt.Errorf("unpack %s: %w", ev.Name, err)
	}
	p.ObservedBlock = log.BlockNumber
	switch ev.Name {
	case "Swap":
		p.SqrtPriceX96 = v[4].(*big.Int)
		p.Liquidity = v[5].(*big.Int)
		tick := v[6].(*big.Int)
		p.Tick = int(tick.Int64())
	case "Mint":
		if p.Liquidity == nil {
			_ = a.loadSlot0(context.Background(), p) // 惰性初始化
		}
		p.Liquidity.Add(p.Liquidity, v[3].(*big.Int))
	case "Burn":
		if p.Liquidity == nil {
			_ = a.loadSlot0(context.Background(), p)
		}
		p.Liquidity.Sub(p.Liquidity, v[3].(*big.Int))
		if p.Liquidity.Sign() < 0 {
			p.Liquidity.SetInt64(0)
		}
	}
	return p, nil
}

// QuoteExactIn 本地报价（单 tick 区间内精确，跨 tick 返回近似误差已标记）。
func (a *Adapter) QuoteExactIn(p *Pool, tokenIn common.Address, amountIn *big.Int) (*big.Int, error) {
	if amountIn.Sign() <= 0 {
		return big.NewInt(0), nil
	}
	// 扣手续费
	feeNum := new(big.Int).SetUint64(1_000_000 - uint64(p.Fee))
	amountAfterFee := new(big.Int).Mul(amountIn, feeNum)
	amountAfterFee.Div(amountAfterFee, big.NewInt(1_000_000))

	Q := new(big.Int).Set(p.SqrtPriceX96)
	L := new(big.Int).Set(p.Liquidity)
	if L.Sign() <= 0 {
		return nil, fmt.Errorf("pool %s has zero liquidity", p.Address.Hex())
	}
	numerator1 := new(big.Int).Lsh(L, 96)
	product := new(big.Int).Mul(amountAfterFee, Q)
	denominator := new(big.Int).Add(numerator1, product)
	// Q' = ceil(numerator1 * Q / denominator)
	Qp := new(big.Int).Mul(numerator1, Q)
	Qp.Add(Qp, new(big.Int).Sub(denominator, big.NewInt(1)))
	Qp.Div(Qp, denominator)

	// out = L * |Q' - Q| * 2^96 / (Q * Q')
	dQ := new(big.Int).Sub(Qp, Q)
	dQ.Abs(dQ)
	out := new(big.Int).Mul(L, dQ)
	out.Lsh(out, 96)
	out.Div(out, new(big.Int).Mul(Q, Qp))
	return out, nil
}

// BuildSwap 构建 Router.exactInputSingle 的 calldata。tokenIn 决定交易方向。
func (a *Adapter) BuildSwap(p *Pool, tokenIn, recipient common.Address, amountIn, minOut *big.Int, deadline uint64, sqrtPriceLimit *big.Int) ([]byte, error) {
	// exactInputSingle((address tokenIn,address tokenOut,uint24 fee,address recipient,uint256 deadline,uint256 amountIn,uint256 amountOutMinimum,uint160 sqrtPriceLimitX96))
	sig := "exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
	selector := crypto.Keccak256([]byte(sig))[:4]
	enc := make([]byte, 0, 32*8+4)
	enc = append(enc, selector...)
	if p.Token0 == p.Token1 {
		return nil, fmt.Errorf("degenerate pool")
	}
	tokenOut := p.Token1
	if tokenIn == p.Token0 {
		tokenOut = p.Token1
	} else if tokenIn == p.Token1 {
		tokenOut = p.Token0
	} else {
		return nil, fmt.Errorf("tokenIn %s not in pool %s", tokenIn.Hex(), p.Address.Hex())
	}
	// 参数按 ABI 编码（uint160 补零）
	enc = append(enc, leftPad(tokenIn.Bytes())...)
	enc = append(enc, leftPad(tokenOut.Bytes())...)
	enc = append(enc, leftPadUint(uint64(p.Fee), 24)...)
	enc = append(enc, leftPad(recipient.Bytes())...)
	enc = append(enc, leftPadUint(deadline, 256)...)
	enc = append(enc, leftPad(amountIn.Bytes())...)
	enc = append(enc, leftPad(minOut.Bytes())...)
	limit := sqrtPriceLimit
	if limit == nil {
		limit = big.NewInt(0)
	}
	enc = append(enc, leftPad(limit.Bytes())...)
	return enc, nil
}

// buildUniversalSwap UniversalRouter.execute 的 V3_SWAP_EXACT_IN (command 0x08)。
// input = abi.encode(recipient, amountIn, amountOutMinimum, path, payerIsUser)，
// payerIsUser=false → payer = msg.sender（执行合约，调用前已 approve）。
func buildUniversalSwap(tokenIn, tokenOut common.Address, fee uint32, recipient common.Address, amountIn, minOut *big.Int) ([]byte, error) {
	path := make([]byte, 0, 43)
	path = append(path, tokenIn.Bytes()...)
	path = append(path, byte(fee>>16), byte(fee>>8), byte(fee))
	path = append(path, tokenOut.Bytes()...)

	executeABI := `[{"inputs":[{"internalType":"bytes","name":"commands","type":"bytes"},{"internalType":"bytes[]","name":"inputs","type":"bytes[]"}],"name":"execute","outputs":[],"stateMutability":"payable","type":"function"}]`
	parsed, err := abi.JSON(strings.NewReader(executeABI))
	if err != nil {
		return nil, fmt.Errorf("parse universal router abi: %w", err)
	}

	v3SwapInput := make([]byte, 0, 128+len(path))
	v3SwapInput = append(v3SwapInput, leftPad(recipient.Bytes())...)
	v3SwapInput = append(v3SwapInput, leftPad(amountIn.Bytes())...)
	v3SwapInput = append(v3SwapInput, leftPad(minOut.Bytes())...)
	// bytes path 的动态编码：offset, len, data（pad 到 32）
	pathOff := 32 * 3
	v3SwapInput = append(v3SwapInput, leftPadUint(uint64(pathOff), 256)...)
	v3SwapInput = append(v3SwapInput, leftPadUint(uint64(len(path)), 256)...)
	v3SwapInput = append(v3SwapInput, path...)
	v3SwapInput = append(v3SwapInput, make([]byte, (32-len(path)%32)%32)...)
	// bool payerIsUser
	v3SwapInput = append(v3SwapInput, make([]byte, 31)...)
	v3SwapInput = append(v3SwapInput, 0)

	return parsed.Pack("execute", []byte{0x08}, [][]byte{v3SwapInput})
}

// leftPad 按 32 字节左补零。
func leftPad(b []byte) []byte {
	out := make([]byte, 32)
	copy(out[32-len(b):], b)
	return out
}

func leftPadUint(v uint64, bits int) []byte {
	b := big.NewInt(int64(v)).Bytes()
	return leftPad(b)
}

// PoolState 适配 dex.PoolState 接口。
type stateAdapter struct{ p *Pool }

func (s stateAdapter) Pool() dex.Pool { return s.p.Pool() }

// PoolByAddress 按地址构造池并读取 token0/token1/fee（用于事件驱动下发现的新池）。
func (a *Adapter) PoolByAddress(ctx context.Context, addr common.Address) (*Pool, error) {
	callData := func(sig string) []byte { return crypto.Keccak256([]byte(sig))[:4] }
	read := func(data []byte) ([]byte, error) {
		return a.cli.CallContract(ctx, ethereum.CallMsg{To: &addr, Data: data}, nil)
	}
	t0raw, err := read(callData("token0()"))
	if err != nil {
		return nil, err
	}
	t1raw, err := read(callData("token1()"))
	if err != nil {
		return nil, err
	}
	feeraw, err := read(callData("fee()"))
	if err != nil {
		return nil, err
	}
	p := &Pool{
		Address: addr, Exchange: a.exchange,
		Token0: common.BytesToAddress(t0raw[12:32]),
		Token1: common.BytesToAddress(t1raw[12:32]),
		Fee:    uint32(new(big.Int).SetBytes(feeraw[29:32]).Uint64()),
	}
	_ = a.loadSlot0(ctx, p)
	return p, nil
}
