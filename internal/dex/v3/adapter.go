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
)

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
	fullABI := fmt.Sprintf(`[{"anonymous":false,"inputs":[{"indexed":true,"name":"token0","type":"address"},{"indexed":true,"name":"token1","type":"address"},{"indexed":true,"name":"fee","type":"uint24"},{"indexed":false,"name":"tickSpacing","type":"int24"},{"indexed":false,"name":"pool","type":"address"}],"name":"PoolCreated","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"sender","type":"address"},{"indexed":true,"name":"recipient","type":"address"},{"indexed":false,"name":"amount0","type":"int256"},{"indexed":false,"name":"amount1","type":"int256"},{"indexed":false,"name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"name":"liquidity","type":"uint128"},{"indexed":false,"name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"name":"sender","type":"address"},{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"tickLower","type":"int24"},{"indexed":true,"name":"tickUpper","type":"int24"},{"indexed":false,"name":"amount","type":"uint128"},{"indexed":false,"name":"amount0","type":"uint256"},{"indexed":false,"name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"owner","type":"address"},{"indexed":true,"name":"tickLower","type":"int24"},{"indexed":true,"name":"tickUpper","type":"int24"},{"indexed":false,"name":"amount","type":"uint128"},{"indexed":false,"name":"amount0","type":"uint256"},{"indexed":false,"name":"amount1","type":"uint256"}],"name":"Burn","type":"event"}]`)
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
		tickSpacing := int(v[0].(*big.Int).Int64())
		if tickSpacing <= 0 {
			continue // 非法 spacing（防御）
		}
		p := &Pool{
			Address: poolAddr, Exchange: a.exchange,
			Token0:        common.BytesToAddress(l.Topics[1][12:]),
			Token1:        common.BytesToAddress(l.Topics[2][12:]),
			Fee:           uint32(new(big.Int).SetBytes(l.Topics[3][29:]).Uint64()),
			TickSpacing:   tickSpacing,
			ObservedBlock: l.BlockNumber,
			ticks:         make(map[int]*Tick),
			bitmap:        make(map[int64]*big.Int),
			bitmapLoaded:  make(map[int64]bool),
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

// DecodeLog 解码一条事件到池状态。返回 (池状态变更闭包, 是否认识的事件)。
// 事件 ABI（data 字段顺序，indexed 参数在 topics）：
//   Swap:   amount0, amount1, sqrtPriceX96, liquidity, tick
//   Mint:   sender, amount, amount0, amount1（sender 非 indexed！）
//   Burn:   amount, amount0, amount1
func (a *Adapter) DecodeLog(p *Pool, log types.Log) (apply func(), err error) {
	ev, ok := a.events[log.Topics[0]]
	if !ok {
		return nil, nil // 不认识的事件，忽略
	}
	v, err := ev.Inputs.Unpack(log.Data)
	if err != nil {
		return nil, fmt.Errorf("unpack %s: %w", ev.Name, err)
	}
	switch ev.Name {
	case "Swap":
		if len(v) < 5 {
			return nil, fmt.Errorf("Swap: %d values, want 5", len(v))
		}
		sqrtPriceX96 := v[2].(*big.Int)
		liquidity := v[3].(*big.Int)
		tick := v[4].(*big.Int)
		return func() { p.ApplySwap(log, sqrtPriceX96, liquidity, int(tick.Int64())) }, nil
	case "Mint":
		if len(v) < 4 {
			return nil, fmt.Errorf("Mint: %d values, want 4", len(v))
		}
		amount := v[1].(*big.Int)
		tickLower := decodeInt24(log.Topics[2])
		tickUpper := decodeInt24(log.Topics[3])
		return func() { p.ApplyMintBurn(log, tickLower, tickUpper, amount, true) }, nil
	case "Burn":
		if len(v) < 3 {
			return nil, fmt.Errorf("Burn: %d values, want 3", len(v))
		}
		amount := v[0].(*big.Int)
		// Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper, ...)
		tickLower := decodeInt24(log.Topics[2])
		tickUpper := decodeInt24(log.Topics[3])
		return func() { p.ApplyMintBurn(log, tickLower, tickUpper, amount, false) }, nil
	}
	return nil, nil
}

// decodeInt24 从 topics 中解码 int24（右对齐 3 字节，补码）。
func decodeInt24(topic common.Hash) int {
	raw := new(big.Int).SetBytes(topic[29:32])
	n := raw.Int64()
	if n >= 1<<23 {
		n -= 1 << 24
	}
	return int(n)
}

// LoadBitmapWord 按需读取池 tickBitmap 的一个 word（区分"未加载"与"真实为 0"）。
func (a *Adapter) LoadBitmapWord(ctx context.Context, p *Pool, wordPos int64) error {
	sel := crypto.Keccak256([]byte("tickBitmap(int16)"))[:4]
	arg := new(big.Int).SetInt64(wordPos)
	argB := leftPad(arg.Bytes())
	data := append(sel, argB...)
	res, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: data}, nil)
	if err != nil {
		return err
	}
	if len(res) < 32 {
		return fmt.Errorf("short tickBitmap response %d bytes", len(res))
	}
	if p.bitmap == nil {
		p.bitmap = make(map[int64]*big.Int)
	}
	if p.bitmapLoaded == nil {
		p.bitmapLoaded = make(map[int64]bool)
	}
	p.bitmap[wordPos] = new(big.Int).SetBytes(res[:32])
	p.bitmapLoaded[wordPos] = true
	return nil
}

// PoolMeta PoolCreated 事件解码出的池元数据。
type PoolMeta struct {
	Pool        common.Address
	Token0      common.Address
	Token1      common.Address
	Fee         uint32
	TickSpacing int
}

// DecodePoolCreated 从 PoolCreated 日志解码池地址与元数据。
// 注意：l.Address 是 Factory 地址，新池地址在 data 中（tickSpacing, pool）。
func (a *Adapter) DecodePoolCreated(log types.Log) (*PoolMeta, error) {
	if len(log.Topics) < 4 {
		return nil, fmt.Errorf("PoolCreated: %d topics, want 4", len(log.Topics))
	}
	ev, ok := a.events[log.Topics[0]]
	if !ok || ev.Name != "PoolCreated" {
		return nil, fmt.Errorf("not a PoolCreated event")
	}
	v, err := ev.Inputs.Unpack(log.Data)
	if err != nil || len(v) < 2 {
		return nil, fmt.Errorf("unpack PoolCreated: %w", err)
	}
	tickSpacing := int(v[0].(*big.Int).Int64())
	if tickSpacing <= 0 {
		return nil, fmt.Errorf("invalid tickSpacing %d", tickSpacing)
	}
	return &PoolMeta{
		Pool:        v[1].(common.Address),
		Token0:      common.BytesToAddress(log.Topics[1][12:]),
		Token1:      common.BytesToAddress(log.Topics[2][12:]),
		Fee:         uint32(new(big.Int).SetBytes(log.Topics[3][29:]).Uint64()),
		TickSpacing: tickSpacing,
	}, nil
}

// QuoteExactIn 本地精确报价（严格单 tick 模式）。
// - 双向公式与官方 SqrtPriceMath 一致（token0 输入走 getNextSqrtPriceFromAmount0RoundingUp(add=false)，
//   token1 输入走 getNextSqrtPriceFromAmount1RoundingDown(add=true)）
// - 输入超过下一 initialized tick 边界 → ErrTickCrossed（绝不近似）
// - tick 索引不完整（无 slot0/流动性）→ ErrStateIncomplete
func (a *Adapter) QuoteExactIn(p *Pool, tokenIn common.Address, amountIn *big.Int) (*big.Int, error) {
	if amountIn.Sign() <= 0 {
		return big.NewInt(0), nil
	}
	if p.SqrtPriceX96 == nil || p.Liquidity == nil || p.Liquidity.Sign() <= 0 || p.TickSpacing <= 0 {
		return nil, ErrStateIncomplete
	}
	zeroForOne := tokenIn == p.Token0
	if !zeroForOne && tokenIn != p.Token1 {
		return nil, fmt.Errorf("tokenIn %s not in pool %s", tokenIn.Hex(), p.Address.Hex())
	}
	// 扣手续费
	amount := new(big.Int).Mul(amountIn, new(big.Int).SetUint64(uint64(1_000_000-p.Fee)))
	amount.Div(amount, big.NewInt(1_000_000))
	if amount.Sign() <= 0 {
		return big.NewInt(0), nil
	}

	Q := p.SqrtPriceX96
	L := p.Liquidity
	var out *big.Int
	if zeroForOne {
		// token0 → token1：用户给池 token0（池 token0 增加）→ 价格（token1/token0）下降。
		// 官方 SwapMath: getNextSqrtPriceFromAmount0RoundingUp(add=true)，Q' < Q。
		// 边界 = 下一 initialized tick（向下，价格更低的方向）。
		boundTick, _ := p.nextInitializedTick(p.Tick, true)
		Qb := GetSqrtRatioAtTick(boundTick)
		if Qb.Cmp(Q) >= 0 {
			return nil, ErrTickCrossed
		}
		// x_max：Q' = ceil(n1*Q/(n1+x*Q)) → x = floor(n1*(Q-Q')/(Q*Q'))
		xMax := new(big.Int).Lsh(L, 96)
		xMax.Mul(xMax, new(big.Int).Sub(Q, Qb))
		xMax.Div(xMax, new(big.Int).Mul(Q, Qb))
		if amount.Cmp(xMax) > 0 {
			return nil, ErrTickCrossed
		}
		Qp := getNextSqrtPriceFromAmount0RoundingUp(Q, L, amount, true)
		out = getAmount1Delta(Qp, Q, L, false) // Qp < Q
	} else {
		// token1 → token0：池 token1 增加 → 价格上升。add=true，Q' > Q。
		boundTick, _ := p.nextInitializedTick(p.Tick, false)
		Qb := GetSqrtRatioAtTick(boundTick)
		if Qb.Cmp(Q) <= 0 {
			return nil, ErrTickCrossed
		}
		// y_max：Q' = Q + y*2^96/L → y = floor((Qb-Q)*L/2^96)
		yMax := new(big.Int).Sub(Qb, Q)
		yMax.Mul(yMax, L)
		yMax.Rsh(yMax, 96)
		if amount.Cmp(yMax) > 0 {
			return nil, ErrTickCrossed
		}
		Qp := getNextSqrtPriceFromAmount1RoundingDown(Q, L, amount, true)
		out = getAmount0Delta(Q, Qp, L, false) // Qp > Q
	}
	return out, nil
}



// PoolByAddress 按地址构造池并读取 token0/token1/fee/tickSpacing（含返回长度校验 + Factory 归属验证）。
func (a *Adapter) PoolByAddress(ctx context.Context, addr common.Address) (*Pool, error) {
	read := func(data []byte) ([]byte, error) {
		res, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &addr, Data: data}, nil)
		if err != nil {
			return nil, err
		}
		if len(res) < 32 {
			return nil, fmt.Errorf("short response %d bytes", len(res))
		}
		return res, nil
	}
	t0raw, err := read(crypto.Keccak256([]byte("token0()"))[:4])
	if err != nil {
		return nil, err
	}
	t1raw, err := read(crypto.Keccak256([]byte("token1()"))[:4])
	if err != nil {
		return nil, err
	}
	feeraw, err := read(crypto.Keccak256([]byte("fee()"))[:4])
	if err != nil {
		return nil, err
	}
	tsraw, err := read(crypto.Keccak256([]byte("tickSpacing()"))[:4])
	if err != nil {
		return nil, err
	}
	// Factory 归属验证：factory.getPool(token0, token1, fee) == addr
	sel := crypto.Keccak256([]byte("getPool(address,address,uint24)"))[:4]
	args := append(append(append(sel,
		leftPad(t0raw[12:32])...), leftPad(t1raw[12:32])...), leftPad(feeraw[29:32])...)
	got, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &a.factory, Data: args}, nil)
	if err != nil || len(got) < 32 || common.BytesToAddress(got[12:32]) != addr {
		return nil, fmt.Errorf("pool %s not verified by factory %s", addr.Hex(), a.factory.Hex())
	}
	ts := int(new(big.Int).SetBytes(tsraw[29:32]).Int64())
	if ts <= 0 {
		return nil, fmt.Errorf("pool %s invalid tickSpacing %d", addr.Hex(), ts)
	}
	p := &Pool{
		Address: addr, Exchange: a.exchange,
		Token0:      common.BytesToAddress(t0raw[12:32]),
		Token1:      common.BytesToAddress(t1raw[12:32]),
		Fee:         uint32(new(big.Int).SetBytes(feeraw[29:32]).Uint64()),
		TickSpacing: ts,
		ticks:       make(map[int]*Tick),
		bitmap:      make(map[int64]*big.Int),
		bitmapLoaded: make(map[int64]bool),
	}
	_ = a.loadSlot0(ctx, p)
	return p, nil
}

// BuildSwap 构建 swap calldata。按 router kind 分支：
//   - swaprouter: SwapRouter.exactInputSingle
//   - universal:  UniversalRouter.execute(commands, inputs) 的 V3_SWAP_EXACT_IN (0x08)
// tokenIn 决定交易方向。
func (a *Adapter) BuildSwap(p *Pool, tokenIn, recipient common.Address, amountIn, minOut *big.Int, deadline uint64, sqrtPriceLimit *big.Int) ([]byte, error) {
	tokenOut := p.Token1
	if tokenIn == p.Token0 {
		tokenOut = p.Token1
	} else if tokenIn == p.Token1 {
		tokenOut = p.Token0
	} else {
		return nil, fmt.Errorf("tokenIn %s not in pool %s", tokenIn.Hex(), p.Address.Hex())
	}
	if a.routerKind == "universal" {
		return buildUniversalSwap(tokenIn, tokenOut, p.Fee, recipient, amountIn, minOut)
	}
	return buildSwapRouterCall(tokenIn, tokenOut, p.Fee, recipient, amountIn, minOut, deadline, sqrtPriceLimit)
}

// buildSwapRouterCall SwapRouter.exactInputSingle calldata。
func buildSwapRouterCall(tokenIn, tokenOut common.Address, fee uint32, recipient common.Address, amountIn, minOut *big.Int, deadline uint64, sqrtPriceLimit *big.Int) ([]byte, error) {
	sig := "exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
	selector := crypto.Keccak256([]byte(sig))[:4]
	enc := make([]byte, 0, 32*8+4)
	enc = append(enc, selector...)
	enc = append(enc, leftPad(tokenIn.Bytes())...)
	enc = append(enc, leftPad(tokenOut.Bytes())...)
	enc = append(enc, leftPadUint(uint64(fee), 24)...)
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
	pathOff := 32 * 3
	v3SwapInput = append(v3SwapInput, leftPadUint(uint64(pathOff), 256)...)
	v3SwapInput = append(v3SwapInput, leftPadUint(uint64(len(path)), 256)...)
	v3SwapInput = append(v3SwapInput, path...)
	v3SwapInput = append(v3SwapInput, make([]byte, (32-len(path)%32)%32)...)
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
