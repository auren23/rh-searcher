// Package v3 Uniswap V3-compatible 池适配器。
// 多个 V3 DEX 只需配置不同 Factory/Router/Quoter/initCodeHash。
package v3

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
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
	mc3          *multicall3 // 批量只读调用（token-group 快照）
	// bitmapWordCache 跨 head 持久：tickBitmap word 只在 mint/burn 时变化
	// （远慢于价格变动），wordPos 覆盖 ~51k ticks。缓存 (pool, wordPos)→word，
	// 避免每次评估为全部组池重复拉 bitmap（省一次 RPC 往返）。
	bitmapWordCache map[common.Address]map[int64]*big.Int
}

func NewAdapter(cli *ethclient.Client, exchange string, factory, router common.Address, routerKind string, initCodeHash common.Hash, factoryBlock uint64) (*Adapter, error) {
	a := &Adapter{
		cli: cli, exchange: exchange, factory: factory, router: router,
		routerKind: routerKind, initCodeHash: initCodeHash, factoryBlock: factoryBlock,
		events:          make(map[common.Hash]abi.Event),
		mc3:             newMulticall3(cli),
		bitmapWordCache: make(map[common.Address]map[int64]*big.Int),
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

// MintTopic Mint 事件 topic0（Mint 有 sender + owner 两个 address）。
func MintTopic() common.Hash {
	return crypto.Keccak256Hash([]byte("Mint(address,address,int24,int24,uint128,uint256,uint256)"))
}

// BurnTopic Burn 事件 topic0（Burn 只有 owner 一个 address）。
func BurnTopic() common.Hash {
	return crypto.Keccak256Hash([]byte("Burn(address,int24,int24,uint128,uint256,uint256)"))
}

// DiscoverPools 扫描工厂 PoolCreated 日志，构造池（惰性状态，不读 slot0/liquidity）。
func (a *Adapter) DiscoverPools(ctx context.Context, fromBlock uint64, toBlock uint64) ([]*Pool, error) {
	logs, err := a.factoryLogs(ctx, fromBlock, toBlock, nil)
	if err != nil {
		return nil, err
	}
	out := make([]*Pool, 0, len(logs))
	for _, l := range logs {
		p, err := a.poolFromCreatedLog(l)
		if err != nil {
			continue // 单条解码失败不影响批次（防御）
		}
		out = append(out, p)
	}
	return out, nil
}

// DiscoverWETHPools 只返回包含 weth 的池（节点端 topics 过滤）：
// PoolCreated(token0, token1, fee, ...) 的 indexed token0/token1 各查一次。
// Bootstrap WETH 宇宙用：响应体积小、客户端零过滤开销。
func (a *Adapter) DiscoverWETHPools(ctx context.Context, fromBlock, toBlock uint64, weth common.Address) ([]*Pool, error) {
	wethTopic := common.BytesToHash(common.LeftPadBytes(weth.Bytes(), 32))
	var out []*Pool
	for _, topics := range [][][]common.Hash{
		{{PoolCreatedTopic()}, {wethTopic}, {}}, // token0 == weth
		{{PoolCreatedTopic()}, {}, {wethTopic}}, // token1 == weth
	} {
		logs, err := a.factoryLogs(ctx, fromBlock, toBlock, topics)
		if err != nil {
			return nil, err
		}
		for _, l := range logs {
			p, err := a.poolFromCreatedLog(l)
			if err != nil {
				continue
			}
			out = append(out, p)
		}
	}
	return out, nil
}

// factoryLogs 工厂 PoolCreated 日志查询（429 退避重试；topics 可为 nil=不过滤）。
func (a *Adapter) factoryLogs(ctx context.Context, fromBlock, toBlock uint64, topics [][]common.Hash) ([]types.Log, error) {
	q := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   big.NewInt(int64(toBlock)),
		Addresses: []common.Address{a.factory},
		Topics:    topics,
	}
	var lastErr error
	backoff := time.Second
	for attempt := 0; attempt < 120; attempt++ {
		logs, err := a.cli.FilterLogs(ctx, q)
		if err == nil {
			return logs, nil
		}
		lastErr = err
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "429") || strings.Contains(msg, "rate limit") ||
			strings.Contains(msg, "timeout") || strings.Contains(msg, "timed out") {
			// 公共 RPC 限速/查询超时风暴：指数退避（1s→60s 封顶），持续重试
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// poolFromCreatedLog 从单条 PoolCreated 日志构造池（indexed: token0/token1/fee；
// data: tickSpacing, pool）。解码失败返回错误（由调用方决定跳过或中止）。
func (a *Adapter) poolFromCreatedLog(l types.Log) (*Pool, error) {
	ev := a.events[PoolCreatedTopic()]
	if ev.Name == "" {
		return nil, fmt.Errorf("PoolCreated event not found in abi")
	}
	if len(l.Topics) < 4 {
		return nil, fmt.Errorf("PoolCreated: %d topics, want 4", len(l.Topics))
	}
	v, err := ev.Inputs.Unpack(l.Data)
	if err != nil || len(v) < 2 {
		return nil, fmt.Errorf("unpack PoolCreated: %w", err)
	}
	poolAddr := v[1].(common.Address)
	tickSpacing := int(v[0].(*big.Int).Int64())
	if tickSpacing <= 0 {
		return nil, fmt.Errorf("invalid tickSpacing %d", tickSpacing)
	}
	return &Pool{
		Address: poolAddr, Exchange: a.exchange,
		Token0:           common.BytesToAddress(l.Topics[1][12:]),
		Token1:           common.BytesToAddress(l.Topics[2][12:]),
		Fee:              uint32(new(big.Int).SetBytes(l.Topics[3][29:]).Uint64()),
		TickSpacing:      tickSpacing,
		ObservedBlock:    l.BlockNumber,
		CreatedBlock:     l.BlockNumber,
		CreatedBlockHash: l.BlockHash,
		ProvenanceSource: "pool_created_log",
		ticks:            make(map[int]*Tick),
		bitmap:           make(map[int64]*big.Int),
		bitmapLoaded:     make(map[int64]bool),
	}, nil
}

// loadSlot0 读取池的 slot0（sqrtPriceX96/tick）与 liquidity。
// block 为 nil 时读 latest；指定时全部读取固定在同一高度（区块原子性）。
// 失败关闭：任一读取失败或响应过短都返回错误，绝不保留旧值（防止混合状态）。
func (a *Adapter) loadSlot0(ctx context.Context, p *Pool, block *big.Int) error {
	data := crypto.Keccak256([]byte("slot0()"))[:4]
	res, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: data}, block)
	if err != nil {
		return err
	}
	if len(res) < 64 {
		return fmt.Errorf("short slot0 response %d bytes", len(res))
	}
	sqrtPrice := new(big.Int).SetBytes(res[0:32])
	raw := new(big.Int).SetBytes(res[61:64])
	t := raw.Int64()
	if t >= 1<<23 {
		t -= 1 << 24
	}
	liqData := crypto.Keccak256([]byte("liquidity()"))[:4]
	liqRes, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: liqData}, block)
	if err != nil {
		return err
	}
	if len(liqRes) < 32 {
		return fmt.Errorf("short liquidity response %d bytes", len(liqRes))
	}
	// 全部成功后才原子更新（任一步失败不污染状态）
	p.SqrtPriceX96 = sqrtPrice
	p.Tick = int(t)
	p.Liquidity = new(big.Int).SetBytes(liqRes[:32])
	return nil
}

// DecodeLog 解码一条事件到池状态。返回 (池状态变更闭包, 是否认识的事件)。
// 事件 ABI（data 字段顺序，indexed 参数在 topics）：
//
//	Swap:   amount0, amount1, sqrtPriceX96, liquidity, tick
//	Mint:   sender, amount, amount0, amount1（sender 非 indexed！）
//	Burn:   amount, amount0, amount1
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

// DecodeTickBounds 从 Mint/Burn 日志解码 tick 区间（topics 布局：owner, tickLower, tickUpper）。
func DecodeTickBounds(log types.Log) (tickLower, tickUpper int, err error) {
	if len(log.Topics) < 4 {
		return 0, 0, fmt.Errorf("need 4 topics, got %d", len(log.Topics))
	}
	return decodeInt24(log.Topics[2]), decodeInt24(log.Topics[3]), nil
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
// 注意：wordPos 可能为负（负 tick 池），必须用 ABI 编码 int16 二补码，
// 不能用 big.Int.Bytes()（负数返回绝对值）。
func (a *Adapter) LoadBitmapWord(ctx context.Context, p *Pool, wordPos int64) error {
	return a.LoadBitmapWordAt(ctx, p, wordPos, nil)
}

// LoadBitmapWordAt 指定高度的 tickBitmap 读取（区块原子性）。
func (a *Adapter) LoadBitmapWordAt(ctx context.Context, p *Pool, wordPos int64, block *big.Int) error {
	int16Type, err := abi.NewType("int16", "", nil)
	if err != nil {
		return err
	}
	encoded, err := abi.Arguments{{Type: int16Type}}.Pack(int16(wordPos))
	if err != nil {
		return err
	}
	sel := crypto.Keccak256([]byte("tickBitmap(int16)"))[:4]
	data := append(sel, encoded...)
	res, err := a.cli.CallContract(ctx, ethereum.CallMsg{To: &p.Address, Data: data}, block)
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

// RefreshPoolStateAt 单池固定高度刷新：委托批量路径（Multicall3 单次往返），
// 与 token-group 批量刷新共享同一实现与错误语义。
func (a *Adapter) RefreshPoolStateAt(ctx context.Context, p *Pool, block *big.Int) error {
	_, err := a.RefreshPoolsStateAt(ctx, []*Pool{p}, block)
	return err
}

// RefreshPoolsStateAt 批量刷新多个池在固定高度的状态（slot0/liquidity/tickBitmap
// 全部固定在同一 block，消除混合状态）。两次 RPC 往返：
//  1. Multicall3.aggregate3 读全部 slot0() + liquidity()（+缺失的 tickSpacing()）
//  2. 按新 tick 计算 wordPos 后读全部 tickBitmap(word)
//
// Multicall3 不可用时回退 JSON-RPC batch（分块）。返回实际 RPC 调用次数。
// 任一步失败返回错误（调用方保持整批未评估，绝不落成混合状态）。
func (a *Adapter) RefreshPoolsStateAt(ctx context.Context, pools []*Pool, block *big.Int) (int, error) {
	rpcCalls := 0
	if len(pools) == 0 {
		return 0, nil
	}
	// 旧 bitmap 缓存不带区块版本：先清空再按固定区块重载（与单池刷新一致）。
	// 已加载的 word 先并入持久 word 缓存（跨 head 复用）。
	for _, p := range pools {
		for w, word := range p.bitmap {
			if a.bitmapWordCache[p.Address] == nil {
				a.bitmapWordCache[p.Address] = make(map[int64]*big.Int)
			}
			a.bitmapWordCache[p.Address][w] = new(big.Int).Set(word)
		}
		p.bitmap = make(map[int64]*big.Int)
		p.bitmapLoaded = make(map[int64]bool)
	}
	slot0Sel := crypto.Keccak256([]byte("slot0()"))[:4]
	liqSel := crypto.Keccak256([]byte("liquidity()"))[:4]
	tsSel := crypto.Keccak256([]byte("tickSpacing()"))[:4]
	bitmapSel := crypto.Keccak256([]byte("tickBitmap(int16)"))[:4]

	// 阶段 1：slot0 + liquidity（+ tickSpacing 若缺失）
	type poolIdx struct {
		p                       *Pool
		slot0Idx, liqIdx, tsIdx int
	}
	calls := make([]mcCall, 0, 2*len(pools))
	order := make([]poolIdx, 0, len(pools))
	for _, p := range pools {
		pi := poolIdx{p: p, slot0Idx: len(calls), tsIdx: -1}
		calls = append(calls, mcCall{Target: p.Address, CallData: slot0Sel})
		pi.liqIdx = len(calls)
		calls = append(calls, mcCall{Target: p.Address, CallData: liqSel})
		if p.TickSpacing <= 0 {
			pi.tsIdx = len(calls)
			calls = append(calls, mcCall{Target: p.Address, CallData: tsSel})
		}
		order = append(order, pi)
	}
	res, err := a.mcAggregate(ctx, calls, block)
	rpcCalls++
	if err != nil {
		return rpcCalls, err
	}
	for _, pi := range order {
		r := res[pi.slot0Idx]
		if !r.Success || len(r.ReturnData) < 64 {
			return rpcCalls, fmt.Errorf("slot0 %s at %s: success=%v len=%d", pi.p.Address.Hex(), block, r.Success, len(r.ReturnData))
		}
		sqrtPrice := new(big.Int).SetBytes(r.ReturnData[0:32])
		rawTick := new(big.Int).SetBytes(r.ReturnData[61:64])
		t := rawTick.Int64()
		if t >= 1<<23 {
			t -= 1 << 24
		}
		r = res[pi.liqIdx]
		if !r.Success || len(r.ReturnData) < 32 {
			return rpcCalls, fmt.Errorf("liquidity %s at %s: success=%v len=%d", pi.p.Address.Hex(), block, r.Success, len(r.ReturnData))
		}
		liq := new(big.Int).SetBytes(r.ReturnData[:32])
		pi.p.SqrtPriceX96 = sqrtPrice
		pi.p.Tick = int(t)
		pi.p.Liquidity = liq
		if pi.tsIdx >= 0 {
			r = res[pi.tsIdx]
			if !r.Success || len(r.ReturnData) < 32 {
				return rpcCalls, fmt.Errorf("tickSpacing %s at %s: success=%v", pi.p.Address.Hex(), block, r.Success)
			}
			ts := int(new(big.Int).SetBytes(r.ReturnData[29:32]).Int64())
			if ts <= 0 {
				return rpcCalls, fmt.Errorf("invalid tickSpacing %d for %s", ts, pi.p.Address.Hex())
			}
			pi.p.TickSpacing = ts
		}
	}
	// 阶段 2：tickBitmap（wordPos 依赖阶段 1 的新 tick）
	int16Type, err := abi.NewType("int16", "", nil)
	if err != nil {
		return rpcCalls, err
	}
	bmCalls := make([]mcCall, 0, len(pools))
	bmOrder := make([]*Pool, 0, len(pools))
	for _, p := range pools {
		if p.Liquidity == nil || p.Liquidity.Sign() <= 0 || p.TickSpacing <= 0 {
			continue // 无流动性池无需 bitmap（报价本来就会失败）
		}
		wordPos := p.WordPos()
		if cached, ok := a.bitmapWordCache[p.Address][wordPos]; ok {
			// 持久缓存命中：word 几乎不变，直接复用（省一次 RPC 往返）
			p.bitmap[wordPos] = new(big.Int).Set(cached)
			p.bitmapLoaded[wordPos] = true
			continue
		}
		encoded, err := abi.Arguments{{Type: int16Type}}.Pack(int16(wordPos))
		if err != nil {
			return rpcCalls, err
		}
		bmCalls = append(bmCalls, mcCall{Target: p.Address, CallData: append(append([]byte{}, bitmapSel...), encoded...)})
		bmOrder = append(bmOrder, p)
	}
	if len(bmCalls) > 0 {
		res, err := a.mcAggregate(ctx, bmCalls, block)
		rpcCalls++
		if err != nil {
			return rpcCalls, err
		}
		for i, p := range bmOrder {
			r := res[i]
			if !r.Success || len(r.ReturnData) < 32 {
				return rpcCalls, fmt.Errorf("tickBitmap %s at %s: success=%v len=%d", p.Address.Hex(), block, r.Success, len(r.ReturnData))
			}
			wordPos := p.WordPos()
			word := new(big.Int).SetBytes(r.ReturnData[:32])
			p.bitmap[wordPos] = word
			p.bitmapLoaded[wordPos] = true
			if a.bitmapWordCache[p.Address] == nil {
				a.bitmapWordCache[p.Address] = make(map[int64]*big.Int)
			}
			a.bitmapWordCache[p.Address][wordPos] = new(big.Int).Set(word)
		}
	}
	return rpcCalls, nil
}

// InvalidateBitmapCache 使池的跨 head bitmap word 缓存失效（Mint/Burn 事件
// 改变 initialized ticks 时调用）。只删该池条目，保留其他池缓存。
// 注意：invalidate 后下一次 RefreshPoolsStateAt 会重新 RPC 读取，
// 不会继续使用旧 word——这是 bitmap 缓存正确性的唯一失效路径。
func (a *Adapter) InvalidateBitmapCache(pool common.Address) {
	delete(a.bitmapWordCache, pool)
}

// mcAggregate Multicall3 主路径，失败回退 JSON-RPC batch（分块 eth_call）。
func (a *Adapter) mcAggregate(ctx context.Context, calls []mcCall, block *big.Int) ([]mcResult, error) {
	if a.mc3 != nil {
		if res, err := a.mc3.aggregate3(ctx, calls, block); err == nil {
			return res, nil
		} else {
			// Multicall3 不可用（无代码/被拒）：回退 batch，不重复尝试
			a.mc3 = nil
		}
	}
	return a.batchEthCall(ctx, calls, block)
}

// batchEthCall JSON-RPC batch 回退：分块（≤64/请求）顺序执行。
func (a *Adapter) batchEthCall(ctx context.Context, calls []mcCall, block *big.Int) ([]mcResult, error) {
	rc := a.cli.Client()
	blockArg := hexutil.EncodeBig(block)
	out := make([]mcResult, len(calls))
	const chunk = 64
	for start := 0; start < len(calls); start += chunk {
		end := start + chunk
		if end > len(calls) {
			end = len(calls)
		}
		elems := make([]rpc.BatchElem, 0, end-start)
		for i := start; i < end; i++ {
			elems = append(elems, rpc.BatchElem{
				Method: "eth_call",
				Args: []interface{}{
					map[string]interface{}{"to": calls[i].Target.Hex(), "data": hexutil.Encode(calls[i].CallData)},
					blockArg,
				},
				Result: new(hexutil.Bytes),
			})
		}
		if err := rc.BatchCallContext(ctx, elems); err != nil {
			return nil, fmt.Errorf("batch eth_call %d..%d: %w", start, end, err)
		}
		for i, e := range elems {
			if e.Error != nil {
				out[start+i] = mcResult{Success: false}
				continue
			}
			hb, ok := e.Result.(*hexutil.Bytes)
			if !ok || hb == nil {
				out[start+i] = mcResult{Success: false}
				continue
			}
			out[start+i] = mcResult{Success: true, ReturnData: append([]byte{}, *hb...)}
		}
	}
	return out, nil
}

// EnsureQuoteState 报价前统一确保池状态就绪：
//
//	tickSpacing → 读取 tickSpacing()
//	slot0/liquidity → 读取 slot0() 与 liquidity()
//	当前 bitmap word → 读取 tickBitmap(wordPos)
//
// 任一失败返回错误（调用方不得降级报价）。
func (a *Adapter) EnsureQuoteState(ctx context.Context, p *Pool) error {
	if p.TickSpacing <= 0 {
		ts, err := a.readTickSpacing(ctx, p.Address)
		if err != nil {
			return err
		}
		p.TickSpacing = ts
	}
	if p.SqrtPriceX96 == nil || p.Liquidity == nil {
		if err := a.loadSlot0(ctx, p, nil); err != nil {
			return err
		}
	}
	if p.SqrtPriceX96 == nil || p.Liquidity == nil || p.Liquidity.Sign() <= 0 {
		return ErrStateIncomplete
	}
	if !p.BitmapLoaded(p.WordPos()) {
		if err := a.LoadBitmapWord(ctx, p, p.WordPos()); err != nil {
			return err
		}
	}
	return nil
}

// readTickSpacing 读取池的 tickSpacing()。
func (a *Adapter) readTickSpacing(ctx context.Context, pool common.Address) (int, error) {
	res, err := a.cli.CallContract(ctx, ethereum.CallMsg{
		To:   &pool,
		Data: crypto.Keccak256([]byte("tickSpacing()"))[:4],
	}, nil)
	if err != nil {
		return 0, err
	}
	if len(res) < 32 {
		return 0, fmt.Errorf("short tickSpacing response %d bytes", len(res))
	}
	ts := int(new(big.Int).SetBytes(res[29:32]).Int64())
	if ts <= 0 {
		return 0, fmt.Errorf("invalid tickSpacing %d", ts)
	}
	return ts, nil
}

// ResyncMintBurn 收到 Mint/Burn 后重读链上 bitmap word 与 active liquidity
// （不自行推断历史 LiquidityGross —— 程序中途启动时本地历史不完整）。
func (a *Adapter) ResyncMintBurn(ctx context.Context, p *Pool, tickLower, tickUpper int) error {
	// 受影响的两个 word 重新加载
	for _, w := range []int64{
		int64(p.compressedTick(tickLower) >> 8),
		int64(p.compressedTick(tickUpper) >> 8),
	} {
		if err := a.LoadBitmapWord(ctx, p, w); err != nil {
			return err
		}
	}
	// active liquidity 重读
	if err := a.loadSlot0(ctx, p, nil); err != nil {
		return err
	}
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
//   - 双向公式与官方 SqrtPriceMath 一致（token0 输入走 getNextSqrtPriceFromAmount0RoundingUp(add=false)，
//     token1 输入走 getNextSqrtPriceFromAmount1RoundingDown(add=true)）
//   - 输入超过下一 initialized tick 边界 → ErrTickCrossed（绝不近似）
//   - tick 索引不完整（无 slot0/流动性）→ ErrStateIncomplete
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

// ErrNotFactoryPool 表示地址不属于本 Factory（确认非本池，可安全跳过；
// 区别于 RPC 类错误——那些必须阻止区块游标推进）。
var ErrNotFactoryPool = errors.New("not a factory pool")

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
	if err != nil {
		return nil, fmt.Errorf("pool %s factory check: %w", addr.Hex(), err)
	}
	if len(got) < 32 || common.BytesToAddress(got[12:32]) != addr {
		return nil, fmt.Errorf("%w: pool %s", ErrNotFactoryPool, addr.Hex())
	}
	ts := int(new(big.Int).SetBytes(tsraw[29:32]).Int64())
	if ts <= 0 {
		return nil, fmt.Errorf("pool %s invalid tickSpacing %d", addr.Hex(), ts)
	}
	p := &Pool{
		Address: addr, Exchange: a.exchange,
		Token0:       common.BytesToAddress(t0raw[12:32]),
		Token1:       common.BytesToAddress(t1raw[12:32]),
		Fee:          uint32(new(big.Int).SetBytes(feeraw[29:32]).Uint64()),
		TickSpacing:  ts,
		ticks:        make(map[int]*Tick),
		bitmap:       make(map[int64]*big.Int),
		bitmapLoaded: make(map[int64]bool),
	}
	_ = a.loadSlot0(ctx, p, nil)
	return p, nil
}

// PoolCreatedByTokens 按 PoolCreated 的三个 indexed 参数（token0, token1, fee）
// 精确查询池的真实创建区块——无需扫描全量日志（节点端 topics 过滤）。
// 返回 (创建块, 创建块 hash, error)。
func (a *Adapter) PoolCreatedByTokens(ctx context.Context, token0, token1 common.Address, fee uint32) (uint64, common.Hash, error) {
	feeTopic := common.BytesToHash(common.LeftPadBytes(new(big.Int).SetUint64(uint64(fee)).Bytes(), 32))
	q := ethereum.FilterQuery{
		FromBlock: new(big.Int).SetUint64(a.factoryBlock),
		ToBlock:   nil, // latest
		Addresses: []common.Address{a.factory},
		Topics: [][]common.Hash{{
			PoolCreatedTopic(),
		}, {
			common.BytesToHash(token0.Bytes()),
		}, {
			common.BytesToHash(token1.Bytes()),
		}, {
			feeTopic,
		}},
	}
	logs, err := a.cli.FilterLogs(ctx, q)
	if err != nil {
		return 0, common.Hash{}, err
	}
	if len(logs) == 0 {
		return 0, common.Hash{}, fmt.Errorf("no PoolCreated log for %s/%s/%d", token0.Hex(), token1.Hex(), fee)
	}
	l := logs[len(logs)-1]
	return l.BlockNumber, l.BlockHash, nil
}

// BuildSwap 构建 swap calldata。按 router kind 分支：
//   - swaprouter: SwapRouter.exactInputSingle
//   - universal:  UniversalRouter.execute(commands, inputs) 的 V3_SWAP_EXACT_IN (0x08)
//
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

// HeaderAt 读取区块头（nil = latest）。供状态固定与 hash 校验。
func (a *Adapter) HeaderAt(ctx context.Context, block *big.Int) (*types.Header, error) {
	return a.cli.HeaderByNumber(ctx, block)
}
