package arbitrage

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// LocalSearcher 基于本地池状态的候选搜索器。
// 候选搜索必须用本地状态，禁止把 Quoter 塞进搜索热路径。
type LocalSearcher struct {
	graph    *dex.Graph
	registry *dex.Registry
	v3       V3Client
	weth     common.Address
	// 池状态缓存：(headHash + pool) → 已刷新到该 head 的不可变克隆。
	// 同一 head 内多个 pending 块反复访问相同池时复用；新 head 整体失效。
	snapshotCache     map[string]*v3.Pool
	snapshotCacheHead common.Hash
	snapshotHits      uint64
	snapshotMisses    uint64
	// headerCache 按区块号缓存 header（hash/baseFee）：实时路径同一 head 的
	// 多次评估共享；reorg 罕见且浅，canary 研究场景容忍（上限 16 条防膨胀）。
	headerCache map[uint64]*types.Header
	// headerHint 流推送的当前 head header（WSS newHeads 同流交付，零 RPC）；
	// 由上游 goroutine 更新，仅当 block == hint 高度时使用。
	hintMu      sync.Mutex
	headerHint  *types.Header
	maxInputWei *big.Int // 单笔资金上限（nil = 不限）
	minInputWei *big.Int // 单笔下限（nil = 默认 1e-5 WETH）
	contractBal *big.Int // 执行合约 WETH 余额（nil = 未知）
}

// V3Client 报价所需的 v3 客户端能力（抽象以便历史快照测试注入 fake）。
type V3Client interface {
	HeaderAt(ctx context.Context, block *big.Int) (*types.Header, error)
	RefreshPoolStateAt(ctx context.Context, p *v3.Pool, block *big.Int) error
	// RefreshPoolsStateAt 批量刷新多个池在固定高度的状态（Multicall3/batch），
	// 返回实际 RPC 调用次数（token-group 快照主路径）。
	RefreshPoolsStateAt(ctx context.Context, pools []*v3.Pool, block *big.Int) (int, error)
	QuoteExactIn(p *v3.Pool, tokenIn common.Address, amountIn *big.Int) (*big.Int, error)
}

func NewLocalSearcher(g *dex.Graph, reg *dex.Registry, a V3Client, weth common.Address) *LocalSearcher {
	return &LocalSearcher{graph: g, registry: reg, v3: a, weth: weth,
		snapshotCache: make(map[string]*v3.Pool),
		headerCache:   make(map[uint64]*types.Header)}
}

// SnapshotCacheStats 缓存命中率（吞吐指标用）。
func (s *LocalSearcher) SnapshotCacheStats() (hits, misses uint64) {
	return s.snapshotHits, s.snapshotMisses
}

// SetFunding 注入资金限制（搜索上限 = min(池深度, maxInputWei, 合约余额)）。
// RouteSnapshot 路由在固定区块的不可变评估视图：所有池都是已刷新到
// Block 的克隆。本地报价链（TopKOptimize/Optimize/computeBounds/quoteRoute）
// 必须显式使用 snapshot——不能用正式 Registry 的实时池（历史报价与实时
// 状态混用会选出错误的输入量与候选）。
// SnapshotHop 快照中的一跳（按路由顺序）。
type SnapshotHop struct {
	Pool    *v3.Pool
	TokenIn common.Address
}

type RouteSnapshot struct {
	Block     uint64
	BlockHash common.Hash
	Hops      []SnapshotHop // 与 Route.Hops 同序（报价按序执行）
	Pools     map[common.Address]*v3.Pool
	BaseFee   *big.Int // 快照区块的 base fee（local_only 成本计算用）
}

func (s *LocalSearcher) SetFunding(maxInputWei, contractBal *big.Int) {
	s.maxInputWei = maxInputWei
	s.contractBal = contractBal
}

// SetMinInput 注入单笔下限（浅池保护；nil = 默认 1e-5 WETH）。
func (s *LocalSearcher) SetMinInput(minInputWei *big.Int) {
	s.minInputWei = minInputWei
}

// FindRoutes 找包含触发池（第一跳或第二跳）的 WETH 循环。
func (s *LocalSearcher) FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route {
	cycles := s.graph.FindCycles(weth, pool)
	out := make([]Route, 0, len(cycles))
	for _, cyc := range cycles {
		hops := make([]Hop, 0, len(cyc.Pools))
		for i, ref := range cyc.Pools {
			tokenIn := weth
			if i == 1 {
				tokenIn = midToken(cyc.Pools[0])
			}
			hops = append(hops, Hop{
				Pool: ref.Address, Exchange: ref.Exchange, Fee: ref.Fee,
				TokenIn: tokenIn, TokenOut: otherSide(ref, tokenIn),
			})
		}
		if len(hops) == 2 {
			out = append(out, Route{Hops: hops})
		}
	}
	_ = maxHops
	return out
}

// Optimize 最大化净利润的输入量搜索：
//  1. 由第一跳池的单 tick 可承载量决定搜索上限（深度自适应）
//  2. 对数网格粗搜（32 点）
//  3. 最佳点邻域三分搜索细化
//  4. 记录每跳输入输出
//
// OptimizeAt 在固定 snapshot 上做本地 Top-K 优化（报价/边界全部使用快照池）。
func (s *LocalSearcher) OptimizeAt(ctx context.Context, r Route, snapshot *RouteSnapshot, block uint64, ts int64) *Candidate {
	if snapshot == nil {
		c := emptyCandidate(r, block, ts, s.weth)
		c.RejectReason = "state-incomplete: no snapshot"
		return c
	}
	profitOf := func(amount *big.Int) *big.Int {
		outs, ok := s.quoteRoute(ctx, snapshot, amount)
		if !ok || outs == nil || len(outs) != 2 {
			return big.NewInt(0)
		}
		return new(big.Int).Sub(outs[1], amount) // 最终 WETH - 初始 WETH
	}

	// 搜索上限：min(第一池深度, 第二池深度, 配置上限, 合约余额)
	lo, hi, err := s.computeBounds(ctx, snapshot)
	if err != nil {
		c := emptyCandidate(r, block, ts, s.weth)
		c.RejectReason = err.Error()
		return c
	}

	// 1. 对数网格粗搜（32 点，几何分布 lo..hi）
	//    纯整数等比网格：ratio^(i/31) 用 big.Float 计算后 Int(nil) 取整（不走 int64）
	best := big.NewInt(0)
	bestProfit := big.NewInt(0)
	loF := new(big.Float).SetInt(lo)
	hiF := new(big.Float).SetInt(hi)
	ratioF := new(big.Float).Quo(hiF, loF)
	ratio64, _ := ratioF.Float64()
	for i := 0; i < 32; i++ {
		amtF := new(big.Float).Mul(loF, big.NewFloat(math.Pow(ratio64, float64(i)/31)))
		amt, _ := amtF.Int(nil) // big.Int，无 int64 溢出
		if amt.Sign() <= 0 {
			amt = big.NewInt(1)
		}
		profit := profitOf(amt)
		if profit.Cmp(bestProfit) > 0 {
			bestProfit = profit
			best = amt
		}
	}

	// 2. 局部三分搜索（best 邻域 [best/2, best*2]）
	span := new(big.Int).Set(best)
	if span.Sign() <= 0 {
		// 所有搜索点毛利润都不为正：不再强行优化（lo3>hi3 会出错）
		c := emptyCandidate(r, block, ts, s.weth)
		c.InputAmount = new(big.Int).Set(lo)
		c.RejectReason = "no positive local profit"
		return c
	}
	lo3 := new(big.Int).Div(span, big.NewInt(2))
	hi3 := new(big.Int).Mul(span, big.NewInt(2))
	if hi3.Cmp(hi) > 0 {
		hi3 = hi
	}
	for i := 0; i < 24; i++ {
		m1 := new(big.Int).Add(lo3, new(big.Int).Div(new(big.Int).Sub(hi3, lo3), big.NewInt(3)))
		m2 := new(big.Int).Add(lo3, new(big.Int).Div(new(big.Int).Mul(new(big.Int).Sub(hi3, lo3), big.NewInt(2)), big.NewInt(3)))
		p1 := profitOf(m1)
		p2 := profitOf(m2)
		if p1.Cmp(p2) >= 0 {
			hi3 = m2
			if p1.Cmp(bestProfit) > 0 {
				bestProfit = p1
				best = m1
			}
		} else {
			lo3 = m1
			if p2.Cmp(bestProfit) > 0 {
				bestProfit = p2
				best = m2
			}
		}
	}

	c := &Candidate{
		ObservedBlock: block,
		ObservedAt:    ts,
		SourceEvent:   "swap",
		Route:         cloneHops(r.Hops),
		InputAsset:    s.weth,
		InputAmount:   best,
		GrossProfit:   bestProfit,
		GasEstimate:   big.NewInt(0),
		SwapCost:      big.NewInt(0),
		SlippageCost:  big.NewInt(0),
	}
	// 记录每跳输入输出（完整复盘）
	if outs, ok := s.quoteRoute(ctx, snapshot, best); ok && len(outs) == 2 {
		c.Route[0].AmountIn = best
		c.Route[0].AmountOut = outs[0]
		c.Route[1].AmountIn = outs[0]
		c.Route[1].AmountOut = outs[1]
	}
	return c
}

// prepareRoute 优化前统一准备整条路径的报价状态。
// SnapshotRoute 构建路由在固定区块的不可变评估视图：
// 每个 hop 池克隆 + 刷新到该区块（slot0/liquidity/bitmap）。
// 返回的 snapshot 供 TopKOptimize 的整个本地报价链显式使用——
// 正式 Registry 只提供静态元数据（含创建高度），评估绝不修改实时池。
// InvalidatePool 使该池的 (headHash, pool) 快照缓存失效（Mint/Burn 改变
// initialized ticks 后调用）：下一次 SnapshotRoute/SnapshotTokenGroup 会重新
// RPC 刷新，而不是复用旧状态。只删该池条目，其他池缓存保留。
func (s *LocalSearcher) InvalidatePool(pool common.Address) {
	delete(s.snapshotCache, pool.Hex())
}

// SetHeaderHint 注入流推送的当前 head header（newHeads 订阅，与日志同延迟）。
// 评估时 block == hint 高度直接复用，省一次 HTTP header RPC。
func (s *LocalSearcher) SetHeaderHint(h *types.Header) {
	s.hintMu.Lock()
	s.headerHint = h
	s.hintMu.Unlock()
}

// cachedHeaderAt 读取区块头：hint（同高）→ 按号缓存 → RPC。
func (s *LocalSearcher) cachedHeaderAt(ctx context.Context, block uint64) (*types.Header, error) {
	s.hintMu.Lock()
	hint := s.headerHint
	s.hintMu.Unlock()
	if hint != nil && hint.Number != nil && hint.Number.Uint64() == block {
		return hint, nil
	}
	if h, ok := s.headerCache[block]; ok {
		return h, nil
	}
	h, err := s.v3.HeaderAt(ctx, new(big.Int).SetUint64(block))
	if err != nil {
		return nil, err
	}
	if len(s.headerCache) >= 16 {
		s.headerCache = make(map[uint64]*types.Header)
	}
	s.headerCache[block] = h
	return h, nil
}

func (s *LocalSearcher) SnapshotRoute(ctx context.Context, r Route, block uint64) (*RouteSnapshot, error) {
	header, err := s.cachedHeaderAt(ctx, block)
	if err != nil {
		// 历史 header 不可读 = 基础设施/archive 问题（可重试）
		return nil, fmt.Errorf("%w: header %d: %v", ErrInfra, block, err)
	}
	snap := &RouteSnapshot{Block: header.Number.Uint64(), BlockHash: header.Hash(),
		BaseFee: header.BaseFee,
		Pools:   make(map[common.Address]*v3.Pool, len(r.Hops))}
	// 新 head：整体清空缓存（用 blockHash 防 reorg 复用错误状态）
	if s.snapshotCacheHead != header.Hash() {
		s.snapshotCache = make(map[string]*v3.Pool)
		s.snapshotCacheHead = header.Hash()
	}
	for _, h := range r.Hops {
		state := s.registry.Pool(h.Pool)
		if state == nil {
			return nil, fmt.Errorf("pool %s missing", h.Pool.Hex())
		}
		p := v3.UnwrapState(state)
		if p == nil {
			return nil, fmt.Errorf("pool %s unsupported", h.Pool.Hex())
		}
		// 历史资格：评估区块早于池创建（未来池）→ 确定拒绝该路由
		if p.CreatedBlock > 0 && p.CreatedBlock > block {
			return nil, fmt.Errorf("pool %s created at %d (after evaluation block %d)",
				h.Pool.Hex(), p.CreatedBlock, block)
		}
		// 池级缓存：(headHash, pool)。命中 → 克隆缓存（不可变，调用方随便用）；
		// 未命中 → 克隆原池 + 刷新 + 缓存。绝不修改实时 Registry。
		key := h.Pool.Hex()
		var cp *v3.Pool
		if cached, ok := s.snapshotCache[key]; ok {
			s.snapshotHits++
			cp = cached.Clone()
		} else {
			s.snapshotMisses++
			cp = p.Clone()
			if err := s.v3.RefreshPoolStateAt(ctx, cp, new(big.Int).SetUint64(block)); err != nil {
				return nil, fmt.Errorf("%w: pool %s refresh at %d: %v",
					ErrInfra, h.Pool.Hex(), block, err)
			}
			s.snapshotCache[key] = cp
		}
		snap.Pools[h.Pool] = cp
		snap.Hops = append(snap.Hops, SnapshotHop{Pool: cp, TokenIn: h.TokenIn})
	}
	return snap, nil
}

// TokenGroupSnapshot token 组的固定区块不可变视图：一次批量刷新得到该 token
// 全部 WETH 池的状态（Multicall3/batch，2 次 RPC 往返 + 1 次 header），
// 组内所有 route 的本地报价共享同一份池状态（禁止 route 级重复 RPC）。
type TokenGroupSnapshot struct {
	Block     uint64
	BlockHash common.Hash
	BaseFee   *big.Int
	Pools     map[common.Address]*v3.Pool
	RpcCalls  int // 本次快照实际 RPC 调用数（含 header）
}

// SnapshotTokenGroup 刷新 token 的全部 WETH 池到固定区块（一次批量调用），
// 并写入 (headHash, pool) 快照缓存——后续 per-route SnapshotRoute 全命中。
// 静态池（长期无 Swap）同样在组内：它们正是两池套利最可能的第二腿。
func (s *LocalSearcher) SnapshotTokenGroup(ctx context.Context, token common.Address, block uint64) (*TokenGroupSnapshot, error) {
	// 命中判定必须在读取前（cachedHeaderAt 会填充缓存）
	_, headerCached := s.headerCache[block]
	s.hintMu.Lock()
	if s.headerHint != nil && s.headerHint.Number != nil && s.headerHint.Number.Uint64() == block {
		headerCached = true // hint 命中：不计 RPC
	}
	s.hintMu.Unlock()
	header, err := s.cachedHeaderAt(ctx, block)
	if err != nil {
		return nil, fmt.Errorf("%w: header %d: %v", ErrInfra, block, err)
	}
	snap := &TokenGroupSnapshot{
		Block:     header.Number.Uint64(),
		BlockHash: header.Hash(),
		BaseFee:   header.BaseFee,
		Pools:     make(map[common.Address]*v3.Pool),
	}
	if !headerCached {
		snap.RpcCalls = 1 // header（缓存命中不计数）
	}
	if s.snapshotCacheHead != header.Hash() {
		s.snapshotCache = make(map[string]*v3.Pool)
		s.snapshotCacheHead = header.Hash()
	}
	var toRefresh []*v3.Pool
	for _, ref := range s.graph.PoolsWithToken(token) {
		if ref.Token0 != s.weth && ref.Token1 != s.weth {
			continue // 两跳 WETH 循环只需要 WETH 对
		}
		state := s.registry.Pool(ref.Address)
		if state == nil {
			continue // 图/注册表不一致：per-route 路径同样会拒绝
		}
		p := v3.UnwrapState(state)
		if p == nil {
			continue
		}
		if p.CreatedBlock > 0 && p.CreatedBlock > block {
			continue // 未来池（评估区块早于创建）：无资格
		}
		key := ref.Address.Hex()
		if cached, ok := s.snapshotCache[key]; ok {
			s.snapshotHits++
			snap.Pools[ref.Address] = cached.Clone()
			continue
		}
		s.snapshotMisses++
		cp := p.Clone()
		toRefresh = append(toRefresh, cp)
		snap.Pools[ref.Address] = cp
	}
	if len(toRefresh) > 0 {
		calls, err := s.v3.RefreshPoolsStateAt(ctx, toRefresh, new(big.Int).SetUint64(block))
		if err != nil {
			return nil, fmt.Errorf("%w: token group %s refresh at %d: %v", ErrInfra, token.Hex(), block, err)
		}
		for _, cp := range toRefresh {
			s.snapshotCache[cp.Address.Hex()] = cp
		}
		snap.RpcCalls += calls
	}
	return snap, nil
}

// TopKOptimize 本地毛利最高的 k 个输入量（供逐个 eth_call 后选优）。
// 所有采样金额 clamp 到 [lo, hi]（资金/深度边界）并去重。
// TopKOptimizeAt 在固定 snapshot 上做 Top-K 优化（与 OptimizeAt 同一视图）。
func (s *LocalSearcher) TopKOptimizeAt(ctx context.Context, r Route, snapshot *RouteSnapshot, k int, block uint64, ts int64) []*Candidate {
	if k <= 0 {
		k = 1
	}
	base := s.OptimizeAt(ctx, r, snapshot, block, ts)
	if base.RejectReason != "" || base.InputAmount.Sign() <= 0 {
		return []*Candidate{base}
	}
	lo, hi, err := s.computeBounds(ctx, snapshot)
	if err != nil {
		return []*Candidate{base}
	}
	// 在最优量附近取 k 个采样点（最优 ± 网格邻域）
	best := base.InputAmount
	span := new(big.Int).Div(best, big.NewInt(4))
	if span.Sign() <= 0 {
		span = big.NewInt(1)
	}
	amounts := make([]*big.Int, 0, k)
	for i := 0; i < k; i++ {
		off := new(big.Int).Mul(span, big.NewInt(int64(i-(k-1)/2)))
		a := new(big.Int).Add(best, off)
		if a.Sign() <= 0 {
			a = big.NewInt(1)
		}
		amounts = append(amounts, a)
	}
	out := make([]*Candidate, 0, k)
	seen := make(map[string]struct{})
	for _, a := range amounts {
		// clamp 到资金/深度边界并去重（失败样本也必须保留真实 InputAmount，避免 ID 冲突）
		amt := clampAmount(a, lo, hi)
		if amt.Sign() <= 0 {
			continue
		}
		key := amt.String()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		c := emptyCandidate(r, block, ts, s.weth)
		c.InputAmount = new(big.Int).Set(amt)
		outs, ok := s.quoteRoute(ctx, snapshot, amt)
		if !ok || len(outs) != 2 {
			c.RejectReason = "route quote failed"
			out = append(out, c)
			continue
		}
		c.GrossProfit = new(big.Int).Sub(outs[1], amt)
		c.Route[0].AmountIn = new(big.Int).Set(amt)
		c.Route[0].AmountOut = outs[0]
		c.Route[1].AmountIn = outs[0]
		c.Route[1].AmountOut = outs[1]
		out = append(out, c)
	}
	return out
}

// clampAmount 限制到 [lo, hi]。
func clampAmount(a, lo, hi *big.Int) *big.Int {
	if a.Cmp(lo) < 0 {
		return new(big.Int).Set(lo)
	}
	if a.Cmp(hi) > 0 {
		return new(big.Int).Set(hi)
	}
	return new(big.Int).Set(a)
}

// computeBounds 搜索边界：lo=min_input_wei（默认 1e-5 WETH），hi=min(整条 route 可报价容量,
// 配置上限, 合约余额)。hi < lo（浅池）返回 ErrInsufficientCapacity。
func (s *LocalSearcher) computeBounds(ctx context.Context, snapshot *RouteSnapshot) (*big.Int, *big.Int, error) {
	lo := big.NewInt(1e13) // 默认 1e-5 WETH
	if s.minInputWei != nil && s.minInputWei.Sign() > 0 {
		lo = new(big.Int).Set(s.minInputWei)
	}
	hi := s.routeMaxInput(ctx, snapshot, lo) // 整条 route 最大可报价输入（二分）
	if hi == nil || hi.Sign() <= 0 {
		return nil, nil, fmt.Errorf("no route capacity")
	}
	if s.maxInputWei != nil && s.maxInputWei.Sign() > 0 && hi.Cmp(s.maxInputWei) > 0 {
		hi = new(big.Int).Set(s.maxInputWei)
	}
	if s.contractBal != nil && s.contractBal.Sign() >= 0 && hi.Cmp(s.contractBal) > 0 {
		hi = new(big.Int).Set(s.contractBal)
	}
	if hi.Sign() <= 0 {
		return nil, nil, fmt.Errorf("funding bound zero")
	}
	if hi.Cmp(lo) < 0 {
		return nil, nil, fmt.Errorf("insufficient capacity: hi=%s < lo=%s", hi.String(), lo.String())
	}
	return lo, hi, nil
}

// routeMaxInput 二分求整条 route 的最大可报价输入。
// 第一跳容量给上限估计（5% reserve），二分缩到整条 route 可报价（第二跳容量自然包含）。
func (s *LocalSearcher) routeMaxInput(ctx context.Context, snapshot *RouteSnapshot, lo *big.Int) *big.Int {
	hi := s.firstHopBound(ctx, snapshot)
	if hi == nil || hi.Sign() <= 0 {
		return nil
	}
	if _, ok := s.quoteRoute(ctx, snapshot, hi); ok {
		return hi // 上限本身可报价
	}
	// 二分：找 [lo, hi] 中最大的可报价输入
	best := new(big.Int)
	low := new(big.Int).Set(lo)
	high := new(big.Int).Set(hi)
	for low.Cmp(high) <= 0 {
		mid := new(big.Int).Add(low, high)
		mid.Rsh(mid, 1)
		if mid.Sign() <= 0 {
			break
		}
		if _, ok := s.quoteRoute(ctx, snapshot, mid); ok {
			best = mid
			low = new(big.Int).Add(mid, big.NewInt(1))
		} else {
			high = new(big.Int).Sub(mid, big.NewInt(1))
		}
	}
	if best.Sign() <= 0 {
		return nil
	}
	return best
}

// firstHopBound 第一跳池深度上限（5% reserve；二分起点）。
func (s *LocalSearcher) firstHopBound(ctx context.Context, snapshot *RouteSnapshot) *big.Int {
	if len(snapshot.Hops) == 0 {
		return nil
	}
	p := snapshot.Hops[0].Pool // 快照池（已刷新到固定区块）
	if p.SqrtPriceX96 == nil || p.SqrtPriceX96.Sign() <= 0 || p.Liquidity == nil || p.Liquidity.Sign() <= 0 {
		return nil
	}
	// token0 reserve = L * 2^96 / Q；token1 reserve = L * Q / 2^96
	zeroForOne := snapshot.Hops[0].TokenIn == p.Token0
	reserve := new(big.Int).Lsh(p.Liquidity, 96)
	if zeroForOne {
		reserve.Div(reserve, p.SqrtPriceX96)
	} else {
		reserve.Mul(reserve, p.SqrtPriceX96)
		reserve.Rsh(reserve, 96)
	}
	bound := new(big.Int).Div(reserve, big.NewInt(20)) // 5% 深度
	if bound.Sign() <= 0 {
		return nil
	}
	return bound
}

// quoteRoute 本地报价整条路由，返回每跳输出（含中间跳）。
// quoteRoute 在 snapshot 上报价（snapshot 池已刷新到固定区块，按路由顺序）。
func (s *LocalSearcher) quoteRoute(ctx context.Context, snapshot *RouteSnapshot, amountIn *big.Int) ([]*big.Int, bool) {
	outs := make([]*big.Int, 0, len(snapshot.Hops))
	cur := amountIn
	for _, hp := range snapshot.Hops {
		out, err := s.v3.QuoteExactIn(hp.Pool, hp.TokenIn, cur)
		if err != nil || out.Sign() <= 0 {
			return nil, false
		}
		outs = append(outs, out)
		cur = out
	}
	return outs, true
}

func midToken(ref dex.PoolRef) common.Address {
	if ref.TokenInIsToken0 {
		return ref.Token1
	}
	return ref.Token0
}

func otherSide(ref dex.PoolRef, tokenIn common.Address) common.Address {
	if ref.TokenInIsToken0 {
		if tokenIn == ref.Token0 {
			return ref.Token1
		}
		return ref.Token0
	}
	if tokenIn == ref.Token1 {
		return ref.Token0
	}
	return ref.Token1
}

// 数学辅助（避免引入 math 包冲突）
func powf(base, exp float64) float64 {
	// base^exp = e^(exp*ln(base))；ln/exp 用标准库 math
	return math.Pow(base, exp)
}

func float64FromBig(b *big.Int) float64 {
	f, _ := new(big.Float).SetInt(b).Float64()
	return f
}

// cloneHops 深拷贝路由（Top-K 候选共享同一底层数组会互相覆盖每跳金额）。
func cloneHops(src []Hop) []Hop {
	dst := make([]Hop, len(src))
	for i, h := range src {
		dst[i] = h
		if h.AmountIn != nil {
			dst[i].AmountIn = new(big.Int).Set(h.AmountIn)
		}
		if h.AmountOut != nil {
			dst[i].AmountOut = new(big.Int).Set(h.AmountOut)
		}
	}
	return dst
}

// emptyCandidate 字段完整的空候选（无论搜索成败都不允许 nil 字段进入下游）。
func emptyCandidate(r Route, block uint64, ts int64, weth common.Address) *Candidate {
	return &Candidate{
		ObservedBlock:     block,
		ObservedAt:        ts,
		Route:             cloneHops(r.Hops),
		RouteJSON:         MarshalRoute(r.Hops),
		InputAsset:        weth,
		InputAmount:       new(big.Int),
		GrossProfit:       new(big.Int),
		GasEstimate:       new(big.Int),
		SwapCost:          new(big.Int),
		SlippageCost:      new(big.Int),
		ExpectedNetProfit: new(big.Int),
	}
}
