package arbitrage

import (
	"context"
	"fmt"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// LocalSearcher 基于本地池状态的候选搜索器。
// 候选搜索必须用本地状态，禁止把 Quoter 塞进搜索热路径。
type LocalSearcher struct {
	graph        *dex.Graph
	registry     *dex.Registry
	v3           *v3.Adapter
	weth         common.Address
	maxInputWei  *big.Int // 单笔资金上限（nil = 不限）
	minInputWei  *big.Int // 单笔下限（nil = 默认 1e-5 WETH）
	contractBal  *big.Int // 执行合约 WETH 余额（nil = 未知）
}

func NewLocalSearcher(g *dex.Graph, reg *dex.Registry, a *v3.Adapter, weth common.Address) *LocalSearcher {
	return &LocalSearcher{graph: g, registry: reg, v3: a, weth: weth}
}

// SetFunding 注入资金限制（搜索上限 = min(池深度, maxInputWei, 合约余额)）。
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
//   1. 由第一跳池的单 tick 可承载量决定搜索上限（深度自适应）
//   2. 对数网格粗搜（32 点）
//   3. 最佳点邻域三分搜索细化
//   4. 记录每跳输入输出
func (s *LocalSearcher) Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate {
	// 先统一准备整条路径状态（spacing/slot0/liquidity/bitmap），
	// 再谈任何优化 —— maxInputBound 依赖池状态，恢复池必须在此完成加载。
	if err := s.prepareRoute(ctx, r); err != nil {
		c := emptyCandidate(r, block, ts, s.weth)
		c.RejectReason = "state-incomplete: " + err.Error()
		return c
	}

	profitOf := func(amount *big.Int) *big.Int {
		outs, ok := s.quoteRoute(ctx, r, amount)
		if !ok || outs == nil || len(outs) != 2 {
			return big.NewInt(0)
		}
		return new(big.Int).Sub(outs[1], amount) // 最终 WETH - 初始 WETH
	}

	// 搜索上限：min(第一池深度, 第二池深度, 配置上限, 合约余额)
	lo, hi, err := s.computeBounds(ctx, r)
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
	if outs, ok := s.quoteRoute(ctx, r, best); ok && len(outs) == 2 {
		c.Route[0].AmountIn = best
		c.Route[0].AmountOut = outs[0]
		c.Route[1].AmountIn = outs[0]
		c.Route[1].AmountOut = outs[1]
	}
	return c
}

// prepareRoute 优化前统一准备整条路径的报价状态。
func (s *LocalSearcher) prepareRoute(ctx context.Context, r Route) error {
	for _, h := range r.Hops {
		state := s.registry.Pool(h.Pool)
		if state == nil {
			return fmt.Errorf("pool %s missing", h.Pool.Hex())
		}
		p, ok := state.(*v3.Pool)
		if !ok {
			return fmt.Errorf("pool %s unsupported type", h.Pool.Hex())
		}
		if err := s.v3.EnsureQuoteState(ctx, p); err != nil {
			return fmt.Errorf("pool %s: %w", h.Pool.Hex(), err)
		}
	}
	return nil
}

// RefreshRoute 执行 Shadow 模式：先固定一个状态区块（blockNumber + blockHash），
// 路由所有池的状态读取（slot0/liquidity/bitmap）全部固定在该高度。
// 返回 (stateBlock, stateHash, err)。调用方需将 stateBlock 用于 eth_call 与 hash 校验。
func (s *LocalSearcher) RefreshRoute(ctx context.Context, r Route, block uint64) (uint64, common.Hash, error) {
	header, err := s.v3.HeaderAt(ctx, new(big.Int).SetUint64(block))
	if err != nil {
		return 0, common.Hash{}, fmt.Errorf("header %d: %w", block, err)
	}
	for _, h := range r.Hops {
		state := s.registry.Pool(h.Pool)
		if state == nil {
			return 0, common.Hash{}, fmt.Errorf("pool %s missing", h.Pool.Hex())
		}
		p, ok := state.(*v3.Pool)
		if !ok {
			return 0, common.Hash{}, fmt.Errorf("pool %s unsupported", h.Pool.Hex())
		}
		if err := s.v3.RefreshPoolStateAt(ctx, p, new(big.Int).SetUint64(block)); err != nil {
			return 0, common.Hash{}, fmt.Errorf("pool %s refresh: %w", h.Pool.Hex(), err)
		}
	}
	return header.Number.Uint64(), header.Hash(), nil
}

// TopKOptimize 本地毛利最高的 k 个输入量（供逐个 eth_call 后选优）。
// 所有采样金额 clamp 到 [lo, hi]（资金/深度边界）并去重。
func (s *LocalSearcher) TopKOptimize(ctx context.Context, r Route, k int, block uint64, ts int64) []*Candidate {
	if k <= 0 {
		k = 1
	}
	base := s.Optimize(ctx, r, block, ts)
	if base.RejectReason != "" || base.InputAmount.Sign() <= 0 {
		return []*Candidate{base}
	}
	lo, hi, err := s.computeBounds(ctx, r)
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
		outs, ok := s.quoteRoute(ctx, r, amt)
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
func (s *LocalSearcher) computeBounds(ctx context.Context, r Route) (*big.Int, *big.Int, error) {
	lo := big.NewInt(1e13) // 默认 1e-5 WETH
	if s.minInputWei != nil && s.minInputWei.Sign() > 0 {
		lo = new(big.Int).Set(s.minInputWei)
	}
	hi := s.routeMaxInput(ctx, r, lo) // 整条 route 最大可报价输入（二分）
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
func (s *LocalSearcher) routeMaxInput(ctx context.Context, r Route, lo *big.Int) *big.Int {
	hi := s.firstHopBound(ctx, r)
	if hi == nil || hi.Sign() <= 0 {
		return nil
	}
	if _, ok := s.quoteRoute(ctx, r, hi); ok {
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
		if _, ok := s.quoteRoute(ctx, r, mid); ok {
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
func (s *LocalSearcher) firstHopBound(ctx context.Context, r Route) *big.Int {
	if len(r.Hops) == 0 {
		return nil
	}
	state := s.registry.Pool(r.Hops[0].Pool)
	if state == nil {
		return nil
	}
	p := state.(*v3.Pool)
	if p.SqrtPriceX96 == nil || p.SqrtPriceX96.Sign() <= 0 || p.Liquidity == nil || p.Liquidity.Sign() <= 0 {
		return nil
	}
	// token0 reserve = L * 2^96 / Q；token1 reserve = L * Q / 2^96
	zeroForOne := r.Hops[0].TokenIn == p.Token0
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
func (s *LocalSearcher) quoteRoute(ctx context.Context, r Route, amountIn *big.Int) ([]*big.Int, bool) {
	outs := make([]*big.Int, 0, len(r.Hops))
	cur := amountIn
	for _, h := range r.Hops {
		state := s.registry.Pool(h.Pool)
		if state == nil {
			return nil, false
		}
		p := state.(*v3.Pool)
		// 统一状态入口：spacing/slot0/liquidity/bitmap 任缺 → 本次不评估
		if err := s.v3.EnsureQuoteState(ctx, p); err != nil {
			return nil, false
		}
		out, err := s.v3.QuoteExactIn(p, h.TokenIn, cur)
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
		ObservedBlock:    block,
		ObservedAt:       ts,
		Route:            cloneHops(r.Hops),
		RouteJSON:        MarshalRoute(r.Hops),
		InputAsset:       weth,
		InputAmount:      new(big.Int),
		GrossProfit:      new(big.Int),
		GasEstimate:      new(big.Int),
		SwapCost:         new(big.Int),
		SlippageCost:     new(big.Int),
		ExpectedNetProfit: new(big.Int),
	}
}
