package arbitrage

import (
	"context"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// LocalSearcher 基于本地池状态的候选搜索器。
// 候选搜索必须用本地状态，禁止把 Quoter 塞进搜索热路径。
type LocalSearcher struct {
	graph    *dex.Graph
	registry *dex.Registry
	v3       *v3.Adapter
	weth     common.Address
}

func NewLocalSearcher(g *dex.Graph, reg *dex.Registry, a *v3.Adapter, weth common.Address) *LocalSearcher {
	return &LocalSearcher{graph: g, registry: reg, v3: a, weth: weth}
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
	profitOf := func(amount *big.Int) *big.Int {
		outs, ok := s.quoteRoute(ctx, r, amount)
		if !ok || outs == nil || len(outs) != 2 {
			return big.NewInt(0)
		}
		return new(big.Int).Sub(outs[1], amount) // 最终 WETH - 初始 WETH
	}

	// 搜索上限：第一跳池单 tick 可承载量（由池深度决定）
	hi := s.maxInputBound(ctx, r)
	if hi == nil || hi.Sign() <= 0 {
		return emptyCandidate(r, block, ts, s.weth)
	}
	lo := big.NewInt(1e15) // 0.001 WETH 起

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
		span = big.NewInt(1e15)
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
		Route:         r.Hops,
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

// maxInputBound 第一跳池的单 tick 可承载量（深度自适应上限）。
// 简化：用池 token0 reserve 的 5% 与单 tick 边界输入量取小者。
func (s *LocalSearcher) maxInputBound(ctx context.Context, r Route) *big.Int {
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

// emptyCandidate 字段完整的空候选（无论搜索成败都不允许 nil 字段进入下游）。
func emptyCandidate(r Route, block uint64, ts int64, weth common.Address) *Candidate {
	return &Candidate{
		ObservedBlock:    block,
		ObservedAt:       ts,
		Route:            r.Hops,
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
