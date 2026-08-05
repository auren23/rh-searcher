package arbitrage

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// LocalSearcher 基于本地池状态的候选搜索器。
// 候选搜索必须用本地状态，禁止把 Quoter 塞进搜索热路径。
type LocalSearcher struct {
	graph   *dex.Graph
	registry *dex.Registry
	v3      *v3.Adapter
	weth    common.Address
}

func NewLocalSearcher(g *dex.Graph, reg *dex.Registry, a *v3.Adapter, weth common.Address) *LocalSearcher {
	return &LocalSearcher{graph: g, registry: reg, v3: a, weth: weth}
}

// FindRoutes 找包含触发池的 WETH 循环。
func (s *LocalSearcher) FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route {
	cycles := s.graph.FindCycles(weth, pool)
	out := make([]Route, 0, len(cycles))
	for _, cyc := range cycles {
		hops := make([]Hop, 0, len(cyc.Pools))
		for i, ref := range cyc.Pools {
			state := s.registry.Pool(ref.Address)
			if state == nil {
				continue
			}
			tokenIn := weth
			if i == 1 {
				tokenIn = midToken(cyc.Pools[0])
			}
			hop := Hop{
				Pool: ref.Address, Exchange: ref.Exchange, Fee: ref.Fee,
				TokenIn: tokenIn, TokenOut: otherSide(ref, tokenIn),
			}
			hops = append(hops, hop)
		}
		if len(hops) == 2 {
			out = append(out, Route{Hops: hops})
		}
	}
	_ = maxHops
	return out
}

// Optimize 对每条路由二分搜索最优 amountIn（最大化净 WETH 差）。
func (s *LocalSearcher) Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate {
	// 简化二分：扫描 amountIn 对数网格（100 点），取 WETH-out - WETH-in 最大者。
	bestAmount := big.NewInt(0)
	bestOut := big.NewInt(0)
	// 采样区间假设：0.01 .. 10 WETH（MVP 固定值，生产应从池深度推导）
	low := big.NewInt(0)
	high := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	high.Mul(high, big.NewInt(10))
	for i := 0; i < 30; i++ { // 30 轮二分
		mid := new(big.Int).Add(low, high)
		mid.Div(mid, big.NewInt(2))
		out, ok := s.quoteRoute(ctx, r, mid)
		if !ok {
			high = mid
			continue
		}
		if out.Cmp(mid) > 0 {
			bestAmount = new(big.Int).Set(mid)
			bestOut = new(big.Int).Set(out)
			low = mid
		} else {
			high = mid
		}
	}
	gross := new(big.Int).Sub(bestOut, bestAmount)
	c := &Candidate{
		ID:            common.Hash{}.Hex() + "-" + itoa(block),
		ObservedBlock: block,
		ObservedAt:    ts,
		SourceEvent:   "swap",
		Route:         r.Hops,
		InputAsset:    s.weth,
		InputAmount:   bestAmount,
		GrossProfit:   gross,
		GasEstimate:   big.NewInt(0),
		SwapCost:      big.NewInt(0),
		SlippageCost:  big.NewInt(0),
	}
	if gross.Sign() > 0 {
		c.ExpectedNetProfit = new(big.Int).Set(gross)
	}
	return c
}

// quoteRoute 本地报价整条路由。
func (s *LocalSearcher) quoteRoute(ctx context.Context, r Route, amountIn *big.Int) (*big.Int, bool) {
	cur := amountIn
	for i, h := range r.Hops {
		state := s.registry.Pool(h.Pool)
		if state == nil {
			return nil, false
		}
		p := state.(*v3.Pool)
		out, err := s.v3.QuoteExactIn(p, h.TokenIn, cur)
		if err != nil || out.Sign() <= 0 {
			return nil, false
		}
		cur = out
		_ = i
	}
	return cur, true
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

func itoa(v uint64) string {
	return new(big.Int).SetUint64(v).String()
}
