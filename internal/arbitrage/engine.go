// Package arbitrage 套利引擎：候选发现、优化、评估、执行。
// MVP 只做 WETH→TOKEN→WETH 两池循环；shadow 模式只记录不发送。
package arbitrage

import (
	"context"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Candidate 一个套利候选（observed 字段与执行结果严格分离，禁止 look-ahead）。
type Candidate struct {
	ID            string
	ObservedBlock uint64
	ObservedAt    int64  // unix ms
	SourceEvent   string // 触发来源：如 "swap:0x...:0x..."
	Route         []Hop
	InputAsset    common.Address
	InputAmount   *big.Int
	// 以下为本地估算
	GrossProfit       *big.Int
	GasEstimate       *big.Int
	SwapCost          *big.Int
	SlippageCost      *big.Int
	ExpectedNetProfit *big.Int
	// 以下为链上验证结果（模拟后）
	SimulationResult string
	Decision         string // "accepted" | "rejected"
	RejectReason     string
}

// Hop 一跳。
type Hop struct {
	Pool      common.Address
	Exchange  string
	Fee       uint32
	TokenIn   common.Address
	TokenOut  common.Address
	AmountIn  *big.Int
	AmountOut *big.Int
}

// Sink 候选落盘接口：所有候选（含拒绝）必须记录，而不是只记成交。
type Sink interface {
	SaveCandidate(ctx context.Context, c *Candidate) error
}

// Engine 套利引擎主循环（shadow 模式默认）。
type Engine struct {
	cfg       Config
	sink      Sink
	searcher  Searcher
	evaluator Evaluator
	executor  Executor
}

type Config struct {
	WETH            common.Address
	MinProfitWei    *big.Int
	SafetyMarginWei *big.Int
	MaxHops         int
	Mode            string // dry | shadow | live
}

func NewEngine(cfg Config, sink Sink, searcher Searcher, evaluator Evaluator, executor Executor) *Engine {
	return &Engine{cfg: cfg, sink: sink, searcher: searcher, evaluator: evaluator, executor: executor}
}

// OnSwap 收到 Swap 事件后调用：更新池状态 → 找循环 → 优化 → 评估 → 落盘。
func (e *Engine) OnSwap(ctx context.Context, pool common.Address, block uint64, ts int64) {
	routes := e.searcher.FindRoutes(ctx, pool, e.cfg.WETH, e.cfg.MaxHops)
	for _, r := range routes {
		c := e.searcher.Optimize(ctx, r, block, ts)
		verdict, reason, profit := e.evaluator.Evaluate(ctx, c, e.cfg)
		c.Decision = verdict
		c.RejectReason = reason
		c.ExpectedNetProfit = profit
		if c.Decision == "accepted" && e.cfg.Mode == "live" {
			e.executor.Execute(ctx, c)
		} else {
			slog.Info("candidate", "block", block, "decision", c.Decision, "reason", reason,
				"net_profit_wei", profit.String(), "route", routeID(r))
		}
		if e.sink != nil {
			_ = e.sink.SaveCandidate(ctx, c)
		}
	}
}

func routeID(r Route) string {
	out := ""
	for _, h := range r.Hops {
		out += h.Pool.Hex() + ";"
	}
	return out
}

// Route 从 searcher 返回的候选路径。
type Route struct {
	Hops []Hop
}

// Searcher 候选搜索：路径发现 + 输入量优化。
type Searcher interface {
	FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route
	Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate
}

// Evaluator 评估：模拟验证 + 成本核算。
type Evaluator interface {
	Evaluate(ctx context.Context, c *Candidate, cfg Config) (decision, reason string, netProfit *big.Int)
}

// Executor 执行：构建、签名、广播、确认。
type Executor interface {
	Execute(ctx context.Context, c *Candidate) (hash common.Hash, err error)
}
