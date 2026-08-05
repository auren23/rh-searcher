// Package arbitrage 套利引擎：候选发现、优化、评估、执行。
// MVP 只做 WETH→TOKEN→WETH 两池循环；shadow 模式只记录不发送。
package arbitrage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/crypto/sha3"
)

// Candidate 一个套利候选（observed 字段与执行结果严格分离，禁止 look-ahead）。
type Candidate struct {
	ID            string // keccak256(chainId, blockHash, txHash, logIndex, route, amountIn)
	ObservedBlock uint64
	BlockHash     common.Hash
	TxHash        common.Hash
	LogIndex      uint
	ObservedAt    int64 // unix ms（接收时间，不是区块号）
	SourceEvent   string
	Route         []Hop
	RouteJSON     string // 完整路由 JSON（可重放）
	InputAsset    common.Address
	InputAmount   *big.Int
	// 以下为本地估算
	GrossProfit       *big.Int
	GasEstimate       *big.Int
	SwapCost          *big.Int
	SlippageCost      *big.Int
	ExpectedNetProfit *big.Int
	// 以下为链上验证结果（模拟后）
	SimulationResult   string
	Decision           string // local_candidate | simulation_accepted | simulation_rejected
	RejectReason       string
	SimulatedProfitWei *big.Int // eth_call 返回的 WETH 净利（未扣 gas）
	GasUsed            uint64
	GasPriceWei        *big.Int
	GasCostWei         *big.Int
	CalldataHash       string
	StateBlock         uint64 // 池状态对应区块（0 = 混合/未知）
	SimulationBlock    uint64 // eth_call 执行时的链头（0 = 未知）

	// Arbitrum 费用组件（仅分析，不参与净利扣减）
	L1GasUnits         uint64
	L2BaseFeeWei       *big.Int
	L1BaseFeeEstimateWei *big.Int

	// 同一机会（事件+路由）的 Top-K 分组：仅 Selected 候选可发送
	OpportunityGroupID string // = 事件 ID + 路由（不含金额）
	Rank               int    // 组内本地毛利排名
	Selected           bool   // 模拟后选中的唯一候选
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
	ChainID         uint64
	WETH            common.Address
	MinProfitWei    *big.Int
	SafetyMarginWei *big.Int
	MaxHops         int
	TopK            int    // 本地 Top-K 输入量逐个链上模拟
	Mode            string // dry | shadow | live
}

func NewEngine(cfg Config, sink Sink, searcher Searcher, evaluator Evaluator, executor Executor) *Engine {
	return &Engine{cfg: cfg, sink: sink, searcher: searcher, evaluator: evaluator, executor: executor}
}

// SwapEvent 触发一次评估的链上事件上下文。
type SwapEvent struct {
	Pool        common.Address
	BlockNumber uint64
	BlockHash   common.Hash
	TxHash      common.Hash
	LogIndex    uint
	ReceivedAt  int64 // unix ms
}

// OnBlockBatch 区块级评估：应用完整区块的日志后，收集所有受影响池的路由，
// 按池序列全局去重，每条 route 只评估一次（避免两池同区块 Swap 时重复模拟）。
func (e *Engine) OnBlockBatch(ctx context.Context, ev SwapEvent, affectedPools []common.Address) {
	allRoutes := make([]Route, 0, len(affectedPools)*2)
	seenRoutes := make(map[string]struct{})
	for _, pool := range affectedPools {
		for _, r := range e.searcher.FindRoutes(ctx, pool, e.cfg.WETH, e.cfg.MaxHops) {
			key := routeID(r)
			if _, dup := seenRoutes[key]; dup {
				continue
			}
			seenRoutes[key] = struct{}{}
			allRoutes = append(allRoutes, r)
		}
	}
	for _, r := range allRoutes {
		e.evaluateRoute(ctx, ev, r)
	}
}

// OnSwap 收到 Swap 事件后调用：找循环 → 优化 → 评估 → 落盘（含拒绝）。
func (e *Engine) OnSwap(ctx context.Context, ev SwapEvent) {
	routes := e.searcher.FindRoutes(ctx, ev.Pool, e.cfg.WETH, e.cfg.MaxHops)
	for _, r := range routes {
		e.evaluateRoute(ctx, ev, r)
	}
}

// evaluateRoute 单条路由的 Top-K 模拟与统一落盘。
func (e *Engine) evaluateRoute(ctx context.Context, ev SwapEvent, r Route) {
	// 执行 Shadow 模式：路由所有池统一刷新到 latest，禁止事件/latest 混合状态
	stateHead, err := e.searcher.RefreshRoute(ctx, r)
	if err != nil {
		slog.Warn("route state refresh failed", "route", routeID(r), "err", err)
		return // 状态不可用：本区块不评估（下一事件再试），不产生伪造候选
	}
	// Top-K 输入量逐个链上模拟，选模拟净利最高者；先全部模拟，再统一落盘
	cands := e.searcher.TopKOptimize(ctx, r, e.cfg.TopK, ev.BlockNumber, ev.ReceivedAt)
	groupID := fmt.Sprintf("%d/%s", ev.BlockNumber, routeID(r))
	var best *Candidate
	for rank, c := range cands {
		if c == nil || c.InputAmount == nil {
			slog.Error("searcher returned incomplete candidate", "route", routeID(r))
			continue // 记录并跳过，绝不 panic
		}
		c.BlockHash = ev.BlockHash
		c.TxHash = ev.TxHash
		c.LogIndex = ev.LogIndex
		c.SourceEvent = "block_swap_batch"
		c.StateBlock = stateHead
		c.RouteJSON = MarshalRoute(c.Route) // 用候选自己的路由（含每跳金额）
		c.OpportunityGroupID = groupID
		c.Rank = rank + 1 // 1 起（存储层 0 视为 NULL）
		c.ID = CandidateID(e.cfg.ChainID, ev.BlockHash, ev.TxHash, ev.LogIndex, c.RouteJSON, c.InputAmount)
		if c.RejectReason != "" {
			// searcher 已判定（state-incomplete / route quote failed）：不再交给模拟器覆盖
			c.Decision = "local_rejected"
			c.ExpectedNetProfit = new(big.Int)
			continue
		}
		verdict, reason, profit := e.evaluator.Evaluate(ctx, c, e.cfg)
		c.Decision = verdict
		c.RejectReason = reason
		c.ExpectedNetProfit = profit
		// 只有通过链上模拟的候选才能成为 best（rejected 永远不 selected）
		if verdict == SimulationAccepted {
			if best == nil || c.ExpectedNetProfit.Cmp(best.ExpectedNetProfit) > 0 {
				best = c
			}
		}
	}
	// 统一落盘：best 标记 selected=true；其他通过模拟的降级为 simulation_valid
	for _, c := range cands {
		if c.ID == "" {
			continue // 未进入评估流程（searcher 异常）
		}
		if best != nil && c.ID == best.ID {
			c.Selected = true
		} else if c.Decision == SimulationAccepted {
			c.Decision = "simulation_valid"
			c.RejectReason = "not selected (lower net profit in group)"
		}
		if e.sink != nil {
			if err := e.sink.SaveCandidate(ctx, c); err != nil {
				slog.Error("candidate persist failed", "err", err, "id", c.ID)
			}
		}
	}
	if best == nil {
		return
	}
	// 仅 Selected 候选可发送
	if best.Decision == SimulationAccepted && e.cfg.Mode == "live" {
		e.executor.Execute(ctx, best)
	}
	slog.Info("route evaluated", "block", ev.BlockNumber, "best", best.Decision,
		"net_profit_wei", best.ExpectedNetProfit.String(), "amount_in", best.InputAmount.String(),
		"route", routeID(r))
}
func routeID(r Route) string {
	out := ""
	for _, h := range r.Hops {
		out += h.Pool.Hex() + ";"
	}
	return out
}

// CandidateID 生成可重放的机会 ID。
func CandidateID(chainID uint64, blockHash, txHash common.Hash, logIndex uint, routeJSON string, amountIn *big.Int) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(strings.Join([]string{
		hex.EncodeToString(new(big.Int).SetUint64(chainID).Bytes()),
		blockHash.Hex(), txHash.Hex(),
		hex.EncodeToString(new(big.Int).SetUint64(uint64(logIndex)).Bytes()),
		routeJSON, amountIn.String(),
	}, "|")))
	return "0x" + hex.EncodeToString(h.Sum(nil))
}

// MarshalRoute 序列化路由（完整可重放）。
func MarshalRoute(hops []Hop) string {
	raw, _ := json.Marshal(hops)
	return string(raw)
}

// SimulationAccepted 通过链上模拟的决策值（live 发送门槛）。
const SimulationAccepted = "simulation_accepted"

// Route 从 searcher 返回的候选路径。
type Route struct {
	Hops []Hop
}

// Searcher 候选搜索：路径发现 + 输入量优化。
type Searcher interface {
	FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route
	Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate
	// TopKOptimize 返回本地毛利最高的 k 个输入量候选（供逐个链上模拟后选优）。
	TopKOptimize(ctx context.Context, r Route, k int, block uint64, ts int64) []*Candidate
	// RefreshRoute 执行 Shadow 模式：路由所有池统一读取 latest 状态，返回链头（StateBlock）。
	RefreshRoute(ctx context.Context, r Route) (uint64, error)
}

// Evaluator 评估：模拟验证 + 成本核算。
type Evaluator interface {
	Evaluate(ctx context.Context, c *Candidate, cfg Config) (decision, reason string, netProfit *big.Int)
}

// Executor 执行：构建、签名、广播、确认。
type Executor interface {
	Execute(ctx context.Context, c *Candidate) (hash common.Hash, err error)
}
