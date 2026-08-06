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
	"sort"
	"strings"
	"sync"

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
	StateBlock      *big.Int // 评估固定区块（eth_call 与该高度对齐；nil = latest）
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

// BlockResult 一个区块的完整处理结果。Engine 不直接写数据库：
// 由调用方通过 CommitBlockResult 在单个事务内提交（pools + candidates + checkpoint）。
type BlockResult struct {
	Block      uint64
	BlockHash  common.Hash
	Candidates []*Candidate
	NewPools   []PoolMeta
}

// PoolMeta 动态发现的新池（提交时写入 dex_pools）。
type PoolMeta struct {
	Address     common.Address
	Exchange    string
	Token0      common.Address
	Token1      common.Address
	Fee         uint32
	TickSpacing int
}

// ProcessBlock 区块级评估：应用完整区块的日志后，收集所有受影响池的路由，
// 按池序列全局去重，每条 route 只评估一次（避免两池同区块 Swap 时重复模拟）。
func (e *Engine) ProcessBlock(ctx context.Context, ev SwapEvent, affectedPools []common.Address) *BlockResult {
	res := &BlockResult{Block: ev.BlockNumber, BlockHash: ev.BlockHash}
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
		res.Candidates = append(res.Candidates, e.evaluateRoute(ctx, ev, r)...)
	}
	return res
}

// OnSwap 收到 Swap 事件后调用：找循环 → 优化 → 评估 → 落盘（含拒绝）。
func (e *Engine) OnSwap(ctx context.Context, ev SwapEvent) {
	routes := e.searcher.FindRoutes(ctx, ev.Pool, e.cfg.WETH, e.cfg.MaxHops)
	for _, r := range routes {
		e.evaluateRoute(ctx, ev, r)
	}
}

// evaluateRoute 单条路由的 Top-K 模拟与统一落盘。返回本路由产生的候选（不写数据库）。
func (e *Engine) evaluateRoute(ctx context.Context, ev SwapEvent, r Route) []*Candidate {
	// 执行 Shadow 模式：固定状态区块，路由所有池的状态读取都在该高度
	stateHead, stateHash, err := e.searcher.RefreshRoute(ctx, r)
	if err != nil {
		// 状态不可用：打指标并记录 group 级拒绝（不静默从漏斗消失）
		routeRefreshFailures.Inc()
		slog.Warn("route state refresh failed", "route", routeID(r), "err", err)
		return []*Candidate{{
			ID:            CandidateID(e.cfg.ChainID, ev.BlockHash, ev.TxHash, ev.LogIndex, MarshalRoute(r.Hops), big.NewInt(0)),
			ObservedBlock: ev.BlockNumber,
			ObservedAt:    ev.ReceivedAt,
			Route:         cloneHops(r.Hops),
			RouteJSON:     MarshalRoute(r.Hops),
			InputAmount:   new(big.Int),
			GrossProfit:   new(big.Int),
			GasEstimate:   new(big.Int),
			SwapCost:      new(big.Int),
			SlippageCost:  new(big.Int),
			ExpectedNetProfit: new(big.Int),
			Decision:      "local_rejected",
			RejectReason:  "state-incomplete: " + err.Error(),
		}}
	}
	// read RPC 与 sim RPC 的区块 hash 一致性校验（不一致 → 整组拒绝）
	if v, ok := e.evaluator.(interface {
		VerifyBlockHash(ctx context.Context, block uint64, want common.Hash) error
	}); ok {
		if err := v.VerifyBlockHash(ctx, stateHead, stateHash); err != nil {
			slog.Warn("read/sim block hash mismatch", "route", routeID(r), "err", err)
			c := emptyCandidate(r, ev.BlockNumber, ev.ReceivedAt, e.cfg.WETH)
			c.Decision = "simulation_rejected"
			c.RejectReason = "read_sim_block_hash_mismatch"
			return []*Candidate{c}
		}
	}
	// Top-K 输入量逐个链上模拟，选模拟净利最高者；先全部模拟，再统一落盘
	cands := e.searcher.TopKOptimize(ctx, r, e.cfg.TopK, ev.BlockNumber, ev.ReceivedAt)
	// Rank 必须是真实利润排名：按本地毛利降序
	sort.Slice(cands, func(i, j int) bool {
		if cands[i] == nil || cands[j] == nil {
			return false
		}
		gi, gj := cands[i].GrossProfit, cands[j].GrossProfit
		if gi == nil {
			return false
		}
		if gj == nil {
			return true
		}
		return gi.Cmp(gj) > 0
	})
	// GroupID：chainID + blockHash + route（重组后同高度新块不会与旧块同组）
	groupID := fmt.Sprintf("%d/%s/%s", e.cfg.ChainID, ev.BlockHash.Hex(), routeID(r))
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
		simCfg := e.cfg
		simCfg.StateBlock = new(big.Int).SetUint64(stateHead)
		verdict, reason, profit := e.evaluator.Evaluate(ctx, c, simCfg)
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
	// 统一收集：best 标记 selected=true；其他通过模拟的降级为 simulation_valid
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
	}
	if best == nil {
		return cands
	}
	// 仅 Selected 候选可发送
	if best.Decision == SimulationAccepted && e.cfg.Mode == "live" {
		e.executor.Execute(ctx, best)
	}
	slog.Info("route evaluated", "block", ev.BlockNumber, "best", best.Decision,
		"net_profit_wei", best.ExpectedNetProfit.String(), "amount_in", best.InputAmount.String(),
		"route", routeID(r))
	return cands
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

type atomicCounter struct {
	mu sync.Mutex
	n  uint64
}

func newAtomicCounter() *atomicCounter { return &atomicCounter{} }
func (c *atomicCounter) Inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

// MarshalRoute 序列化路由（完整可重放）。
func MarshalRoute(hops []Hop) string {
	raw, _ := json.Marshal(hops)
	return string(raw)
}

// SimulationAccepted 通过链上模拟的决策值（live 发送门槛）。
const SimulationAccepted = "simulation_accepted"

// routeRefreshFailures 状态刷新失败计数（轻量指标，无 Prometheus 依赖）。
var routeRefreshFailures = newAtomicCounter()

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
	// RefreshRoute 执行 Shadow 模式：固定一个状态区块，路由所有池的状态读取都在该高度。
	// 返回 (stateBlock, stateHash, err)。
	RefreshRoute(ctx context.Context, r Route) (uint64, common.Hash, error)
}

// Evaluator 评估：模拟验证 + 成本核算。
type Evaluator interface {
	Evaluate(ctx context.Context, c *Candidate, cfg Config) (decision, reason string, netProfit *big.Int)
}

// Executor 执行：构建、签名、广播、确认。
type Executor interface {
	Execute(ctx context.Context, c *Candidate) (hash common.Hash, err error)
}
