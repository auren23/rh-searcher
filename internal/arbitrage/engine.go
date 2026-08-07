// Package arbitrage 套利引擎：候选发现、优化、评估、执行。
// MVP 只做 WETH→TOKEN→WETH 两池循环；shadow 模式只记录不发送。
package arbitrage

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
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
	GasEstimateMode    string // latest_approximation | max_gas_fallback | historical
	// 观察模式字段（local_only / latest_observe / historical_strict）
	SimulationMode   string // 本候选的评估模式
	StateQuality     string // historical | latest_consistent | latest_mixed_state | local
	StateAgeMs       int64  // 评估开始 - 原始事件接收（基于持久化 received_at_ms）
	StateLagBlocks   uint64 // state_block - observed_block
	AnalysisSelected bool   // 研究用组内最佳（与 live selected 分离）
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
	SimulationMode  string // local_only | latest_observe | historical_strict（显式，禁止静默降级）
	StateBlock      *big.Int // 评估固定区块（eth_call 与该高度对齐；nil = latest）
	// 状态对齐信息（latest_observe 用）
	HeadAtSnapshot  uint64 // 快照构建时的 head（状态年龄计算）
	HeadAtSnapshotMs int64
	// local_only 保守成本：units × head baseFee × stress multiplier
	LocalGasUnits            uint64
	LocalGasStressMultiplier int
}

func NewEngine(cfg Config, sink Sink, searcher Searcher, evaluator Evaluator, executor Executor) *Engine {
	return &Engine{cfg: cfg, sink: sink, searcher: searcher, evaluator: evaluator, executor: executor}
}

// SetConfig 运行时更新配置（latest_observe 模式每批评估前更新 head 对齐信息）。
func (e *Engine) SetConfig(cfg Config) {
	e.cfg = cfg
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
	// 创建溯源：动态发现的池从 Factory PoolCreated 日志查询（非首次观察块）
	CreatedBlock     uint64
	CreatedBlockHash common.Hash
	// ProvenanceSource: "pool_created_log" | "observed_swap_fallback" | ""
	ProvenanceSource string
}

// finalizeCandidate 补全候选元数据（ID/BlockHash/GroupID/SourceEvent/StateBlock）。
// 所有路径（含拒绝）都必须经过：空 ID 或零 hash 的候选会在落盘时被 ON CONFLICT 吞掉。
func (e *Engine) finalizeCandidate(c *Candidate, ev SwapEvent, r Route, stateBlock uint64) {
	c.BlockHash = ev.BlockHash
	c.TxHash = ev.TxHash
	c.LogIndex = ev.LogIndex
	c.SourceEvent = "block_swap_batch"
	c.StateBlock = stateBlock
	c.OpportunityGroupID = fmt.Sprintf("%d/%s/%s", e.cfg.ChainID, ev.BlockHash.Hex(), routeID(r))
	c.RouteJSON = MarshalRoute(c.Route)
	if c.ID == "" {
		c.ID = CandidateID(e.cfg.ChainID, ev.BlockHash, ev.TxHash, ev.LogIndex, c.RouteJSON, c.InputAmount)
	}
}

// ProcessBlock 区块级评估：应用完整区块的日志后，收集所有受影响池的路由，
// 按池序列全局去重，每条 route 只评估一次（避免两池同区块 Swap 时重复模拟）。
// ProcessBlock 按评估区块高度固定状态评估（stateBlock = ev.BlockNumber）。
// 不再存在"跨区块聚合到 latest 状态"的路径：每个队列区块独立固定评估。
func (e *Engine) ProcessBlock(ctx context.Context, ev SwapEvent, affectedPools []common.Address) (*BlockResult, error) {
	return e.ProcessBlockAt(ctx, ev, affectedPools, ev.BlockNumber)
}

// ProcessBlockAt 与 ProcessBlock 相同，但状态区块可显式指定
// （恢复路径：评估终点可能低于事件区块）。
// 返回 error = 可重试基础设施错误：调用方不得提交该区块的候选，
// 评估游标必须保持（下轮重试整个区块）。
func (e *Engine) ProcessBlockAt(ctx context.Context, ev SwapEvent, affectedPools []common.Address, stateBlock uint64) (*BlockResult, error) {
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
		cands, err := e.evaluateRoute(ctx, ev, r, stateBlock)
		if err != nil {
			return res, err
		}
		res.Candidates = append(res.Candidates, cands...)
	}
	return res, nil
}

// OnSwap 收到 Swap 事件后调用：找循环 → 优化 → 评估 → 落盘（含拒绝）。
func (e *Engine) OnSwap(ctx context.Context, ev SwapEvent) {
	routes := e.searcher.FindRoutes(ctx, ev.Pool, e.cfg.WETH, e.cfg.MaxHops)
	for _, r := range routes {
		e.evaluateRoute(ctx, ev, r, ev.BlockNumber)
	}
}

// evaluateRoute 单条路由的 Top-K 模拟与统一落盘。返回本路由产生的候选（不写数据库）。
func (e *Engine) evaluateRoute(ctx context.Context, ev SwapEvent, r Route, stateBlock uint64) ([]*Candidate, error) {
	// 执行 Shadow 模式：固定状态区块（逐块评估 = 队列区块本身），
	// 构建不可变快照（克隆池已刷新到该高度）供整个本地报价链使用
	snapshot, err := e.searcher.SnapshotRoute(ctx, r, stateBlock)
	if err != nil {
		if errors.Is(err, ErrInfra) {
			// 基础设施错误（RPC 超时/限流/历史状态不可用）：
			// 区块保持未评估，由上层重试——不能落成永久拒绝
			return nil, err
		}
		// 确定性错误（池不存在/未来池等）：正常拒绝候选
		routeRefreshFailures.Inc()
		slog.Warn("route snapshot failed", "route", routeID(r), "err", err)
		rej := emptyCandidate(r, ev.BlockNumber, ev.ReceivedAt, e.cfg.WETH)
		rej.Decision = "local_rejected"
		rej.RejectReason = "state-incomplete: " + err.Error()
		e.finalizeCandidate(rej, ev, r, 0)
		return []*Candidate{rej}, nil
	}
	// read RPC 与 sim RPC 的区块 hash 一致性校验：
	// 不一致 = sim 节点落后/分歧（基础设施），区块保持未评估等待追平
	if v, ok := e.evaluator.(interface {
		VerifyBlockHash(ctx context.Context, block uint64, want common.Hash) error
	}); ok {
		if err := v.VerifyBlockHash(ctx, snapshot.Block, snapshot.BlockHash); err != nil {
			slog.Warn("read/sim block hash mismatch", "route", routeID(r), "err", err)
			return nil, fmt.Errorf("read/sim block hash mismatch at %d: %w", snapshot.Block, err)
		}
	}
	// Top-K 输入量逐个链上模拟，选模拟净利最高者；先全部模拟，再统一落盘。
	// 本地报价链显式使用 snapshot（历史状态），不触碰实时 Registry。
	cands := e.searcher.TopKOptimizeAt(ctx, r, snapshot, e.cfg.TopK, ev.BlockNumber, ev.ReceivedAt)
	// 标记观察模式与状态质量（每个候选）
	for _, c := range cands {
		c.SimulationMode = e.cfg.SimulationMode
		c.StateAgeMs = 0
		if e.cfg.HeadAtSnapshotMs > 0 && c.ObservedAt > 0 {
			c.StateAgeMs = e.cfg.HeadAtSnapshotMs - c.ObservedAt
			if c.StateAgeMs < 0 {
				c.StateAgeMs = 0
			}
		}
		switch e.cfg.SimulationMode {
		case "local_only":
			c.StateQuality = "local"
		case "latest_observe":
			c.StateQuality = "latest_consistent"
		default:
			c.StateQuality = "historical"
		}
	}
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
	var best *Candidate
	for rank, c := range cands {
		if c == nil || c.InputAmount == nil {
			slog.Error("searcher returned incomplete candidate", "route", routeID(r))
			continue // 记录并跳过，绝不 panic
		}
		e.finalizeCandidate(c, ev, r, snapshot.Block)
		c.Rank = rank + 1 // 1 起（存储层 0 视为 NULL）
		if c.RejectReason != "" {
			// searcher 已判定（state-incomplete / route quote failed）：不再交给模拟器覆盖
			c.Decision = "local_rejected"
			c.ExpectedNetProfit = new(big.Int)
			continue
		}
		if e.cfg.SimulationMode == "local_only" {
			// 零资金观察：本地毛利 - 保守 gas（不调合约、不要求 executor）。
			// 成本 = local_gas_units × head baseFee × stress multiplier + safety margin。
			// 失败关闭：未配置 gas units 或 base fee 缺失 → 整块保持未评估
			if e.cfg.LocalGasUnits == 0 {
				return nil, fmt.Errorf("local_only requires arbitrage.local_gas_units > 0")
			}
			if snapshot.BaseFee == nil || snapshot.BaseFee.Sign() <= 0 {
				return nil, fmt.Errorf("local_only: head %d missing base fee", snapshot.Block)
			}
			mult := e.cfg.LocalGasStressMultiplier
			if mult <= 0 {
				mult = 2
			}
			// 单位语义：gas_estimate = gas units；gas_price_wei = baseFee × mult；
			// gas_cost_wei = units × price（不含 safety margin）；净利再扣 margin
			gasPriceWei := new(big.Int).Mul(snapshot.BaseFee, big.NewInt(int64(mult)))
			gasCostWei := new(big.Int).Mul(
				new(big.Int).SetUint64(e.cfg.LocalGasUnits), gasPriceWei)
			c.GasEstimate = new(big.Int).SetUint64(e.cfg.LocalGasUnits)
			c.GasPriceWei = new(big.Int).Set(gasPriceWei)
			c.GasCostWei = new(big.Int).Set(gasCostWei)
			if c.GrossProfit == nil || c.GrossProfit.Sign() <= 0 {
				c.Decision = "local_unprofitable"
				c.ExpectedNetProfit = new(big.Int)
			} else {
				net := new(big.Int).Sub(c.GrossProfit, gasCostWei)
				net.Sub(net, e.cfg.SafetyMarginWei)
				c.ExpectedNetProfit = net
				if net.Sign() <= 0 {
					c.Decision = "local_unprofitable"
				} else {
					c.Decision = "local_profitable_observed"
				}
			}
			if c.Decision == "local_profitable_observed" &&
				(best == nil || c.ExpectedNetProfit.Cmp(best.ExpectedNetProfit) > 0) {
				best = c
			}
			continue
		}
		simCfg := e.cfg
		simCfg.StateBlock = new(big.Int).SetUint64(snapshot.Block)
		verdict, reason, profit, err := e.evaluator.Evaluate(ctx, c, simCfg)
		if err != nil {
			return nil, fmt.Errorf("evaluate route %s: %w", routeID(r), err)
		}
		c.Decision = verdict
		c.RejectReason = reason
		c.ExpectedNetProfit = profit
		// best 选取按模式：historical_strict 只要 simulation_accepted；
		// latest_observe 允许 cost_approx 且正净利（标记 analysis_selected，不 selected）
		eligible := false
		switch e.cfg.SimulationMode {
		case "latest_observe":
			eligible = verdict == "simulation_valid_cost_approx" &&
				profit != nil && profit.Sign() > 0
		default:
			eligible = verdict == SimulationAccepted
		}
		if eligible {
			if best == nil || c.ExpectedNetProfit.Cmp(best.ExpectedNetProfit) > 0 {
				best = c
			}
		}
	}
	// 统一收集：best 标记 selected=true；其他通过模拟的降级为 simulation_valid。
	// local_only/latest_observe 下 best 只标 analysis_selected（live selected 保持 false）
	for _, c := range cands {
		if c.ID == "" {
			continue // 未进入评估流程（searcher 异常）
		}
		if best != nil && c.ID == best.ID {
			if e.cfg.SimulationMode == "local_only" || e.cfg.SimulationMode == "latest_observe" {
				c.AnalysisSelected = true
			} else {
				c.Selected = true
			}
		} else if c.Decision == SimulationAccepted {
			c.Decision = "simulation_valid"
			c.RejectReason = "not selected (lower net profit in group)"
		}
	}
	if best == nil {
		return cands, nil
	}
	// 仅 Selected 候选可发送
	if best.Decision == SimulationAccepted && e.cfg.Mode == "live" {
		e.executor.Execute(ctx, best)
	}
	slog.Info("route evaluated", "block", ev.BlockNumber, "best", best.Decision,
		"net_profit_wei", best.ExpectedNetProfit.String(), "amount_in", best.InputAmount.String(),
		"route", routeID(r))
	return cands, nil
}

// ErrInfra 标记可重试的基础设施错误（RPC 超时/限流/节点落后/历史状态不可用）。
// 区别于确定性的合约/路由错误——前者必须保持区块未评估，后者落成拒绝候选。
var ErrInfra = errors.New("infrastructure error")
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
// SimulationCostApprox 模拟成功但 gas 成本非 historical（估算近似/兜底）：
// 记录利润数据，但不得进入正式 Selected / 净 EV 统计。
const SimulationCostApprox = "simulation_valid_cost_approx"
const SimulationAccepted = "simulation_accepted"

// routeRefreshFailures 状态刷新失败计数（轻量指标，无 Prometheus 依赖）。
var routeRefreshFailures = newAtomicCounter()

// Route 从 searcher 返回的候选路径。
type Route struct {
	Hops []Hop
}

// Searcher 候选搜索：路径发现 + 固定区块快照 + 快照内优化。
type Searcher interface {
	FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route
	// SnapshotRoute 把路由所有池的状态固定读取到 block 高度并克隆成不可变视图。
	// 逐块评估时 block 必须是该队列区块本身，不能读取 latest。
	SnapshotRoute(ctx context.Context, r Route, block uint64) (*RouteSnapshot, error)
	// TopKOptimizeAt 在固定 snapshot 上返回本地毛利最高的 k 个输入量候选
	// （供逐个链上模拟后选优）。报价链必须只用 snapshot，不得触碰实时 Registry。
	TopKOptimizeAt(ctx context.Context, r Route, snapshot *RouteSnapshot, k int, block uint64, ts int64) []*Candidate
}

// Evaluator 评估：模拟验证 + 成本核算。
type Evaluator interface {
	// Evaluate 返回 err 表示基础设施错误（可重试）：调用方必须让整个区块
	// 保持未评估（游标不前进），不能落成永久拒绝。
	Evaluate(ctx context.Context, c *Candidate, cfg Config) (decision, reason string, netProfit *big.Int, err error)
}

// Executor 执行：构建、签名、广播、确认。
type Executor interface {
	Execute(ctx context.Context, c *Candidate) (hash common.Hash, err error)
}
