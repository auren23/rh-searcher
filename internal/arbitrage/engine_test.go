package arbitrage

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// 假 Searcher：返回固定 Top-K 候选
type fakeSearcher struct {
	cands []*Candidate
}

func (f *fakeSearcher) FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route {
	return []Route{{Hops: []Hop{{Pool: pool}}}}
}
func (f *fakeSearcher) SnapshotRoute(ctx context.Context, r Route, block uint64) (*RouteSnapshot, error) {
	return &RouteSnapshot{Block: 100, BlockHash: common.Hash{}, BaseFee: big.NewInt(1e8),
		Hops: []SnapshotHop{{}}}, nil
}
func (f *fakeSearcher) SnapshotTokenGroup(ctx context.Context, token common.Address, block uint64) (*TokenGroupSnapshot, error) {
	return &TokenGroupSnapshot{Block: 100, BlockHash: common.Hash{}, BaseFee: big.NewInt(1e8),
		Pools:    map[common.Address]*v3.Pool{{1}: {Address: common.Address{1}, TickSpacing: 60, Liquidity: big.NewInt(1), SqrtPriceX96: new(big.Int).Lsh(big.NewInt(1), 96)}},
		RpcCalls: 2}, nil
}
func (f *fakeSearcher) TopKOptimizeAt(ctx context.Context, r Route, snapshot *RouteSnapshot, k int, block uint64, ts int64) []*Candidate {
	return f.cands
}

// 假 Evaluator：全部 simulation_accepted
type fakeEvaluator struct{}

func (f *fakeEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int, error) {
	return SimulationAccepted, "", big.NewInt(100), nil
}

// 假 Executor：记录调用
type fakeExecutor struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeExecutor) Execute(ctx context.Context, c *Candidate) (common.Hash, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	return common.Hash{}, nil
}

// 假 Sink：收集落盘
type fakeSink struct {
	mu    sync.Mutex
	saved []*Candidate
}

func (f *fakeSink) SaveCandidate(ctx context.Context, c *Candidate) error {
	f.mu.Lock()
	f.saved = append(f.saved, c)
	f.mu.Unlock()
	return nil
}

// 同一组 Top-K 只能有一个 selected=true；其余通过者降级 simulation_valid。
func TestEngineTopKSingleSelected(t *testing.T) {
	exec := &fakeExecutor{}
	eng := NewEngine(
		Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 3, Mode: "live"},
		nil,
		&fakeSearcher{cands: []*Candidate{
			{InputAmount: big.NewInt(1), GrossProfit: big.NewInt(10), Route: []Hop{{}}, RouteJSON: "[]"},
			{InputAmount: big.NewInt(2), GrossProfit: big.NewInt(20), Route: []Hop{{}}, RouteJSON: "[]"},
			{InputAmount: big.NewInt(3), GrossProfit: big.NewInt(30), Route: []Hop{{}}, RouteJSON: "[]"},
		}},
		&fakeEvaluator{},
		exec,
	)
	res, _ := eng.ProcessBlock(context.Background(), SwapEvent{
		Pool: common.Address{1}, BlockNumber: 100,
		BlockHash: common.Hash{1}, TxHash: common.Hash{2}, LogIndex: 3,
	}, []common.Address{{1}})

	selected := 0
	valid := 0
	for _, c := range res.Candidates {
		if c.Selected {
			selected++
			if c.Decision != SimulationAccepted {
				t.Errorf("selected candidate decision=%s want simulation_accepted", c.Decision)
			}
		}
		if c.Decision == "simulation_valid" {
			valid++
		}
	}
	if selected != 1 {
		t.Errorf("selected=%d want exactly 1", selected)
	}
	if valid != 2 {
		t.Errorf("valid=%d want 2 (other accepted candidates downgraded)", valid)
	}
	// live 模式只发送一次（best）
	exec.mu.Lock()
	calls := exec.calls
	exec.mu.Unlock()
	if calls != 1 {
		t.Errorf("execute calls=%d want 1", calls)
	}
}

// ProcessBlock：同一区块多个受影响池去重（每条 route 只评估一次）。
func TestProcessBlockDedupe(t *testing.T) {
	eng := NewEngine(Config{TopK: 1}, nil,
		&countingSearcher{}, &fakeEvaluator{}, &fakeExecutor{})
	poolA := common.Address{0xaa}
	poolB := common.Address{0xbb}
	res, _ := eng.ProcessBlock(context.Background(), SwapEvent{BlockNumber: 5},
		[]common.Address{poolA, poolA, poolB})
	_ = res
	if cs := eng.searcher.(*countingSearcher).count; cs != 2 {
		t.Errorf("evaluate calls=%d want 2 (dedupe A)", cs)
	}
}

type countingSearcher struct{ count int }

func (c *countingSearcher) FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route {
	return []Route{{Hops: []Hop{{Pool: pool}}}}
}
func (c *countingSearcher) Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate {
	c.count++
	return &Candidate{InputAmount: big.NewInt(1), Route: r.Hops, RouteJSON: "[]"}
}
func (c *countingSearcher) TopKOptimizeAt(ctx context.Context, r Route, snapshot *RouteSnapshot, k int, block uint64, ts int64) []*Candidate {
	c.count++
	return []*Candidate{{InputAmount: big.NewInt(1), Route: r.Hops, RouteJSON: "[]"}}
}
func (c *countingSearcher) SnapshotRoute(ctx context.Context, r Route, block uint64) (*RouteSnapshot, error) {
	return &RouteSnapshot{Block: 100, BlockHash: common.Hash{}, BaseFee: big.NewInt(1e8),
		Hops: []SnapshotHop{{}}}, nil
}
func (c *countingSearcher) SnapshotTokenGroup(ctx context.Context, token common.Address, block uint64) (*TokenGroupSnapshot, error) {
	return &TokenGroupSnapshot{Block: 100, BlockHash: common.Hash{}, BaseFee: big.NewInt(1e8),
		Pools:    map[common.Address]*v3.Pool{{1}: {Address: common.Address{1}, TickSpacing: 60, Liquidity: big.NewInt(1), SqrtPriceX96: new(big.Int).Lsh(big.NewInt(1), 96)}},
		RpcCalls: 1}, nil
}

// 全部 rejected 时：不得有任何 selected=true（best 必须来自 simulation_accepted）。
func TestEngineNoSelectedWhenAllRejected(t *testing.T) {
	sink := &fakeSink{}
	eng := NewEngine(
		Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 3},
		sink,
		&fakeSearcher{cands: []*Candidate{
			{InputAmount: big.NewInt(1), GrossProfit: big.NewInt(1), Route: []Hop{{}}, RouteJSON: "[]"},
			{InputAmount: big.NewInt(2), GrossProfit: big.NewInt(1), Route: []Hop{{}}, RouteJSON: "[]"},
		}},
		&rejectEvaluator{},
		&fakeExecutor{},
	)
	eng.OnSwap(context.Background(), SwapEvent{
		Pool: common.Address{1}, BlockNumber: 100,
		BlockHash: common.Hash{1}, TxHash: common.Hash{2}, LogIndex: 3,
	})
	for _, c := range sink.saved {
		if c.Selected {
			t.Errorf("candidate %s selected=true but decision=%s (must never select rejected)",
				c.ID, c.Decision)
		}
		if c.Decision == SimulationAccepted {
			t.Errorf("decision=%s with rejecting evaluator", c.Decision)
		}
	}
}

type rejectEvaluator struct{}

func (r *rejectEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int, error) {
	return "simulation_rejected", "revert: test", big.NewInt(0), nil
}

// Engine 的 read/sim hash 校验必须真实执行（接口断言 + 转发）。
type verifyingEvaluator struct {
	checked bool
}

func (v *verifyingEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int, error) {
	return SimulationAccepted, "", big.NewInt(100), nil
}
func (v *verifyingEvaluator) VerifyBlockHash(ctx context.Context, block uint64, want common.Hash) error {
	v.checked = true
	return nil
}

func TestEngineHashVerificationRuns(t *testing.T) {
	eval := &verifyingEvaluator{}
	sink := &fakeSink{}
	eng := NewEngine(
		Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 1},
		sink,
		&fakeSearcher{cands: []*Candidate{{InputAmount: big.NewInt(1), GrossProfit: big.NewInt(1), Route: []Hop{{}}, RouteJSON: "[]"}}},
		eval,
		&fakeExecutor{},
	)
	eng.OnSwap(context.Background(), SwapEvent{
		Pool: common.Address{1}, BlockNumber: 100,
		BlockHash: common.Hash{1}, TxHash: common.Hash{2}, LogIndex: 3,
	})
	if !eval.checked {
		t.Errorf("VerifyBlockHash never invoked (interface assertion must match)")
	}
}

// fakeV3 最小 v3 client：HeaderAt 返回区块号即 hash；RefreshPoolStateAt 把
// 池价格设为区块号（block 200 → 价格 200e6；block 100 → 价格 100e6）。
type fakeV3 struct {
	mu        sync.Mutex
	refreshed []string
}

func (f *fakeV3) HeaderAt(ctx context.Context, block *big.Int) (*types.Header, error) {
	n := uint64(200)
	if block != nil {
		n = block.Uint64()
	}
	return &types.Header{Number: new(big.Int).SetUint64(n), BaseFee: big.NewInt(1e8)}, nil
}
func (f *fakeV3) RefreshPoolsStateAt(ctx context.Context, pools []*v3.Pool, block *big.Int) (int, error) {
	for _, p := range pools {
		if err := f.RefreshPoolStateAt(ctx, p, block); err != nil {
			return 0, err
		}
	}
	return 1, nil
}

func (f *fakeV3) RefreshPoolStateAt(ctx context.Context, p *v3.Pool, block *big.Int) error {
	f.mu.Lock()
	f.refreshed = append(f.refreshed, p.Address.Hex()+":"+block.String())
	f.mu.Unlock()
	// 价格按池地址区分：addr{1} 平价 1.00，addr{4} 溢价 1.04 → WETH→X 走
	// 平价池、X→WETH 走溢价池 = 正毛利（环可获利）
	price := block.Uint64()*1e6 + uint64(p.Address[0])*1e6/4
	p.SqrtPriceX96 = new(big.Int).SetUint64(price)
	p.Liquidity = big.NewInt(1e18)
	p.Tick = 0
	p.TickSpacing = 60
	return nil
}
func (f *fakeV3) QuoteExactIn(p *v3.Pool, tokenIn common.Address, amountIn *big.Int) (*big.Int, error) {
	// 简化报价：输出 = 输入 × 价格/100（价格 = SqrtPriceX96/1e6）
	price := new(big.Int).Div(p.SqrtPriceX96, big.NewInt(1e6))
	out := new(big.Int).Mul(amountIn, price)
	out.Div(out, big.NewInt(100))
	return out, nil
}

// P0-1 回归：ProcessBlockAt(100) 的本地报价必须来自区块 100 的快照
// （不是正式 Registry 的区块 200 价格），且评估后正式 Registry 零污染。
func TestSnapshotQuotesHistoricalState(t *testing.T) {
	ctx := context.Background()
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	live := v3.NewPoolFromMeta(common.Address{1}, "uniswap-v3",
		common.Address{2}, common.Address{3}, 3000, 60)
	live.SqrtPriceX96 = new(big.Int).SetUint64(200e6) // 实时价格 = 区块 200
	live.Liquidity = big.NewInt(1e18)
	live.Tick = 0
	reg.UpsertPool(v3.State(live))
	graph.AddPool(live.Pool(), live.Address)
	// 第二个同对池：WETH→X→WETH 两跳环需要两个池
	live2 := v3.NewPoolFromMeta(common.Address{4}, "uniswap-v3",
		common.Address{2}, common.Address{3}, 3000, 60)
	live2.SqrtPriceX96 = new(big.Int).SetUint64(200e6)
	live2.Liquidity = big.NewInt(1e18)
	live2.Tick = 0
	reg.UpsertPool(v3.State(live2))
	graph.AddPool(live2.Pool(), live2.Address)

	fv := &fakeV3{}
	searcher := NewLocalSearcher(graph, reg, fv, common.Address{2})
	searcher.SetFunding(nil, big.NewInt(1e18))

	snap, err := searcher.SnapshotRoute(ctx, Route{
		Hops: []Hop{{Pool: common.Address{1}, TokenIn: common.Address{2}, TokenOut: common.Address{3}}},
	}, 100)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Block != 100 {
		t.Fatalf("snapshot block=%d want 100", snap.Block)
	}
	if got := snap.Hops[0].Pool.SqrtPriceX96.Uint64(); got != 100250000 {
		t.Fatalf("snapshot price=%d want 100250000 (historical state)", got)
	}
	// 正式 Registry 零污染：仍是区块 200 价格
	if got := v3.UnwrapState(reg.Pool(common.Address{1})).SqrtPriceX96.Uint64(); got != 200e6 {
		t.Fatalf("live registry price=%d want 200e6 (must not be mutated)", got)
	}
	// 本地优化在快照上报价：价格 100 → 输入 100 输出 100 → 无毛利。
	// 若错误使用实时 Registry（价格 200）→ 输入 100 输出 200 → 毛利 100。
	// 候选的 GrossProfit 必须严格来自历史快照（=0），InputAmount 也是历史深度产物。
	r := Route{
		Hops: []Hop{{Pool: common.Address{1}, TokenIn: common.Address{2}, TokenOut: common.Address{3}}},
	}
	cands := searcher.TopKOptimizeAt(ctx, r, snap, 3, 100, 0)
	allZeroProfit := true
	for _, c := range cands {
		if c.GrossProfit == nil || c.GrossProfit.Sign() != 0 {
			allZeroProfit = false
			t.Logf("candidate gross=%v (must be 0 from block-100 snapshot)", c.GrossProfit)
		}
		if c.InputAmount == nil || c.InputAmount.Sign() <= 0 {
			t.Fatalf("candidate input amount missing")
		}
	}
	if !allZeroProfit {
		t.Fatalf("gross profit must come from the historical snapshot (price 100 => 0 profit)")
	}
	// 完整引擎重放：ProcessBlockAt 两次（finalizeCandidate 生成真实 ID）。
	// 断言 ID/RouteJSON 非空且两次完全一致，InputAmount/GrossProfit 一致。
	ev := SwapEvent{BlockNumber: 100, BlockHash: common.Hash{0x64}, ReceivedAt: 0}
	engine := NewEngine(Config{ChainID: 4663, WETH: common.Address{2}, MinProfitWei: big.NewInt(1),
		TopK: 3, Mode: "shadow"}, nil, searcher, &fakeEvaluator{}, &fakeExecutor{})
	res1, err := engine.ProcessBlock(ctx, ev, []common.Address{{1}})
	if err != nil {
		t.Fatalf("process1: %v", err)
	}
	res2, err := engine.ProcessBlock(ctx, ev, []common.Address{{1}})
	if err != nil {
		t.Fatalf("process2: %v", err)
	}
	if len(res1.Candidates) != len(res2.Candidates) || len(res1.Candidates) == 0 {
		t.Fatalf("candidate counts %d vs %d (want equal, non-zero)", len(res1.Candidates), len(res2.Candidates))
	}
	for i := range res1.Candidates {
		c1, c2 := res1.Candidates[i], res2.Candidates[i]
		if c1.ID == "" {
			t.Fatalf("candidate %d ID is empty", i)
		}
		if c1.ID != c2.ID || c1.RouteJSON != c2.RouteJSON {
			t.Fatalf("replay candidate %d differs (id %q vs %q)", i, c1.ID, c2.ID)
		}
		if c1.InputAmount.Cmp(c2.InputAmount) != 0 || c1.GrossProfit.Cmp(c2.GrossProfit) != 0 {
			t.Fatalf("replay candidate %d amounts differ", i)
		}
	}
}

// local_only：本地毛利为正 → local_profitable_observed + analysis_selected=true
// 且 selected 保持 false；不调用 evaluator（nil 安全）。
func TestLocalOnlyModeDecision(t *testing.T) {
	searcher := &fakeSearcher{cands: []*Candidate{
		{InputAmount: big.NewInt(5), GrossProfit: big.NewInt(1e16), Route: []Hop{{}}, RouteJSON: "[]",
			GasEstimate: big.NewInt(2e5)},
	}}
	eng := NewEngine(Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 1,
		Mode: "shadow", SimulationMode: "local_only",
		LocalGasUnits: 5e5, LocalGasStressMultiplier: 2,
		SafetyMarginWei: big.NewInt(0)}, nil, searcher, nil, &fakeExecutor{})
	res, err := eng.ProcessBlock(context.Background(), SwapEvent{
		BlockNumber: 100, BlockHash: common.Hash{1}, ReceivedAt: 1,
	}, []common.Address{{1}})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(res.Candidates) != 1 {
		t.Fatalf("candidates=%d want 1", len(res.Candidates))
	}
	c := res.Candidates[0]
	if c.Decision != "local_profitable_observed" {
		t.Fatalf("decision=%s want local_profitable_observed", c.Decision)
	}
	// Gas 成本必须非零且真实：gross 1e16，成本 = 5e5 units × baseFee(0.1gwei=1e8) × 2 = 1e14
	if c.GasEstimate == nil || c.GasEstimate.Sign() <= 0 {
		t.Fatalf("local_only gas cost must be non-zero")
	}
	if !c.AnalysisSelected {
		t.Fatalf("analysis_selected must be true in local_only mode")
	}
	if c.Selected {
		t.Fatalf("live selected must stay false in local_only mode")
	}
	if c.SimulationMode != "local_only" || c.StateQuality != "local" {
		t.Fatalf("mode fields wrong: %s/%s", c.SimulationMode, c.StateQuality)
	}
	if c.ExpectedNetProfit.Cmp(big.NewInt(0)) <= 0 {
		t.Fatalf("net profit must be positive (gross 10 - 2x gas)")
	}
}

// local_only 毛利为负 → local_unprofitable，无 analysis_selected。
func TestLocalOnlyUnprofitable(t *testing.T) {
	searcher := &fakeSearcher{cands: []*Candidate{
		{InputAmount: big.NewInt(5), GrossProfit: big.NewInt(1), Route: []Hop{{}}, RouteJSON: "[]",
			GasEstimate: big.NewInt(2e6)},
	}}
	// gross=1 wei，成本 = 2e6*2*2e8 = 8e14 → 必为 unprofitable
	eng := NewEngine(Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 1,
		Mode: "shadow", SimulationMode: "local_only",
		LocalGasUnits: 5e5, LocalGasStressMultiplier: 2,
		SafetyMarginWei: big.NewInt(0)}, nil, searcher, nil, &fakeExecutor{})
	res, err := eng.ProcessBlock(context.Background(), SwapEvent{
		BlockNumber: 100, BlockHash: common.Hash{1}, ReceivedAt: 1,
	}, []common.Address{{1}})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if res.Candidates[0].Decision != "local_unprofitable" {
		t.Fatalf("decision=%s want local_unprofitable", res.Candidates[0].Decision)
	}
	if res.Candidates[0].AnalysisSelected {
		t.Fatalf("no analysis_selected for unprofitable")
	}
}

// 真实 LocalSearcher + 完整引擎的 local_only 集成：
// 候选 GasEstimate 由引擎按 local_gas_units × baseFee × multiplier 计算，
// 不允许出现"未知 Gas = 零成本"。
func TestLocalOnlyRealSearcherGasNonZero(t *testing.T) {
	ctx := context.Background()
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	mk := func(addr byte) *v3.Pool {
		p := v3.NewPoolFromMeta(common.Address{addr}, "uniswap-v3",
			common.Address{2}, common.Address{3}, 3000, 60)
		p.SqrtPriceX96 = new(big.Int).SetUint64(100e6)
		p.Liquidity = big.NewInt(1e18)
		p.Tick = 0
		reg.UpsertPool(v3.State(p))
		graph.AddPool(p.Pool(), p.Address)
		return p
	}
	mk(1)
	mk(4) // WETH→X→WETH 两跳环需要两个池
	fv := &fakeV3{}
	searcher := NewLocalSearcher(graph, reg, fv, common.Address{2})
	searcher.SetFunding(nil, big.NewInt(1e18))
	eng := NewEngine(Config{ChainID: 4663, WETH: common.Address{2}, MinProfitWei: big.NewInt(1),
		TopK: 3, Mode: "shadow", SimulationMode: "local_only",
		LocalGasUnits: 5e5, LocalGasStressMultiplier: 2,
		SafetyMarginWei: big.NewInt(5e12)}, nil, searcher, nil, &fakeExecutor{})
	res, err := eng.ProcessBlock(ctx, SwapEvent{BlockNumber: 100, BlockHash: common.Hash{0x64}, ReceivedAt: 1},
		[]common.Address{{1}})
	if err != nil {
		t.Fatalf("process: %v", err)
	}
	if len(res.Candidates) == 0 {
		t.Fatalf("no candidates from real searcher")
	}
	for _, c := range res.Candidates {
		// 真实链路：GasEstimate 必须由引擎填充（>0），不允许 0
		t.Logf("cand id=%s decision=%s gross=%v gas=%v reject=%s", c.ID[:16], c.Decision, c.GrossProfit, c.GasEstimate, c.RejectReason)
		if c.GasEstimate == nil || c.GasEstimate.Sign() <= 0 {
			t.Fatalf("candidate %s gas cost must be non-zero (got %v)", c.ID, c.GasEstimate)
		}
		if c.Decision != "local_unprofitable" && c.Decision != "local_profitable_observed" {
			t.Fatalf("decision=%s must be local_*", c.Decision)
		}
	}
}

// token-group/head-batch：3 个 WETH 池共享同一 token → 6 条 route，
// 全部路由只做一次组快照（RpcCalls=header+1 批量），每池只刷一次。
func TestEngineProcessTokenGroupAt(t *testing.T) {
	ctx := context.Background()
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	weth := common.Address{2}
	token := common.Address{3}
	// 三个 WETH/TOKEN 池（不同 fee 档）：pools {1,4,5}，token 侧一致
	for _, addr := range []byte{1, 4, 5} {
		p := v3.NewPoolFromMeta(common.Address{addr}, "uniswap-v3",
			weth, token, uint32(addr)*100, 60)
		p.SqrtPriceX96 = new(big.Int).SetUint64(100e6)
		p.Liquidity = big.NewInt(1e18)
		p.Tick = 0
		reg.UpsertPool(v3.State(p))
		graph.AddPool(p.Pool(), p.Address)
	}
	fv := &fakeV3{}
	searcher := NewLocalSearcher(graph, reg, fv, weth)
	searcher.SetFunding(nil, big.NewInt(1e18))
	eng := NewEngine(Config{ChainID: 4663, WETH: weth, MinProfitWei: big.NewInt(1),
		TopK: 2, Mode: "shadow", SimulationMode: "local_only",
		LocalGasUnits: 5e5, LocalGasStressMultiplier: 2,
		SafetyMarginWei: big.NewInt(5e12)}, nil, searcher, nil, &fakeExecutor{})
	res, err := eng.ProcessTokenGroupAt(ctx, SwapEvent{
		BlockNumber: 100, BlockHash: common.Hash{0x64}, TxHash: common.Hash{1}, LogIndex: 0, ReceivedAt: 1,
	}, []common.Address{{1}}, token, 100)
	if err != nil {
		t.Fatalf("process token group: %v", err)
	}
	// 3 池 → 触发池 {1} 的 route = (1→4, 1→5, 4→1, 5→1) = 4 条
	if res.RouteCount != 4 {
		t.Fatalf("route_count=%d want 4", res.RouteCount)
	}
	if res.UniquePools != 3 {
		t.Fatalf("unique_pools=%d want 3", res.UniquePools)
	}
	// header(1) + 批量刷新(1)（fake 记 1 次批量调用）
	if res.RpcCalls != 2 {
		t.Fatalf("rpc_calls=%d want 2", res.RpcCalls)
	}
	// 每池只刷新一次（3 个池各 1 条记录），不允许 route 级重复 RPC
	fv.mu.Lock()
	defer fv.mu.Unlock()
	seen := map[string]int{}
	for _, r := range fv.refreshed {
		seen[r]++
	}
	if len(seen) != 3 {
		t.Fatalf("refreshed pools=%d want 3: %v", len(seen), seen)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("pool %s refreshed %d times, want 1 (route-level dedup failed)", k, n)
		}
	}
	// TopK 2 但最优量已触到资金上限 1e18：邻域采样点 clamp 后去重 → 每 route 1 候选。
	// 断言重点是 route 全覆盖（4 条 route 各至少 1 候选），不是数量恒等于 8。
	if len(res.Candidates) < 4 {
		t.Fatalf("candidates=%d want >=4 (one per route)", len(res.Candidates))
	}
	routeSeen := map[string]bool{}
	for _, c := range res.Candidates {
		routeSeen[routeID(Route{Hops: c.Route})] = true
	}
	if len(routeSeen) != 4 {
		t.Fatalf("routes covered=%d want 4: %v", len(routeSeen), routeSeen)
	}
	if res.TotalEvalMs < 0 || res.StateFetchMs < 0 || res.LocalQuoteMs < 0 {
		t.Fatalf("eval ms stats invalid: %+v", res)
	}
}
