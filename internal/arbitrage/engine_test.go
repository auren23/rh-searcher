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
	return &RouteSnapshot{Block: 100, BlockHash: common.Hash{}, Hops: []SnapshotHop{{}}}, nil
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
	mu   sync.Mutex
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
	return &RouteSnapshot{Block: 100, BlockHash: common.Hash{}, Hops: []SnapshotHop{{}}}, nil
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
	return &types.Header{Number: new(big.Int).SetUint64(n)}, nil
}
func (f *fakeV3) RefreshPoolStateAt(ctx context.Context, p *v3.Pool, block *big.Int) error {
	f.mu.Lock()
	f.refreshed = append(f.refreshed, p.Address.Hex()+":"+block.String())
	f.mu.Unlock()
	price := block.Uint64()
	p.SqrtPriceX96 = new(big.Int).SetUint64(price * 1e6) // 区块号即价格
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
	if got := snap.Hops[0].Pool.SqrtPriceX96.Uint64(); got != 100e6 {
		t.Fatalf("snapshot price=%d want 100e6 (historical state)", got)
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
	// 重放：同一区块两次评估 → 候选 ID/RouteJSON 完全一致（确定性）
	snapB, err := searcher.SnapshotRoute(ctx, r, 100)
	if err != nil {
		t.Fatalf("snapshotB: %v", err)
	}
	candsB := searcher.TopKOptimizeAt(ctx, r, snapB, 3, 100, 0)
	if len(cands) != len(candsB) {
		t.Fatalf("replay candidate count %d != %d", len(cands), len(candsB))
	}
	for i := range cands {
		if cands[i].ID != candsB[i].ID || cands[i].RouteJSON != candsB[i].RouteJSON {
			t.Fatalf("replay candidate %d differs: %s vs %s", i, cands[i].ID, candsB[i].ID)
		}
	}
	// 再次重放：相同快照（确定性）
	snap2, err := searcher.SnapshotRoute(ctx, Route{
		Hops: []Hop{{Pool: common.Address{1}, TokenIn: common.Address{2}, TokenOut: common.Address{3}}},
	}, 100)
	if err != nil {
		t.Fatalf("snapshot2: %v", err)
	}
	if snap2.Hops[0].Pool.SqrtPriceX96.Cmp(snap.Hops[0].Pool.SqrtPriceX96) != 0 {
		t.Fatalf("replay not deterministic")
	}
}
