package arbitrage

import (
	"context"
	"math/big"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// 假 Searcher：返回固定 Top-K 候选
type fakeSearcher struct {
	cands []*Candidate
}

func (f *fakeSearcher) FindRoutes(ctx context.Context, pool common.Address, weth common.Address, maxHops int) []Route {
	return []Route{{Hops: []Hop{{Pool: pool}}}}
}
func (f *fakeSearcher) Optimize(ctx context.Context, r Route, block uint64, ts int64) *Candidate {
	return f.cands[0]
}
func (f *fakeSearcher) TopKOptimize(ctx context.Context, r Route, k int, block uint64, ts int64) []*Candidate {
	return f.cands
}
func (f *fakeSearcher) RefreshRoute(ctx context.Context, r Route) (uint64, common.Hash, error) { return 100, common.Hash{}, nil }

// 假 Evaluator：全部 simulation_accepted
type fakeEvaluator struct{}

func (f *fakeEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int) {
	return SimulationAccepted, "", big.NewInt(100)
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
	sink := &fakeSink{}
	exec := &fakeExecutor{}
	eng := NewEngine(
		Config{ChainID: 4663, MinProfitWei: big.NewInt(1), TopK: 3, Mode: "live"},
		sink,
		&fakeSearcher{cands: []*Candidate{
			{InputAmount: big.NewInt(1), GrossProfit: big.NewInt(10), Route: []Hop{{}}, RouteJSON: "[]"},
			{InputAmount: big.NewInt(2), GrossProfit: big.NewInt(20), Route: []Hop{{}}, RouteJSON: "[]"},
			{InputAmount: big.NewInt(3), GrossProfit: big.NewInt(30), Route: []Hop{{}}, RouteJSON: "[]"},
		}},
		&fakeEvaluator{},
		exec,
	)
	eng.OnSwap(context.Background(), SwapEvent{
		Pool: common.Address{1}, BlockNumber: 100,
		BlockHash: common.Hash{1}, TxHash: common.Hash{2}, LogIndex: 3,
	})

	selected := 0
	valid := 0
	for _, c := range sink.saved {
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

// OnBlockBatch：同一区块多个受影响池去重（每池一次 OnSwap）。
func TestOnBlockBatchDedupe(t *testing.T) {
	eng := NewEngine(Config{TopK: 1}, nil,
		&countingSearcher{}, &fakeEvaluator{}, &fakeExecutor{})
	poolA := common.Address{0xaa}
	poolB := common.Address{0xbb}
	eng.OnBlockBatch(context.Background(), SwapEvent{BlockNumber: 5},
		[]common.Address{poolA, poolA, poolB})
	if cs := eng.searcher.(*countingSearcher).count; cs != 2 {
		t.Errorf("OnSwap calls=%d want 2 (dedupe A)", cs)
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
func (c *countingSearcher) TopKOptimize(ctx context.Context, r Route, k int, block uint64, ts int64) []*Candidate {
	c.count++
	return []*Candidate{{InputAmount: big.NewInt(1), Route: r.Hops, RouteJSON: "[]"}}
}
func (c *countingSearcher) RefreshRoute(ctx context.Context, r Route) (uint64, common.Hash, error) { return 100, common.Hash{}, nil }

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

func (r *rejectEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int) {
	return "simulation_rejected", "revert: test", big.NewInt(0)
}

// Engine 的 read/sim hash 校验必须真实执行（接口断言 + 转发）。
type verifyingEvaluator struct {
	checked bool
}

func (v *verifyingEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int) {
	return SimulationAccepted, "", big.NewInt(100)
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
