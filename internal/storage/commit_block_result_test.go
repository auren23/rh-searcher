package storage

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/arbitrage"
)

func testDB(t *testing.T) *DB {
	t.Helper()
	url := os.Getenv("RH_TEST_PG_URL")
	if url == "" {
		t.Skip("RH_TEST_PG_URL not set (CI postgres service)")
	}
	ctx := context.Background()
	db, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func deref(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func testCandidate(id string, block uint64, hash string) *arbitrage.Candidate {
	return &arbitrage.Candidate{
		ID:            id,
		ObservedBlock: block,
		ObservedAt:    int64(block),
		BlockHash:     common.HexToHash(hash),
		SourceEvent:   "block_swap_batch",
		RouteJSON:     `[{"Pool":"0x0000000000000000000000000000000000000001"}]`,
		InputAsset:    common.Address{},
		InputAmount:   big.NewInt(1),
		GrossProfit:   big.NewInt(2),
		GasEstimate:   big.NewInt(0),
		SwapCost:      big.NewInt(0),
		SlippageCost:  big.NewInt(0),
		ExpectedNetProfit: big.NewInt(2),
		Decision:      "simulation_accepted",
		Route:         []arbitrage.Hop{{}},
	}
}

// Fresh DB：空库 CommitBlockResult 成功后 checkpoint + processed_blocks 都写入；
// 校验字段完整（hash/parent 不为空），可被 LoadWithHash 读回。
func TestCommitBlockResultFreshDB(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	err := db.CommitBlockIngest(ctx, 100, "0xaaa", "0x999", nil, []string{"0xaa01"})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	err = db.CommitEvaluation(ctx, 100, "0xaaa",
		[]*arbitrage.Candidate{testCandidate("fresh-db-1", 100, "0xaaa")})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	// LoadWithHash 读回
	n, h, err := NewPGCheckpoint(db).LoadWithHash(ctx, CheckpointBlocks)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if n != 100 || h != common.HexToHash("0xaaa") {
		t.Fatalf("checkpoint got %d %s", n, h.Hex())
	}
	// processed_blocks 有规范行
	var cnt int
	if err := db.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM processed_blocks WHERE strategy=$1 AND block_hash='0xaaa' AND canonical`,
		CheckpointBlocks).Scan(&cnt); err != nil || cnt != 1 {
		t.Fatalf("processed_blocks=%d err=%v", cnt, err)
	}
}

// 两层 reorg：commit 5 块 → MarkOrphans(above=3) → 孤块 candidate/区块 canonical=false，
// 共同祖先（≤3）保持 canonical=true。
func TestMarkOrphansTwoLevelReorg(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;
		DELETE FROM dex_pools;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	var prev string
	for i := uint64(1); i <= 5; i++ {
		h := common.BigToHash(new(big.Int).SetUint64(i)).Hex()
		parent := prev
		prev = h
		// 每个区块携带一个新池（创建 hash = 该区块 hash；孤块 4/5 的池应被标记）
		poolAddr := common.BigToHash(new(big.Int).SetUint64(1000 + i)).Hex()[24:]
		pools := []Pool{{Address: "0x" + poolAddr, Exchange: "uniswap-v3",
			Protocol: "v3", Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}}
		if err := db.CommitBlockIngest(ctx, i, h, parent, pools,
			[]string{"0xaa01"}); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
		if err := db.CommitEvaluation(ctx, i, h,
			[]*arbitrage.Candidate{testCandidate(
				"reorg-cand-" + h, i, h)}); err != nil {
			t.Fatalf("evaluate %d: %v", i, err)
		}
	}
	if err := db.RollbackToAncestor(ctx, CheckpointBlocks, 3, "0x3", "0x2"); err != nil {
		t.Fatalf("mark: %v", err)
	}
	// checkpoint 必须回退到祖先（P0-2）
	var ckptN uint64
	var ckptH *string
	if err := db.pool.QueryRow(ctx,
		`SELECT block_number, block_hash FROM strategy_checkpoints WHERE strategy=$1`,
		CheckpointBlocks).Scan(&ckptN, &ckptH); err != nil {
		t.Fatal(err)
	}
	if ckptN != 3 || ckptH == nil || *ckptH != "0x3" {
		t.Fatalf("checkpoint not rolled back: n=%d h=%v", ckptN, ckptH)
	}
	var orphan, kept int
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM opportunities WHERE canonical = FALSE`).Scan(&orphan); err != nil {
		t.Fatal(err)
	}
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM opportunities WHERE canonical = TRUE`).Scan(&kept); err != nil {
		t.Fatal(err)
	}
	if orphan != 2 || kept != 3 {
		t.Fatalf("canonical: orphan=%d kept=%d want 2/3", orphan, kept)
	}
	var pbOrphan int
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM processed_blocks WHERE canonical = FALSE`).Scan(&pbOrphan); err != nil {
		t.Fatal(err)
	}
	if pbOrphan != 2 {
		t.Fatalf("processed_blocks orphan=%d want 2", pbOrphan)
	}
	// 孤块中创建的池也被标记（dex_pools canonical）
	var poolOrphan int
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM dex_pools WHERE canonical = FALSE`).Scan(&poolOrphan); err != nil {
		t.Fatal(err)
	}
	if poolOrphan != 2 {
		t.Fatalf("dex_pools orphan=%d want 2", poolOrphan)
	}
	// 同高度 canonical 行不允许重复（历史表主键策略）
	var dup int
	if err := db.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM (
			SELECT block_number FROM processed_blocks
			WHERE strategy=$1 AND canonical = TRUE
			GROUP BY block_number HAVING COUNT(*) > 1) t`,
		CheckpointBlocks).Scan(&dup); err != nil || dup != 0 {
		t.Fatalf("dup canonical rows=%d err=%v", dup, err)
	}
}

// 事务失败原子性：候选含非法 ID（重复 + 修改后旧行唯一约束冲突）时整组回滚，
// checkpoint 不前进（游标可安全重试同一区块）。
func TestCommitBlockResultRollsBackOnCandidateFailure(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if err := db.CommitBlockIngest(ctx, 200, "0xbbb", "0x999", nil, nil); err != nil {
		t.Fatalf("first ingest: %v", err)
	}
	if err := db.CommitEvaluation(ctx, 200, "0xbbb",
		[]*arbitrage.Candidate{testCandidate("rollback-dupe", 200, "0xbbb")}); err != nil {
		t.Fatalf("first evaluate: %v", err)
	}
	// 同 group 两个不同 id 的新行都 selected=true → 唯一索引必须拒绝整组（事务回滚）
	dup := testCandidate("rollback-dup-id-2", 200, "0xbbb")
	dup.OpportunityGroupID = "g1"
	dup.Selected = true
	good := testCandidate("rollback-dup-id-1", 200, "0xbbb")
	good.OpportunityGroupID = "g1"
	good.Selected = true
	err := db.CommitEvaluation(ctx, 200, "0xbbb", []*arbitrage.Candidate{good, dup})
	if err == nil {
		t.Fatalf("duplicate selected in one group must fail the transaction")
	}
	// checkpoint 仍是 200（事务回滚后无变化），processed_blocks 无 0xccc
	var n uint64
	if err := db.pool.QueryRow(ctx,
		`SELECT block_number FROM strategy_checkpoints WHERE strategy=$1`,
		CheckpointBlocks).Scan(&n); err != nil || n != 200 {
		t.Fatalf("checkpoint n=%d err=%v want 200", n, err)
	}
}

// P0-1: RestorePools 必须排除孤块池（LoadPools WHERE canonical=TRUE），
// 否则孤池会重新进入 Graph。
func TestRestorePoolsExcludesOrphans(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;
		DELETE FROM dex_pools;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	// 两个池：孤儿（created_block_hash 属于孤块）与规范池
	orphanHash := common.BigToHash(big.NewInt(9001)).Hex()
	goodHash := common.BigToHash(big.NewInt(9002)).Hex()
	orphan := Pool{Address: "0x0000000000000000000000000000000000000aa1",
		Exchange: "uniswap-v3", Protocol: "v3", Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}
	good := Pool{Address: "0x0000000000000000000000000000000000000aa2",
		Exchange: "uniswap-v3", Protocol: "v3", Token0: "0xaaa", Token1: "0xccc", Fee: 3000, TickSpacing: 60}
	if _, err := db.pool.Exec(ctx, `
		INSERT INTO dex_pools (address, exchange, protocol, token0, token1, fee, tick_spacing,
			canonical, created_block, created_block_hash)
		VALUES ($1,$2,$3,$4,$5,$6,$7,FALSE,10,$8), ($9,$10,$11,$12,$13,$14,$15,TRUE,10,$16)`,
		orphan.Address, orphan.Exchange, orphan.Protocol, orphan.Token0, orphan.Token1, orphan.Fee, orphan.TickSpacing, orphanHash,
		good.Address, good.Exchange, good.Protocol, good.Token0, good.Token1, good.Fee, good.TickSpacing, goodHash); err != nil {
		t.Fatalf("seed: %v", err)
	}
	pools, err := db.LoadPools(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(pools) != 1 || pools[0].Address != good.Address {
		t.Fatalf("LoadPools returned %d pools, want only the canonical one", len(pools))
	}
}

// P0-1: 升级崩溃恢复——ingest 提交后 evaluate 游标落后，重启后队列仍被读取
func TestEvaluationQueueSurvivesCrash(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;
		DELETE FROM block_affected_pools;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	// 模拟：ingest 100~102 已提交，evaluate 只到 100（崩溃点）
	if err := db.InitializeBlockCheckpoint(ctx, CheckpointBlocks, 99, "0x999", "0x888"); err != nil {
		t.Fatalf("init: %v", err)
	}
	for i := uint64(100); i <= 102; i++ {
		h := common.BigToHash(new(big.Int).SetUint64(i)).Hex()
		if err := db.CommitBlockIngest(ctx, i, h, common.BigToHash(new(big.Int).SetUint64(i-1)).Hex(),
			nil, []string{"0xaa01"}); err != nil {
			t.Fatalf("ingest %d: %v", i, err)
		}
	}
	// evaluate 游标落后（100），重启后队列必须包含 101、102
	pending, err := db.LoadPendingAffected(ctx, 100)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(pending) != 2 || pending[0].Block != 101 || pending[1].Block != 102 {
		t.Fatalf("pending=%+v want blocks 101,102", pending)
	}
	// 评估 101 后，剩余 102
	if err := db.CommitEvaluation(ctx, 101, common.BigToHash(big.NewInt(101)).Hex(), nil); err != nil {
		t.Fatalf("eval 101: %v", err)
	}
	pending, err = db.LoadPendingAffected(ctx, 101)
	if err != nil {
		t.Fatalf("load2: %v", err)
	}
	if len(pending) != 1 || pending[0].Block != 102 {
		t.Fatalf("pending after 101=%+v", pending)
	}
}

// P0-2: reorg 时 evaluate 只退不进（evaluate=100 < ancestor=105 保持 100）
func TestReorgDoesNotAdvanceLaggingEvaluate(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `
		DELETE FROM opportunities;
		DELETE FROM processed_blocks;
		DELETE FROM strategy_checkpoints;
		DELETE FROM block_affected_pools;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	if err := db.InitializeBlockCheckpoint(ctx, CheckpointBlocks, 110, "0xaaa", "0x999"); err != nil {
		t.Fatalf("init: %v", err)
	}
	// 把 evaluate 手动拉到 100（落后）
	if err := db.CommitEvaluation(ctx, 100, "0x100", nil); err != nil {
		t.Fatalf("eval: %v", err)
	}
	if err := db.RollbackToAncestor(ctx, CheckpointBlocks, 105, "0x105", "0x104"); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	n, h, err := NewPGCheckpoint(db).LoadWithHash(ctx, CheckpointEvaluate)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if n != 100 || h != common.HexToHash("0x100") {
		t.Fatalf("evaluate after reorg = %d %s, want 100 (no advance)", n, h.Hex())
	}
	// evaluate 超前时（120 > ancestor 105）才回退到 105
	if err := db.CommitEvaluation(ctx, 120, "0x120", nil); err != nil {
		t.Fatalf("eval2: %v", err)
	}
	if err := db.RollbackToAncestor(ctx, CheckpointBlocks, 105, "0x105", "0x104"); err != nil {
		t.Fatalf("rollback2: %v", err)
	}
	n, _, err = NewPGCheckpoint(db).LoadWithHash(ctx, CheckpointEvaluate)
	if err != nil {
		t.Fatalf("load2: %v", err)
	}
	if n != 105 {
		t.Fatalf("evaluate after reorg2 = %d, want 105", n)
	}
}

// P0-4: CommitPools 保存真实创建高度与 hash（而非 bootstrap 结束块/NULL）
func TestCommitPoolsExactProvenance(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `DELETE FROM dex_pools; DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	p := Pool{Address: "0x0000000000000000000000000000000000000bb1",
		Exchange: "uniswap-v3", Protocol: "v3", Token0: "0xaaa", Token1: "0xbbb",
		Fee: 3000, TickSpacing: 60, CreatedBlock: 42, CreatedBlockHash: "0x2a",
		ProvenanceSource: "pool_created_log"}
	if err := db.CommitPools(ctx, []Pool{p}, 9999); err != nil {
		t.Fatalf("commit: %v", err)
	}
	var cb uint64
	var ch *string
	if err := db.pool.QueryRow(ctx,
		`SELECT created_block, created_block_hash FROM dex_pools WHERE address=$1`, p.Address).
		Scan(&cb, &ch); err != nil {
		t.Fatal(err)
	}
	if cb != 42 || ch == nil || *ch != "0x2a" {
		t.Fatalf("created = %d %v, want 42 0x2a", cb, ch)
	}
	// 真实信息覆盖零 hash 占位
	if err := db.CommitPools(ctx, []Pool{p}, 9999); err != nil {
		t.Fatalf("recommit: %v", err)
	}
	if err := db.pool.QueryRow(ctx,
		`SELECT created_block, created_block_hash FROM dex_pools WHERE address=$1`, p.Address).
		Scan(&cb, &ch); err != nil {
		t.Fatal(err)
	}
	if cb != 42 || ch == nil || *ch != "0x2a" {
		t.Fatalf("created after recommit = %d %v, want 42 0x2a", cb, ch)
	}
}

// P0-4: 观察块兜底（observed_swap_fallback）必须被 bootstrap 的真实
// pool_created_log 信息覆盖（覆盖规则按 provenance_source，不靠猜零值）。
func TestCommitPoolsOverridesFallbackProvenance(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `DELETE FROM dex_pools; DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	addr := "0x0000000000000000000000000000000000000cc1"
	// 模拟动态发现：CommitBlockIngest 写观察块兜底（observed_swap_fallback）
	if err := db.CommitBlockIngest(ctx, 110, "0x6e", "0x6d",
		[]Pool{{Address: addr, Exchange: "uniswap-v3", Protocol: "v3",
			Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}}, nil); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var cb uint64
	var ch, prov *string
	q := func() {
		if err := db.pool.QueryRow(ctx,
			`SELECT created_block, created_block_hash, provenance_source FROM dex_pools WHERE address=$1`, addr).
			Scan(&cb, &ch, &prov); err != nil {
			t.Fatal(err)
		}
	}
	q()
	if cb != 110 || ch == nil || prov == nil || *prov != "observed_swap_fallback" {
		t.Fatalf("fallback = %d %v %v, want 110 observed_swap_fallback", cb, ch, prov)
	}
	// bootstrap 真实信息（pool_created_log）必须覆盖
	if err := db.CommitPools(ctx, []Pool{{Address: addr, Exchange: "uniswap-v3", Protocol: "v3",
		Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60,
		CreatedBlock: 42, CreatedBlockHash: "0x2a", ProvenanceSource: "pool_created_log"}}, 9999); err != nil {
		t.Fatalf("commitpools: %v", err)
	}
	q()
	if cb != 42 || ch == nil || *ch != "0x2a" || prov == nil || *prov != "pool_created_log" {
		t.Fatalf("after bootstrap = %d %q %q, want 42 0x2a pool_created_log", cb, deref(ch), deref(prov))
	}
	// 已确认的 pool_created_log 不允许被新观察块覆盖（提交不同观察块）
	if err := db.CommitBlockIngest(ctx, 200, "0xc8", "0xc7",
		[]Pool{{Address: addr, Exchange: "uniswap-v3", Protocol: "v3",
			Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}}, nil); err != nil {
		t.Fatalf("ingest2: %v", err)
	}
	q()
	if cb != 42 || ch == nil || *ch != "0x2a" {
		t.Fatalf("provenance overwritten: %d %v", cb, ch)
	}
}

// P0-1: CommitBlockIngest 的权威溯源升级必须原子——block/hash/source
// 一起替换（观察块不能被"认证"成权威来源）。
func TestCommitBlockIngestAtomicProvenanceUpgrade(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	if _, err := db.pool.Exec(ctx, `DELETE FROM dex_pools; DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	addr := "0x0000000000000000000000000000000000000dd1"
	base := Pool{Address: addr, Exchange: "uniswap-v3", Protocol: "v3",
		Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}
	// 1) 观察块兜底：110 / hash110 / observed_swap_fallback
	if err := db.CommitBlockIngest(ctx, 110, "0x6e", "0x6d", []Pool{base}, nil); err != nil {
		t.Fatalf("ingest fallback: %v", err)
	}
	// 2) 真实日志：42 / 0x2a / pool_created_log → 三字段必须一起变
	authoritative := base
	authoritative.CreatedBlock = 42
	authoritative.CreatedBlockHash = "0x2a"
	authoritative.ProvenanceSource = "pool_created_log"
	if err := db.CommitBlockIngest(ctx, 130, "0x82", "0x81", []Pool{authoritative}, nil); err != nil {
		t.Fatalf("ingest authoritative: %v", err)
	}
	var cb uint64
	var ch, prov *string
	if err := db.pool.QueryRow(ctx,
		`SELECT created_block, created_block_hash, provenance_source FROM dex_pools WHERE address=$1`, addr).
		Scan(&cb, &ch, &prov); err != nil {
		t.Fatal(err)
	}
	if cb != 42 || ch == nil || *ch != "0x2a" || prov == nil || *prov != "pool_created_log" {
		t.Fatalf("after upgrade = %d %q %q, want 42 0x2a pool_created_log", cb, deref(ch), deref(prov))
	}
	// 3) 后续观察块（observed_swap_fallback）不能降级权威
	if err := db.CommitBlockIngest(ctx, 150, "0x96", "0x95", []Pool{base}, nil); err != nil {
		t.Fatalf("ingest fallback2: %v", err)
	}
	if err := db.pool.QueryRow(ctx,
		`SELECT created_block, created_block_hash, provenance_source FROM dex_pools WHERE address=$1`, addr).
		Scan(&cb, &ch, &prov); err != nil {
		t.Fatal(err)
	}
	if cb != 42 || ch == nil || *ch != "0x2a" || prov == nil || *prov != "pool_created_log" {
		t.Fatalf("after fallback2 = %d %q %q, want unchanged 42 0x2a pool_created_log", cb, deref(ch), deref(prov))
	}
}

// P0-2: 0011 语义——空 gas 模式落库为 not_estimated（不是 historical）
func TestGasModeDefaultNotEstimated(t *testing.T) {
	db := testDB(t)
	ctx := context.Background()
	c := testCandidate("gas-mode-1", 1, "0x1")
	c.GasEstimateMode = "" // 未进入模拟的拒绝候选
	if err := db.SaveCandidate(ctx, c); err != nil {
		t.Fatalf("save: %v", err)
	}
	var mode string
	if err := db.pool.QueryRow(ctx,
		`SELECT gas_estimate_mode FROM opportunities WHERE id=$1`, c.ID).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "not_estimated" {
		t.Fatalf("gas_estimate_mode=%q want not_estimated", mode)
	}
}
