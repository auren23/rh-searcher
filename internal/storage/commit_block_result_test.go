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
	err := db.CommitBlockResult(ctx, 100, "0xaaa", "0x999", nil,
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
		DELETE FROM strategy_checkpoints;`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	var prev string
	for i := uint64(1); i <= 5; i++ {
		h := common.BigToHash(new(big.Int).SetUint64(i)).Hex()
		parent := prev
		prev = h
		// 每个区块携带一个新池（孤块创建的池应被标记 canonical=false）
		poolAddr := common.BigToHash(new(big.Int).SetUint64(1000 + i)).Hex()[24:]
		pools := []Pool{{Address: "0x" + poolAddr, Exchange: "uniswap-v3",
			Protocol: "v3", Token0: "0xaaa", Token1: "0xbbb", Fee: 3000, TickSpacing: 60}}
		if err := db.CommitBlockResult(ctx, i, h, parent, pools,
			[]*arbitrage.Candidate{testCandidate(
				"reorg-cand-"+h, i, h)}); err != nil {
			t.Fatalf("commit %d: %v", i, err)
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
	if err := db.CommitBlockResult(ctx, 200, "0xbbb", "0x999", nil,
		[]*arbitrage.Candidate{testCandidate("rollback-dupe", 200, "0xbbb")}); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// 同 group 两个不同 id 的新行都 selected=true → 唯一索引必须拒绝整组（事务回滚）
	dup := testCandidate("rollback-dup-id-2", 200, "0xbbb")
	dup.OpportunityGroupID = "g1"
	dup.Selected = true
	good := testCandidate("rollback-dup-id-1", 200, "0xbbb")
	good.OpportunityGroupID = "g1"
	good.Selected = true
	err := db.CommitBlockResult(ctx, 200, "0xbbb", "0x999", nil,
		[]*arbitrage.Candidate{good, dup})
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
