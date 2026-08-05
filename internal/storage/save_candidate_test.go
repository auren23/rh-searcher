package storage

import (
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/arbitrage"
)

// 真实 PostgreSQL 集成测试：由 CI 的 postgres service 提供 RH_TEST_PG_URL。
// 覆盖 P0：模拟字段为 nil 的 local_rejected 候选必须能落盘（nullableBigInt 不产生 "<nil>"）。
func TestSaveLocalRejectedCandidate(t *testing.T) {
	url := os.Getenv("RH_TEST_PG_URL")
	if url == "" {
		t.Skip("RH_TEST_PG_URL not set (CI postgres service)")
	}
	ctx := context.Background()
	db, err := New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer db.Close()

	// 模拟相关字段保持 nil（emptyCandidate 场景）
	c := &arbitrage.Candidate{
		ID:            "test-local-rejected-1",
		ObservedBlock: 1,
		ObservedAt:    1,
		SourceEvent:   "block_swap_batch",
		RouteJSON:     `[{"Pool":"0x0000000000000000000000000000000000000001"}]`,
		InputAsset:    common.Address{},
		InputAmount:   big.NewInt(0),
		GrossProfit:   big.NewInt(0),
		GasEstimate:   big.NewInt(0),
		SwapCost:      big.NewInt(0),
		SlippageCost:  big.NewInt(0),
		Decision:      "local_rejected",
		RejectReason:  "no positive local profit",
	}
	if err := db.SaveCandidate(ctx, c); err != nil {
		t.Fatalf("SaveCandidate(local_rejected with nil sim fields): %v", err)
	}
}
