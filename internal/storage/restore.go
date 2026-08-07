package storage

import (
	"context"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
)

// RestorePools 启动时从 PostgreSQL 恢复池元数据并重建 Registry/Graph。
// indexer 与 arbitrage 共用，避免两套启动逻辑漂移。
// wethOnly 非零时只恢复包含该 token 的池（MVP 两跳路线 WETH→TOKEN→WETH
// 的池必然含 WETH；历史库可能残留非 WETH 池）。
func RestorePools(ctx context.Context, db *DB, reg *dex.Registry, graph *dex.Graph, wethOnly common.Address) (int, error) {
	saved, err := db.LoadPools(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, sp := range saved {
		t0 := common.HexToAddress(sp.Token0)
		t1 := common.HexToAddress(sp.Token1)
		if wethOnly != (common.Address{}) && t0 != wethOnly && t1 != wethOnly {
			continue
		}
		p := v3.NewPoolFromMetaWithCreated(
			common.HexToAddress(sp.Address), sp.Exchange,
			t0, t1, sp.Fee, sp.TickSpacing,
			sp.CreatedBlock, common.HexToHash(sp.CreatedBlockHash), sp.ProvenanceSource)
		reg.UpsertPool(v3.State(p))
		graph.AddPool(p.Pool(), p.Address)
		n++
	}
	slog.Info("restored pools from db", "count", n, "skipped_non_weth", len(saved)-n)
	return n, nil
}
