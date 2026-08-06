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
func RestorePools(ctx context.Context, db *DB, reg *dex.Registry, graph *dex.Graph) (int, error) {
	saved, err := db.LoadPools(ctx)
	if err != nil {
		return 0, err
	}
	for _, sp := range saved {
		p := v3.NewPoolFromMetaWithCreated(
			common.HexToAddress(sp.Address), sp.Exchange,
			common.HexToAddress(sp.Token0), common.HexToAddress(sp.Token1), sp.Fee, sp.TickSpacing,
			sp.CreatedBlock, common.HexToHash(sp.CreatedBlockHash))
		reg.UpsertPool(v3.State(p))
		graph.AddPool(p.Pool(), p.Address)
	}
	slog.Info("restored pools from db", "count", len(saved))
	return len(saved), nil
}
