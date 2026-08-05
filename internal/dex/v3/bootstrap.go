package v3

import (
	"context"
	"log/slog"

	"github.com/auren23/rh-searcher/internal/dex"
)

// BootstrapOptions 池引导配置。
type BootstrapOptions struct {
	FactoryBlock uint64
	BatchSize    uint64
	MaxBatches   int // 0 = 不限
}

// Bootstrap 从 factory 开始发现池并填充 registry/graph。
// 返回最后处理到的区块高度（供 checkpoint 使用）。
func Bootstrap(ctx context.Context, a *Adapter, reg *dex.Registry, graph *dex.Graph, fromBlock uint64, opt BootstrapOptions) (uint64, error) {
	if opt.BatchSize == 0 {
		opt.BatchSize = 100_000
	}
	last := fromBlock
	for batch := 0; ; batch++ {
		if opt.MaxBatches > 0 && batch >= opt.MaxBatches {
			break
		}
		to := fromBlock + opt.BatchSize - 1
		pools, err := a.DiscoverPools(ctx, fromBlock, to)
		if err != nil {
			slog.Warn("discover batch", "from", fromBlock, "err", err)
			return last, err
		}
		for _, p := range pools {
			reg.UpsertPool(State(p))
			graph.AddPool(p.Pool(), p.Address)
		}
		if len(pools) > 0 {
			slog.Info("discovered", "batch_from", fromBlock, "pools", len(pools))
		}
		last = to
		if len(pools) == 0 {
			break
		}
		fromBlock = to + 1
	}
	slog.Info("bootstrap done", "total_pools", len(reg.AllPools()), "last_block", last)
	return last, nil
}
