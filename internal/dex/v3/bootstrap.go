package v3

import (
	"context"
	"log/slog"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/dex"
)

// BootstrapOptions 池引导配置。
type BootstrapOptions struct {
	BatchSize  uint64
	MaxBatches int // 0 = 不限
	// WETHOnly 非零时：只把包含该 token 的池加入 Registry/Graph
	// （MVP 两跳路线 WETH→TOKEN→WETH 的池必然含 WETH；39 万池里 99% 无用）
	WETHOnly common.Address
}

// Bootstrap 从 fromBlock 一直扫描到链头（head），空批次不停。
// 返回最后处理到的区块高度（供 checkpoint 使用）。
// 注意：head 必须先于调用读取（防止查询未来区块并写入 checkpoint）。
func Bootstrap(ctx context.Context, a *Adapter, reg *dex.Registry, graph *dex.Graph, fromBlock, head uint64, opt BootstrapOptions) (uint64, error) {
	if opt.BatchSize == 0 {
		opt.BatchSize = 100_000
	}
	last := fromBlock
	batchSize := opt.BatchSize
	fails := 0
	for batch := 0; fromBlock <= head; batch++ {
		if opt.MaxBatches > 0 && batch >= opt.MaxBatches {
			break
		}
		to := fromBlock + batchSize - 1
		if to > head {
			to = head
		}
		pools, err := a.DiscoverPools(ctx, fromBlock, to)
		if err != nil {
			// 自适应：查询失败（超时/429）缩小批次重试，最小 1000 块
			fails++
			if batchSize > 1_000 && fails <= 3 {
				batchSize /= 2
				slog.Warn("discover batch shrank", "from", fromBlock, "new_size", batchSize, "err", err)
				continue
			}
			slog.Error("bootstrap failed", "from", fromBlock, "err", err)
			return last, err
		}
		fails = 0
		if batchSize < opt.BatchSize {
			batchSize = opt.BatchSize // 成功后恢复
		}
		for _, p := range pools {
			if opt.WETHOnly != (common.Address{}) &&
				p.Token0 != opt.WETHOnly && p.Token1 != opt.WETHOnly {
				continue // 非 WETH 池：当前策略用不到，不进入运行集
			}
			reg.UpsertPool(State(p))
			graph.AddPool(p.Pool(), p.Address)
		}
		if len(pools) > 0 {
			slog.Info("discovered", "batch_from", fromBlock, "pools", len(pools))
		}
		last = to
		fromBlock = to + 1
	}
	slog.Info("bootstrap done", "total_pools", len(reg.AllPools()), "last_block", last)
	return last, nil
}
