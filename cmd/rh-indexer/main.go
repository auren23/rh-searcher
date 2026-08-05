// rh-indexer: 链数据索引器。订阅区块与日志，维护 V3 池状态，写入 checkpoint。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/chain"
	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
	"github.com/auren23/rh-searcher/internal/storage"
	"github.com/auren23/rh-searcher/internal/telemetry"
)

func main() {
	cfgPath := flag.String("config", "configs/robinhood.yaml", "config file")
	flag.Parse()

	telemetry.SetupLogging(slog.LevelInfo)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	if len(cfg.RPC.Groups.Read) == 0 {
		slog.Error("no read RPC configured")
		os.Exit(1)
	}

	ws, err := chain.NewWSClient(ctx, cfg.RPC.Groups.Read[0])
	if err != nil {
		slog.Error("dial ws", "err", err)
		os.Exit(1)
	}

	// 池 registry
	reg := dex.NewRegistry()
	graph := dex.NewGraph()

	// V3 adapter（MVP：配置里第一个 V3 DEX）
	if len(cfg.Dexes.V3) > 0 {
		d := cfg.Dexes.V3[0]
		adapter, err := v3.NewAdapter(
			ws.Client(), d.Name,
			common.HexToAddress(d.Factory),
			common.HexToAddress(d.Router),
			d.RouterKind,
			common.HexToHash(d.InitCodeHash),
			d.FactoryBlock,
		)
		if err != nil {
			slog.Error("v3 adapter", "err", err)
			os.Exit(1)
		}
		pools, err := adapter.DiscoverPools(ctx, d.FactoryBlock, 0)
		if err != nil {
			slog.Warn("discover pools (continuing)", "err", err)
		}
		for _, p := range pools {
			reg.UpsertPool(v3.State(p))
			graph.AddPool(p.Pool(), p.Address)
		}
		slog.Info("discovered pools", "count", len(pools))
	}

	// checkpoint
	ckpt := storage.NewCheckpoint("deployments/checkpoint.json")

	// 区块订阅 + 连续性检测
	heads, errs := ws.SubscribeBlocks(ctx)
	gaps := chain.NewGapDetector()
	go func() {
		for err := range errs {
			slog.Warn("block subscription error", "err", err)
			telemetry.NewMetrics().WebsocketReconnects.Inc()
		}
	}()
	_ = gaps

	for {
		select {
		case <-ctx.Done():
			slog.Info("indexer stopped")
			return
		case h := <-heads:
			if err := ckpt.Save("indexer", h.Number); err != nil {
				slog.Warn("checkpoint save", "err", err)
			}
			slog.Debug("head", "block", h.Number, "hash", h.Hash.Hex())
		}
	}
}
