// rh-indexer: 链数据索引器。订阅区块与日志，维护 V3 池状态，写入 checkpoint。
// Robinhood 公共端点无 WS（wss://.../ws = 404），默认走轮询；配置 WS 端点时优先 WS。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

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

	cfg, err := config.LoadMerged(*cfgPath, "configs/dexes.yaml", "configs/morpho.yaml")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	if len(cfg.RPC.Groups.Read) == 0 {
		slog.Error("no read RPC configured")
		os.Exit(1)
	}

	readURL := cfg.RPC.Groups.Read[0]
	var src chain.Source
	ws, wsErr := chain.NewWSClient(ctx, readURL)
	if wsErr != nil {
		slog.Warn("WS unavailable, falling back to polling", "url", readURL, "err", wsErr)
		httpCli, err := ethclient.Dial(cfg.RPC.Groups.Archive[0])
		if err != nil {
			slog.Error("dial http rpc", "err", err)
			os.Exit(1)
		}
		src = chain.NewPollingSource(httpCli)
	} else {
		src = ws
	}

	// 池 registry + V3 adapter
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	var adapter *v3.Adapter
	if len(cfg.Dexes.V3) > 0 {
		d := cfg.Dexes.V3[0]
		var cli *ethclient.Client
		if ws != nil {
			cli = ws.Client()
		} else {
			cli, _ = ethclient.Dial(cfg.RPC.Groups.Archive[0])
		}
		adapter, err = v3.NewAdapter(
			cli, d.Name,
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
	}

	// checkpoint 恢复：从上次高度继续发现池
	ckpt := storage.NewCheckpoint("deployments/checkpoint.json")
	heights, _ := ckpt.Load()
	startBlock := heights["pools"]
	if startBlock < cfg.Dexes.V3[0].FactoryBlock {
		startBlock = cfg.Dexes.V3[0].FactoryBlock
	}
	slog.Info("discovering pools", "from", startBlock)

	head, err := src.BlockByNumber(ctx, 0) // 最新头
	if err != nil {
		slog.Warn("head query failed", "err", err)
	}
	_ = head

	// 分批发现池（每批 100k 块，公共 RPC 限速友好），断点续跑
	if adapter != nil {
		lastPoolBlock, err := v3.Bootstrap(ctx, adapter, reg, graph, startBlock, v3.BootstrapOptions{})
		if err != nil {
			slog.Warn("bootstrap incomplete", "err", err)
		} else {
			_ = ckpt.Save("pools", lastPoolBlock)
		}
	}
	slog.Info("total pools", "count", len(reg.AllPools()))

	// 区块订阅 + 连续性检测
	heads, errs := src.SubscribeBlocks(ctx)
	gaps := chain.NewGapDetector()
	_ = gaps
	metrics := telemetry.NewMetrics()
	go func() {
		for err := range errs {
			slog.Warn("block subscription error", "err", err)
			metrics.WebsocketReconnects.Inc()
		}
	}()

	slog.Info("indexer running", "mode", "polling")
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
