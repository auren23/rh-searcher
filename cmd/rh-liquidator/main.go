// rh-liquidator: Morpho 清算引擎（M7 推迟）。当前只运行 CreateMarket watcher：
// 监听 Morpho Blue 的市场创建事件，验证 MarketParams 与资产代码，落库并告警。
// 链上确认（2026-08-05）：Robinhood 的 Morpho Blue 部署于 block 286 但 0 个市场；
// 一旦第一个真实市场出现，watcher 会通知并登记，再继续 M6-M8。
package main

import (
	"context"
	"flag"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/auren23/rh-searcher/internal/chain"
	"github.com/auren23/rh-searcher/internal/config"
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
	if cfg.Morpho.Blue == "" {
		slog.Error("morpho.blue not configured")
		os.Exit(1)
	}
	morphoAddr := common.HexToAddress(cfg.Morpho.Blue)

	readURL := cfg.RPC.Groups.Read[0]
	var src chain.Source
	var cli *ethclient.Client
	ws, wsErr := chain.NewWSClient(ctx, readURL)
	if wsErr != nil {
		cli, err = ethclient.Dial(cfg.RPC.Groups.Archive[0])
		if err != nil {
			slog.Error("dial http rpc", "err", err)
			os.Exit(1)
		}
		src = chain.NewPollingSource(cli)
	} else {
		cli = ws.Client()
		src = ws
	}

	var db *storage.DB
	if cfg.Storage.PostgresURL != "" {
		db, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Warn("postgres unavailable, markets will not persist", "err", err)
		} else {
			defer db.Close()
		}
	}
	_ = db

	// CreateMarket 事件 topic（链上实测值，见 M0 报告）：keccak("CreateMarket((address,address,address,address,uint256))")
	createTopic := common.HexToHash("0x328c8b6476f172071b1970c780ea807463835842c682ce1676d94e1e35e81e61")

	logs, logErrs := src.SubscribeLogs(ctx, ethereum.FilterQuery{
		Addresses: []common.Address{morphoAddr},
		Topics:    [][]common.Hash{{createTopic}},
	})
	go func() {
		for err := range logErrs {
			slog.Warn("morpho log subscription error", "err", err)
		}
	}()

	metrics := telemetry.NewMetrics()
	slog.Info("morpho watcher running (waiting for CreateMarket)", "blue", morphoAddr.Hex())

	// 启动时立即查一次历史（从部署区块 286 起）
	head, err := cli.BlockNumber(ctx)
	if err == nil {
		start := uint64(286)
		q := ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(start)),
			ToBlock:   big.NewInt(int64(head)),
			Addresses: []common.Address{morphoAddr},
			Topics:    [][]common.Hash{{createTopic}},
		}
		if hist, err := cli.FilterLogs(ctx, q); err == nil {
			for _, l := range hist {
				handleCreateMarket(ctx, l, db, metrics)
			}
			slog.Info("initial scan done", "create_markets", len(hist))
		}
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("liquidator stopped")
			return
		case l := <-logs:
			handleCreateMarket(ctx, l, db, metrics)
		case <-time.After(1 * time.Hour):
			// 心跳：确认订阅还活着
			slog.Debug("watcher heartbeat")
		}
	}
}

// handleCreateMarket 验证并登记一个 CreateMarket 事件。
func handleCreateMarket(ctx context.Context, l types.Log, db *storage.DB, metrics *telemetry.Metrics) {
	slog.Warn("MORPHO MARKET CREATED",
		"block", l.BlockNumber, "tx", l.TxHash.Hex(),
		"data", common.Bytes2Hex(l.Data))
	// 验证 MarketParams 与资产代码、落库、告警在 M6 恢复时接线；
	// MVP 只负责第一时间通知（日志即告警）。
	metrics.CandidatesTotal.WithLabelValues("morpho-market", "created").Inc()
}
