// rh-arbitrage: 套利引擎。默认 shadow 模式：发现、模拟、落盘，不发送交易。
package main

import (
	"context"
	"flag"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/arbitrage"
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
	ws, err := chain.NewWSClient(ctx, cfg.RPC.Groups.Read[0])
	if err != nil {
		slog.Error("dial ws", "err", err)
		os.Exit(1)
	}

	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	adapter, err := v3.NewAdapter(ws.Client(), "robinhood-swap",
		common.HexToAddress(cfg.Dexes.V3[0].Factory),
		common.HexToAddress(cfg.Dexes.V3[0].Router),
		common.HexToHash(cfg.Dexes.V3[0].InitCodeHash),
		cfg.Dexes.V3[0].FactoryBlock)
	if err != nil {
		slog.Error("v3 adapter", "err", err)
		os.Exit(1)
	}

	weth := common.HexToAddress(cfg.Chain.WETH)
	engine := arbitrage.NewEngine(
		arbitrage.Config{
			WETH:            weth,
			MinProfitWei:    big.NewInt(1e15), // 0.001 ETH 门槛，M5 前只作 shadow
			SafetyMarginWei: big.NewInt(2e14),
			MaxHops:         2,
			Mode:            "shadow",
		},
		nil, // sink 在 storage 可用时注入
		arbitrage.NewLocalSearcher(graph, reg, adapter, weth),
		arbitrage.NewLocalEvaluator(),
		arbitrage.NewExecutor(),
	)

	// 可选的 storage sink
	var sink *storage.DB
	if cfg.Storage.PostgresURL != "" {
		sink, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Warn("postgres unavailable, candidates will not persist", "err", err)
		} else {
			defer sink.Close()
		}
	}

	// 订阅 Swap 事件
	swapTopic := v3.SwapTopic()
	logs, logErrs := ws.SubscribeLogs(ctx, ethereum.FilterQuery{Topics: [][]common.Hash{{swapTopic}}})
	go func() {
		for err := range logErrs {
			slog.Warn("log subscription error", "err", err)
		}
	}()

	slog.Info("arbitrage engine started (shadow)", "mode", "shadow")
	for {
		select {
		case <-ctx.Done():
			slog.Info("arbitrage stopped")
			return
		case l := <-logs:
			poolAddr := l.Address
			state := reg.Pool(poolAddr)
			if state == nil {
				continue
			}
			if _, err := adapter.ApplyLog(state.(*v3.Pool), l); err != nil {
				slog.Warn("apply log", "err", err)
				continue
			}
			reg.UpsertPool(state)
			engine.OnSwap(ctx, poolAddr, l.BlockNumber, int64(l.BlockNumber))
		}
	}
}
