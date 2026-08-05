// rh-arbitrage: 套利引擎。默认 shadow 模式：发现、模拟、落盘，不发送交易。
// Robinhood 公共端点无 WS，默认轮询；配置 WS 端点时优先 WS。
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
	"github.com/ethereum/go-ethereum/ethclient"

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

	cfg, err := config.LoadMerged(*cfgPath, "configs/dexes.yaml", "configs/morpho.yaml")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	if len(cfg.Dexes.V3) == 0 {
		slog.Error("no v3 dex configured")
		os.Exit(1)
	}

	readURL := cfg.RPC.Groups.Read[0]
	var src chain.Source
	var cli *ethclient.Client
	ws, wsErr := chain.NewWSClient(ctx, readURL)
	if wsErr != nil {
		slog.Warn("WS unavailable, falling back to polling", "url", readURL, "err", wsErr)
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

	d := cfg.Dexes.V3[0]
	adapter, err := v3.NewAdapter(cli, d.Name,
		common.HexToAddress(d.Factory), common.HexToAddress(d.Router), d.RouterKind,
		common.HexToHash(d.InitCodeHash), d.FactoryBlock)
	if err != nil {
		slog.Error("v3 adapter", "err", err)
		os.Exit(1)
	}

	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	ckpt := storage.NewCheckpoint("deployments/checkpoint.json")
	heights, _ := ckpt.Load()
	fromBlock := heights["pools"]
	if fromBlock < d.FactoryBlock {
		fromBlock = d.FactoryBlock
	}
	// 池引导（已完成的从 checkpoint 续跑；未完成的补全）
	lastPoolBlock, _ := v3.Bootstrap(ctx, adapter, reg, graph, fromBlock, v3.BootstrapOptions{})
	_ = ckpt.Save("pools", lastPoolBlock)

	weth := common.HexToAddress(cfg.Chain.WETH)
	engine := arbitrage.NewEngine(
		arbitrage.Config{
			WETH:            weth,
			MinProfitWei:    big.NewInt(1e15), // shadow 阶段仅记录，0.001 ETH 门槛
			SafetyMarginWei: big.NewInt(2e14),
			MaxHops:         2,
			Mode:            "shadow",
		},
		nil,
		arbitrage.NewLocalSearcher(graph, reg, adapter, weth),
		arbitrage.NewLocalEvaluator(),
		arbitrage.NewExecutor(),
	)

	var sink *storage.DB
	if cfg.Storage.PostgresURL != "" {
		sink, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Warn("postgres unavailable, candidates will not persist", "err", err)
		} else {
			defer sink.Close()
		}
	}

	// 订阅 Swap 事件（轮询源内部处理重连/补块）
	swapTopic := v3.SwapTopic()
	logs, logErrs := src.SubscribeLogs(ctx, ethereum.FilterQuery{Topics: [][]common.Hash{{swapTopic}}})
	go func() {
		for err := range logErrs {
			slog.Warn("log subscription error", "err", err)
		}
	}()

	metrics := telemetry.NewMetrics()
	slog.Info("arbitrage engine started (shadow)", "mode", "shadow", "pools", len(reg.AllPools()))
	for {
		select {
		case <-ctx.Done():
			slog.Info("arbitrage stopped")
			return
		case l := <-logs:
			state := reg.Pool(l.Address)
			if state == nil {
				// 未发现的池（如刚创建）：从链上惰性初始化
				if l.BlockNumber > 0 {
					pool, err := adapter.PoolByAddress(ctx, l.Address)
					if err != nil {
						continue
					}
					reg.UpsertPool(v3.State(pool))
					graph.AddPool(pool.Pool(), pool.Address)
					state = reg.Pool(l.Address)
				}
				if state == nil {
					continue
				}
			}
			if _, err := adapter.ApplyLog(state.(*v3.Pool), l); err != nil {
				slog.Warn("apply log", "err", err)
				continue
			}
			reg.UpsertPool(state)
			engine.OnSwap(ctx, l.Address, l.BlockNumber, int64(l.BlockNumber))
			metrics.CandidatesTotal.WithLabelValues("weth-2hop", "observed").Inc()
		}
	}
}
