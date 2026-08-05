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

	"github.com/ethereum/go-ethereum"
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

	// 池 registry + V3 adapter
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	var adapter *v3.Adapter
	if len(cfg.Dexes.V3) > 0 {
		d := cfg.Dexes.V3[0]
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

	ckpt := storage.NewCheckpoint("deployments/checkpoint.json")
	heights, _ := ckpt.Load()

	// 可选 PostgreSQL：池元数据落库/恢复
	var db *storage.DB
	if cfg.Storage.PostgresURL != "" {
		db, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Warn("postgres unavailable, pool persistence disabled", "err", err)
		} else {
			defer db.Close()
			saved, err := db.LoadPools(ctx)
			if err != nil {
				slog.Error("load pools from db", "err", err)
				os.Exit(1) // 不允许忽略恢复错误继续运行（漏池）
			}
			for _, sp := range saved {
				p := v3.NewPoolFromMeta(
					common.HexToAddress(sp.Address), sp.Exchange,
					common.HexToAddress(sp.Token0), common.HexToAddress(sp.Token1), sp.Fee)
				reg.UpsertPool(v3.State(p))
				graph.AddPool(p.Pool(), p.Address)
			}
			slog.Info("restored pools from db", "count", len(saved))
		}
	}

	// 启动时先读链头：Bootstrap 只扫到当前链头，不查询未来区块
	headNum, err := cli.BlockNumber(ctx)
	if err != nil {
		slog.Error("read chain head", "err", err)
		os.Exit(1)
	}
	slog.Info("chain head", "block", headNum)

	// 池引导（断点续扫到链头；bootstrap 错误不忽略）
	if adapter != nil {
		startBlock := heights["pools"]
		if startBlock < cfg.Dexes.V3[0].FactoryBlock {
			startBlock = cfg.Dexes.V3[0].FactoryBlock
		}
		slog.Info("discovering pools", "from", startBlock, "to", headNum)
		lastPoolBlock, err := v3.Bootstrap(ctx, adapter, reg, graph, startBlock, headNum, v3.BootstrapOptions{})
		if err != nil {
			slog.Error("bootstrap failed (pools may be missing)", "err", err)
			os.Exit(1)
		}
		_ = ckpt.Save("pools", lastPoolBlock)
	}
	slog.Info("total pools", "count", len(reg.AllPools()))

	// 落库新发现池（若 db 可用）
	if db != nil {
		for _, st := range reg.AllPools() {
			p := st.Pool()
			if err := db.SavePool(ctx, p.ID, p.Exchange, p.Protocol, p.Token0, p.Token1, p.Fee); err != nil {
				slog.Warn("save pool", "err", err)
			}
		}
	}

	// 订阅新池创建（PoolCreated）+ 区块
	poolCreatedTopic := v3.PoolCreatedTopic()
	createdLogs, createdErrs := src.SubscribeLogs(ctx, ethereum.FilterQuery{
		Addresses: []common.Address{common.HexToAddress(cfg.Dexes.V3[0].Factory)},
		Topics:    [][]common.Hash{{poolCreatedTopic}},
	})
	go func() {
		for err := range createdErrs {
			slog.Warn("pool-created subscription error", "err", err)
		}
	}()

	heads, errs := src.SubscribeBlocks(ctx)
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
		case l := <-createdLogs:
			// 新池：从链上初始化并注册
			p, err := adapter.PoolByAddress(ctx, l.Address)
			if err != nil {
				slog.Warn("new pool init", "addr", l.Address.Hex(), "err", err)
				continue
			}
			reg.UpsertPool(v3.State(p))
			graph.AddPool(p.Pool(), p.Address)
			if db != nil {
				_ = db.SavePool(ctx, p.Address.Hex(), p.Exchange, "v3", p.Token0, p.Token1, p.Fee)
			}
			slog.Info("new pool", "addr", p.Address.Hex(), "t0", p.Token0.Hex(), "t1", p.Token1.Hex(), "fee", p.Fee)
		}
	}
}
