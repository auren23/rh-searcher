// rh-cli: 运维命令行工具。检查配置、RPC 延迟基准、余额查询。
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/spf13/cobra"

	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/dex/v3"
	"github.com/auren23/rh-searcher/internal/rpc"
	"github.com/auren23/rh-searcher/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

var cfgPath string

func main() {
	root := &cobra.Command{Use: "rh-cli", Short: "rh-searcher ops tool"}
	root.PersistentFlags().StringVar(&cfgPath, "config", "configs/robinhood.yaml", "config file")

	root.AddCommand(&cobra.Command{
		Use:   "config-check",
		Short: "Validate config file and print redacted summary",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "config error:", err)
				os.Exit(1)
			}
			fmt.Printf("chain_id: %d\n", cfg.Chain.ID)
			fmt.Printf("weth: %s\n", cfg.Chain.WETH)
			fmt.Printf("read_rpcs: %d, send_rpcs: %d, sim_rpcs: %d, archive_rpcs: %d\n",
				len(cfg.RPC.Groups.Read), len(cfg.RPC.Groups.Send), len(cfg.RPC.Groups.Sim), len(cfg.RPC.Groups.Archive))
			fmt.Printf("v3_dexes: %d\n", len(cfg.Dexes.V3))
			fmt.Printf("run_mode: %s\n", cfg.Mode.Run)
			// 不打印任何密钥
		},
	})

	root.AddCommand(&cobra.Command{
		Use:   "migrate",
		Short: "Run pending DB migrations (idempotent, advisory-locked)",
		Run: func(cmd *cobra.Command, args []string) {
			url := os.Getenv("RH_POSTGRES_URL")
			if url == "" {
				fmt.Fprintln(os.Stderr, "RH_POSTGRES_URL not set")
				os.Exit(1)
			}
			ctx := context.Background()
			pool, err := pgxpool.New(ctx, url)
			if err != nil {
				fmt.Fprintln(os.Stderr, "connect:", err)
				os.Exit(1)
			}
			defer pool.Close()
			if err := storage.Migrate(ctx, pool); err != nil {
				fmt.Fprintln(os.Stderr, "migrate:", err)
				os.Exit(1)
			}
			fmt.Println("migrations up to date")
		},
	})

	root.AddCommand(&cobra.Command{
		Use:   "bench",
		Short: "Benchmark RPC latency (P50/P95/P99) for all configured endpoints",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "config error:", err)
				os.Exit(1)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
			defer cancel()
			pool := rpc.NewPool()
			for name, urls := range map[string][]string{
				"archive": cfg.RPC.Groups.Archive,
				"read":    cfg.RPC.Groups.Read,
				"sim":     cfg.RPC.Groups.Sim,
				"send":    cfg.RPC.Groups.Send,
			} {
				if len(urls) == 0 {
					continue
				}
				if err := pool.AddGroup(ctx, name, urls); err != nil {
					fmt.Fprintf(os.Stderr, "group %s: %v\n", name, err)
				}
			}
			b := rpc.NewBenchmarker(pool, 10)
			b.RunOnce(ctx)
		},
	})

	root.AddCommand(&cobra.Command{
		Use:   "balance <address>",
		Short: "Query WETH balance for an address via read RPC",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Load(cfgPath)
			if err != nil || len(cfg.RPC.Groups.Read) == 0 {
				fmt.Fprintln(os.Stderr, "config error or no read RPC")
				os.Exit(1)
			}
			cli, err := ethclient.Dial(cfg.RPC.Groups.Read[0])
			if err != nil {
				fmt.Fprintln(os.Stderr, "dial:", err)
				os.Exit(1)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			bal, err := cli.BalanceAt(ctx, common.HexToAddress(args[0]), nil)
			if err != nil {
				fmt.Fprintln(os.Stderr, "balance:", err)
				os.Exit(1)
			}
			fmt.Printf("%s: %s wei (%s ETH)\n", args[0], bal.String(), bal.String())
		},
	})

	root.AddCommand(poolsBootstrapCmd())
	root.AddCommand(poolsCountCmd())

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// poolsBootstrapCmd WETH 池宇宙一次性 bootstrap：
// 从 Factory PoolCreated 日志扫描全部 WETH 池（节点端 topics 过滤），
// 增量落 JSONL（每行一个池）+ 检查点（断点续扫），完成后可选写 PG。
// 输出文件是 canary/arbitrage 的池宇宙备份——不再依赖实时 Swap 自发现，
// 静态池（长期无 Swap 的第二腿）同样保留。
func poolsBootstrapCmd() *cobra.Command {
	var (
		rpcURL     string
		fromFlag   uint64
		toFlag     uint64
		batch      uint64
		out        string
		maxBatches int
	)
	cmd := &cobra.Command{
		Use:   "pools bootstrap",
		Short: "Scan factory PoolCreated logs and persist the full WETH pool universe",
		Run: func(cmd *cobra.Command, args []string) {
			ctx := context.Background()
			cfg, err := config.LoadMerged(cfgPath, "configs/dexes.yaml", "configs/morpho.yaml")
			if err != nil {
				slog.Error("load config", "err", err)
				os.Exit(1)
			}
			if len(cfg.Dexes.V3) == 0 {
				slog.Error("no v3 dex configured")
				os.Exit(1)
			}
			weth := common.HexToAddress(cfg.Chain.WETH)
			d := cfg.Dexes.V3[0]
			if rpcURL == "" {
				rpcURL = firstNonEmptyCfg(cfg.RPC.Groups.Archive)
			}
			cli, err := ethclient.Dial(rpcURL)
			if err != nil {
				slog.Error("dial rpc", "url", rpcURL, "err", err)
				os.Exit(1)
			}
			defer cli.Close()
			adapter, err := v3.NewAdapter(cli, d.Name,
				common.HexToAddress(d.Factory), common.HexToAddress(d.Router), d.RouterKind,
				common.HexToHash(d.InitCodeHash), d.FactoryBlock)
			if err != nil {
				slog.Error("v3 adapter", "err", err)
				os.Exit(1)
			}
			from := fromFlag
			if from < d.FactoryBlock {
				from = d.FactoryBlock
			}
			head := toFlag
			if head == 0 {
				// 公共 RPC 限速风暴：head 读取带退避重试
				var h uint64
				var lastErr error
				backoff := time.Second
				for attempt := 0; attempt < 60; attempt++ {
					h, lastErr = cli.BlockNumber(ctx)
					if lastErr == nil {
						break
					}
					select {
					case <-ctx.Done():
						slog.Error("read chain head", "err", lastErr)
						os.Exit(1)
					case <-time.After(backoff):
					}
					backoff *= 2
					if backoff > 30*time.Second {
						backoff = 30 * time.Second
					}
				}
				if lastErr != nil {
					slog.Error("read chain head", "err", lastErr)
					os.Exit(1)
				}
				head = h
			}
			if batch == 0 {
				batch = 100_000
			}
			// 断点续扫：ckpt 文件记录已完成末块
			ckptPath := out + ".ckpt"
			if raw, err := os.ReadFile(ckptPath); err == nil {
				var last uint64
				if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%d", &last); err == nil && last >= from {
					from = last + 1
					slog.Info("resuming from checkpoint", "block", last)
				}
			}
			if from > head {
				slog.Info("universe already complete", "head", head)
				os.Exit(0)
			}
			ckpt := func(block uint64) {
				_ = os.WriteFile(ckptPath, []byte(fmt.Sprintf("%d\n", block)), 0o644)
			}
			slog.Info("WETH pool universe bootstrap",
				"factory", d.Factory, "from", from, "to", head, "batch", batch,
				"rpc", rpcURL, "out", out, "weth", weth.Hex())
			n := 0
			batches := 0
			for from <= head {
				if maxBatches > 0 && batches >= maxBatches {
					break
				}
				to := from + batch - 1
				if to > head {
					to = head
				}
				pools, err := adapter.DiscoverWETHPools(ctx, from, to, weth)
				if err != nil {
					// 公共 RPC 降级（429 风暴/查询超时）：缩小批次重试，最小 1000 块
					if batch > 1_000 {
						batch /= 2
						slog.Warn("discover batch shrank", "from", from, "new_batch", batch, "err", err)
						select {
						case <-ctx.Done():
							os.Exit(1)
						case <-time.After(5 * time.Second):
						}
						continue
					}
					slog.Error("discover batch failed", "from", from, "to", to, "err", err)
					os.Exit(1)
				}
				if batch < 100_000 {
					batch *= 2 // 成功后恢复
					if batch > 100_000 {
						batch = 100_000
					}
				}
				// 公共 RPC 限速防护：批间小停顿，避免持续满速触发 429 风暴
				if rpcURL != "" && strings.Contains(rpcURL, "rpc.mainnet.chain.robinhood.com") {
					select {
					case <-ctx.Done():
						os.Exit(1)
					case <-time.After(400 * time.Millisecond):
					}
				}
				for _, p := range pools {
					up := v3.UniversePool{
						Address: p.Address.Hex(), Exchange: p.Exchange,
						Token0: p.Token0.Hex(), Token1: p.Token1.Hex(),
						Fee: p.Fee, TickSpacing: p.TickSpacing,
						CreatedBlock: p.CreatedBlock, CreatedBlockHash: p.CreatedBlockHash.Hex(),
						ProvenanceSource: p.ProvenanceSource,
					}
					if err := v3.AppendUniverseLine(out, up); err != nil {
						slog.Error("append universe", "err", err)
						os.Exit(1)
					}
					n++
				}
				batches++
				ckpt(to)
				if batches%10 == 0 || pools == nil || to == head {
					slog.Info("bootstrap progress", "batches", batches,
						"from", from, "to", to, "head", head, "weth_pools", n)
				}
				from = to + 1
			}
			slog.Info("bootstrap done", "batches", batches, "weth_pools", n, "last_block", from-1)
			// 可选：写 PG（RH_POSTGRES_URL 配置时）
			if url := os.Getenv("RH_POSTGRES_URL"); url != "" {
				sink, err := storage.New(ctx, url)
				if err != nil {
					slog.Warn("postgres unavailable, universe kept in file only", "err", err)
					return
				}
				defer sink.Close()
				uni, err := v3.LoadUniverse(out)
				if err != nil {
					slog.Error("reload universe for pg", "err", err)
					os.Exit(1)
				}
				pools := make([]storage.Pool, 0, len(uni))
				for _, u := range uni {
					pools = append(pools, storage.Pool{
						Address: u.Address, Exchange: u.Exchange, Protocol: "v3",
						Token0: u.Token0, Token1: u.Token1, Fee: u.Fee,
						TickSpacing: u.TickSpacing, CreatedBlock: u.CreatedBlock,
						CreatedBlockHash: u.CreatedBlockHash, ProvenanceSource: u.ProvenanceSource,
					})
				}
				if err := sink.CommitPools(ctx, pools, from-1); err != nil {
					slog.Error("commit pools to pg", "err", err)
					os.Exit(1)
				}
				slog.Info("universe persisted to postgres", "pools", len(pools))
			}
		},
	}
	cmd.Flags().StringVar(&rpcURL, "rpc", "", "RPC url (default: config archive group; use public RPC — Alchemy free caps getLogs at 10 blocks)")
	cmd.Flags().Uint64Var(&fromFlag, "from", 0, "start block (default: factory block)")
	cmd.Flags().Uint64Var(&toFlag, "to", 0, "end block (default: chain head)")
	cmd.Flags().Uint64Var(&batch, "batch", 100_000, "blocks per getLogs query")
	cmd.Flags().StringVar(&out, "out", "data/canary/weth-universe.jsonl", "output universe file (JSONL; .json = array)")
	cmd.Flags().IntVar(&maxBatches, "max-batches", 0, "stop after N batches (0 = all)")
	return cmd
}

// poolsCountCmd 统计宇宙文件：池总数、token 数、可成环 token 分布。
func poolsCountCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:   "pools count",
		Short: "Summarize the WETH pool universe file",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Load(cfgPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "config error:", err)
				os.Exit(1)
			}
			weth := strings.ToLower(cfg.Chain.WETH)
			uni, err := v3.LoadUniverse(file)
			if err != nil {
				fmt.Fprintln(os.Stderr, "load universe:", err)
				os.Exit(1)
			}
			tokens := map[string]int{}
			byToken := map[string][]string{}
			for _, u := range uni {
				t := u.Token0
				if strings.EqualFold(t, weth) {
					t = u.Token1
				}
				tokens[t]++
				byToken[t] = append(byToken[t], u.Address)
			}
			fmt.Printf("pools: %d\n", len(uni))
			fmt.Printf("tokens: %d\n", len(tokens))
			fmt.Printf("tokens with >=2 WETH pools (two-hop routes possible): %d\n", func() int {
				n := 0
				for _, ps := range byToken {
					if len(ps) >= 2 {
						n++
					}
				}
				return n
			}())
			fmt.Println("--- top tokens by WETH pool count ---")
			type kv struct {
				t string
				n int
			}
			var list []kv
			for t, n := range tokens {
				list = append(list, kv{t, n})
			}
			for i := 0; i < len(list); i++ {
				for j := i + 1; j < len(list); j++ {
					if list[j].n > list[i].n {
						list[i], list[j] = list[j], list[i]
					}
				}
			}
			if len(list) > 20 {
				list = list[:20]
			}
			for _, e := range list {
				fmt.Printf("  %s %d\n", e.t, e.n)
			}
		},
	}
	cmd.Flags().StringVar(&file, "file", "data/canary/weth-universe.jsonl", "universe file")
	return cmd
}

// firstNonEmptyCfg 取第一个非空配置项。
func firstNonEmptyCfg(list []string) string {
	for _, s := range list {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
