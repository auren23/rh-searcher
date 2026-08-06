// rh-cli: 运维命令行工具。检查配置、RPC 延迟基准、余额查询。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/spf13/cobra"

	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/auren23/rh-searcher/internal/rpc"
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

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
