// rh-liquidator: Morpho 清算引擎。M7 骨架：扫描可清算仓位并记录候选。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/liquidation"
	"github.com/auren23/rh-searcher/internal/morpho"
	"github.com/auren23/rh-searcher/internal/telemetry"
)

func main() {
	cfgPath := flag.String("config", "configs/morpho.yaml", "config file")
	interval := flag.Duration("interval", 30*time.Second, "scan interval")
	flag.Parse()

	telemetry.SetupLogging(slog.LevelInfo)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if _, err := config.Load(*cfgPath); err != nil {
		slog.Warn("config load (continuing with defaults)", "err", err)
	}

	ix := morpho.NewIndexer()
	engine := liquidation.NewEngine(ix, nil, liquidation.NewLocalEvaluator())

	slog.Info("liquidation engine started (M7 skeleton)")
	t := time.NewTicker(*interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			slog.Info("liquidator stopped")
			return
		case <-t.C:
			cands := engine.Scan(ctx)
			slog.Info("liquidation scan", "candidates", len(cands))
		}
	}
}
