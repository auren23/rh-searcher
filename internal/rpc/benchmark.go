package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Benchmarker 延迟基准：对每个端点跑固定次数的 eth_blockNumber，输出 P50/P95/P99。
type Benchmarker struct {
	pool    *Pool
	samples int
	latency *prometheus.HistogramVec
	mu      sync.Mutex
}

func NewBenchmarker(pool *Pool, samples int) *Benchmarker {
	return &Benchmarker{
		pool:    pool,
		samples: samples,
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "rh_rpc_latency_ms",
			Help:    "RPC request latency in milliseconds",
			Buckets: []float64{5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000},
		}, []string{"group", "endpoint"}),
	}
}

func (b *Benchmarker) Latency() *prometheus.HistogramVec { return b.latency }

// RunOnce 对每个端点测一轮延迟并打点。
func (b *Benchmarker) RunOnce(ctx context.Context) {
	for _, g := range b.pool.groups {
		for _, c := range g.clients {
			var total time.Duration
			ok := 0
			for i := 0; i < b.samples; i++ {
				start := time.Now()
				_, err := c.Client.BlockNumber(ctx)
				d := time.Since(start)
				b.latency.WithLabelValues(g.Name, c.URL).Observe(float64(d.Milliseconds()))
				if err == nil {
					total += d
					ok++
				}
			}
			if ok > 0 {
				slog.Info("rpc benchmark",
					"group", g.Name, "endpoint", c.URL,
					"avg_ms", (total / time.Duration(ok)).Milliseconds(),
					"ok", ok, "total", b.samples)
			}
		}
	}
}

// Run 每 interval 跑一轮。
func (b *Benchmarker) Run(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				b.RunOnce(ctx)
			}
		}
	}()
}
