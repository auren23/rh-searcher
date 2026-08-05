// Package telemetry 指标、结构化日志与告警。
package telemetry

import (
	"context"
	"log/slog"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics 全局指标集合。命名遵循 rh_ 前缀。
type Metrics struct {
	ChainHeadLag        prometheus.Gauge
	WebsocketReconnects prometheus.Counter
	RPCErrors           *prometheus.CounterVec
	PoolStateAgeMs      prometheus.Gauge
	CandidatesTotal     *prometheus.CounterVec
	SimulationSuccess   prometheus.Counter
	SimulationFailure   prometheus.Counter
	BroadcastLatencyMs  prometheus.Histogram
	ReceiptLatencyMs    prometheus.Histogram
	TxReverts           prometheus.Counter
	ExpectedProfitWei   prometheus.Counter
	ActualProfitWei     prometheus.Counter
	GasLossWei          prometheus.Counter
	NonceGap            prometheus.Gauge
	WalletBalanceWei    *prometheus.GaugeVec
}

func NewMetrics() *Metrics {
	f := promauto.With(prometheus.DefaultRegisterer)
	return &Metrics{
		ChainHeadLag:        f.NewGauge(prometheus.GaugeOpts{Name: "rh_chain_head_lag", Help: "Blocks behind chain head"}),
		WebsocketReconnects: f.NewCounter(prometheus.CounterOpts{Name: "rh_websocket_reconnect_total", Help: "WebSocket reconnects"}),
		RPCErrors:           f.NewCounterVec(prometheus.CounterOpts{Name: "rh_rpc_error_total", Help: "RPC errors by group"}, []string{"group"}),
		PoolStateAgeMs:      f.NewGauge(prometheus.GaugeOpts{Name: "rh_pool_state_age_ms", Help: "Age of oldest pool state"}),
		CandidatesTotal:     f.NewCounterVec(prometheus.CounterOpts{Name: "rh_candidate_total", Help: "Candidates by decision"}, []string{"strategy", "decision"}),
		SimulationSuccess:   f.NewCounter(prometheus.CounterOpts{Name: "rh_simulation_success_total", Help: "Successful simulations"}),
		SimulationFailure:   f.NewCounter(prometheus.CounterOpts{Name: "rh_simulation_failure_total", Help: "Failed simulations"}),
		BroadcastLatencyMs:  f.NewHistogram(prometheus.HistogramOpts{Name: "rh_broadcast_latency_ms", Help: "Broadcast latency", Buckets: []float64{50, 100, 250, 500, 1000, 2500}}),
		ReceiptLatencyMs:    f.NewHistogram(prometheus.HistogramOpts{Name: "rh_receipt_latency_ms", Help: "Receipt latency", Buckets: []float64{250, 500, 1000, 2000, 5000, 10000}}),
		TxReverts:           f.NewCounter(prometheus.CounterOpts{Name: "rh_tx_revert_total", Help: "Reverted transactions"}),
		ExpectedProfitWei:   f.NewCounter(prometheus.CounterOpts{Name: "rh_expected_profit_wei", Help: "Expected profit"}),
		ActualProfitWei:     f.NewCounter(prometheus.CounterOpts{Name: "rh_actual_profit_wei", Help: "Actual profit"}),
		GasLossWei:          f.NewCounter(prometheus.CounterOpts{Name: "rh_gas_loss_wei", Help: "Gas spent on losses"}),
		NonceGap:            f.NewGauge(prometheus.GaugeOpts{Name: "rh_nonce_gap", Help: "Nonce gap"}),
		WalletBalanceWei:    f.NewGaugeVec(prometheus.GaugeOpts{Name: "rh_wallet_balance_wei", Help: "Wallet balance"}, []string{"wallet"}),
	}
}

// SetupLogging 初始化 slog：JSON 输出到 stdout。
func SetupLogging(level slog.Level) {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))
}

// Alert 告警通道：MVP 只写日志，后续接 webhook。
type Alert struct{}

func (a *Alert) Fire(ctx context.Context, severity, msg string) {
	slog.Warn("ALERT", "severity", severity, "msg", msg)
}
