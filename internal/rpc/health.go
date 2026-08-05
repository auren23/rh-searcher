package rpc

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Broadcaster 交易发送端。GMGN 快速 RPC 是生产首选发送端，
// Robinhood Sequencer Endpoint 为备用。发送端与读取端完全分离。
type Broadcaster interface {
	// SendRawTransaction 广播原始交易，返回交易哈希。
	SendRawTransaction(ctx context.Context, rawTx []byte) (common.Hash, error)
}

// Simulator 交易模拟端。MVP 用 eth_call，后续可换 ForkSimulator。
type Simulator interface {
	// Simulate 模拟执行交易，返回结果与 gas 估算。
	Simulate(ctx context.Context, tx *types.Transaction, from common.Address) (*types.Transaction, error)
}

// HealthChecker RPC 健康检查器：定时 ping 每个端点并更新健康分。
type HealthChecker struct {
	pool    *Pool
	pingURL string // 用于探测的 eth_call 目标（如 WETH 余额）
}

func NewHealthChecker(pool *Pool) *HealthChecker {
	return &HealthChecker{pool: pool}
}

// Run 启动健康检查循环，每 interval 检查一次。
func (h *HealthChecker) Run(ctx context.Context, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				h.checkOnce(ctx)
			}
		}
	}()
}

func (h *HealthChecker) checkOnce(ctx context.Context) {
	for _, g := range h.pool.groups {
		for _, c := range g.clients {
			start := time.Now()
			ctx2, cancel := context.WithTimeout(ctx, 3*time.Second)
			_, err := c.Client.BlockNumber(ctx2)
			cancel()
			c.Record(err == nil, time.Since(start))
		}
	}
}
