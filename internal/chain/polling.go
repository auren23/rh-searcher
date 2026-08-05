package chain

import (
	"context"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// PollingSource 基于 HTTP RPC 轮询的数据源。
// Robinhood 公共端点没有 WebSocket（wss://.../ws 返回 404），
// 生产应换供应商 WS 或自建节点；轮询是公共端点下的现实约束。
type PollingSource struct {
	cli *ethclient.Client
}

func NewPollingSource(cli *ethclient.Client) *PollingSource {
	return &PollingSource{cli: cli}
}

// SubscribeBlocks 轮询最新高度并补齐缺口。
func (p *PollingSource) SubscribeBlocks(ctx context.Context) (<-chan BlockEvent, <-chan error) {
	out := make(chan BlockEvent, 256)
	errCh := make(chan error, 4)
	go func() {
		defer close(out)
		gaps := NewGapDetector()
		t := time.NewTicker(700 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				head, err := p.cli.HeaderByNumber(ctx, nil)
				if err != nil {
					errCh <- err
					continue
				}
				n := head.Number.Uint64()
				if gap := gaps.Observe(n); gap != nil {
					// 补缺口（从旧到新）
					for bn := gap.From; bn <= gap.To; bn++ {
						b, err := p.BlockByNumber(ctx, bn)
						if err != nil {
							errCh <- err
							continue
						}
						out <- BlockEvent{Number: bn, Hash: b.Hash(), Parent: b.ParentHash(), Time: b.Time()}
					}
				}
				select {
				case out <- BlockEvent{Number: n, Hash: head.Hash(), Parent: head.ParentHash, Time: head.Time}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out, errCh
}

// SubscribeLogs 轮询 eth_getLogs。每轮从 lastBlock+1 到当前头，逐块推进。
func (p *PollingSource) SubscribeLogs(ctx context.Context, query ethereum.FilterQuery) (<-chan types.Log, <-chan error) {
	out := make(chan types.Log, 1024)
	errCh := make(chan error, 4)
	go func() {
		defer close(out)
		last := uint64(0)
		haveLast := false
		seen := make(map[uint64]bool) // 本轮已处理的区块（重组保护：同一高度只发一次）
		t := time.NewTicker(700 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				head, err := p.cli.BlockNumber(ctx)
				if err != nil {
					errCh <- err
					continue
				}
				start := last + 1
				if !haveLast {
					start = head
					haveLast = true
				}
				if start > head {
					continue
				}
				q := query
				q.FromBlock = big.NewInt(int64(start))
				q.ToBlock = big.NewInt(int64(head))
				logs, err := p.cli.FilterLogs(ctx, q)
				if err != nil {
					errCh <- err
					continue // 下轮重试同一区间
				}
				for _, l := range logs {
					if seen[l.BlockNumber] {
						continue // 已处理过该高度（重组场景）
					}
					out <- l
				}
				for bn := start; bn <= head; bn++ {
					seen[bn] = true
				}
				last = head
				// 裁剪 seen（只保留最近窗口，防止无界增长）
				if len(seen) > 10000 {
					cutoff := head - 5000
					for bn := range seen {
						if bn < cutoff {
							delete(seen, bn)
						}
					}
				}
			}
		}
	}()
	return out, errCh
}

func (p *PollingSource) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	return p.cli.BlockByNumber(ctx, big.NewInt(int64(number)))
}

func (p *PollingSource) HistoricalLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return p.cli.FilterLogs(ctx, query)
}

var _ Source = (*PollingSource)(nil)
