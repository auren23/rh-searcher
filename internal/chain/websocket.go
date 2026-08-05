package chain

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// WSClient 基于 go-ethereum 客户端的 WebSocket 数据源，带自动重连。
type WSClient struct {
	url   string
	mu    sync.Mutex
	cli   *ethclient.Client
	rpc   *rpc.Client
	close func()
}

func NewWSClient(ctx context.Context, url string) (*WSClient, error) {
	if !strings.HasPrefix(url, "ws://") && !strings.HasPrefix(url, "wss://") {
		return nil, fmt.Errorf("not a websocket url: %s", url)
	}
	rc, err := rpc.DialContext(ctx, url)
	if err != nil {
		return nil, err
	}
	ec := ethclient.NewClient(rc)
	return &WSClient{url: url, rpc: rc, cli: ec, close: rc.Close}, nil
}

// Client 暴露底层 ethclient（供 adapter 使用）。
func (w *WSClient) Client() *ethclient.Client {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cli
}

func (w *WSClient) reconnect(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.close != nil {
		w.close()
	}
	rc, err := rpc.DialContext(ctx, w.url)
	if err != nil {
		return err
	}
	w.rpc = rc
	w.cli = ethclient.NewClient(rc)
	w.close = rc.Close
	return nil
}

// SubscribeBlocks 订阅新区块头，断线自动重连并补齐缺口。
func (w *WSClient) SubscribeBlocks(ctx context.Context) (<-chan BlockEvent, <-chan error) {
	out := make(chan BlockEvent, 256)
	errCh := make(chan error, 4)
	go func() {
		defer close(out)
		last := uint64(0)
		haveLast := false
		for {
			if err := ctx.Err(); err != nil {
				return
			}
			heads := make(chan *types.Header, 64)
			sub, err := w.rpc.EthSubscribe(ctx, heads, "newHeads")
			if err != nil {
				errCh <- err
				time.Sleep(2 * time.Second)
				_ = w.reconnect(ctx)
				continue
			}
			for {
				select {
				case <-ctx.Done():
					sub.Unsubscribe()
					return
				case err := <-sub.Err():
					errCh <- err
					sub.Unsubscribe()
					_ = w.reconnect(ctx)
					// ponytail: 断线后用 BlockByNumber 逐块补齐，连续补块会慢，
					// 长时间离线时后续可换成批量 Range 补块
					goto reconnect
				case h := <-heads:
					n := h.Number.Uint64()
					ev := BlockEvent{Number: n, Hash: h.Hash(), Parent: h.ParentHash, Time: h.Time}
					if haveLast && n > last+1 {
						// 缺口：重放中间区块
						for bn := last + 1; bn < n; bn++ {
							if b, err := w.BlockByNumber(ctx, bn); err == nil {
								out <- BlockEvent{Number: bn, Hash: b.Hash(), Parent: b.ParentHash(), Time: b.Time()}
							} else {
								errCh <- err
							}
						}
					}
					last = n
					haveLast = true
					out <- ev
				}
			}
		reconnect:
			time.Sleep(2 * time.Second)
		}
	}()
	return out, errCh
}

// SubscribeLogs 订阅日志，断线自动重连。
// LogCursor 日志游标：区块内部分日志丢失时，断线补扫必须从当前块开始（包含式），
// 并按 (blockHash, txHash, logIndex) 身份去重。
type LogCursor struct {
	BlockNumber uint64
	BlockHash   common.Hash
	TxHash      common.Hash
	LogIndex    uint
	Have        bool
}

// Seen 该日志是否已处理（身份去重）。
func (c *LogCursor) Seen(l types.Log) bool {
	if !c.Have {
		return false
	}
	if l.BlockNumber < c.BlockNumber {
		return true
	}
	if l.BlockNumber > c.BlockNumber {
		return false
	}
	// 同区块：按身份去重（hash + tx + index）
	return l.BlockHash == c.BlockHash && l.TxHash == c.TxHash && l.Index == c.LogIndex
}

// Advance 推进游标（仅允许向前）。
func (c *LogCursor) Advance(l types.Log) {
	if !c.Have || l.BlockNumber > c.BlockNumber {
		c.BlockNumber = l.BlockNumber
		c.BlockHash = l.BlockHash
		c.TxHash = l.TxHash
		c.LogIndex = l.Index
		c.Have = true
	}
}

func (w *WSClient) SubscribeLogs(ctx context.Context, query ethereum.FilterQuery) (<-chan types.Log, <-chan error) {
	out := make(chan types.Log, 1024)
	errCh := make(chan error, 4)
	go func() {
		defer close(out)
		var cursor LogCursor
		for {
			if ctx.Err() != nil {
				return
			}
			// 首次连接（或每次重连后）：补扫 cursor 到当前头的窗口（含 cursor 所在区块）
			if cursor.Have {
				backfilled, err := w.backfillLogs(ctx, query, cursor.BlockNumber, &cursor)
				if err != nil {
					errCh <- err
				} else {
					for _, l := range backfilled {
						out <- l
					}
				}
			}
			logs := make(chan types.Log, 512)
			sub, err := w.rpc.EthSubscribe(ctx, logs, "logs", query)
			if err != nil {
				errCh <- err
				time.Sleep(2 * time.Second)
				_ = w.reconnect(ctx)
				continue
			}
			// 内层循环：持续消费日志，只有订阅错误才重建（不得每条日志重建订阅）
			subErr := false
			for !subErr {
				select {
				case <-ctx.Done():
					sub.Unsubscribe()
					return
				case err, ok := <-sub.Err():
					if !ok {
						subErr = true
						break
					}
					errCh <- err
					subErr = true
				case l, ok := <-logs:
					if !ok {
						subErr = true
						break
					}
					if cursor.Seen(l) {
						continue // 重复投递
					}
					out <- l
					cursor.Advance(l)
				}
			}
			sub.Unsubscribe()
			_ = w.reconnect(ctx)
			time.Sleep(2 * time.Second)
		}
	}()
	return out, errCh
}

// backfillLogs 用 HTTP FilterLogs 补齐 [from, head] 区间的日志（断线窗口）。
// from 为游标所在区块（包含式：该区块内未处理的日志也会补回）；按身份去重并保持 (tx,log) 顺序。
func (w *WSClient) backfillLogs(ctx context.Context, query ethereum.FilterQuery, from uint64, cursor *LogCursor) ([]types.Log, error) {
	head, err := w.cli.BlockNumber(ctx)
	if err != nil {
		return nil, err
	}
	if from > head {
		return nil, nil
	}
	q := query
	q.FromBlock = big.NewInt(int64(from))
	q.ToBlock = big.NewInt(int64(head))
	logs, err := w.cli.FilterLogs(ctx, q)
	if err != nil {
		return nil, err
	}
	out := make([]types.Log, 0, len(logs))
	for _, l := range logs {
		if cursor.Seen(l) {
			continue
		}
		out = append(out, l)
		cursor.Advance(l)
	}
	return out, nil
}

func (w *WSClient) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	return w.cli.BlockByNumber(ctx, big.NewInt(int64(number)))
}

func (w *WSClient) HistoricalLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return w.cli.FilterLogs(ctx, query)
}
