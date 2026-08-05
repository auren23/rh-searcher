package chain

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
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
func (w *WSClient) SubscribeLogs(ctx context.Context, query ethereum.FilterQuery) (<-chan types.Log, <-chan error) {
	out := make(chan types.Log, 1024)
	errCh := make(chan error, 4)
	go func() {
		defer close(out)
		for {
			if ctx.Err() != nil {
				return
			}
			logs := make(chan types.Log, 512)
			sub, err := w.rpc.EthSubscribe(ctx, logs, "logs", query)
			if err != nil {
				errCh <- err
				time.Sleep(2 * time.Second)
				_ = w.reconnect(ctx)
				continue
			}
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				return
			case err := <-sub.Err():
				errCh <- err
				sub.Unsubscribe()
				_ = w.reconnect(ctx)
				time.Sleep(2 * time.Second)
			case l := <-logs:
				out <- l
			}
		}
	}()
	return out, errCh
}

func (w *WSClient) BlockByNumber(ctx context.Context, number uint64) (*types.Block, error) {
	return w.cli.BlockByNumber(ctx, big.NewInt(int64(number)))
}

func (w *WSClient) HistoricalLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	return w.cli.FilterLogs(ctx, query)
}
