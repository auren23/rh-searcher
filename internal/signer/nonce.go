package signer

import (
	"context"
	"log/slog"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// LocalNonceManager 内存 + 链上校准的 nonce 管理器。
// 与签名钱包 1:1 绑定；两个进程绝不共用同一个实例。
type LocalNonceManager struct {
	cli      *ethclient.Client
	from     common.Address
	mu       sync.Mutex
	next     uint64
	inflight map[common.Hash]uint64 // 已广播未确认
}

func NewLocalNonceManager(cli *ethclient.Client, from common.Address) *LocalNonceManager {
	return &LocalNonceManager{
		cli:      cli,
		from:     from,
		inflight: make(map[common.Hash]uint64),
	}
}

// Next 返回下一个 nonce；首次或重置后从链上 pending 校准。
func (m *LocalNonceManager) Next(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.next == 0 {
		nonce, err := m.cli.PendingNonceAt(ctx, m.from)
		if err != nil {
			return 0, err
		}
		m.next = nonce
	}
	n := m.next
	m.next++
	return n, nil
}

// Observe 记录已广播交易。
func (m *LocalNonceManager) Observe(txHash common.Hash, nonce uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inflight[txHash] = nonce
}

// Confirm 交易上链后回收状态。
func (m *LocalNonceManager) Confirm(txHash common.Hash) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.inflight, txHash)
}

// Reset 从链上重新同步 nonce。usePending=true 时取 pending，并保守跳过 inflight 高度。
func (m *LocalNonceManager) Reset(ctx context.Context, usePending bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var (
		nonce uint64
		err   error
	)
	if usePending {
		nonce, err = m.cli.PendingNonceAt(ctx, m.from)
	} else {
		nonce, err = m.cli.NonceAt(ctx, m.from, nil)
	}
	if err != nil {
		return err
	}
	for _, n := range m.inflight {
		if n >= nonce {
			nonce = n + 1
		}
	}
	m.next = nonce
	slog.Info("nonce reset", "address", m.from.Hex(), "next", nonce, "usePending", usePending)
	return nil
}

// LatestKnown 返回当前已知的下一个 nonce（不校准）。
func (m *LocalNonceManager) LatestKnown() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.next
}
