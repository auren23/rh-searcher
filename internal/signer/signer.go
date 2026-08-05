// Package signer 负责本地签名与 Nonce 管理。
// 每个策略进程持有独立钱包与独立 Nonce Manager，进程间不共享。
package signer

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Signer 交易签名接口。MVP 只做本地签名（local.go），
// 后续可按需加远程/HSM 实现。
type Signer interface {
	// Address 签名者地址。
	Address() common.Address
	// SignTx 签名交易。
	SignTx(tx *types.Transaction) (*types.Transaction, error)
}

// NonceManager 管理单一地址的 nonce：内存计数 + 链上校准。
// 必须与签名钱包一一对应，绝不跨进程共享。
type NonceManager interface {
	// Next 返回下一个可用 nonce 并占用。
	Next(ctx context.Context) (uint64, error)
	// Observe 记录已广播的交易，更新 nonce 基线。
	Observe(txHash common.Hash, nonce uint64)
	// Reset 重新从链上同步 nonce（pending 或 latest）。
	Reset(ctx context.Context, usePending bool) error
}

// GasEstimator 估算交易的 gas 用量与价格。
type GasEstimator interface {
	// EstimateGas 估算 gas limit（eth_estimateGas）。
	EstimateGas(ctx context.Context, tx *types.Transaction) (uint64, error)
	// SuggestPrice 建议 gas 价格（eth_gasPrice）。
	SuggestPrice(ctx context.Context) (*big.Int, error)
}
