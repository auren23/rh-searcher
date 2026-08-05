// Package simulation 交易模拟层：eth_call 验证 + gas 估算 + 结果判定。
// 只在最终阶段用链上验证，候选搜索用本地池状态。
package simulation

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// SimulationResult 一次模拟的结果。
type SimulationResult struct {
	Success    bool
	RevertMsg  string
	GasUsed    uint64
	GasPrice   *big.Int
	OutputWETH *big.Int // 模拟后合约的 WETH 余额（执行合约返回）
}

// Simulator 交易模拟器。
type Simulator interface {
	// Simulate 对执行合约调用做 eth_call。
	// tx 是待签名交易的预演（from 必须带签名者地址）。
	Simulate(ctx context.Context, tx *types.Transaction) (SimulationResult, error)
}

// GasEstimator 独立于 RPC 组的 gas 服务。
type GasEstimator interface {
	EstimateGas(ctx context.Context, to common.Address, data []byte, from common.Address) (uint64, error)
	SuggestPrice(ctx context.Context) (*big.Int, error)
}
