// Package dex DEX 池适配层：发现、状态应用、本地报价、交易构建。
// MVP 只实现 V3-compatible adapter；V2 确认链上需求后再加。
package dex

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// Pool 池的元数据与当前状态。
type Pool struct {
	ID           string // 地址小写
	Protocol     string // "v3" | "v2"
	Exchange     string // 配置名，如 "robinhood-swap"
	Token0       common.Address
	Token1       common.Address
	Fee          uint32
	Reserve0     *big.Int
	Reserve1     *big.Int
	Liquidity    *big.Int // V3 的 L
	SqrtPriceX96 *big.Int
	Tick         int
	ObservedAt   uint64 // 最后状态更新时间（区块号）
}

// PoolState 供 adapter 内部维护的完整池状态。
type PoolState interface {
	Pool() Pool
}

// PoolAdapter 池适配器：协议相关的发现、状态机与报价。
type PoolAdapter interface {
	Protocol() string
	// DiscoverPools 从 fromBlock 开始发现工厂创建的所有池。
	DiscoverPools(ctx context.Context, fromBlock uint64) ([]Pool, error)
	// ApplyLog 将一条日志应用到池状态（Swap/Mint/Burn/slot0...）。
	ApplyLog(state PoolState, log types.Log) (PoolState, error)
	// QuoteExactIn 本地计算 tokenIn→tokenOut 的期望输出。
	QuoteExactIn(state PoolState, tokenIn common.Address, amountIn *big.Int) (*big.Int, error)
	// BuildSwap 构建一笔单跳 swap 的 calldata（走 Router）。
	BuildSwap(route Route, amountIn, minOut *big.Int) ([]byte, error)
}

// Route 一条已解析的路径：中间 token 序列 + 每跳使用的池。
type Route struct {
	TokenIn  common.Address
	TokenOut common.Address
	AmountIn *big.Int
	MinOut   *big.Int
	Pools    []PoolRef // 按执行顺序
	Path     []common.Address
}

// PoolRef 路由中的一跳。
type PoolRef struct {
	Address  common.Address
	Exchange string
	Protocol string
	Fee      uint32
	Token0   common.Address
	Token1   common.Address
	// TokenInIsToken0 为 true 表示本跳从 token0 方向进入
	TokenInIsToken0 bool
}

// QuoteResult 报价结果。
type QuoteResult struct {
	AmountOut *big.Int
	Price     *big.Float
}
