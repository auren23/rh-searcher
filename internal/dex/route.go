package dex

import (
	"github.com/ethereum/go-ethereum/common"
)

// EncodePath V3 多跳路径编码：token0, fee, token1, fee, token2...
func EncodePath(path []common.Address, fees []uint32) []byte {
	out := []byte{}
	out = append(out, path[0].Bytes()...)
	for i, fee := range fees {
		feeB := make([]byte, 3)
		feeB[0] = byte(fee >> 16)
		feeB[1] = byte(fee >> 8)
		feeB[2] = byte(fee)
		out = append(out, feeB...)
		out = append(out, path[i+1].Bytes()...)
	}
	return out
}

// RouteCost 估算一条路由的滑点与手续费损耗（比例，0..1）。
// MVP 简化：每跳固定滑点 + 每跳协议费率。
func RouteCost(pools []PoolRef, slippagePerHop float64) float64 {
	cost := 0.0
	for _, p := range pools {
		fee := float64(p.Fee) / 1e6
		cost += fee + slippagePerHop
	}
	return cost
}
