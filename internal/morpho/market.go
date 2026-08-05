// Package morpho Morpho Blue 市场与仓位索引、利息与健康度计算。
// 链上事件实时维护；官方 API 只用于初始发现、Bootstrap 与交叉验证。
// MVP 只做 Morpho Blue，不做 Midnight。
package morpho

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// MarketParams 市场五元组（市场彼此隔离）。
type MarketParams struct {
	LoanToken   common.Address
	CollateralToken common.Address
	Oracle      common.Address
	IRM         common.Address
	LLTV        *big.Int
}

// Market 市场状态。
type Market struct {
	ID                  common.Hash // keccak256(abi.encode(MarketParams))
	Params              MarketParams
	TotalSupplyAssets   *big.Int
	TotalSupplyShares   *big.Int
	TotalBorrowAssets   *big.Int
	TotalBorrowShares   *big.Int
	LastUpdate          uint64
	Fee                 *big.Int
	OraclePrice         *big.Int // 最新 oracle 价格
	UpdatedAtBlock      uint64
}

// Position 一个用户的仓位。
type Position struct {
	User            common.Address
	MarketID        common.Hash
	SupplyShares    *big.Int
	BorrowShares    *big.Int
	Collateral      *big.Int
	UpdatedAtBlock  uint64
}

// LiquidationInfo 清算计算结果。
type LiquidationInfo struct {
	Liquidatable    bool
	HealthFactor    *big.Float
	RepayAssets     *big.Int   // 最多可偿还的借款资产
	SeizedCollateral *big.Int  // 可获得的抵押品
	CloseFactor     *big.Int
}
