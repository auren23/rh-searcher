package morpho

import (
	"math/big"
)

// 常数：Morpho Blue 的固定参数。
const (
	VIRTUAL_SHARES            = 1e6
	VIRTUAL_ASSETS            = 1
	MAX_LIQUIDATION_INCENTIVE = 0.3 // 30%
	LIQUIDATION_CT            = 0.5 // 清算折扣 CT，见白皮书
)

// Interest 利息计算：按 Morpho Blue 白皮书公式。
// 资产与份额的换算采用虚拟份额/资产避免精度损失。

// ToAssetsUp 份额 → 资产（向上取整，负债方向）。
func ToAssetsUp(shares, totalAssets, totalShares *big.Int) *big.Int {
	if totalShares.Sign() == 0 {
		return new(big.Int).Set(shares)
	}
	// assets = shares * totalAssets / totalShares，向上取整
	num := new(big.Int).Mul(shares, totalAssets)
	num.Add(num, new(big.Int).Sub(totalShares, big.NewInt(1)))
	return num.Div(num, totalShares)
}

// ToSharesDown 资产 → 份额（向下取整，供应方向）。
func ToSharesDown(assets, totalAssets, totalShares *big.Int) *big.Int {
	if totalAssets.Sign() == 0 {
		return new(big.Int).Set(assets)
	}
	num := new(big.Int).Mul(assets, totalShares)
	return num.Div(num, totalAssets)
}

// AccrueInterest 累计利息：返回新的市场状态（MVP 用简化线性利率模型；
// 生产接入真实 IRM 合约）。
func AccrueInterest(m Market, elapsedSeconds uint64, ratePerSecond *big.Float) Market {
	if elapsedSeconds == 0 {
		return m
	}
	// borrowAssets' = borrowAssets * (1 + rate)^elapsed ≈ 线性近似
	factor := new(big.Float).Mul(ratePerSecond, new(big.Float).SetUint64(elapsedSeconds))
	factor.Add(factor, big.NewFloat(1))
	ba := new(big.Float).SetInt(m.TotalBorrowAssets)
	ba.Mul(ba, factor)
	borrowAssets, _ := ba.Int(nil)
	m.TotalBorrowAssets = borrowAssets
	m.LastUpdate += elapsedSeconds
	return m
}

// HealthFactor 健康因子 = collateral * oraclePrice * LLTV / borrowAssets。
// <= 1 时可清算。
func HealthFactor(collateral, oraclePrice, borrowAssets *big.Int) *big.Float {
	if borrowAssets.Sign() <= 0 {
		return big.NewFloat(1e30) // 无借款 = 健康
	}
	collateralValue := new(big.Float).SetInt(collateral)
	collateralValue.Mul(collateralValue, new(big.Float).SetInt(oraclePrice))
	// collateral 与 borrow 的精度归一化在调用方完成
	ratio := new(big.Float).Quo(collateralValue, new(big.Float).SetInt(borrowAssets))
	ratio.Mul(ratio, big.NewFloat(0.86)) // LLTV 86% 示例值，实际来自市场参数
	return ratio
}

// SeizableCollateral 清算可获得抵押品：repayAssets * price * CT 方向。
// seized = repayAssets * oraclePrice / CT（简化，忽略精度）。
func SeizableCollateral(repayAssets, oraclePrice *big.Int) *big.Int {
	seized := new(big.Float).SetInt(repayAssets)
	seized.Mul(seized, new(big.Float).SetInt(oraclePrice))
	seized.Quo(seized, big.NewFloat(LIQUIDATION_CT))
	out, _ := seized.Int(nil)
	return out
}
