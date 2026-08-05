package simulation

import "math/big"

// Cost 一笔执行的全部成本。
type Cost struct {
	GasUsed  *big.Int // wei
	GasPrice *big.Int // wei per gas
	SwapFees *big.Int // DEX 手续费（以 WETH 计价）
	Slippage *big.Int // 滑点损耗（估算）
}

// TotalWei 总成本（wei）。
func (c Cost) TotalWei() *big.Int {
	total := new(big.Int).Mul(c.GasUsed, c.GasPrice)
	total.Add(total, c.SwapFees)
	total.Add(total, c.Slippage)
	return total
}

// ExpectedNetProfit 净收益：最终 WETH - 初始 WETH - 总成本 - 安全边际。
// 安全边际是硬门槛的一部分，不允许只看中间价差。
func ExpectedNetProfit(finalWETH, initialWETH, totalCostWei, safetyMarginWei *big.Int) *big.Int {
	profit := new(big.Int).Sub(finalWETH, initialWETH)
	profit.Sub(profit, totalCostWei)
	profit.Sub(profit, safetyMarginWei)
	return profit
}

// IsProfitable 净收益是否达到最小门槛。
func IsProfitable(netProfit, minProfit *big.Int) bool {
	return netProfit.Cmp(minProfit) >= 0
}
