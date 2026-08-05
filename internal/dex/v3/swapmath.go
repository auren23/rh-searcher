package v3

import (
	"math/big"
)

// swapmath 移植自 Uniswap V3 SqrtPriceMath.sol / FullMath.sol（v1.0.0）。
// 所有函数与 EVM 舍入行为一致（mulDiv 向上/向下、两次除法路径）。

var (
	q96  = new(big.Int).Lsh(big.NewInt(1), 96)
	oneU = big.NewInt(1)
)

// mulDivDown = floor(a*b/denominator)，无 512 位溢出限制（big.Int）。
func mulDivDown(a, b, denominator *big.Int) *big.Int {
	if denominator.Sign() == 0 {
		panic("denominator zero")
	}
	return new(big.Int).Div(new(big.Int).Mul(a, b), denominator)
}

// mulDivUp = ceil(a*b/denominator)。
func mulDivUp(a, b, denominator *big.Int) *big.Int {
	num := new(big.Int).Mul(a, b)
	// ceil = (num + denom - 1) / denom
	num.Add(num, new(big.Int).Sub(denominator, oneU))
	return num.Div(num, denominator)
}

// getNextSqrtPriceFromAmount0RoundingUp：token0 变化后的 sqrtPriceX96。
// add=false 表示 token0 被买入（swap zeroForOne），价格上升。
func getNextSqrtPriceFromAmount0RoundingUp(sqrtPX96, liquidity, amount *big.Int, add bool) *big.Int {
	if amount.Sign() == 0 {
		return new(big.Int).Set(sqrtPX96)
	}
	numerator1 := new(big.Int).Lsh(liquidity, 96)
	if add {
		// Q' = ceil(n1*Q / (n1 + x*Q))
		product := new(big.Int).Mul(amount, sqrtPX96)
		if new(big.Int).Div(product, amount).Cmp(sqrtPX96) == 0 {
			denominator := new(big.Int).Add(numerator1, product)
			if denominator.Cmp(numerator1) >= 0 {
				return mulDivUp(numerator1, sqrtPX96, denominator)
			}
		}
		// 溢出回退：ceil(n1 / (n1/Q + x))
		denom := new(big.Int).Div(numerator1, sqrtPX96)
		denom.Add(denom, amount)
		return divRoundingUp(numerator1, denom)
	}
	// Q' = ceil(n1*Q / (n1 - x*Q))
	product := new(big.Int).Mul(amount, sqrtPX96)
	if new(big.Int).Div(product, amount).Cmp(sqrtPX96) == 0 && numerator1.Cmp(product) > 0 {
		denominator := new(big.Int).Sub(numerator1, product)
		return mulDivUp(numerator1, sqrtPX96, denominator)
	}
	// 分母下溢：回退公式
	denom := new(big.Int).Div(numerator1, sqrtPX96)
	denom.Add(denom, amount)
	return divRoundingUp(numerator1, denom)
}

// getNextSqrtPriceFromAmount1RoundingDown：token1 变化后的 sqrtPriceX96。
// add=true 表示 token1 被买入（swap oneForZero），价格下降。
func getNextSqrtPriceFromAmount1RoundingDown(sqrtPX96, liquidity, amount *big.Int, add bool) *big.Int {
	if add {
		// Q' = Q + x*2^96/L
		quotient := mulDivDown(amount, q96, liquidity)
		return new(big.Int).Add(sqrtPX96, quotient)
	}
	// Q' = Q - ceil(x*2^96/L)
	quotient := divRoundingUp(new(big.Int).Lsh(amount, 96), liquidity)
	if sqrtPX96.Cmp(quotient) <= 0 {
		panic("sqrt price underflow")
	}
	return new(big.Int).Sub(sqrtPX96, quotient)
}

// getAmount0Delta：给定价格区间与流动性的 token0 数量（roundDown 用于输出）。
func getAmount0Delta(sqrtRatioAX96, sqrtRatioBX96, liquidity *big.Int, roundUp bool) *big.Int {
	if sqrtRatioAX96.Cmp(sqrtRatioBX96) > 0 {
		sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96
	}
	if sqrtRatioAX96.Sign() == 0 {
		panic("sqrt ratio zero")
	}
	numerator1 := new(big.Int).Lsh(liquidity, 96)
	numerator2 := new(big.Int).Sub(sqrtRatioBX96, sqrtRatioAX96)
	if roundUp {
		inner := mulDivUp(numerator1, numerator2, sqrtRatioBX96)
		return divRoundingUp(inner, sqrtRatioAX96)
	}
	inner := mulDivDown(numerator1, numerator2, sqrtRatioBX96)
	return new(big.Int).Div(inner, sqrtRatioAX96)
}

// getAmount1Delta：给定价格区间与流动性的 token1 数量（roundDown 用于输出）。
func getAmount1Delta(sqrtRatioAX96, sqrtRatioBX96, liquidity *big.Int, roundUp bool) *big.Int {
	if sqrtRatioAX96.Cmp(sqrtRatioBX96) > 0 {
		sqrtRatioAX96, sqrtRatioBX96 = sqrtRatioBX96, sqrtRatioAX96
	}
	dQ := new(big.Int).Sub(sqrtRatioBX96, sqrtRatioAX96)
	if roundUp {
		return mulDivUp(liquidity, dQ, q96)
	}
	return mulDivDown(liquidity, dQ, q96)
}

func divRoundingUp(a, b *big.Int) *big.Int {
	num := new(big.Int).Set(a)
	num.Add(num, new(big.Int).Sub(b, oneU))
	return num.Div(num, b)
}
