package v3

import (
	"math/big"
)

// TickMath 移植自 Uniswap V3 TickMath.sol（v1.0.0）。
// 用 big.Int 做定点运算，行为与 EVM 舍入一致。

const (
	MinTick = -887272
	MaxTick = 887272
)

// tickConsts 是 1.0001^(2^k) 的 Q128 定点常量（官方 TickMath.sol 原值）。
var tickConsts = map[uint]string{
	0x1:     "0xfffcb933bd6fad37aa2d162d1a594001",
	0x2:     "0xfff97272373d413259a46990580e213a",
	0x4:     "0xfff2e50f5f656932ef12357cf3c7fdcc",
	0x8:     "0xffe5caca7e10e4e61c3624eaa0941cd0",
	0x10:    "0xffcb9843d60f6159c9db58835c926644",
	0x20:    "0xff973b41fa98c081472e6896dfb254c0",
	0x40:    "0xff2ea16466c96a3843ec78b326b52861",
	0x80:    "0xfe5dee046a99a2a811c461f1969c3053",
	0x100:   "0xfcbe86c7900a88aedcffc83b479aa3a4",
	0x200:   "0xf987a7253ac413176f2b074cf7815e54",
	0x400:   "0xf3392b0822b70005940c7a398e4b70f3",
	0x800:   "0xe7159475a2c29b7443b29c7fa6e889d9",
	0x1000:  "0xd097f3bdfd2022b8845ad8f792aa5825",
	0x2000:  "0xa9f746462d870fdf8a65dc1f90e061e5",
	0x4000:  "0x70d869a156d2a1b890bb3df62baf32f7",
	0x8000:  "0x31be135f97d08fd981231505542fcfa6",
	0x10000: "0x9aa508b5b7a84e1c677de54f3e99bc9",
	0x20000: "0x5d6af8dedb81196699c329225ee604",
	0x40000: "0x2216e584f5fa1ea926041bedfe98",
	0x80000: "0x48a170391f7dc42444e8fa2",
}

var (
	oneQ128 = new(big.Int).Lsh(big.NewInt(1), 128) // 2^128
	maxUint = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
)

var sqrtTable [20]*big.Int

func init() {
	for k := 0; k < 20; k++ {
		sqrtTable[k] = new(big.Int)
		sqrtTable[k].SetString(tickConsts[uint(1)<<uint(k)], 0)
	}
}

// GetSqrtRatioAtTick 返回 tick 对应的 sqrtPriceX96（Q64.96，官方算法）。
func GetSqrtRatioAtTick(tick int) *big.Int {
	absTick := tick
	if absTick < 0 {
		absTick = -absTick
	}
	if absTick > MaxTick {
		panic("tick out of range")
	}
	ratio := new(big.Int).Set(oneQ128)
	for k := 0; k < 20; k++ {
		if absTick&(1<<uint(k)) != 0 {
			ratio.Mul(ratio, sqrtTable[k])
			ratio.Rsh(ratio, 128)
		}
	}
	if tick > 0 {
		// ratio = type(uint256).max / ratio（官方取倒数）
		ratio.Div(maxUint, ratio)
	}
	// sqrtPriceX96 = (ratio >> 32) + (ratio % 2^32 == 0 ? 0 : 1)
	shifted := new(big.Int).Rsh(ratio, 32)
	rem := new(big.Int).Mod(ratio, new(big.Int).Lsh(big.NewInt(1), 32))
	if rem.Sign() != 0 {
		shifted.Add(shifted, big.NewInt(1))
	}
	return shifted
}

// TickSpacingBounds 返回 tick 所在间距区间的边界（含：tickLower = floor(tick/spacing)*spacing）。
func TickSpacingBounds(tick, spacing int) (int, int) {
	lower := tick / spacing
	if tick < 0 && tick%spacing != 0 {
		lower-- // floor 除法（Go 的 / 是截断）
	}
	return lower * spacing, lower*spacing + spacing
}
