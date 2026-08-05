package v3

import (
	"math/big"
	"testing"
)

// 金标准：真实链上 slot0 的 (tick, sqrtPriceX96) 对（Robinhood mainnet，2026-08-05 抓取）。
// 注意：V3 的 tick 是离散的（区间边界），价格在区间内连续浮动，因此合法约束是
//   SPX(tick) <= Q < SPX(tick+1)  （即 getTickAtSqrtRatio(Q) == tick）
// 而不是 SPX(tick) == Q。GetSqrtRatioAtTick 的 EVM 舍入级正确性
// 由 python mpmath 高精度独立验证（本文件 TestTickMathPythonCrossCheck）。
var goldenSlot0 = []struct {
	name string
	tick int
	sqrt string // hex, 无 0x
}{
	{"WETH/Safevano-10000", 204092, "6985f185dea904b2320c20842b14"},
	{"WETH/meme-3000", 28825, "439d0c93ad31857988338dd5d"},
	{"WETH/meme-3000-negative", -6666, "b7714068ed5b517174962a75"},
}

func TestTickMathGolden(t *testing.T) {
	for _, g := range goldenSlot0 {
		q, ok := new(big.Int).SetString(g.sqrt, 16)
		if !ok {
			t.Fatalf("bad fixture %s", g.sqrt)
		}
		lo := GetSqrtRatioAtTick(g.tick)
		hi := GetSqrtRatioAtTick(g.tick + 1)
		if q.Cmp(lo) < 0 || q.Cmp(hi) >= 0 {
			t.Errorf("%s: tick=%d slot0 Q=%s outside [SPX(tick)=%s, SPX(tick+1)=%s)",
				g.name, g.tick, q.Text(16), lo.Text(16), hi.Text(16))
		}
	}
}

// python mpmath 高精度独立验证值（floor(2^96 * 1.0001^(tick/2))）。
// 允许 ±1 的 EVM 舍入差异。
var pythonCrossCheck = []struct {
	tick int
	sqrt string
}{
	{1, "1000346d6ff11672ae55ad00f"},
	{-1, "fffcb933bd6fad37aa2d162d"},
	{-6666, "b770f19eb34389829ed65d07"},
	{28825, "439c58a6e0dbc1778dc575077"},
}

func TestTickMathPythonCrossCheck(t *testing.T) {
	for _, c := range pythonCrossCheck {
		got := GetSqrtRatioAtTick(c.tick)
		want, _ := new(big.Int).SetString(c.sqrt, 16)
		diff := new(big.Int).Sub(got, want)
		diff.Abs(diff)
		if diff.Cmp(big.NewInt(1)) > 0 {
			t.Errorf("tick=%d: got %s want %s diff %s", c.tick, got.Text(16), want.Text(16), diff.String())
		}
	}
}

func TestTickMathEdges(t *testing.T) {
	if got := GetSqrtRatioAtTick(0); got.Text(16) != "1000000000000000000000000" {
		t.Errorf("tick=0: got %s, want 2^96", got.Text(16))
	}
	// 官方常量：MIN_SQRT_RATIO = 4295128739，MAX_SQRT_RATIO = 1461446703485210103287273052203988822378723970342
	if got := GetSqrtRatioAtTick(MaxTick); got.String() != "1461446703485210103287273052203988822378723970342" {
		t.Errorf("tick=%d: got %s, want official MAX_SQRT_RATIO", MaxTick, got.String())
	}
	if got := GetSqrtRatioAtTick(MinTick); got.String() != "4295128739" {
		t.Errorf("tick=%d: got %s, want official MIN_SQRT_RATIO", MinTick, got.String())
	}
}

// 双向报价：构造对称池，两个方向输出应一致（同价格同流动性，0.1% 手续费对称）。
func TestQuoteBidirectional(t *testing.T) {
	p := &Pool{
		Token0:      addr(1),
		Token1:      addr(2),
		Fee:         3000,
		TickSpacing: 60,
		Tick:        30, // 区间 [0,60) 中间，两个方向都有空间
		SqrtPriceX96: GetSqrtRatioAtTick(30),
		Liquidity:   big.NewInt(1_000_000_000_000_000_000), // 1e18
		ticks:       make(map[int]*Tick),
		bitmap:      make(map[int64]*big.Int),
	}
	amount := big.NewInt(1_000_000) // 1e6 units
	out0, err := (&Adapter{}).QuoteExactIn(p, p.Token0, amount)
	if err != nil {
		t.Fatalf("token0->token1: %v", err)
	}
	out1, err := (&Adapter{}).QuoteExactIn(p, p.Token1, amount)
	if err != nil {
		t.Fatalf("token1->token0: %v", err)
	}
	if out0.Sign() <= 0 || out1.Sign() <= 0 {
		t.Fatalf("non-positive outputs: %s %s", out0.String(), out1.String())
	}
	// 价值守恒（价格 P = 1.0001^30 ≈ 1.003003）：
	//   out0(token1) ≈ amount_after_fee * P，out1(token0) ≈ amount_after_fee / P
	// 滑点使输出略低于无滑点值；允许 0.1% 偏差。
	price := new(big.Float).SetPrec(128)
	price.SetInt(GetSqrtRatioAtTick(30))
	price.Mul(price, new(big.Float).SetInt(GetSqrtRatioAtTick(30)))
	price.Quo(price, new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 192)))
	afterFee := big.NewFloat(997000)
	want0, _ := new(big.Float).Mul(afterFee, price).Int(nil)
	want1, _ := new(big.Float).Quo(afterFee, price).Int(nil)
	check := func(name string, got, want *big.Int) {
		d := new(big.Int).Sub(got, want)
		d.Abs(d)
		// 允许 0.1% + 1 的偏差（滑点 + 舍入）
		tol := new(big.Int).Div(want, big.NewInt(1000))
		tol.Add(tol, big.NewInt(1))
		if d.Cmp(tol) > 0 {
			t.Errorf("%s: got=%s want≈%s diff=%s", name, got.String(), want.String(), d.String())
		}
	}
	check("token0->token1", out0, want0)
	check("token1->token0", out1, want1)
}

// 跨 tick：输入超过下一 initialized tick 边界必须 ErrTickCrossed。
func TestQuoteTickCrossed(t *testing.T) {
	p := &Pool{
		Token0:      addr(1),
		Token1:      addr(2),
		Fee:         3000,
		TickSpacing: 60,
		Tick:        0,
		SqrtPriceX96: new(big.Int).Lsh(big.NewInt(1), 96),
		Liquidity:   big.NewInt(1_000_000), // 小流动性
		ticks:       make(map[int]*Tick),
		bitmap:      make(map[int64]*big.Int),
	}
	// 无 initialized tick → 兜底边界 = spacing 区间（tick 0 的区间 [0,60)）
	// 价格到 tick 60 只需要很少 token0；大输入必须被拒绝
	bigAmt := new(big.Int).Exp(big.NewInt(10), big.NewInt(24), nil)
	_, err := (&Adapter{}).QuoteExactIn(p, p.Token0, bigAmt)
	if err != ErrTickCrossed {
		t.Errorf("want ErrTickCrossed, got %v", err)
	}
	// 极小输入应成功
	small := big.NewInt(1)
	out, err := (&Adapter{}).QuoteExactIn(p, p.Token0, small)
	if err != nil || out.Sign() < 0 {
		t.Errorf("small input: err=%v out=%v", err, out)
	}
}

// 状态不完整 → ErrStateIncomplete。
func TestQuoteStateIncomplete(t *testing.T) {
	p := &Pool{Token0: addr(1), Token1: addr(2), Fee: 3000}
	_, err := (&Adapter{}).QuoteExactIn(p, p.Token0, big.NewInt(1))
	if err != ErrStateIncomplete {
		t.Errorf("want ErrStateIncomplete, got %v", err)
	}
}

func addr(n int) commonAddr {
	var a commonAddr
	a[19] = byte(n)
	return a
}

// 最小地址别名避免依赖 common 包全名
type commonAddr = [20]byte
