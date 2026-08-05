package v3

import (
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/auren23/rh-searcher/internal/dex"
)

// 报价错误：绝不使用近似值产生"可交易机会"。
var (
	// ErrTickCrossed 输入量会跨过下一 initialized tick（MVP 严格单 tick 模式）。
	ErrTickCrossed = errors.New("quote crosses initialized tick")
	// ErrStateIncomplete 池状态不完整（无 slot0/流动性），无法报价。
	ErrStateIncomplete = errors.New("pool state incomplete")
)

// Tick 一个 initialized tick 的流动性净变化。
type Tick struct {
	LiquidityNet *big.Int // 跨过该 tick（向更高价）时流动性变化
}

// Pool 内存中的 V3 池状态。
type Pool struct {
	Address      common.Address
	Exchange     string
	Token0       common.Address
	Token1       common.Address
	Fee          uint32
	TickSpacing  int
	Tick         int
	Liquidity    *big.Int
	SqrtPriceX96 *big.Int

	ticks         map[int]*Tick      // tick -> net liquidity（事件流增量维护）
	bitmap        map[int64]*big.Int // wordPos -> 256bit 位图
	ObservedBlock uint64
}

// NewPoolFromMeta 从持久化元数据构造池（ticks/bitmap 惰性初始化）。
func NewPoolFromMeta(address common.Address, exchange string, token0, token1 common.Address, fee uint32) *Pool {
	return &Pool{
		Address: address, Exchange: exchange,
		Token0: token0, Token1: token1, Fee: fee,
		ticks:  make(map[int]*Tick),
		bitmap: make(map[int64]*big.Int),
	}
}

func (p *Pool) Pool() dex.Pool {
	return dex.Pool{
		ID: p.Address.Hex(), Protocol: "v3", Exchange: p.Exchange,
		Token0: p.Token0, Token1: p.Token1, Fee: p.Fee,
		Liquidity: p.Liquidity, SqrtPriceX96: p.SqrtPriceX96, Tick: p.Tick,
	}
}

// clone 深拷贝（事件应用前使用，避免并发读竞争）。
func (p *Pool) clone() *Pool {
	np := *p
	np.Liquidity = new(big.Int).Set(p.Liquidity)
	np.SqrtPriceX96 = new(big.Int).Set(p.SqrtPriceX96)
	np.ticks = make(map[int]*Tick, len(p.ticks))
	for k, v := range p.ticks {
		np.ticks[k] = &Tick{LiquidityNet: new(big.Int).Set(v.LiquidityNet)}
	}
	np.bitmap = make(map[int64]*big.Int, len(p.bitmap))
	for k, v := range p.bitmap {
		np.bitmap[k] = new(big.Int).Set(v)
	}
	return &np
}

// compressedTick tick 按 spacing 压缩（向下取整）。
func (p *Pool) compressedTick(tick int) int {
	c := tick / p.TickSpacing
	if tick < 0 && tick%p.TickSpacing != 0 {
		c--
	}
	return c
}

// flipTick 翻转位图（与官方 TickBitmap.flipTick 一致，纯 XOR）。
// 注意：事件流中途接入时 bitmap 可能错位（历史 flip 缺失）；QuoteExactIn 的
// spacing 边界兜底保证不会因此产生越界报价。tick 的 net liquidity 单独累计。
func (p *Pool) flipTick(tick int) {
	compressed := p.compressedTick(tick)
	wordPos := int64(compressed >> 8)
	bitPos := uint(compressed & 0xff)
	mask := new(big.Int).Lsh(big.NewInt(1), bitPos)
	word := p.bitmap[wordPos]
	if word == nil {
		word = new(big.Int)
		p.bitmap[wordPos] = word
	}
	word.Xor(word, mask)
}

// addLiquidityNet 更新 tick 的 net liquidity（链上 Tick.update 的 liquidityNet 部分）。
func (p *Pool) addLiquidityNet(tick int, delta *big.Int) {
	t := p.ticks[tick]
	if t == nil {
		t = &Tick{LiquidityNet: new(big.Int)}
		p.ticks[tick] = t
	}
	t.LiquidityNet.Add(t.LiquidityNet, delta)
}

// nextInitializedTick 找当前 tick 相邻的 initialized tick（单 word 内，与官方一致）。
// lte=true 向下找；false 向上找。未找到时返回 spacing 区间边界（保守兜底）。
func (p *Pool) nextInitializedTick(tick int, lte bool) (int, bool) {
	compressed := p.compressedTick(tick)
	wordPos := int64(compressed >> 8)
	bitPos := uint(compressed & 0xff)
	word := p.bitmap[wordPos]
	if word == nil {
		word = new(big.Int)
	}
	if lte {
		// mask = (1 << bitPos) - 1 + (1 << bitPos)：含当前位
		mask := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), bitPos), big.NewInt(1))
		mask.Or(mask, new(big.Int).Lsh(big.NewInt(1), bitPos))
		masked := new(big.Int).And(word, mask)
		if masked.Sign() != 0 {
			msb := masked.BitLen() - 1
			return (compressed - int(bitPos) + msb) * p.TickSpacing, true
		}
		return (compressed - int(bitPos)) * p.TickSpacing, false
	}
	// 向上：从 bitPos+1 起找
	mask := new(big.Int).Lsh(big.NewInt(1), bitPos+1)
	masked := new(big.Int).And(word, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), mask))
	if masked.Sign() != 0 {
		lsb := masked.TrailingZeroBits()
		return (compressed + 1 + int(lsb) - int(bitPos)) * p.TickSpacing, true
	}
	return (compressed + 1 + 255 - int(bitPos)) * p.TickSpacing, false
}

// ApplySwap 应用 Swap 事件（用事件里的 sqrtPriceX96/liquidity/tick 直接覆盖）。
func (p *Pool) ApplySwap(log types.Log, sqrtPriceX96, liquidity *big.Int, tick int) {
	p.SqrtPriceX96 = new(big.Int).Set(sqrtPriceX96)
	p.Liquidity = new(big.Int).Set(liquidity)
	p.Tick = tick
	p.ObservedBlock = log.BlockNumber
}

// ApplyMintBurn 应用 Mint/Burn：维护 tick net liquidity；仅当前 tick 在 [tickLower, tickUpper) 内时改 active liquidity。
// 返回是否需要更新 active liquidity（由调用方决定是否读链上确认）。
func (p *Pool) ApplyMintBurn(log types.Log, tickLower, tickUpper int, amount *big.Int, isMint bool) {
	delta := new(big.Int).Set(amount)
	if !isMint {
		delta.Neg(delta)
	}
	p.flipTick(tickLower)
	p.flipTick(tickUpper)
	p.addLiquidityNet(tickLower, delta)
	p.addLiquidityNet(tickUpper, new(big.Int).Neg(delta))
	// active liquidity 仅在当前 tick 位于 [tickLower, tickUpper) 内时变化
	// ponytail: 事件流中 tick 是处理时的已知值（可能略滞后于事件块），shadow 阶段可接受
	if p.Tick >= tickLower && p.Tick < tickUpper {
		if p.Liquidity == nil {
			p.Liquidity = new(big.Int)
		}
		p.Liquidity.Add(p.Liquidity, delta)
	}
	p.ObservedBlock = log.BlockNumber
}
