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
	// ErrStateIncomplete 池状态不完整（无 slot0/流动性/tickSpacing/bitmap 未知），无法报价。
	ErrStateIncomplete = errors.New("pool state incomplete")
)

// Tick 一个 initialized tick 的流动性。
type Tick struct {
	LiquidityGross *big.Int // 该 tick 的总流动性（跨零时才翻转 initialized 位）
	LiquidityNet   *big.Int // 跨过该 tick（向更高价）时流动性变化
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

	// 创建溯源：来自 Factory PoolCreated 日志（reorg 池回滚的精确依据）
	CreatedBlock     uint64
	CreatedBlockHash common.Hash

	ticks        map[int]*Tick     // tick -> 流动性数据
	bitmap       map[int64]*big.Int // wordPos -> 256bit 位图（仅已加载的 word）
	bitmapLoaded map[int64]bool    // 区分"未加载"与"真实为 0"；未加载的 word 不得当作空
	ObservedBlock uint64
}

// NewPoolFromMeta 从持久化元数据构造池（ticks/bitmap 惰性初始化）。
func NewPoolFromMeta(address common.Address, exchange string, token0, token1 common.Address, fee uint32, tickSpacing int) *Pool {
	return NewPoolFromMetaWithCreated(address, exchange, token0, token1, fee, tickSpacing, 0, common.Hash{})
}

// NewPoolFromMetaWithCreated 恢复池时携带真实创建溯源（历史资格过滤依赖）。
func NewPoolFromMetaWithCreated(address common.Address, exchange string, token0, token1 common.Address,
	fee uint32, tickSpacing int, createdBlock uint64, createdHash common.Hash) *Pool {
	return &Pool{
		Address: address, Exchange: exchange,
		Token0: token0, Token1: token1, Fee: fee, TickSpacing: tickSpacing,
		CreatedBlock: createdBlock, CreatedBlockHash: createdHash,
		ticks:        make(map[int]*Tick),
		bitmap:       make(map[int64]*big.Int),
		bitmapLoaded: make(map[int64]bool),
	}
}

func (p *Pool) Pool() dex.Pool {
	return dex.Pool{
		ID: p.Address.Hex(), Protocol: "v3", Exchange: p.Exchange,
		Token0: p.Token0, Token1: p.Token1, Fee: p.Fee,
		Liquidity: p.Liquidity, SqrtPriceX96: p.SqrtPriceX96, Tick: p.Tick,
	}
}

// Clone 深拷贝（事件应用前使用；commit 成功前不污染 Registry 状态）。
// cloneBig nil 安全拷贝（惰性 Pool 的 slot0/liquidity 在首次事件前可能为 nil）。
func cloneBig(v *big.Int) *big.Int {
	if v == nil {
		return nil
	}
	return new(big.Int).Set(v)
}

func (p *Pool) Clone() *Pool {
	np := *p
	np.Liquidity = cloneBig(p.Liquidity)
	np.SqrtPriceX96 = cloneBig(p.SqrtPriceX96)
	np.ticks = make(map[int]*Tick, len(p.ticks))
	for k, v := range p.ticks {
		if v == nil {
			np.ticks[k] = nil
			continue
		}
		np.ticks[k] = &Tick{
			LiquidityGross: cloneBig(v.LiquidityGross),
			LiquidityNet:   cloneBig(v.LiquidityNet),
		}
	}
	np.bitmap = make(map[int64]*big.Int, len(p.bitmap))
	for k, v := range p.bitmap {
		np.bitmap[k] = cloneBig(v)
	}
	np.bitmapLoaded = make(map[int64]bool, len(p.bitmapLoaded))
	for k, v := range p.bitmapLoaded {
		np.bitmapLoaded[k] = v
	}
	return &np
}

// WordPos 当前 tick 所在 bitmap word。
func (p *Pool) WordPos() int64 {
	return int64(p.compressedTick(p.Tick) >> 8)
}

// BitmapLoaded word 是否已从链上加载（区分未知与真实为 0）。
func (p *Pool) BitmapLoaded(wordPos int64) bool {
	if p.bitmapLoaded == nil {
		return false
	}
	return p.bitmapLoaded[wordPos]
}

// compressedTick tick 按 spacing 压缩（向下取整）。
func (p *Pool) compressedTick(tick int) int {
	c := tick / p.TickSpacing
	if tick < 0 && tick%p.TickSpacing != 0 {
		c--
	}
	return c
}

// updateTick 更新 tick 的 gross/net 流动性；gross 跨零边界时翻转 initialized 位。
// 与官方 Tick.update + TickBitmap.flipTick 语义一致。
// updateTick 按 gross/net 分别更新（Mint/Burn 语义不同，见 ApplyMintBurn）。
func (p *Pool) updateTick(tick int, grossDelta, netDelta *big.Int) {
	t := p.ticks[tick]
	if t == nil {
		t = &Tick{LiquidityGross: new(big.Int), LiquidityNet: new(big.Int)}
		p.ticks[tick] = t
	}
	grossBefore := new(big.Int).Set(t.LiquidityGross)
	t.LiquidityGross.Add(t.LiquidityGross, grossDelta)
	if grossBefore.Sign() == 0 && t.LiquidityGross.Sign() != 0 {
		// 0 → 非0：打开 initialized
		p.setBit(tick, true)
	} else if grossBefore.Sign() != 0 && t.LiquidityGross.Sign() == 0 {
		// 非0 → 0：关闭 initialized
		p.setBit(tick, false)
	}
	t.LiquidityNet.Add(t.LiquidityNet, netDelta)
}

// setBit 设置/清除 bit（不影响 bitmapLoaded：只对已加载 word 操作）。
func (p *Pool) setBit(tick int, on bool) {
	compressed := p.compressedTick(tick)
	wordPos := int64(compressed >> 8)
	bitPos := uint(compressed & 0xff)
	if !p.bitmapLoaded[wordPos] {
		return // 该 word 未加载：事件流无法重建历史位图，跳过（按需加载兜底）
	}
	word := p.bitmap[wordPos]
	if word == nil {
		word = new(big.Int)
		p.bitmap[wordPos] = word
	}
	mask := new(big.Int).Lsh(big.NewInt(1), bitPos)
	if on {
		word.Or(word, mask)
	} else {
		word.AndNot(word, mask)
	}
}

// nextInitializedTick 找当前 tick 相邻的 initialized tick（单 word 内，与官方一致）。
// lte=true 向下找；false 向上找。
// 返回值： (tick, found)。相邻 word 未加载时返回 (word 边界 tick, false)，
// 由调用方决定：found=false 时只能安全使用 spacing 区间边界（仍需经 QuoteExactIn 检查）。
func (p *Pool) nextInitializedTick(tick int, lte bool) (int, bool) {
	compressed := p.compressedTick(tick)
	wordPos := int64(compressed >> 8)
	bitPos := uint(compressed & 0xff)
	if !p.bitmapLoaded[wordPos] {
		// 未加载：保守返回 spacing 区间边界（不得当作真实 initialized tick）
		lo, hi := TickSpacingBounds(tick, p.TickSpacing)
		if lte {
			return lo, false
		}
		return hi, false
	}
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
	// 向上：官方语义 —— 从 position(compressed+1) 开始，mask 含当前 bitPos。
	// 当前 tick 自身状态无关紧要（lte=false 只关心严格更高的 tick）。
	nextCompressed := compressed + 1
	nextWordPos := int64(nextCompressed >> 8)
	nextBitPos := uint(nextCompressed & 0xff)
	if !p.bitmapLoaded[nextWordPos] {
		lo, hi := TickSpacingBounds(tick, p.TickSpacing)
		_ = lo
		return hi, false
	}
	nw := p.bitmap[nextWordPos]
	if nw == nil {
		nw = new(big.Int)
	}
	mask := new(big.Int).Lsh(big.NewInt(1), nextBitPos)
	masked := new(big.Int).And(nw, new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), mask))
	if masked.Sign() != 0 {
		lsb := masked.TrailingZeroBits()
		return (nextCompressed + int(lsb) - int(nextBitPos)) * p.TickSpacing, true
	}
	return (nextCompressed + 255 - int(nextBitPos)) * p.TickSpacing, false
}

// ApplySwap 应用 Swap 事件（用事件里的 sqrtPriceX96/liquidity/tick 直接覆盖）。
func (p *Pool) ApplySwap(log types.Log, sqrtPriceX96, liquidity *big.Int, tick int) {
	p.SqrtPriceX96 = new(big.Int).Set(sqrtPriceX96)
	p.Liquidity = new(big.Int).Set(liquidity)
	p.Tick = tick
	p.ObservedBlock = log.BlockNumber
}

// ApplyMintBurn 应用 Mint/Burn：更新 tick gross/net；active liquidity 仅当前 tick 在区间内时变化。
// ApplyMintBurn 应用 Mint/Burn 事件（Uniswap V3 语义）：
//
//	Mint:  lower gross +a, net +a；upper gross +a, net -a
//	Burn:  lower gross -a, net -a；upper gross -a, net +a
//
// gross 永远非负增长（只按位置累积），net 才是方向性的。
func (p *Pool) ApplyMintBurn(log types.Log, tickLower, tickUpper int, amount *big.Int, isMint bool) {
	a := new(big.Int).Set(amount)
	if isMint {
		p.updateTick(tickLower, a, a)
		p.updateTick(tickUpper, a, new(big.Int).Neg(a))
	} else {
		neg := new(big.Int).Neg(a)
		p.updateTick(tickLower, neg, neg)
		p.updateTick(tickUpper, neg, a)
	}
	if p.Tick >= tickLower && p.Tick < tickUpper {
		if p.Liquidity == nil {
			p.Liquidity = new(big.Int)
		}
		if isMint {
			p.Liquidity.Add(p.Liquidity, a)
		} else {
			p.Liquidity.Sub(p.Liquidity, a)
		}
	}
	p.ObservedBlock = log.BlockNumber
}
