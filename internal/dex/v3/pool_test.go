package v3

import (
	"math/big"
	"testing"
)

func bitmapPool(tick, spacing int) *Pool {
	return &Pool{
		Tick: tick, TickSpacing: spacing,
		ticks:        map[int]*Tick{},
		bitmap:       map[int64]*big.Int{},
		bitmapLoaded: map[int64]bool{},
	}
}

// 官方 TickBitmap 语义：向上搜索从 position(compressed+1) 开始。
func TestNextInitializedTickUpward(t *testing.T) {
	spacing := 60
	p := bitmapPool(100*spacing, spacing) // compressed = 100
	// word 0：bit 105 set（下一 initialized compressed = 105）
	p.bitmap[0] = new(big.Int).Lsh(big.NewInt(1), 105)
	p.bitmapLoaded[0] = true
	next, found := p.nextInitializedTick(100*spacing, false)
	if !found || next != 105*spacing {
		t.Errorf("upward: got tick=%d found=%v, want %d (off-by-one)", next, found, 105*spacing)
	}

	// 紧邻：当前 compressed=104，下一 bit=105
	p.Tick = 104 * spacing
	next, found = p.nextInitializedTick(104*spacing, false)
	if !found || next != 105*spacing {
		t.Errorf("adjacent: got tick=%d found=%v, want %d", next, found, 105*spacing)
	}

	// 无下一 bit：word 最后一位（255）
	p2 := bitmapPool(100*spacing, spacing)
	p2.bitmap[0] = new(big.Int) // 已加载但全 0
	p2.bitmapLoaded[0] = true
	next, found = p2.nextInitializedTick(100*spacing, false)
	if found || next != (100+1+255-101)*spacing {
		t.Errorf("no next: got tick=%d found=%v, want word-end %d", next, found, (100+1+255-101)*spacing)
	}

	// 负 compressed 的向上搜索（word -1；compressed=-100 → next=-99，bitPos=157）
	// bit 200 set → next compressed = -99 + 200 - 157 = -56
	p3 := bitmapPool(-100*spacing, spacing)
	p3.bitmap[-1] = new(big.Int).Lsh(big.NewInt(1), 200)
	p3.bitmapLoaded[-1] = true
	next, found = p3.nextInitializedTick(-100*spacing, false)
	if !found || next != -56*spacing {
		t.Errorf("negative: got tick=%d found=%v, want %d", next, found, -56*spacing)
	}
}

func TestNextInitializedTickDownward(t *testing.T) {
	spacing := 60
	p := bitmapPool(100*spacing, spacing)
	// bit 98 set（compressed=98）
	p.bitmap[0] = new(big.Int).Lsh(big.NewInt(1), 98)
	p.bitmapLoaded[0] = true
	next, found := p.nextInitializedTick(100*spacing, true)
	if !found || next != 98*spacing {
		t.Errorf("downward: got tick=%d found=%v, want %d", next, found, 98*spacing)
	}

	// 含当前位：当前 compressed=98 自身 set
	p2 := bitmapPool(98*spacing, spacing)
	p2.bitmap[0] = new(big.Int).Lsh(big.NewInt(1), 98)
	p2.bitmapLoaded[0] = true
	next, found = p2.nextInitializedTick(98*spacing, true)
	if !found || next != 98*spacing {
		t.Errorf("self: got tick=%d found=%v, want %d", next, found, 98*spacing)
	}
}

// 未加载的 word：必须返回保守边界而非猜测。
func TestNextInitializedTickUnloaded(t *testing.T) {
	p := bitmapPool(100*60, 60)
	next, found := p.nextInitializedTick(100*60, false)
	if found {
		t.Errorf("unloaded word must not report initialized")
	}
	lo, hi := TickSpacingBounds(100*60, 60)
	if next != hi {
		t.Errorf("upward unloaded: got %d want spacing-upper %d", next, hi)
	}
	next, found = p.nextInitializedTick(100*60, true)
	if found || next != lo {
		t.Errorf("downward unloaded: got %d want spacing-lower %d", next, lo)
	}
}
