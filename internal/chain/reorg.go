package chain

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// ReorgTracker 检测区块重组：维护最近 N 个区块的 (number -> hash)，
// 若新头的高度已存在且 hash 不同，说明发生了重组。
type ReorgTracker struct {
	mu     sync.RWMutex
	heads  map[uint64]common.Hash
	window int
}

func NewReorgTracker(window int) *ReorgTracker {
	return &ReorgTracker{heads: make(map[uint64]common.Hash, window*2), window: window}
}

// Observe 返回重组深度（0 = 无重组）。遇到重组时清除受影响高度。
func (t *ReorgTracker) Observe(ev BlockEvent) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	if prev, ok := t.heads[ev.Number]; ok && prev != ev.Hash {
		depth := 0
		for n := ev.Number; n > 0 && n > ev.Number-uint64(t.window); n-- {
			if h, ok := t.heads[n]; ok && h != ev.Hash {
				depth++
			} else {
				break
			}
		}
		t.heads = make(map[uint64]common.Hash, t.window*2)
		t.heads[ev.Number] = ev.Hash
		return depth
	}
	t.heads[ev.Number] = ev.Hash
	if len(t.heads) > t.window*2 {
		// 简单裁剪：只保留最高 window 个高度
		drop := len(t.heads) - t.window
		for n := range t.heads {
			if drop <= 0 {
				break
			}
			if n < ev.Number-uint64(t.window) {
				delete(t.heads, n)
				drop--
			}
		}
	}
	return 0
}

// ContinuityGap 返回上次连续高度到当前高度之间缺失的区块区间。
// 供断线补块使用。
type ContinuityGap struct {
	From uint64 // 缺失区间的起始（不含 last）
	To   uint64 // 缺失区间的结束（含）
}

// GapDetector 维护已观察高度的连续性。
type GapDetector struct {
	mu       sync.Mutex
	lastSeen uint64
	haveSeen bool
}

func NewGapDetector() *GapDetector { return &GapDetector{} }

// Observe 记录新区块高度，返回需要补齐的缺口（无缺口返回 nil）。
func (g *GapDetector) Observe(n uint64) *ContinuityGap {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.haveSeen {
		g.lastSeen = n
		g.haveSeen = true
		return nil
	}
	if n <= g.lastSeen {
		return nil
	}
	if n > g.lastSeen+1 {
		gap := &ContinuityGap{From: g.lastSeen + 1, To: n - 1}
		g.lastSeen = n
		return gap
	}
	g.lastSeen = n
	return nil
}

func (g *GapDetector) LastSeen() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lastSeen
}
