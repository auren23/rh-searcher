package v3

import "github.com/auren23/rh-searcher/internal/dex"

// stateAdapter 把 *Pool 包装为 dex.PoolState。
type stateAdapter struct{ p *Pool }

func (s stateAdapter) Pool() dex.Pool { return s.p.Pool() }

// State 把 *Pool 包装为 dex.PoolState。
func State(p *Pool) dex.PoolState { return stateAdapter{p: p} }

// UnwrapState 从 dex.PoolState 取出 *Pool（兼容 stateAdapter 包装与裸指针）。
func UnwrapState(s dex.PoolState) *Pool {
	if a, ok := s.(stateAdapter); ok {
		return a.p
	}
	if p, ok := s.(*Pool); ok {
		return p
	}
	return nil
}
