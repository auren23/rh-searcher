package v3

import "github.com/auren23/rh-searcher/internal/dex"

// stateAdapter 把 *Pool 包装为 dex.PoolState。
type stateAdapter struct{ p *Pool }

func (s stateAdapter) Pool() dex.Pool { return s.p.Pool() }

// State 把 *Pool 包装为 dex.PoolState。
func State(p *Pool) dex.PoolState { return stateAdapter{p: p} }
