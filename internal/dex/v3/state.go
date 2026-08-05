package v3

import "github.com/auren23/rh-searcher/internal/dex"

// State 把 *Pool 包装为 dex.PoolState。
func State(p *Pool) dex.PoolState { return stateAdapter{p: p} }
