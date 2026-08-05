package dex

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
)

// Registry 管理协议 adapter 与池索引。
type Registry struct {
	mu       sync.RWMutex
	adapters map[string]PoolAdapter              // protocol -> adapter
	pools    map[common.Address]PoolState        // 池地址 -> 状态
	byToken  map[common.Address][]common.Address // token -> 池列表
}

func NewRegistry() *Registry {
	return &Registry{
		adapters: make(map[string]PoolAdapter),
		pools:    make(map[common.Address]PoolState),
		byToken:  make(map[common.Address][]common.Address),
	}
}

func (r *Registry) Register(a PoolAdapter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.adapters[a.Protocol()] = a
}

func (r *Registry) Adapter(protocol string) PoolAdapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.adapters[protocol]
}

// UpsertPool 新增或更新池状态。
func (r *Registry) UpsertPool(state PoolState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := state.Pool()
	addr := common.HexToAddress(p.ID)
	r.pools[addr] = state
	r.byToken[p.Token0] = append(r.byToken[p.Token0], addr)
	r.byToken[p.Token1] = append(r.byToken[p.Token1], addr)
}

func (r *Registry) Pool(addr common.Address) PoolState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.pools[addr]
}

// PoolsForToken 返回包含该 token 的池。
func (r *Registry) PoolsForToken(token common.Address) []PoolState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	addrs := r.byToken[token]
	out := make([]PoolState, 0, len(addrs))
	for _, a := range addrs {
		if s, ok := r.pools[a]; ok {
			out = append(out, s)
		}
	}
	return out
}

// AllPools 全部池快照。
func (r *Registry) AllPools() []PoolState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]PoolState, 0, len(r.pools))
	for _, s := range r.pools {
		out = append(out, s)
	}
	return out
}
