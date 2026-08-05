package dex

import (
	"github.com/ethereum/go-ethereum/common"
)

// Graph 池图：token 邻接表，用于循环路径搜索。
type Graph struct {
	adj map[common.Address][]PoolRef
}

func NewGraph() *Graph {
	return &Graph{adj: make(map[common.Address][]PoolRef)}
}

// AddPool 把池作为两个方向的边加入图。
func (g *Graph) AddPool(p Pool, addr common.Address) {
	g.adj[p.Token0] = append(g.adj[p.Token0], PoolRef{
		Address: addr, Exchange: p.Exchange, Protocol: p.Protocol, Fee: p.Fee,
		Token0: p.Token0, Token1: p.Token1, TokenInIsToken0: true,
	})
	g.adj[p.Token1] = append(g.adj[p.Token1], PoolRef{
		Address: addr, Exchange: p.Exchange, Protocol: p.Protocol, Fee: p.Fee,
		Token0: p.Token0, Token1: p.Token1, TokenInIsToken0: false,
	})
}

// FindCycles 找从 start 出发、经过 startPool 的 start→TOKEN→start 循环（两跳）。
// 返回的 Route 按执行顺序排列，每条恰好包含 startPool 一跳。
func (g *Graph) FindCycles(start common.Address, startPool common.Address) []Route {
	out := []Route{}
	seen := make(map[string]struct{})
	for _, r0 := range g.adj[start] {
		if r0.Address != startPool {
			continue // 必须包含触发池
		}
		mid := r0.other()
		if mid == start {
			continue
		}
		for _, r1 := range g.adj[mid] {
			if r1.Address == startPool {
				continue // 原池折返不算
			}
			if r1.other() != start {
				continue // 第二跳必须回到 WETH
			}
			key := r0.Address.Hex() + "/" + r1.Address.Hex()
			if _, dup := seen[key]; dup {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, Route{
				TokenIn:  start,
				TokenOut: start,
				Pools:    []PoolRef{r0, r1},
				Path:     []common.Address{start, mid, start},
			})
		}
	}
	return out
}

// other 返回这条边的另一端 token。
func (r PoolRef) other() common.Address {
	if r.TokenInIsToken0 {
		return r.Token1
	}
	return r.Token0
}
