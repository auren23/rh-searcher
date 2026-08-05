package dex

import (
	"github.com/ethereum/go-ethereum/common"
)

// Graph 池图：token 邻接表，用于循环路径搜索。
type Graph struct {
	adj       map[common.Address][]PoolRef
	seenEdges map[string]struct{} // 池地址去重（启动时 Bootstrap/Restore 可能重复添加）
}

func NewGraph() *Graph {
	return &Graph{adj: make(map[common.Address][]PoolRef), seenEdges: make(map[string]struct{})}
}

// AddPool 把池作为两个方向的边加入图（同一池只加一次）。
func (g *Graph) AddPool(p Pool, addr common.Address) {
	if _, dup := g.seenEdges[addr.Hex()]; dup {
		return
	}
	g.seenEdges[addr.Hex()] = struct{}{}
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
// 触发池可以出现在第一跳或第二跳（两个方向都评估）。
func (g *Graph) FindCycles(start common.Address, startPool common.Address) []Route {
	out := []Route{}
	seen := make(map[string]struct{})
	add := func(r0, r1 PoolRef) {
		if r0.Address == r1.Address {
			return // 原池折返不算
		}
		mid := r0.other()
		if mid == start || r1.other() != start {
			return
		}
		key := r0.Address.Hex() + "/" + r1.Address.Hex()
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, Route{
			TokenIn:  start,
			TokenOut: start,
			Pools:    []PoolRef{r0, r1},
			Path:     []common.Address{start, mid, start},
		})
	}
	for _, r0 := range g.adj[start] {
		mid := r0.other()
		if mid == start {
			continue
		}
		for _, r1 := range g.adj[mid] {
			if r1.other() != start {
				continue
			}
			if r0.Address == startPool || r1.Address == startPool {
				add(r0, r1) // 触发池在第一跳或第二跳
			}
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
