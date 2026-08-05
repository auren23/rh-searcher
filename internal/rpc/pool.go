// Package rpc 管理多 RPC 池：读取、模拟、发送分组隔离，健康评分与故障切换。
// 核心原则：发送端与读取端分离，GMGN 快速 RPC 只作 Broadcaster，不进策略代码。
package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

// Group 表示一组相同用途的 RPC 端点（archive / read / sim / send）。
type Group struct {
	Name    string
	clients []*PoolClient
	mu      sync.Mutex
	rr      int // round-robin 游标
}

// PoolClient 是单个端点的客户端 + 健康状态。
type PoolClient struct {
	URL      string
	Client   *ethclient.Client
	RPC      *rpc.Client
	Health   float64 // 0..1 健康分，初始 1
	failures int
	lastSeen time.Time
}

// Pool 多 RPC 池。读取按健康分加权轮询，模拟与发送按组。
type Pool struct {
	groups map[string]*Group
	order  []string
}

func NewPool() *Pool {
	return &Pool{groups: make(map[string]*Group)}
}

// AddGroup 建立一组端点。失败端点跳过，至少一个可用才算成功。
func (p *Pool) AddGroup(ctx context.Context, name string, urls []string) error {
	g := &Group{Name: name}
	for _, u := range urls {
		rc, err := rpc.DialContext(ctx, u)
		if err != nil {
			continue // 不可达端点跳过
		}
		g.clients = append(g.clients, &PoolClient{
			URL:      u,
			Client:   ethclient.NewClient(rc),
			RPC:      rc,
			Health:   1,
			lastSeen: time.Now(),
		})
	}
	if len(g.clients) == 0 {
		return &NoHealthyEndpointError{Group: name}
	}
	p.groups[name] = g
	p.order = append(p.order, name)
	return nil
}

// Group 返回指定组。
func (p *Pool) Group(name string) *Group { return p.groups[name] }

// Pick 返回组内健康分最高的客户端（轮询打散）。
func (g *Group) Pick() *PoolClient {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.clients) == 0 {
		return nil
	}
	best := g.clients[0]
	for _, c := range g.clients {
		if c.Health > best.Health {
			best = c
		}
	}
	return best
}

// Record 记录一次调用结果，更新健康分。
func (c *PoolClient) Record(ok bool, dur time.Duration) {
	c.lastSeen = time.Now()
	if ok {
		c.failures = 0
		c.Health = min(1, c.Health+0.05)
		return
	}
	c.failures++
	c.Health = max(0, c.Health-0.2*float64(c.failures))
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

// NoHealthyEndpointError 组内无健康端点。
type NoHealthyEndpointError struct{ Group string }

func (e *NoHealthyEndpointError) Error() string { return "no healthy endpoint in group " + e.Group }
