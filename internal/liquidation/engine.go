// Package liquidation Morpho 清算引擎。
// 第一版只做机会计算与排序（M7），执行合约在 M8。
// 开发顺序：先把 DEX 状态/报价/模拟做好，清算复用其抵押品退出路由。
package liquidation

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/morpho"
)

// Candidate 清算候选。
type Candidate struct {
	MarketID         common.Hash
	User             common.Address
	ObservedBlock    uint64
	HealthFactor     *big.Float
	RepayAssets      *big.Int
	SeizedCollateral *big.Int
	// 退出路径成本
	FlashLoanCost     *big.Int
	SwapCost          *big.Int
	GasCost           *big.Int
	ExpectedNetProfit *big.Int
	Decision          string
	RejectReason      string
}

// Sink 候选落盘接口（与套利一致：拒绝的也要记录）。
type Sink interface {
	SaveLiquidationCandidate(ctx context.Context, c *Candidate) error
}

// Evaluator 清算评估。
type Evaluator interface {
	Evaluate(ctx context.Context, c *Candidate) (string, string)
}

// Engine 清算引擎：候选排序 + 评估。MVP 骨架。
type Engine struct {
	indexer *morpho.Indexer
	sink    Sink
	eval    Evaluator
}

func NewEngine(ix *morpho.Indexer, sink Sink, eval Evaluator) *Engine {
	return &Engine{indexer: ix, sink: sink, eval: eval}
}

// Scan 扫描可清算仓位并按 ExpectedNetProfit 排序。
func (e *Engine) Scan(ctx context.Context) []*Candidate {
	positions := e.indexer.LiquidatablePositions(ctx)
	cands := make([]*Candidate, 0, len(positions))
	for _, p := range positions {
		c := &Candidate{
			MarketID:     p.MarketID,
			User:         p.User,
			HealthFactor: big.NewFloat(0.9), // 简化，M7 接入真实计算
			RepayAssets:  p.BorrowShares,
		}
		if e.eval != nil {
			c.Decision, c.RejectReason = e.eval.Evaluate(ctx, c)
		}
		cands = append(cands, c)
	}
	// 排序：净收益降序（M7 排序规则的第一步）
	for i := 1; i < len(cands); i++ {
		for j := i; j > 0 && netLess(cands[j-1], cands[j]); j-- {
			cands[j-1], cands[j] = cands[j], cands[j-1]
		}
	}
	for _, c := range cands {
		if e.sink != nil {
			_ = e.sink.SaveLiquidationCandidate(ctx, c)
		}
	}
	return cands
}

func netLess(a, b *Candidate) bool {
	an, bn := big.NewInt(0), big.NewInt(0)
	if a.ExpectedNetProfit != nil {
		an = a.ExpectedNetProfit
	}
	if b.ExpectedNetProfit != nil {
		bn = b.ExpectedNetProfit
	}
	return an.Cmp(bn) < 0
}
