package morpho

import (
	"context"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// Indexer Morpho 市场/仓位索引。MVP 提供内存维护接口，
// 链上事件接入在 M6 完成（events.go 占位）。
type Indexer struct {
	markets   map[common.Hash]*Market
	positions map[common.Hash]map[common.Address]*Position
}

func NewIndexer() *Indexer {
	return &Indexer{
		markets:   make(map[common.Hash]*Market),
		positions: make(map[common.Hash]map[common.Address]*Position),
	}
}

func (ix *Indexer) UpsertMarket(m *Market) {
	ix.markets[m.ID] = m
	slog.Debug("morpho market upsert", "id", m.ID.Hex(), "block", m.UpdatedAtBlock)
}

func (ix *Indexer) Market(id common.Hash) *Market { return ix.markets[id] }

func (ix *Indexer) Markets() []*Market {
	out := make([]*Market, 0, len(ix.markets))
	for _, m := range ix.markets {
		out = append(out, m)
	}
	return out
}

func (ix *Indexer) UpsertPosition(p *Position) {
	if ix.positions[p.MarketID] == nil {
		ix.positions[p.MarketID] = make(map[common.Address]*Position)
	}
	ix.positions[p.MarketID][p.User] = p
}

func (ix *Indexer) Position(market common.Hash, user common.Address) *Position {
	if m := ix.positions[market]; m != nil {
		return m[user]
	}
	return nil
}

// LiquidatablePositions 返回健康因子 <= 1 的仓位（M7 候选队列输入）。
func (ix *Indexer) LiquidatablePositions(ctx context.Context) []*Position {
	out := []*Position{}
	for marketID, users := range ix.positions {
		m := ix.markets[marketID]
		if m == nil {
			continue
		}
		for _, p := range users {
			if p.BorrowShares.Sign() <= 0 {
				continue
			}
			borrow := ToAssetsUp(p.BorrowShares, m.TotalBorrowAssets, m.TotalBorrowShares)
			hf := HealthFactor(p.Collateral, m.OraclePrice, borrow)
			if hf.Cmp(big.NewFloat(1)) <= 0 {
				out = append(out, p)
			}
		}
	}
	return out
}
