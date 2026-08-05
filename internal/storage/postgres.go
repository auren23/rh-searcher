// Package storage PostgreSQL 持久化：事件、机会、执行结果。
// 实时池状态在内存；PostgreSQL 用于恢复、审计与研究。
// 不要每收到一个 Swap 就同步写十几张表。
package storage

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/auren23/rh-searcher/internal/arbitrage"
	"github.com/auren23/rh-searcher/internal/liquidation"
)

// DB 存储句柄。
type DB struct {
	pool *pgxpool.Pool
}

func New(ctx context.Context, url string) (*DB, error) {
	p, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := p.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &DB{pool: p}, nil
}

func (d *DB) Close() { d.pool.Close() }

// SaveCandidate 落盘套利候选（含拒绝的）。
func (d *DB) SaveCandidate(ctx context.Context, c *arbitrage.Candidate) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO arbitrage_opportunities (
			strategy, observed_block, observed_at, source_event,
			input_asset, input_amount, gross_profit, gas_estimate,
			swap_cost, slippage_cost, expected_net_profit,
			simulation_result, decision, reject_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
		"weth-2hop", c.ObservedBlock, c.ObservedAt, c.SourceEvent,
		c.InputAsset.Hex(), c.InputAmount.String(), c.GrossProfit.String(),
		c.GasEstimate.String(), c.SwapCost.String(), c.SlippageCost.String(),
		nullableWei(c.ExpectedNetProfit), c.SimulationResult, c.Decision, c.RejectReason,
	)
	return err
}

// SaveLiquidationCandidate 落盘清算候选。
func (d *DB) SaveLiquidationCandidate(ctx context.Context, c *liquidation.Candidate) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO liquidation_candidates (
			market_id, user_address, observed_block,
			repay_assets, seized_collateral, expected_net_profit, decision, reject_reason
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		c.MarketID.Hex(), c.User.Hex(), c.ObservedBlock,
		c.RepayAssets.String(), c.SeizedCollateral.String(),
		nullableWei(c.ExpectedNetProfit), c.Decision, c.RejectReason,
	)
	return err
}

// SaveBlock 记录区块（用于断点恢复与审计）。
func (d *DB) SaveBlock(ctx context.Context, number uint64, hash string) error {
	_, err := d.pool.Exec(ctx,
		`INSERT INTO blocks (number, hash) VALUES ($1,$2) ON CONFLICT (number) DO NOTHING`,
		number, hash)
	return err
}

// LatestBlock 最近记录的区块高度。
func (d *DB) LatestBlock(ctx context.Context) (uint64, error) {
	var n uint64
	err := d.pool.QueryRow(ctx, `SELECT COALESCE(MAX(number),0) FROM blocks`).Scan(&n)
	return n, err
}

func nullableWei(v interface{ String() string }) *string {
	if v == nil {
		return nil
	}
	s := v.String()
	return &s
}
