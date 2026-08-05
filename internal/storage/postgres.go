// Package storage PostgreSQL 持久化：事件、机会、执行结果。
// 实时池状态在内存；PostgreSQL 用于恢复、审计与研究。
// 不要每收到一个 Swap 就同步写十几张表。
package storage

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
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
	db := &DB{pool: p}
	if err := db.checkSchema(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return db, nil
}

// checkSchema 版本检查：确认 opportunities 已含 shadow 运行所需列。
// 旧库需手动执行 migrations/0001..0004（代码不会自动迁移）。
func (d *DB) checkSchema(ctx context.Context) error {
	var has bool
	err := d.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'opportunities' AND column_name = 'opportunity_group_id'
		)`).Scan(&has)
	if err != nil {
		return fmt.Errorf("schema check: %w", err)
	}
	if !has {
		return fmt.Errorf("database schema out of date: run migrations 0001-0004 " +
			"(internal/storage/migrations/) against this database before starting")
	}
	return nil
}

func (d *DB) Close() { d.pool.Close() }

// SaveCandidate 落盘套利候选（含拒绝的；完整可重放字段）。
func (d *DB) SaveCandidate(ctx context.Context, c *arbitrage.Candidate) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO opportunities (
			id, strategy, observed_block, observed_at, source_event,
			block_hash, tx_hash, log_index, route_json,
			input_asset, input_amount, gross_profit, gas_estimate,
			swap_cost, slippage_cost, expected_net_profit,
			simulation_result, decision, reject_reason,
			simulated_profit_wei, gas_used, gas_price_wei, gas_cost_wei,
			calldata_hash, state_block, simulation_block,
			opportunity_group_id, rank, selected,
			l1_gas_units, l2_base_fee_wei, l1_base_fee_estimate_wei
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32)
		ON CONFLICT (id) DO NOTHING`,
		c.ID, "weth-2hop", c.ObservedBlock, c.ObservedAt, c.SourceEvent,
		c.BlockHash.Hex(), c.TxHash.Hex(), c.LogIndex, c.RouteJSON,
		c.InputAsset.Hex(), c.InputAmount.String(), c.GrossProfit.String(),
		c.GasEstimate.String(), c.SwapCost.String(), c.SlippageCost.String(),
		nullableBigInt(c.ExpectedNetProfit), c.SimulationResult, c.Decision, c.RejectReason,
		nullableBigInt(c.SimulatedProfitWei), nullableUint64(c.GasUsed),
		nullableBigInt(c.GasPriceWei), nullableBigInt(c.GasCostWei),
		nullableStr(c.CalldataHash), nullableUint64(c.StateBlock), nullableUint64(c.SimulationBlock),
		nullableStr(c.OpportunityGroupID), nullableInt(c.Rank), c.Selected,
		nullableUint64(c.L1GasUnits), nullableBigInt(c.L2BaseFeeWei), nullableBigInt(c.L1BaseFeeEstimateWei),
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
		nullableBigInt(c.ExpectedNetProfit), c.Decision, c.RejectReason,
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

// SavePool 落盘池元数据（dex_pools）。供重启恢复 Registry/Graph。
func (d *DB) SavePool(ctx context.Context, address string, exchange, protocol string, token0, token1 common.Address, fee uint32, tickSpacing int) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO dex_pools (address, exchange, protocol, token0, token1, fee, tick_spacing)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (address) DO UPDATE SET
			exchange = EXCLUDED.exchange, protocol = EXCLUDED.protocol,
			token0 = EXCLUDED.token0, token1 = EXCLUDED.token1, fee = EXCLUDED.fee,
			tick_spacing = EXCLUDED.tick_spacing`,
		address, exchange, protocol, token0.Hex(), token1.Hex(), fee, tickSpacing)
	return err
}

// Pool 恢复用的池元数据。
type Pool struct {
	Address     string
	Exchange    string
	Protocol    string
	Token0      string
	Token1      string
	Fee         uint32
	TickSpacing int
}

// LoadPools 读取全部池元数据（启动恢复）。tick_spacing 缺失时（旧数据）返回 0，由调用方补查。
func (d *DB) LoadPools(ctx context.Context) ([]Pool, error) {
	rows, err := d.pool.Query(ctx, `SELECT address, exchange, protocol, token0, token1, fee, COALESCE(tick_spacing, 0) FROM dex_pools`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Pool{}
	for rows.Next() {
		var p Pool
		if err := rows.Scan(&p.Address, &p.Exchange, &p.Protocol, &p.Token0, &p.Token1, &p.Fee, &p.TickSpacing); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func nullableUint64(v uint64) *uint64 {
	if v == 0 {
		return nil
	}
	return &v
}

func nullableInt(v int) *int {
	if v == 0 {
		return nil
	}
	return &v
}

func nullableStr(v string) *string {
	if v == "" {
		return nil
	}
	return &v
}

// nullableBigInt 显式 *big.Int：nil 指针返回 NULL。
// 不能用 interface{}（*big.Int(nil) 装箱后接口非 nil，String() 输出 "<nil>" 会破坏 NUMERIC 列）。
func nullableBigInt(v *big.Int) *string {
	if v == nil {
		return nil
	}
	s := v.String()
	return &s
}
