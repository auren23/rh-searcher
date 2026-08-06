// Package storage PostgreSQL 持久化：事件、机会、执行结果。
// 实时池状态在内存；PostgreSQL 用于恢复、审计与研究。
// 不要每收到一个 Swap 就同步写十几张表。
package storage

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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

// Querier 查询接口（checkSchema / verifySchemaAt0011 共用，便于 migrate.go 复用）。
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// verifySchemaAt0011 校验数据库结构完整到达 0011（表/索引/列/默认值）。
// Legacy baseline 与启动检查共用同一套判断——不允许两套不一致的结构认知。
func verifySchemaAt0011(ctx context.Context, q Querier) error {
	var checks int
	expect := 12
	// 5 张核心业务表
	for _, tbl := range []string{"opportunities", "dex_pools", "strategy_checkpoints",
		"processed_blocks", "block_affected_pools"} {
		var ok bool
		if err := q.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM information_schema.tables
				WHERE table_schema='public' AND table_name=$1)`, tbl).Scan(&ok); err != nil {
			return err
		}
		if ok {
			checks++
		}
	}
	// 0004 唯一索引
	var idx bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname='uq_opportunities_group_selected')
	`).Scan(&idx); err != nil {
		return err
	}
	if idx {
		checks++
	}
	// 0005: strategy_checkpoints.hash 列 + opportunities.canonical
	var h, p, canon bool
	if err := q.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='strategy_checkpoints' AND column_name='block_hash'),
			EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='strategy_checkpoints' AND column_name='parent_hash'),
			EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='opportunities' AND column_name='canonical')
	`).Scan(&h, &p, &canon); err != nil {
		return err
	}
	if h && p && canon {
		checks += 3
	}
	// 0007: dex_pools 三列
	var nPoolCols int
	if err := q.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name='dex_pools'
		  AND column_name IN ('canonical', 'created_block', 'created_block_hash')
	`).Scan(&nPoolCols); err != nil {
		return err
	}
	if nPoolCols == 3 {
		checks++
	}
	// 0009: provenance_source
	var prov bool
	if err := q.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM information_schema.columns
			WHERE table_name='dex_pools' AND column_name='provenance_source')
	`).Scan(&prov); err != nil {
		return err
	}
	if prov {
		checks++
	}
	// 0011: gas_estimate_mode 默认 not_estimated
	var def string
	if err := q.QueryRow(ctx, `
		SELECT COALESCE(MAX(column_default), '') FROM information_schema.columns
		WHERE table_name='opportunities' AND column_name='gas_estimate_mode'
	`).Scan(&def); err != nil {
		return err
	}
	if strings.Contains(def, "not_estimated") {
		checks++
	}
	if checks != expect {
		return fmt.Errorf("schema not at 0011: %d/%d structure checks passed", checks, expect)
	}
	return nil
}

// 旧库需手动执行 migrations/0001..0004（代码不会自动迁移）。
func (d *DB) checkSchema(ctx context.Context) error {
	// 结构部分（0001..0011）与 legacy baseline 共用同一套判断
	if err := verifySchemaAt0011(ctx, d.pool); err != nil {
		return fmt.Errorf("database schema out of date: %w", err)
	}
	// 版本门控：全部必须版本都在场（MAX 门槛无法发现中间缺失）。
	// 另检查不存在高于程序支持的未知版本（降级风险）。
	var present, unknown int
	if err := d.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE version = ANY($1)),
			COUNT(*) FILTER (WHERE version > $2)
		FROM schema_migrations
	`, requiredVersions, requiredSchemaVersion).Scan(&present, &unknown); err != nil {
		return fmt.Errorf("schema check: %w", err)
	}
	if present != len(requiredVersions) {
		return fmt.Errorf("database schema out of date: %d/%d required migrations recorded "+
			"(want up to %s)", present, len(requiredVersions), requiredSchemaVersion)
	}
	if unknown > 0 {
		return fmt.Errorf("database schema ahead of this binary: %d unknown migrations > %s",
			unknown, requiredSchemaVersion)
	}
	return nil
}

func (d *DB) Close() { d.pool.Close() }

// CommitCandidatesForExistingBlock 只落盘候选（补扫聚合评估用）：
// 不更新 checkpoint / processed_blocks / pools——这些已被逐块提交过，
// 避免把已正确的 parent hash 覆盖为空。
func (d *DB) CommitCandidatesForExistingBlock(ctx context.Context, block uint64, hash string, candidates []*arbitrage.Candidate) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, c := range candidates {
		if err := insertCandidateTx(ctx, tx, c); err != nil {
			return fmt.Errorf("candidate %s: %w", c.ID, err)
		}
	}
	return tx.Commit(ctx)
}

// SaveCandidate 落盘套利候选（含拒绝的；完整可重放字段）。
func (d *DB) SaveCandidate(ctx context.Context, c *arbitrage.Candidate) error {
	return insertCandidateTx(ctx, d.pool, c)
}

func insertCandidateTx(ctx context.Context, q interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}, c *arbitrage.Candidate) error {
	_, err := q.Exec(ctx, `
		INSERT INTO opportunities (
			id, strategy, observed_block, observed_at, source_event,
			block_hash, tx_hash, log_index, route_json,
			input_asset, input_amount, gross_profit, gas_estimate,
			swap_cost, slippage_cost, expected_net_profit,
			simulation_result, decision, reject_reason,
			simulated_profit_wei, gas_used, gas_price_wei, gas_cost_wei,
			calldata_hash, state_block, simulation_block,
			opportunity_group_id, rank, selected,
			l1_gas_units, l2_base_fee_wei, l1_base_fee_estimate_wei,
			gas_estimate_mode
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33)
		ON CONFLICT (id) DO NOTHING`,
		c.ID, StrategyArbitrage, c.ObservedBlock, c.ObservedAt, c.SourceEvent,
		c.BlockHash.Hex(), c.TxHash.Hex(), c.LogIndex, c.RouteJSON,
		c.InputAsset.Hex(), c.InputAmount.String(), c.GrossProfit.String(),
		c.GasEstimate.String(), c.SwapCost.String(), c.SlippageCost.String(),
		nullableBigInt(c.ExpectedNetProfit), c.SimulationResult, c.Decision, c.RejectReason,
		nullableBigInt(c.SimulatedProfitWei), nullableUint64(c.GasUsed),
		nullableBigInt(c.GasPriceWei), nullableBigInt(c.GasCostWei),
		nullableStr(c.CalldataHash), nullableUint64(c.StateBlock), nullableUint64(c.SimulationBlock),
		nullableStr(c.OpportunityGroupID), nullableInt(c.Rank), c.Selected,
		nullableUint64(c.L1GasUnits), nullableBigInt(c.L2BaseFeeWei), nullableBigInt(c.L1BaseFeeEstimateWei),
		strOr(c.GasEstimateMode, "not_estimated"),
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

// CommitBlockResult 在单个 PostgreSQL 事务内提交一个区块的全部结果：
// 新池 + 候选 + checkpoint（含 hash）。任一步失败整体回滚 ——
// 游标只有在事务提交成功后才允许前进（exactly-once 的落盘侧）。
// CommitBlockIngest 在单个事务内提交区块摄取结果：
// 新池（含 PoolCreated 溯源）+ 受影响池（评估队列）+ 区块 checkpoint（含 hash）
// + 规范区块历史。任一写入失败整体回滚——ingest 游标不前进。
func (d *DB) CommitBlockIngest(ctx context.Context, block uint64, blockHash, parentHash string,
	pools []Pool, affectedPools []string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	for _, sp := range pools {
		createdHash := sp.CreatedBlockHash
		if isUnknownHash(createdHash) {
			createdHash = blockHash // 无真实 PoolCreated 日志时兜底（观察块）
		}
		createdBlock := sp.CreatedBlock
		if createdBlock == 0 {
			createdBlock = block // 观察块兜底（与 hash 一致，不产生矛盾数据）
		}
		source := sp.ProvenanceSource
		if source == "" {
			source = "observed_swap_fallback"
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dex_pools (address, exchange, protocol, token0, token1, fee, tick_spacing,
				canonical, created_block, created_block_hash, provenance_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7, TRUE, $8, $9, $10)
			ON CONFLICT (address) DO UPDATE SET
				exchange = EXCLUDED.exchange, protocol = EXCLUDED.protocol,
				token0 = EXCLUDED.token0, token1 = EXCLUDED.token1,
				fee = EXCLUDED.fee, tick_spacing = EXCLUDED.tick_spacing,
				canonical = TRUE,
				-- 溯源升级必须原子：incoming=pool_created_log 且现有非权威时，
				-- block/hash/source 一起替换（否则观察块会被"认证"成权威来源）
				created_block = CASE
					WHEN COALESCE(EXCLUDED.provenance_source, '') = 'pool_created_log'
						AND COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					THEN EXCLUDED.created_block
					WHEN dex_pools.created_block IS NULL THEN EXCLUDED.created_block
					ELSE dex_pools.created_block END,
				created_block_hash = CASE
					WHEN COALESCE(EXCLUDED.provenance_source, '') = 'pool_created_log'
						AND COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					THEN EXCLUDED.created_block_hash
					WHEN dex_pools.created_block_hash IS NULL THEN EXCLUDED.created_block_hash
					ELSE dex_pools.created_block_hash END,
				provenance_source = CASE
					WHEN COALESCE(EXCLUDED.provenance_source, '') = 'pool_created_log'
						AND COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					THEN 'pool_created_log'
					ELSE dex_pools.provenance_source END`,
			sp.Address, sp.Exchange, sp.Protocol, sp.Token0, sp.Token1, sp.Fee, sp.TickSpacing,
			createdBlock, nullableStr(createdHash), source); err != nil {
			return fmt.Errorf("pool %s: %w", sp.Address, err)
		}
	}
	// 受影响池持久化（评估队列；ingest 提交后崩溃仍可重新聚合评估）
	for _, pa := range affectedPools {
		if _, err := tx.Exec(ctx, `
			INSERT INTO block_affected_pools (strategy, block_number, block_hash, pool_address)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (strategy, block_hash, pool_address) DO NOTHING`,
			CheckpointBlocks, block, blockHash, pa); err != nil {
			return fmt.Errorf("affected pool %s: %w", pa, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, parent_hash, updated_at)
		VALUES ('` + CheckpointBlocks + `', $1, $2, $3, now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			updated_at = now()`,
		block, nullableStr(blockHash), nullableStr(parentHash)); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}
	// 规范区块历史（reorg 共同祖先查找）；同 hash 重处理 → 恢复 canonical
	if _, err := tx.Exec(ctx, `
		INSERT INTO processed_blocks (strategy, block_number, block_hash, parent_hash, canonical)
		VALUES ('` + CheckpointBlocks + `', $1, $2, COALESCE($3, ''), TRUE)
		ON CONFLICT (strategy, block_hash) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			parent_hash = EXCLUDED.parent_hash,
			canonical = TRUE`,
		block, nullableStr(blockHash), nullableStr(parentHash)); err != nil {
		return fmt.Errorf("processed_blocks: %w", err)
	}
	return tx.Commit(ctx)
}

// CommitEvaluation 在单个事务内提交评估结果：
// 候选 + evaluate checkpoint。失败 → evaluate 游标不前进，重启后重新评估同一批
// （ingest 已提交的受影响池仍在队列里）。
func (d *DB) CommitEvaluation(ctx context.Context, block uint64, blockHash string, candidates []*arbitrage.Candidate) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, c := range candidates {
		if err := insertCandidateTx(ctx, tx, c); err != nil {
			return fmt.Errorf("candidate %s: %w", c.ID, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, updated_at)
		VALUES ('` + CheckpointEvaluate + `', $1, $2, now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			updated_at = now()`,
		block, nullableStr(blockHash)); err != nil {
		return fmt.Errorf("evaluate checkpoint: %w", err)
	}
	return tx.Commit(ctx)
}

// PendingBlock 一个待评估区块（固定状态评估的单位）。
type PendingBlock struct {
	Block uint64
	Hash  string
	Pools []string // 块内去重
}

// LoadPendingAffected 读取 evaluate 游标之后尚未评估的区块队列（按块升序）。
// 每块独立评估（固定 stateBlock = 该块）——不能跨块聚合，
// 否则丢失"当时的机会"且双游标恢复不完整。
func (d *DB) LoadPendingAffected(ctx context.Context, fromBlock uint64) ([]PendingBlock, error) {
	rows, err := d.pool.Query(ctx, `
		SELECT block_number, block_hash, pool_address
		FROM block_affected_pools
		WHERE strategy = $1 AND block_number > $2
		ORDER BY block_number ASC, pool_address ASC`,
		CheckpointBlocks, fromBlock)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PendingBlock{}
	idx := map[uint64]int{}
	for rows.Next() {
		var n uint64
		var h, pa string
		if err := rows.Scan(&n, &h, &pa); err != nil {
			return nil, err
		}
		i, ok := idx[n]
		if !ok {
			i = len(out)
			idx[n] = i
			out = append(out, PendingBlock{Block: n, Hash: h})
		}
		pools := out[i].Pools
		if len(pools) > 0 && pools[len(pools)-1] == pa {
			continue
		}
		out[i].Pools = append(pools, pa)
	}
	return out, rows.Err()
}

// QueryRow 执行单行查询（reorg 祖先查找用）。
func (d *DB) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return d.pool.QueryRow(ctx, sql, args...)
}

// requiredSchemaVersion 启动要求的最高迁移版本（0014 统一旧 historical 命名）。
const requiredSchemaVersion = "0014"

// requiredVersions 启动要求的完整迁移版本集合（任何中间缺失都拒绝启动）。
var requiredVersions = []string{
	"0001", "0002", "0003", "0004", "0005", "0006", "0007",
	"0008", "0009", "0010", "0011", "0012", "0013", "0014",
}

// RollbackToAncestor：reorg 单事务回滚——
// 1) processed_blocks 标孤块；2) 本策略候选标孤块（不碰其他策略）；
// 3) checkpoint 回退到共同祖先（含 hash）。全部成功才返回，失败由调用方失败关闭。
func (d *DB) RollbackToAncestor(ctx context.Context, strategy string, ancestor uint64, hash, parent string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		UPDATE processed_blocks SET canonical = FALSE
		WHERE strategy = $1 AND canonical = TRUE AND block_number > $2`,
		strategy, ancestor); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE opportunities SET canonical = FALSE
		WHERE strategy = $1 AND canonical = TRUE
		  AND GREATEST(
				observed_block,
				COALESCE(state_block, 0),
				COALESCE(simulation_block, 0)
			  ) > $2`,
		StrategyArbitrage, ancestor); err != nil {
		return err
	}
	// 孤块中创建并持久化的池标记非规范（Restore 时过滤，不进入 Graph）。
	// 精确判定：池的 created_block_hash 必须落在本次孤块列表里（block_number > ancestor）
	// ——池可能创建于祖先之前、只是首次观察在孤块高度（历史 reorg 留下的
	// canonical=false 区块 hash 不参与，避免重启后误标）。
	if _, err := tx.Exec(ctx, `
		UPDATE dex_pools SET canonical = FALSE
		WHERE canonical = TRUE
		  AND created_block_hash IS NOT NULL
		  AND created_block_hash IN (
			SELECT block_hash FROM processed_blocks
			WHERE strategy = $1 AND canonical = FALSE AND block_number > $2
		  )`,
		strategy, ancestor); err != nil {
		return err
	}
	// 孤块评估队列清理 + evaluate 游标回退（未评估的批次作废，从祖先重新开始）
	if _, err := tx.Exec(ctx, `
		DELETE FROM block_affected_pools
		WHERE strategy = $1 AND block_number > $2`,
		strategy, ancestor); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, parent_hash, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			updated_at = now()`,
		strategy, ancestor, nullableStr(hash), nullableStr(parent)); err != nil {
		return err
	}
	// evaluate 游标只退不进：落后于祖先的部分保留（仍要评估），
	// 只有超过祖先的部分回退（LEAST）
	if _, err := tx.Exec(ctx, `
		UPDATE strategy_checkpoints
		SET block_number = LEAST(block_number, $1),
			block_hash = CASE WHEN block_number > $1 THEN $2 ELSE block_hash END,
			updated_at = now()
		WHERE strategy = '` + CheckpointEvaluate + `'`,
		ancestor, nullableStr(hash)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// InitializeBlockCheckpoint：空库首次启动原子写入（checkpoint + 规范区块历史），
// 保证首次 reorg 的祖先查找能找到初始链头。
func (d *DB) InitializeBlockCheckpoint(ctx context.Context, strategy string, block uint64, hash, parent string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, parent_hash, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			updated_at = now()`,
		strategy, block, nullableStr(hash), nullableStr(parent)); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO processed_blocks (strategy, block_number, block_hash, parent_hash, canonical)
		VALUES ($1, $2, $3, COALESCE($4, ''), TRUE)
		ON CONFLICT (strategy, block_hash) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			parent_hash = EXCLUDED.parent_hash,
			canonical = TRUE`,
		strategy, block, hash, nullableStr(parent)); err != nil {
		return err
	}
	// evaluate 游标同步初始化（无待评估批次）
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, updated_at)
		VALUES ('` + CheckpointEvaluate + `', $1, $2, now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			updated_at = now()`,
		block, nullableStr(hash)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Exec 透出连接执行（reorg 标记等运维操作）。
func (d *DB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return d.pool.Exec(ctx, sql, args...)
}

// CommitPools 在单个事务内保存全部池 + 更新 pools checkpoint。
// 任一池失败整体回滚 → pools 游标不会越过未落库的池。
func (d *DB) CommitPools(ctx context.Context, pools []Pool, checkpointBlock uint64) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, sp := range pools {
		// 真实创建溯源（DiscoverPools 从 PoolCreated 日志读取）；
		// 无真实信息时兜底 bootstrap 结束块，但 hash 保持未知（不造假）
		createdBlock := sp.CreatedBlock
		if createdBlock == 0 {
			createdBlock = checkpointBlock
		}
		createdHash := sp.CreatedBlockHash
		if isUnknownHash(createdHash) {
			createdHash = ""
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dex_pools (address, exchange, protocol, token0, token1, fee, tick_spacing,
				canonical, created_block, created_block_hash, provenance_source)
			VALUES ($1,$2,$3,$4,$5,$6,$7, TRUE, NULLIF($8, 0), NULLIF($9, ''), NULLIF($10, ''))
			ON CONFLICT (address) DO UPDATE SET
				exchange = EXCLUDED.exchange, protocol = EXCLUDED.protocol,
				token0 = EXCLUDED.token0, token1 = EXCLUDED.token1,
				fee = EXCLUDED.fee, tick_spacing = EXCLUDED.tick_spacing,
				canonical = TRUE,
				-- 覆盖规则：PoolCreated 日志（权威）覆盖非权威占位。
				-- 占位包括 NULL/0/全零 hash（未知）与观察块兜底
				created_block = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					AND (dex_pools.created_block IS NULL OR dex_pools.created_block = 0
						OR dex_pools.provenance_source = 'observed_swap_fallback')
					THEN EXCLUDED.created_block ELSE dex_pools.created_block END,
				created_block_hash = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					AND (dex_pools.created_block_hash IS NULL
						OR dex_pools.created_block_hash = ''
						OR dex_pools.created_block_hash = '0x0000000000000000000000000000000000000000000000000000000000000000'
						OR dex_pools.provenance_source = 'observed_swap_fallback')
					THEN EXCLUDED.created_block_hash ELSE dex_pools.created_block_hash END,
				provenance_source = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
					AND COALESCE(EXCLUDED.provenance_source, '') = 'pool_created_log'
					THEN 'pool_created_log' ELSE dex_pools.provenance_source END`,
			sp.Address, sp.Exchange, sp.Protocol, sp.Token0, sp.Token1, sp.Fee, sp.TickSpacing,
			createdBlock, nullableStr(createdHash), sp.ProvenanceSource); err != nil {
			return fmt.Errorf("pool %s: %w", sp.Address, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, updated_at)
		VALUES ($1, $2, now())
		ON CONFLICT (strategy) DO UPDATE SET block_number = EXCLUDED.block_number, updated_at = now()`,
		CheckpointPools, checkpointBlock); err != nil {
		return fmt.Errorf("pools checkpoint: %w", err)
	}
	return tx.Commit(ctx)
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
// SavePool 落盘池元数据（dex_pools）。供重启恢复 Registry/Graph。
// createdBlock/createdHash 来自 Factory PoolCreated 日志（0/空表示未知）。
func (d *DB) SavePool(ctx context.Context, address string, exchange, protocol string, token0, token1 common.Address, fee uint32, tickSpacing int, createdBlock uint64, createdHash string) error {
	_, err := d.pool.Exec(ctx, `
		INSERT INTO dex_pools (address, exchange, protocol, token0, token1, fee, tick_spacing,
			canonical, created_block, created_block_hash, provenance_source)
		VALUES ($1,$2,$3,$4,$5,$6,$7, TRUE, $8, $9, 'pool_created_log')
		ON CONFLICT (address) DO UPDATE SET
			exchange = EXCLUDED.exchange, protocol = EXCLUDED.protocol,
			token0 = EXCLUDED.token0, token1 = EXCLUDED.token1, fee = EXCLUDED.fee,
			tick_spacing = EXCLUDED.tick_spacing,
			canonical = TRUE,
			created_block = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
				THEN EXCLUDED.created_block ELSE dex_pools.created_block END,
			created_block_hash = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
				THEN EXCLUDED.created_block_hash ELSE dex_pools.created_block_hash END,
			provenance_source = CASE WHEN COALESCE(dex_pools.provenance_source, '') <> 'pool_created_log'
				THEN 'pool_created_log' ELSE dex_pools.provenance_source END`,
		address, exchange, protocol, token0.Hex(), token1.Hex(), fee, tickSpacing,
		createdBlock, nullableStr(createdHash))
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
	// 创建溯源（来自 Factory PoolCreated；空表示未知）
	CreatedBlock     uint64
	CreatedBlockHash string
	// ProvenanceSource: "pool_created_log" | "observed_swap_fallback" | ""
	ProvenanceSource string
}

// LoadPools 读取全部池元数据（启动恢复）。tick_spacing 缺失时（旧数据）返回 0，由调用方补查。
func (d *DB) LoadPools(ctx context.Context) ([]Pool, error) {
	rows, err := d.pool.Query(ctx, `SELECT address, exchange, protocol, token0, token1, fee,
		COALESCE(tick_spacing, 0), COALESCE(created_block, 0), COALESCE(created_block_hash, ''), COALESCE(provenance_source, '')
		FROM dex_pools WHERE canonical = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Pool{}
	for rows.Next() {
		var p Pool
		if err := rows.Scan(&p.Address, &p.Exchange, &p.Protocol, &p.Token0, &p.Token1, &p.Fee,
			&p.TickSpacing, &p.CreatedBlock, &p.CreatedBlockHash, &p.ProvenanceSource); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// isUnknownHash 判断创建 hash 是否未知（空串或全零——动态池零值 .Hex() 不是空串）。
func isUnknownHash(s string) bool {
	if s == "" || s == "0x0000000000000000000000000000000000000000000000000000000000000000" {
		return true
	}
	return false
}

func strOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
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
