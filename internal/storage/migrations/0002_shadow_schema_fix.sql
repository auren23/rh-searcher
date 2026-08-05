-- 0002: shadow 运行时 schema 修复（对已存在的旧库增量执行；0001 已含这些列）
-- 用法：psql -v ON_ERROR_STOP=1 -f 0002_shadow_schema_fix.sql
BEGIN;

ALTER TABLE dex_pools
    ADD COLUMN IF NOT EXISTS tick_spacing INT NOT NULL DEFAULT 0;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS block_hash TEXT,
    ADD COLUMN IF NOT EXISTS log_index BIGINT,
    ADD COLUMN IF NOT EXISTS route_json TEXT,
    ADD COLUMN IF NOT EXISTS simulated_profit_wei NUMERIC,
    ADD COLUMN IF NOT EXISTS gas_used BIGINT,
    ADD COLUMN IF NOT EXISTS gas_price_wei NUMERIC,
    ADD COLUMN IF NOT EXISTS gas_cost_wei NUMERIC,
    ADD COLUMN IF NOT EXISTS calldata_hash TEXT,
    ADD COLUMN IF NOT EXISTS state_block BIGINT,
    ADD COLUMN IF NOT EXISTS simulation_block BIGINT;

-- 0001 早期版本 opportunities.id 是 BIGSERIAL：统一为 TEXT（候选 ID = keccak）
-- 必须先断开子表外键并去掉 sequence 默认值，再改类型，最后重建外键。
ALTER TABLE execution_attempts
    DROP CONSTRAINT IF EXISTS execution_attempts_opportunity_id_fkey;

ALTER TABLE opportunities
    ALTER COLUMN id DROP DEFAULT;

ALTER TABLE opportunities
    ALTER COLUMN id TYPE TEXT
    USING id::TEXT;

ALTER TABLE execution_attempts
    ALTER COLUMN opportunity_id TYPE TEXT
    USING opportunity_id::TEXT;

ALTER TABLE execution_attempts
    ADD CONSTRAINT execution_attempts_opportunity_id_fkey
    FOREIGN KEY (opportunity_id) REFERENCES opportunities(id);

COMMIT;
