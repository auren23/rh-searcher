-- 0002: shadow 运行时 schema 修复（对已存在的旧库增量执行；0001 已含这些列）
-- 用法：psql -f 0002_shadow_schema_fix.sql

ALTER TABLE dex_pools
    ADD COLUMN IF NOT EXISTS tick_spacing INT NOT NULL DEFAULT 0;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS block_hash TEXT,
    ADD COLUMN IF NOT EXISTS log_index BIGINT,
    ADD COLUMN IF NOT EXISTS route_json TEXT;

-- 0001 早期版本 opportunities.id 是 BIGSERIAL；统一为 TEXT（候选 ID = keccak）
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'opportunities' AND column_name = 'id'
          AND data_type = 'bigint'
    ) THEN
        ALTER TABLE opportunities ALTER COLUMN id TYPE TEXT;
    END IF;
END $$;

-- 外键类型对齐（若旧库 opportunity_id 仍是 BIGINT）
ALTER TABLE execution_attempts
    ALTER COLUMN opportunity_id TYPE TEXT
    USING opportunity_id::TEXT;
