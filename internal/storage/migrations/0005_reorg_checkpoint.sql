-- 0005: checkpoint 记录区块 hash 与 parent hash（reorg 检测与回退）
BEGIN;

ALTER TABLE strategy_checkpoints
    ADD COLUMN IF NOT EXISTS block_hash TEXT,
    ADD COLUMN IF NOT EXISTS parent_hash TEXT;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS canonical BOOLEAN NOT NULL DEFAULT TRUE;

COMMIT;
