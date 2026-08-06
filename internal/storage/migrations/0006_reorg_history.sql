-- 0006_reorg_history.sql
-- 规范区块历史：reorg 时按高度对比新旧链 hash 找真正共同祖先（processed_blocks）。
CREATE TABLE IF NOT EXISTS processed_blocks (
    strategy     TEXT    NOT NULL,
    block_number BIGINT  NOT NULL,
    block_hash   TEXT    NOT NULL,
    parent_hash  TEXT    NOT NULL DEFAULT '',
    canonical    BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (strategy, block_hash)
);
CREATE INDEX IF NOT EXISTS idx_processed_blocks_strategy_number
    ON processed_blocks (strategy, block_number DESC)
    WHERE canonical = TRUE;
