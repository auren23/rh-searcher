-- 0008_evaluation_cursor.sql
-- 双游标：ingest（区块历史）与 evaluate（候选评估）分离。
-- block_affected_pools 持久化每区块受影响的池：ingest 提交后崩溃，
-- 重启仍能从 evaluate 游标重新聚合评估，候选不再永久丢失。
CREATE TABLE IF NOT EXISTS block_affected_pools (
    strategy     TEXT   NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash   TEXT   NOT NULL,
    pool_address TEXT   NOT NULL,
    PRIMARY KEY (strategy, block_hash, pool_address)
);
CREATE INDEX IF NOT EXISTS idx_bap_strategy_number
    ON block_affected_pools (strategy, block_number);
