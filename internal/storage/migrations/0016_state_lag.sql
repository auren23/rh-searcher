-- 0016_state_lag.sql
-- 状态滞后度量：state_age_ms 必须基于原始事件接收时间（ingest 时持久化），
-- 不能在恢复评估时用 time.Now() 重新填充。
ALTER TABLE block_affected_pools
    ADD COLUMN IF NOT EXISTS received_at_ms BIGINT;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS state_lag_blocks BIGINT;
