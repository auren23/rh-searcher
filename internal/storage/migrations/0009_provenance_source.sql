-- 0009_provenance_source.sql
-- dex_pools 创建溯源来源标记：pool_created_log（Factory PoolCreated 日志，
-- 权威）| observed_swap_fallback（首次观察 Swap 兜底）| NULL（未知）。
-- 覆盖规则：只有非 pool_created_log 的记录才允许被真实日志信息覆盖。
ALTER TABLE dex_pools ADD COLUMN IF NOT EXISTS provenance_source TEXT;
CREATE INDEX IF NOT EXISTS idx_dex_pools_provenance
    ON dex_pools (provenance_source) WHERE canonical = TRUE;
