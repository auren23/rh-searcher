-- Alpha 报告（freshness-first）：只统计 state_lag_blocks <= 2 的样本
-- 用法: psql "$RH_POSTGRES_URL" -f scripts/alpha_report.sql

\echo === Fresh 候选（state_lag<=2）===
SELECT
    decision,
    COUNT(*) AS n,
    COUNT(*) FILTER (WHERE analysis_selected) AS analysis_best,
    COUNT(*) FILTER (WHERE selected) AS live_selected
FROM opportunities
WHERE simulation_mode = 'local_only'
  AND state_lag_blocks <= 2
GROUP BY decision;

\echo === 毛利 vs gas 覆盖（fresh 样本）===
SELECT
    COUNT(*) AS fresh_samples,
    COUNT(*) FILTER (WHERE gross_profit > 0) AS positive_gross,
    COUNT(*) FILTER (WHERE expected_net_profit > 0) AS net_2x_positive
FROM opportunities
WHERE simulation_mode = 'local_only'
  AND state_lag_blocks <= 2;

\echo === 状态滞后分布（评估 vs 跳过）===
SELECT
    status,
    COUNT(*) AS n,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY state_lag_blocks)) AS p50_lag,
    ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY state_lag_blocks)) AS p95_lag
FROM block_evaluations
GROUP BY status;

\echo === 机会衰减（T+0/1/2/4）===
SELECT
    delay_blocks,
    COUNT(*) AS samples,
    COUNT(*) FILTER (WHERE gross_profit_wei::numeric > 0) AS positive_gross,
    COUNT(*) FILTER (WHERE net_1x_gas_wei::numeric > 0) AS net_1x_positive,
    COUNT(*) FILTER (WHERE net_2x_gas_wei::numeric > 0) AS net_2x_positive
FROM opportunity_decay_samples
GROUP BY delay_blocks
ORDER BY delay_blocks;

\echo === 吞吐（最近 10 采样）===
SELECT sampled_at, ingest_bps, evaluate_bps, ingest_lag, evaluate_lag, rpc_429, cache_hit_ratio
FROM throughput_samples
ORDER BY sampled_at DESC
LIMIT 10;
