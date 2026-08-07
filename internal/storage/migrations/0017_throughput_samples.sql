-- 0017_throughput_samples.sql
-- 吞吐与限速指标采样（Canary 判断：ingest/evaluate 速率、双游标 lag、RPC 限速）。
CREATE TABLE IF NOT EXISTS throughput_samples (
    id          BIGSERIAL PRIMARY KEY,
    sampled_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    ingest_bps  DOUBLE PRECISION NOT NULL,
    evaluate_bps DOUBLE PRECISION NOT NULL,
    ingest_lag  BIGINT NOT NULL,
    evaluate_lag BIGINT NOT NULL,
    getlogs_reqs BIGINT NOT NULL,
    rpc_429     BIGINT NOT NULL,
    cache_hit_ratio DOUBLE PRECISION NOT NULL
);
