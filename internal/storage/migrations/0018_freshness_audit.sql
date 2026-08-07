-- 0018_freshness_audit.sql
-- freshness-first 观察审计：
--   block_evaluations     每块评估/跳过记录（区分"没机会"与"没及时看"）
--   opportunity_decay_samples 正毛利机会的 T+1/T+2/T+4 衰减采样
CREATE TABLE IF NOT EXISTS block_evaluations (
    id              BIGSERIAL PRIMARY KEY,
    trigger_block   BIGINT NOT NULL,
    trigger_hash    TEXT,
    state_block     BIGINT,
    state_lag_blocks BIGINT,
    state_age_ms    BIGINT,
    status          TEXT NOT NULL,          -- evaluated | stale_skipped
    skip_reason     TEXT,
    candidate_count INT,
    evaluated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_block_evaluations_status
    ON block_evaluations (status, trigger_block);

CREATE TABLE IF NOT EXISTS opportunity_decay_samples (
    id              BIGSERIAL PRIMARY KEY,
    origin_block    BIGINT NOT NULL,
    delay_blocks    INT NOT NULL,           -- 0 | 1 | 2 | 4
    route_json      TEXT NOT NULL,
    input_amount    TEXT NOT NULL,
    gross_profit_wei TEXT NOT NULL,
    net_1x_gas_wei  TEXT NOT NULL,
    net_2x_gas_wei  TEXT NOT NULL,
    net_3x_gas_wei  TEXT NOT NULL,
    sampled_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_decay_origin
    ON opportunity_decay_samples (origin_block, delay_blocks);
