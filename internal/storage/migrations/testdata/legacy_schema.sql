-- 模拟 0001 早期结构（BIGSERIAL id、无 shadow 列），用于验证 0002 的旧库升级路径。
DROP TABLE IF EXISTS execution_attempts, opportunities, dex_pools CASCADE;

CREATE TABLE opportunities (
    id BIGSERIAL PRIMARY KEY,
    strategy TEXT NOT NULL,
    observed_block BIGINT NOT NULL,
    observed_at BIGINT NOT NULL,
    source_event TEXT,
    input_asset TEXT,
    input_amount NUMERIC,
    gross_profit NUMERIC,
    gas_estimate NUMERIC,
    swap_cost NUMERIC,
    slippage_cost NUMERIC,
    expected_net_profit NUMERIC,
    simulation_result TEXT,
    decision TEXT NOT NULL,
    reject_reason TEXT,
    tx_hash TEXT,
    actual_net_profit NUMERIC,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE execution_attempts (
    id BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT REFERENCES opportunities(id),
    tx_hash TEXT,
    status TEXT NOT NULL,
    error TEXT,
    attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dex_pools (
    address TEXT PRIMARY KEY,
    exchange TEXT NOT NULL,
    protocol TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    fee INT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
