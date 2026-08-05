-- rh-searcher 数据库迁移（PostgreSQL 15+）
-- 运行时状态在内存；这些表用于恢复、审计与研究。

-- 公共表
CREATE TABLE IF NOT EXISTS blocks (
    number      BIGINT PRIMARY KEY,
    hash        TEXT NOT NULL,
    parent_hash TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chain_logs (
    id          BIGSERIAL PRIMARY KEY,
    block_number BIGINT NOT NULL,
    tx_hash     TEXT NOT NULL,
    log_index   INT NOT NULL,
    address     TEXT NOT NULL,
    topics      JSONB NOT NULL,
    data        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (block_number, tx_hash, log_index)
);

CREATE TABLE IF NOT EXISTS rpc_measurements (
    id          BIGSERIAL PRIMARY KEY,
    group_name  TEXT NOT NULL,
    endpoint    TEXT NOT NULL,
    method      TEXT NOT NULL,
    latency_ms  DOUBLE PRECISION NOT NULL,
    ok          BOOLEAN NOT NULL,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transactions (
    hash        TEXT PRIMARY KEY,
    strategy    TEXT NOT NULL,
    nonce       BIGINT,
    from_addr   TEXT,
    to_addr     TEXT,
    value_wei   NUMERIC,
    gas_limit   BIGINT,
    gas_price_wei NUMERIC,
    raw_hex     TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS transaction_receipts (
    tx_hash     TEXT PRIMARY KEY REFERENCES transactions(hash),
    status      INT NOT NULL,
    gas_used    BIGINT,
    effective_gas_price NUMERIC,
    block_number BIGINT,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS strategy_checkpoints (
    strategy    TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 机会与执行：拒绝的机会也必须入库
CREATE TABLE IF NOT EXISTS opportunities (
    id              TEXT PRIMARY KEY,
    strategy        TEXT NOT NULL,
    observed_block  BIGINT NOT NULL,
    observed_at     BIGINT NOT NULL,
    source_event    TEXT,
    block_hash      TEXT,
    tx_hash         TEXT,
    log_index       BIGINT,
    route_json      TEXT,
    input_asset     TEXT,
    input_amount    NUMERIC,
    gross_profit    NUMERIC,
    gas_estimate    NUMERIC,
    swap_cost       NUMERIC,
    slippage_cost   NUMERIC,
    expected_net_profit NUMERIC,
    simulation_result TEXT,
    decision        TEXT NOT NULL,
    reject_reason   TEXT,
    tx_hash         TEXT,
    actual_net_profit NUMERIC,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS execution_attempts (
    id          BIGSERIAL PRIMARY KEY,
    opportunity_id BIGINT REFERENCES opportunities(id),
    tx_hash     TEXT,
    status      TEXT NOT NULL, -- broadcast | confirmed | reverted | dropped
    error       TEXT,
    attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- DEX 表
CREATE TABLE IF NOT EXISTS dex_factories (
    address     TEXT PRIMARY KEY,
    protocol    TEXT NOT NULL,
    name        TEXT,
    deploy_block BIGINT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dex_pools (
    address     TEXT PRIMARY KEY,
    exchange    TEXT NOT NULL,
    protocol    TEXT NOT NULL,
    token0      TEXT NOT NULL,
    token1      TEXT NOT NULL,
    fee         INT,
    tick_spacing INT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dex_pool_tokens (
    pool_address TEXT NOT NULL REFERENCES dex_pools(address),
    token       TEXT NOT NULL,
    decimals    INT,
    PRIMARY KEY (pool_address, token)
);

CREATE TABLE IF NOT EXISTS dex_pool_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    pool_address TEXT NOT NULL REFERENCES dex_pools(address),
    block_number BIGINT NOT NULL,
    liquidity   NUMERIC,
    sqrt_price_x96 NUMERIC,
    tick        INT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS arbitrage_routes (
    id          BIGSERIAL PRIMARY KEY,
    pool_a      TEXT NOT NULL,
    pool_b      TEXT NOT NULL,
    token       TEXT NOT NULL,
    last_seen_block BIGINT,
    hits        BIGINT NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pool_a, pool_b, token)
);

-- arbitrage_opportunities 视图：直接查 opportunities(strategy='weth-2hop')

-- Morpho 表
CREATE TABLE IF NOT EXISTS morpho_markets (
    market_id   TEXT PRIMARY KEY,
    loan_token  TEXT NOT NULL,
    collateral_token TEXT NOT NULL,
    oracle      TEXT NOT NULL,
    irm         TEXT NOT NULL,
    lltv        NUMERIC NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS morpho_market_states (
    market_id   TEXT NOT NULL REFERENCES morpho_markets(market_id),
    block_number BIGINT NOT NULL,
    total_supply_assets NUMERIC,
    total_supply_shares NUMERIC,
    total_borrow_assets NUMERIC,
    total_borrow_shares NUMERIC,
    last_update BIGINT,
    fee         NUMERIC,
    oracle_price NUMERIC,
    PRIMARY KEY (market_id, block_number)
);

CREATE TABLE IF NOT EXISTS morpho_positions (
    market_id   TEXT NOT NULL,
    user_addr   TEXT NOT NULL,
    supply_shares NUMERIC,
    borrow_shares NUMERIC,
    collateral  NUMERIC,
    updated_block BIGINT NOT NULL,
    PRIMARY KEY (market_id, user_addr)
);

CREATE TABLE IF NOT EXISTS morpho_oracles (
    address     TEXT PRIMARY KEY,
    kind        TEXT,
    asset       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS liquidation_candidates (
    id          BIGSERIAL PRIMARY KEY,
    market_id   TEXT NOT NULL,
    user_address TEXT NOT NULL,
    observed_block BIGINT NOT NULL,
    health_factor DOUBLE PRECISION,
    repay_assets NUMERIC,
    seized_collateral NUMERIC,
    flash_loan_cost NUMERIC,
    swap_cost   NUMERIC,
    gas_cost    NUMERIC,
    expected_net_profit NUMERIC,
    decision    TEXT NOT NULL,
    reject_reason TEXT,
    tx_hash     TEXT,
    actual_net_profit NUMERIC,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS liquidation_executions (
    id          BIGSERIAL PRIMARY KEY,
    candidate_id BIGINT REFERENCES liquidation_candidates(id),
    tx_hash     TEXT,
    status      TEXT NOT NULL,
    profit      NUMERIC,
    executed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_opportunities_observed ON opportunities(observed_block);
CREATE INDEX IF NOT EXISTS idx_liquidation_observed ON liquidation_candidates(observed_block);
CREATE INDEX IF NOT EXISTS idx_blocks_number ON blocks(number DESC);
