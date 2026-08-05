-- 0003: 机会分组（Top-K 唯一 Selected）列与索引
BEGIN;

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS opportunity_group_id TEXT,
    ADD COLUMN IF NOT EXISTS rank INT,
    ADD COLUMN IF NOT EXISTS selected BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_opportunities_group
    ON opportunities(opportunity_group_id);

ALTER TABLE opportunities
    ADD COLUMN IF NOT EXISTS l1_gas_units BIGINT,
    ADD COLUMN IF NOT EXISTS l2_base_fee_wei NUMERIC,
    ADD COLUMN IF NOT EXISTS l1_base_fee_estimate_wei NUMERIC;

CREATE INDEX IF NOT EXISTS idx_opportunities_selected
    ON opportunities(selected)
    WHERE selected = TRUE;

COMMIT;
