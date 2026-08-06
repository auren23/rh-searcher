-- 0010_gas_estimate_mode.sql
-- gas 成本来源标记：latest_approximation | max_gas_fallback | historical。
-- max_gas_fallback / latest_approximation 不得参与正式 EV、接受率与 Selected 统计。
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS gas_estimate_mode TEXT NOT NULL DEFAULT 'historical';
