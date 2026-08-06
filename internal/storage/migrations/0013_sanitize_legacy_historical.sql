-- 0013_sanitize_legacy_historical.sql
-- a8de519 版本会把 historical 估算标成 gas_estimate_mode='historical'
-- （不可验证：估算用了另一份 calldata）。0011 只处理迁移前的旧行；
-- 若在 0011 之后、e68be36 之前运行过 a8de519，数据库仍可能有
-- historical + simulation_accepted/selected 的不可验证行。
-- 正式统计只允许 gas_estimate_mode = 'historical_complete'。
UPDATE opportunities
SET
    selected = FALSE,
    decision = CASE
        WHEN decision IN ('simulation_accepted', 'simulation_valid')
        THEN 'simulation_valid_cost_approx'
        ELSE decision
    END,
    reject_reason = CASE
        WHEN decision IN ('simulation_accepted', 'simulation_valid')
        THEN 'legacy historical gas mode is not verifiable'
        ELSE reject_reason
    END
WHERE gas_estimate_mode = 'historical';
