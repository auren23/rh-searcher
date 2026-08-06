-- 0014_legacy_unverifiable.sql
-- 0013 把旧 historical Accepted 行降级为 cost_approx，但 gas_estimate_mode
-- 仍叫 'historical'，与正式 'historical_complete' 并存容易误导报表。
-- 统一改名：任何残留的 'historical' 都是不可验证旧数据。
UPDATE opportunities
SET gas_estimate_mode = 'legacy_unverifiable'
WHERE gas_estimate_mode = 'historical';
