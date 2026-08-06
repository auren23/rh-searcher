-- 0011_gas_mode_truthful.sql
-- 0010 把旧候选默认回填成 'historical'，但旧版本实际使用 latest 估算或
-- maxGas 兜底——必须标成 legacy_unknown，不能混入正式 historical 统计。
UPDATE opportunities
SET gas_estimate_mode = 'legacy_unknown'
WHERE gas_estimate_mode = 'historical';

-- 新行默认 not_estimated（未进入模拟的拒绝候选/未估算）
ALTER TABLE opportunities ALTER COLUMN gas_estimate_mode SET DEFAULT 'not_estimated';
