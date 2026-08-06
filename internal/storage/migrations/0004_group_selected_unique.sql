-- 0004: 每组机会仅允许一个 Selected（防止断线/重放产生多个）
BEGIN;

DROP INDEX IF EXISTS idx_opportunities_selected;

-- 旧数据清理：每组只保留一条 selected（稳定排序：净利降序，同利润按 id 升序取第一）。
-- 只取消 Selected，不删除候选（保留研究数据）。
WITH ranked AS (
    SELECT
        id,
        ROW_NUMBER() OVER (
            PARTITION BY opportunity_group_id
            ORDER BY expected_net_profit DESC NULLS LAST, id ASC
        ) AS rn
    FROM opportunities
    WHERE selected = TRUE
      AND opportunity_group_id IS NOT NULL
)
UPDATE opportunities o
SET selected = FALSE
FROM ranked r
WHERE o.id = r.id
  AND r.rn > 1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunities_group_selected
    ON opportunities(opportunity_group_id)
    WHERE selected = TRUE
      AND opportunity_group_id IS NOT NULL;

COMMIT;
