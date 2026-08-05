-- 0004: 每组机会仅允许一个 Selected（防止断线/重放产生多个）
BEGIN;

DROP INDEX IF EXISTS idx_opportunities_selected;

-- 旧数据清理：每组只保留净利最高的一条 selected（否则唯一索引创建失败）
DELETE FROM opportunities o
WHERE o.selected = TRUE
  AND o.opportunity_group_id IS NOT NULL
  AND EXISTS (
      SELECT 1 FROM opportunities o2
      WHERE o2.opportunity_group_id = o.opportunity_group_id
        AND o2.selected = TRUE
        AND o2.id <> o.id
        AND COALESCE(o2.expected_net_profit, 0) > COALESCE(o.expected_net_profit, 0)
  );

CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunities_group_selected
    ON opportunities(opportunity_group_id)
    WHERE selected = TRUE
      AND opportunity_group_id IS NOT NULL;

COMMIT;
