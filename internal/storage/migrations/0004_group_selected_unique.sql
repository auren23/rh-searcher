-- 0004: 每组机会仅允许一个 Selected（防止断线/重放产生多个）
BEGIN;

DROP INDEX IF EXISTS idx_opportunities_selected;

CREATE UNIQUE INDEX IF NOT EXISTS uq_opportunities_group_selected
    ON opportunities(opportunity_group_id)
    WHERE selected = TRUE
      AND opportunity_group_id IS NOT NULL;

COMMIT;
