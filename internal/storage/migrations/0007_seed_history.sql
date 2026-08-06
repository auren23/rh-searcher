-- 0007_seed_history.sql
-- 1) 旧库升级：把已有 checkpoint 播种进 processed_blocks（reorg 祖先查找需要历史）。
INSERT INTO processed_blocks (
    strategy, block_number, block_hash, parent_hash, canonical
)
SELECT
    strategy,
    block_number,
    block_hash,
    COALESCE(parent_hash, ''),
    TRUE
FROM strategy_checkpoints
WHERE strategy = 'arbitrage:blocks'
  AND block_hash IS NOT NULL
  AND block_hash <> ''
ON CONFLICT (strategy, block_hash) DO NOTHING;

-- 2) dex_pools 增加规范标记（孤块创建的池 reorg 时被标为非规范，Restore 过滤）。
ALTER TABLE dex_pools ADD COLUMN IF NOT EXISTS canonical BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE dex_pools ADD COLUMN IF NOT EXISTS created_block BIGINT;
ALTER TABLE dex_pools ADD COLUMN IF NOT EXISTS created_block_hash TEXT;
