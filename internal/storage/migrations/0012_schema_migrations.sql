-- 0012_schema_migrations.sql
-- 迁移版本表：每个迁移只执行一次（不再靠"列存在"猜版本）。
-- 版本记录完全由迁移 runner 负责（rh-cli migrate / storage.Migrate）：
-- 本脚本不登记任何版本（Fresh DB 时 0012 之前的基础迁移由 runner 从 0001 执行）。
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT NOT NULL PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
