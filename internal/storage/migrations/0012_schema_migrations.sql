-- 0012_schema_migrations.sql
-- 迁移版本表：每个迁移只执行一次（不再靠"列存在"猜版本）。
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT NOT NULL PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 回填：0012 之前的所有迁移都已被手工/CI 应用（0001..0011）
INSERT INTO schema_migrations (version) VALUES
    ('0001'), ('0002'), ('0003'), ('0004'), ('0005'),
    ('0006'), ('0007'), ('0008'), ('0009'), ('0010'), ('0011')
ON CONFLICT (version) DO NOTHING;
INSERT INTO schema_migrations (version) VALUES ('0012') ON CONFLICT (version) DO NOTHING;
