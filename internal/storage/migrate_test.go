package storage

// 迁移集成测试由 CI 的 postgres service 执行（见 .github/workflows/ci.yml）：
//   1. 空库依次执行 0001 + 0002
//   2. 模拟旧结构（0001 早期 BIGSERIAL 版）后只执行 0002
// 本地复现：docker compose up -d postgres 后执行
//   psql postgres://rh:rh@localhost:5432/rh -v ON_ERROR_STOP=1 -f internal/storage/migrations/0001_init.sql
//   psql postgres://rh:rh@localhost:5432/rh -v ON_ERROR_STOP=1 -f internal/storage/migrations/0002_shadow_schema_fix.sql
