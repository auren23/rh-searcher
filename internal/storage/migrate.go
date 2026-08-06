package storage

import (
	"context"
	"embed"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// migrationFile 一个已发现但尚未执行的迁移脚本。
type migrationFile struct {
	version string
	body    string
}

// discoverMigrations 读取嵌入的迁移脚本并按版本排序。
func discoverMigrations() ([]migrationFile, error) {
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return nil, err
	}
	out := []migrationFile{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") {
			continue
		}
		ver := name[:4] // NNNN_*.sql
		body, err := migrationFS.ReadFile("migrations/" + name)
		if err != nil {
			return nil, err
		}
		out = append(out, migrationFile{version: ver, body: string(body)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate 内置迁移 runner：按版本排序执行所有未应用的迁移。
// PG advisory lock 防并发；每个迁移在单事务内执行，成功后写入 schema_migrations。
// 幂等：重复调用跳过已应用版本（0011/0013 的数据更新脚本不会被二次执行）。
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// advisory lock（会话级，整库）
	if _, err := pool.Exec(ctx, `SELECT pg_advisory_lock(0x5248)`); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer pool.Exec(ctx, `SELECT pg_advisory_unlock(0x5248)`)

	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT NOT NULL PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("schema_migrations: %w", err)
	}

	// 0012 之前的手工迁移回填（表刚建、库已手工应用过 0001..0011）
	var cnt int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&cnt); err != nil {
		return err
	}
	if cnt == 0 {
		for _, v := range []string{"0001", "0002", "0003", "0004", "0005", "0006",
			"0007", "0008", "0009", "0010", "0011"} {
			if _, err := pool.Exec(ctx,
				`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, v); err != nil {
				return err
			}
		}
	}

	files, err := discoverMigrations()
	if err != nil {
		return err
	}
	for _, f := range files {
		var applied bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
			f.version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, f.body); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %s: %w", f.version, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, f.version); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("migration %s record: %w", f.version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("migration %s commit: %w", f.version, err)
		}
	}
	return nil
}
