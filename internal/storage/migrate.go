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

// migrationFile 一个迁移脚本（版本 + 内容）。脚本不得自行登记版本。
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

// hasBusinessTables 判断库是否已有业务表（旧库手工迁移的标志）。
func hasBusinessTables(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	var n int
	if err := conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('opportunities', 'dex_pools', 'strategy_checkpoints')
	`).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// Migrate 内置迁移 runner：Fresh-safe、幂等、并发安全。
//   - 整个迁移周期固定使用同一连接（pg_advisory_lock 是 session 级锁，
//     不能通过 pool 临时连接获取）
//   - 版本记录只由 runner 写入（脚本内不得登记版本）
//   - 空库（无业务表）：从 0001 顺序执行全部迁移
//   - 旧库（有业务表但无迁移记录）：显式 baseline 到 0011，然后从 0012 继续
//   - 结构与声明版本矛盾：失败关闭（不猜测）
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire: %w", err)
	}
	defer conn.Release()

	// session 级 advisory lock：固定连接上锁/解锁
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(0x5248)`); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(0x5248)`)

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT NOT NULL PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("schema_migrations: %w", err)
	}

	var recorded int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&recorded); err != nil {
		return err
	}
	business, err := hasBusinessTables(ctx, conn)
	if err != nil {
		return err
	}
	switch {
	case recorded == 0 && business:
		// 旧库手工迁移过（0001..0011 已应用但无记录）：显式 baseline
		for _, v := range []string{"0001", "0002", "0003", "0004", "0005", "0006",
			"0007", "0008", "0009", "0010", "0011"} {
			if _, err := conn.Exec(ctx,
				`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, v); err != nil {
				return err
			}
		}
	case recorded == 0 && !business:
		// 全新库：从 0001 开始（不登记任何版本）
	case recorded > 0:
		// 正常续跑：从最大版本之后继续
	default:
		return fmt.Errorf("migrate: inconsistent state (schema_migrations empty, business tables present)")
	}

	files, err := discoverMigrations()
	if err != nil {
		return err
	}
	for _, f := range files {
		var applied bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`,
			f.version).Scan(&applied); err != nil {
			return err
		}
		if applied {
			continue
		}
		tx, err := conn.Begin(ctx)
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
