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

// isLegacyDatabase 用共享的 verifySchemaAt0011 判断旧库是否完整到达 0011：
// 与启动 checkSchema 完全同一套结构检查（不存在两套不一致的 Schema 认知）。
func isLegacyDatabase(ctx context.Context, conn *pgxpool.Conn) (bool, error) {
	var recorded int
	if err := conn.QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&recorded); err != nil {
		return false, err
	}
	if recorded > 0 {
		return false, nil // 正常续跑库
	}
	err := verifySchemaAt0011(ctx, conn)
	if err == nil {
		return true, nil // 完整 0011 旧库
	}
	// 结构不匹配：区分"全新库"（无任何表）与"残缺库"（失败关闭，不猜测）
	var n int
	if qerr := conn.QueryRow(ctx, `
		SELECT COUNT(*) FROM information_schema.tables
		WHERE table_schema='public' AND table_name <> 'schema_migrations'
	`).Scan(&n); qerr != nil {
		return false, qerr
	}
	if n == 0 {
		return false, nil // 全新库：从 0001 开始
	}
	return false, fmt.Errorf("legacy baseline refused: schema does not match 0011 fingerprint (%v)", err)
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
	if recorded == 0 {
		legacy, err := isLegacyDatabase(ctx, conn)
		if err != nil {
			return err
		}
		if legacy {
			// 旧库：指纹完整匹配 0001..0011 → 单事务写 baseline 记录
			tx, err := conn.Begin(ctx)
			if err != nil {
				return err
			}
			for _, v := range []string{"0001", "0002", "0003", "0004", "0005", "0006",
				"0007", "0008", "0009", "0010", "0011"} {
				if _, err := tx.Exec(ctx,
					`INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT DO NOTHING`, v); err != nil {
					tx.Rollback(ctx)
					return fmt.Errorf("baseline %s: %w", v, err)
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("baseline commit: %w", err)
			}
		}
		// 全新库：不登记任何版本，从 0001 开始
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
