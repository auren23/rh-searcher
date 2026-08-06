package storage

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// Checkpoint 断点：记录各索引器已处理到哪个区块，重启后恢复。
// 生产用 PostgreSQL（strategy_checkpoints 表），本地开发用 JSON 文件。
type Checkpoint struct {
	Path string
}

type checkpointData struct {
	UpdatedAt int64             `json:"updated_at"`
	Heights   map[string]uint64 `json:"heights"` // strategy -> block
}

func NewCheckpoint(path string) *Checkpoint {
	return &Checkpoint{Path: path}
}

func (c *Checkpoint) Load() (map[string]uint64, error) {
	raw, err := os.ReadFile(c.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]uint64{}, nil
		}
		return nil, err
	}
	var d checkpointData
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return d.Heights, nil
}

func (c *Checkpoint) Save(strategy string, block uint64) error {
	heights, _ := c.Load()
	heights[strategy] = block
	d := checkpointData{UpdatedAt: time.Now().UnixMilli(), Heights: heights}
	raw, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(c.Path), 0o755); err != nil {
		return err
	}
	// 原子写：临时文件 + rename，防止写到一半损坏
	tmp := c.Path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, c.Path); err != nil {
		return err
	}
	slog.Debug("checkpoint saved", "strategy", strategy, "block", block)
	return nil
}

// PGCheckpoint PostgreSQL 版断点（生产）。
type PGCheckpoint struct {
	db *DB
}

func NewPGCheckpoint(db *DB) *PGCheckpoint { return &PGCheckpoint{db: db} }

func (p *PGCheckpoint) Load(ctx context.Context) (map[string]uint64, error) {
	rows, err := p.db.pool.Query(ctx, `SELECT strategy, block_number FROM strategy_checkpoints`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var s string
		var n uint64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

func (p *PGCheckpoint) Save(ctx context.Context, strategy string, block uint64) error {
	return p.SaveWithHash(ctx, strategy, block, "", "")
}

// SaveWithHash 保存 checkpoint（含区块 hash，reorg 检测用）。
func (p *PGCheckpoint) SaveWithHash(ctx context.Context, strategy string, block uint64, blockHash, parentHash string) error {
	_, err := p.db.pool.Exec(ctx, `
		INSERT INTO strategy_checkpoints (strategy, block_number, block_hash, parent_hash, updated_at)
		VALUES ($1,$2,$3,$4,now())
		ON CONFLICT (strategy) DO UPDATE SET
			block_number = EXCLUDED.block_number,
			block_hash = EXCLUDED.block_hash,
			parent_hash = EXCLUDED.parent_hash,
			updated_at = now()`,
		strategy, block, nullableStr(blockHash), nullableStr(parentHash))
	return err
}
