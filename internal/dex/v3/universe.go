// 池宇宙（universe）持久化：WETH 池全集的一次性 bootstrap 产物。
// canary/arbitrage 启动时从该文件恢复，不依赖实时 Swap 自发现——
// 静态池可能长期无 Swap，却是两池套利最重要的"第二腿"候选。
// 格式：JSONL（每行一个池）或 JSON 数组，LoadUniverse 自动识别。
package v3

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// UniversePool 宇宙池的持久化元数据（与 dex_pools 表同构）。
type UniversePool struct {
	Address          string `json:"address"`
	Exchange         string `json:"exchange"`
	Token0           string `json:"token0"`
	Token1           string `json:"token1"`
	Fee              uint32 `json:"fee"`
	TickSpacing      int    `json:"tick_spacing"`
	CreatedBlock     uint64 `json:"created_block"`
	CreatedBlockHash string `json:"created_block_hash"`
	ProvenanceSource string `json:"provenance,omitempty"`
}

// AppendUniverseLine 追加一个池到 JSONL 宇宙文件（bootstrap 增量落盘）。
func AppendUniverseLine(path string, p UniversePool) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(raw, '\n')); err != nil {
		return err
	}
	return nil
}

// LoadUniverse 读取宇宙文件（.json = 数组；其他 = JSONL）。
func LoadUniverse(path string) ([]UniversePool, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if len(path) >= 5 && path[len(path)-5:] == ".json" {
		var out []UniversePool
		if err := json.NewDecoder(f).Decode(&out); err != nil {
			return nil, fmt.Errorf("decode universe %s: %w", path, err)
		}
		return out, nil
	}
	var out []UniversePool
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var p UniversePool
		if err := json.Unmarshal(line, &p); err != nil {
			return nil, fmt.Errorf("universe line: %w", err)
		}
		out = append(out, p)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
