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

	"github.com/ethereum/go-ethereum/common"
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

// RouteCapablePools 从宇宙池元数据构造"可成环池集"：
// 仅 token 拥有 >=2 个 WETH 池时，这些池才可能参与 WETH→TOKEN→WETH 两跳环。
// 单池 token 的池对当前策略无实时订阅价值（保留在 universe，但不出现在订阅）。
// 返回池地址列表（去重，顺序稳定）。
func RouteCapablePools(pools []UniversePool, weth common.Address) []common.Address {
	byToken := make(map[common.Address][]common.Address)
	for _, u := range pools {
		t0 := common.HexToAddress(u.Token0)
		t1 := common.HexToAddress(u.Token1)
		var tok common.Address
		if t0 == weth {
			tok = t1
		} else if t1 == weth {
			tok = t0
		} else {
			continue
		}
		byToken[tok] = append(byToken[tok], common.HexToAddress(u.Address))
	}
	var out []common.Address
	seen := make(map[common.Address]struct{})
	for _, ps := range byToken {
		if len(ps) < 2 {
			continue
		}
		for _, p := range ps {
			if _, dup := seen[p]; dup {
				continue
			}
			seen[p] = struct{}{}
			out = append(out, p)
		}
	}
	return out
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
