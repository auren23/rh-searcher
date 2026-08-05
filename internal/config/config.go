// Package config 加载 YAML 配置 + 环境变量覆盖。
// 敏感值（私钥、API Key）一律从环境变量读取，不写入 YAML。
package config

import (
	"fmt"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Chain   ChainConfig   `yaml:"chain"`
	RPC     RPCConfig     `yaml:"rpc"`
	Dexes   DexesConfig   `yaml:"dexes"`
	Morpho  MorphoConfig  `yaml:"morpho"`
	Storage StorageConfig `yaml:"storage"`
	Mode    ModeConfig    `yaml:"mode"`
}

type ChainConfig struct {
	ID       uint64 `yaml:"id"`
	WETH     string `yaml:"weth"`
	WBTC     string `yaml:"wbtc,omitempty"`
	USDC     string `yaml:"usdc,omitempty"`
	GasLimit uint64 `yaml:"gas_limit"`
}

type RPCConfig struct {
	Groups     RPCGroups `yaml:"groups"`
	TimeoutSec int       `yaml:"timeout_sec"`
	MaxRetries int       `yaml:"max_retries"`
}

type RPCGroups struct {
	Archive []string `yaml:"archive"`
	Read    []string `yaml:"read"`
	Sim     []string `yaml:"sim"`
	Send    []string `yaml:"send"`
}

type DexesConfig struct {
	V3 []V3DexConfig `yaml:"v3"`
}

type V3DexConfig struct {
	Name         string `yaml:"name"`
	Factory      string `yaml:"factory"`
	Router       string `yaml:"router"`
	QuoterV2     string `yaml:"quoter_v2"`
	InitCodeHash string `yaml:"init_code_hash"`
	FactoryBlock uint64 `yaml:"factory_block"`
}

type MorphoConfig struct {
	Blue         string   `yaml:"blue"`
	Markets      []string `yaml:"markets,omitempty"`
	BootstrapAPI string   `yaml:"bootstrap_api"`
}

type StorageConfig struct {
	PostgresURL string `yaml:"postgres_url"`
}

type ModeConfig struct {
	// dry: 只发现和记录；shadow: 模拟但不发送；live: 真实发送
	Run string `yaml:"run"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(raw, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyEnv()
	return cfg, nil
}

// applyEnv 用环境变量覆盖 YAML，用于部署时注入密钥与端点。
func (c *Config) applyEnv() {
	if v := os.Getenv("RH_POSTGRES_URL"); v != "" {
		c.Storage.PostgresURL = v
	}
	if v := os.Getenv("RH_CHAIN_ID"); v != "" {
		if id, err := strconv.ParseUint(v, 10, 64); err == nil {
			c.Chain.ID = id
		}
	}
	// 发送 RPC 组可用逗号分隔的环境变量覆盖（生产推荐）
	if v := os.Getenv("RH_SEND_RPCS"); v != "" {
		c.RPC.Groups.Send = splitCSV(v)
	}
	if v := os.Getenv("RH_READ_RPCS"); v != "" {
		c.RPC.Groups.Read = splitCSV(v)
	}
	if v := os.Getenv("RH_ARCHIVE_RPCS"); v != "" {
		c.RPC.Groups.Archive = splitCSV(v)
	}
	if v := os.Getenv("RH_SIM_RPCS"); v != "" {
		c.RPC.Groups.Sim = splitCSV(v)
	}
}

func splitCSV(v string) []string {
	out := []string{}
	cur := ""
	for _, r := range v {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
			}
			cur = ""
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}
