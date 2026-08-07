// Package config 加载 YAML 配置 + 环境变量覆盖。
// 敏感值（私钥、API Key）一律从环境变量读取，不写入 YAML。
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Chain   ChainConfig   `yaml:"chain"`
	RPC     RPCConfig     `yaml:"rpc"`
	Dexes   DexesConfig   `yaml:"dexes"`
	Morpho  MorphoConfig  `yaml:"morpho"`
	Storage  StorageConfig     `yaml:"storage"`
	Mode     ModeConfig        `yaml:"mode"`
	Executor ExecutorConfig    `yaml:"executor"`
	Arbitrage ArbitrageConfig  `yaml:"arbitrage"`
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
	RouterKind   string `yaml:"router_kind"` // swaprouter | universal
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

// ArbitrageConfig 套利策略参数（资金限制与模拟深度）。
type ArbitrageConfig struct {
	MaxInputWei     string `yaml:"max_input_wei"`      // 单笔最大输入（wei）
	MinInputWei     string `yaml:"min_input_wei"`      // 单笔最小输入（wei，浅池保护）
	MinProfitWei    string `yaml:"min_profit_wei"`     // 最小净利（wei）
	SafetyMarginWei string `yaml:"safety_margin_wei"`  // 安全边际（wei）
	SimulationTopK  int    `yaml:"simulation_top_k"`   // 本地 Top-K 输入量逐个 eth_call
	// SimulationMode 显式评估模式（禁止静默降级）：
	//   local_only      - 零资金：本地报价 + 保守 gas，不调合约（无需 executor）
	//   latest_observe  - 需主网 executor：latest 状态对齐模拟，标记 latest
	//   historical_strict - 需 archive RPC：固定块模拟，historical_complete 才准入正式 EV
	SimulationMode string `yaml:"simulation_mode"`
	// local_only 模式的保守 gas 成本：units × head baseFee × stress multiplier
	LocalGasUnits             uint64 `yaml:"local_gas_units"`
	LocalGasStressMultiplier  int    `yaml:"local_gas_stress_multiplier"`
	// MaxObservationLagBlocks 新鲜度阈值：pending 块落后 head 超过该值 →
	// stale_skipped（不评估、只审计+推进游标）。仅 local_only/latest_observe。
	MaxObservationLagBlocks uint64 `yaml:"max_observation_lag_blocks"`
}

// ExecutorConfig 执行合约与热钱包（模拟/发送用）。
type ExecutorConfig struct {
	Contract string `yaml:"contract"`
	Wallet   string `yaml:"wallet"`
}

// LoadMerged 顺序加载多个配置并合并（后者覆盖前者）。
func LoadMerged(paths ...string) (*Config, error) {
	merged := &Config{}
	for _, p := range paths {
		cfg, err := Load(p)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", p, err)
		}
		if cfg.Chain.ID != 0 {
			merged.Chain = cfg.Chain
		}
		if len(cfg.RPC.Groups.Read) > 0 {
			merged.RPC = cfg.RPC
		}
		if len(cfg.Dexes.V3) > 0 {
			merged.Dexes = cfg.Dexes
		}
		if cfg.Morpho.Blue != "" {
			merged.Morpho = cfg.Morpho
		}
		if cfg.Storage.PostgresURL != "" {
			merged.Storage = cfg.Storage
		}
		if cfg.Mode.Run != "" {
			merged.Mode = cfg.Mode
		}
		if cfg.Executor.Contract != "" || cfg.Executor.Wallet != "" {
			merged.Executor = cfg.Executor
		}
		if cfg.Arbitrage.MaxInputWei != "" {
			merged.Arbitrage.MaxInputWei = cfg.Arbitrage.MaxInputWei
		}
		if cfg.Arbitrage.MinInputWei != "" {
			merged.Arbitrage.MinInputWei = cfg.Arbitrage.MinInputWei
		}
		if cfg.Arbitrage.MinProfitWei != "" {
			merged.Arbitrage.MinProfitWei = cfg.Arbitrage.MinProfitWei
		}
		if cfg.Arbitrage.SafetyMarginWei != "" {
			merged.Arbitrage.SafetyMarginWei = cfg.Arbitrage.SafetyMarginWei
		}
		if cfg.Arbitrage.SimulationTopK > 0 {
			merged.Arbitrage.SimulationTopK = cfg.Arbitrage.SimulationTopK
		}
		if cfg.Arbitrage.SimulationMode != "" {
			merged.Arbitrage.SimulationMode = cfg.Arbitrage.SimulationMode
		}
		if cfg.Arbitrage.LocalGasUnits > 0 {
			merged.Arbitrage.LocalGasUnits = cfg.Arbitrage.LocalGasUnits
		}
		if cfg.Arbitrage.LocalGasStressMultiplier > 0 {
			merged.Arbitrage.LocalGasStressMultiplier = cfg.Arbitrage.LocalGasStressMultiplier
		}
		if cfg.Arbitrage.MaxObservationLagBlocks > 0 {
			merged.Arbitrage.MaxObservationLagBlocks = cfg.Arbitrage.MaxObservationLagBlocks
		}
	}
	return merged, nil
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
	expandEnvVars(reflect.ValueOf(cfg).Elem())
	return cfg, nil
}

// expandEnvVars 递归展开所有 string 字段中的 ${VAR} 与 ${VAR:-default}。
func expandEnvVars(v reflect.Value) {
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		switch f.Kind() {
		case reflect.String:
			if f.CanSet() {
				f.SetString(expand(f.String()))
			}
		case reflect.Struct:
			expandEnvVars(f)
		case reflect.Slice:
			if f.Type().Elem().Kind() == reflect.String {
				for j := 0; j < f.Len(); j++ {
					if f.Index(j).CanSet() {
						f.Index(j).SetString(expand(f.Index(j).String()))
					}
				}
			} else if f.Type().Elem().Kind() == reflect.Struct {
				for j := 0; j < f.Len(); j++ {
					expandEnvVars(f.Index(j))
				}
			}
		}
	}
}

// expand 展开 ${VAR} 与 ${VAR:-default}（- 为前缀时变量为空也取默认）。
func expand(s string) string {
	var out strings.Builder
	for {
		start := strings.Index(s, "${")
		if start < 0 {
			out.WriteString(s)
			return out.String()
		}
		out.WriteString(s[:start])
		rest := s[start+2:]
		end := strings.Index(rest, "}")
		if end < 0 {
			out.WriteString(s[start:])
			return out.String()
		}
		expr := rest[:end]
		name, def, hasDef := expr, "", false
		if i := strings.Index(expr, ":-"); i >= 0 {
			name, def, hasDef = expr[:i], expr[i+2:], true
		}
		val, ok := os.LookupEnv(name)
		if !ok || (val == "" && hasDef && strings.HasPrefix(expr, name+":-")) {
			if hasDef {
				val = def
			} else {
				val = ""
			}
		}
		out.WriteString(val)
		s = rest[end+1:]
	}
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
	if v := os.Getenv("RH_EXECUTOR_CONTRACT"); v != "" {
		c.Executor.Contract = v
	}
	if v := os.Getenv("RH_HOT_WALLET"); v != "" {
		c.Executor.Wallet = v
	}
	if v := os.Getenv("RH_MAX_INPUT_WEI"); v != "" {
		c.Arbitrage.MaxInputWei = v
	}
	if v := os.Getenv("RH_MIN_PROFIT_WEI"); v != "" {
		c.Arbitrage.MinProfitWei = v
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
