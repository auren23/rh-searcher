// rh-canary: Robinhood Chain stream-first freshness canary（换链/付费 RPC 决策的前置实验）。
//
// 目标：验证 Alchemy Robinhood WSS 能否把 Swap 事件以 state_lag<=2 的新鲜状态
// 送进本地报价评估。只回答一个问题：链本身是否可低延迟观测。
//
// 架构（与 rh-arbitrage 轮询管线完全隔离，不触碰生产代码路径）：
//   - 摄取 stream-first：WSS eth_subscribe(logs)，topic0=Swap（全池订阅），
//     本地用 WETH 池集过滤（~5,900 池）。实时路径不做逐块/批量 getLogs。
//   - 恢复 polling-recovery：启动与断线缺口用 Alchemy HTTP getLogs 补齐，
//     每次查询 ≤ recovery-chunk（默认 10）blocks（Alchemy 免费版限制），
//     与订阅事件经 chain.LogCursor 按 (block, tx, logIndex) 身份去重。
//   - 启动对齐：H0 = 启动时链头，只处理 [H0, head] 的微小新鲜窗口，不碰旧 backlog。
//   - 评估：事件 → 当前 head 快照 → local_only 本地报价（复用 arbitrage engine），
//     记录 state_lag_blocks / event_to_evaluation_ms。
//   - 交叉校验：公共 Robinhood RPC 仅作 secondary（head 对照）。
//
// 输出：data/canary/results-<ts>.jsonl（每候选一行）+ 结束时的 p50/p95/p99 汇总。
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"math/big"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/auren23/rh-searcher/internal/arbitrage"
	"github.com/auren23/rh-searcher/internal/chain"
	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/dex"
	v3 "github.com/auren23/rh-searcher/internal/dex/v3"
	"github.com/auren23/rh-searcher/internal/storage"
	"github.com/auren23/rh-searcher/internal/telemetry"
)

const (
	decisionProfitable = "local_profitable_observed"
)

// metricsView 无锁指标视图（snapshot 输出，禁止复制带锁结构）。
type metricsView struct {
	startedAt       time.Time
	eventsTotal     uint64     // 收到的全部 Swap 日志（订阅 + 恢复）
	eventsNonWeth   uint64     // 本地 WETH 池集过滤掉的事件
	eventsEvaluated uint64     // 进入评估的事件
	eventsFresh     uint64     // lag <= maxObsLag 的评估（fresh 样本）
	eventsStaleSkip uint64     // lag > staleSkipLag 未评估（不制造噪音）
	evalErrors      uint64     // 基础设施错误（快照/head 失败）
	disconnects     uint64     // WSS 断线重连次数
	recoveries      uint64     // 缺口恢复次数
	recoveredBlocks uint64     // 恢复覆盖的区块数
	recoveredLogs   uint64     // 恢复拉到的日志数
	grossPositive   uint64     // gross > 0 候选数
	netPos          [3]uint64  // net1x/2x/3x > 0 候选数
	poolsDiscovered uint64     // 运行时新发现的 WETH 池数（自发现模式）
	crossChecks     uint64     // 公共 RPC head 对照次数
	crossMismatches uint64     // 公共 RPC 与 Alchemy head 不一致次数
	lagSamples      []uint64   // state_lag_blocks（全部 WETH 事件）
	evalMsSamples   []int64    // event_to_evaluation_ms（评估完成 - 事件接收）
	evalStats       []evalStat // 每次 token 组评估的吞吐明细
}

// evalStat 单次 token-group 评估的吞吐指标（性能验收：p50/p95）。
type evalStat struct {
	rpcCalls     int   // 状态读取 RPC 调用数（header + multicall 往返）
	uniquePools  int   // 组内唯一池数（每池只刷一次）
	routeCount   int   // 本地报价的 route 数
	stateFetchMs int64 // 组快照耗时（Multicall3/batch）
	localQuoteMs int64 // 全部 route 本地报价耗时
	totalMs      int64 // 引擎评估总耗时
	fresh        bool  // 本批含 lag<=maxObsLag 的事件
}

type metrics struct {
	mu sync.Mutex
	v  metricsView
}

func (m *metrics) inc(f func(*metricsView)) {
	m.mu.Lock()
	f(&m.v)
	m.mu.Unlock()
}

func (m *metrics) recordLag(lag uint64) {
	m.mu.Lock()
	m.v.lagSamples = append(m.v.lagSamples, lag)
	m.mu.Unlock()
}

func (m *metrics) recordEvalMs(ms int64) {
	m.mu.Lock()
	m.v.evalMsSamples = append(m.v.evalMsSamples, ms)
	m.mu.Unlock()
}

func (m *metrics) recordEvalStat(st evalStat) {
	m.mu.Lock()
	m.v.evalStats = append(m.v.evalStats, st)
	m.mu.Unlock()
}

func (m *metrics) snapshot() metricsView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.v
	out.lagSamples = append([]uint64(nil), m.v.lagSamples...)
	out.evalMsSamples = append([]int64(nil), m.v.evalMsSamples...)
	return out
}

// candidateRecord 每个评估候选一行（JSONL，供研究侧直接分析）。
type candidateRecord struct {
	TS                  int64  `json:"ts"`
	Block               uint64 `json:"block"`
	BlockHash           string `json:"block_hash"`
	TxHash              string `json:"tx_hash"`
	LogIndex            uint   `json:"log_index"`
	Pool                string `json:"pool"`
	Head                uint64 `json:"head"`
	StateLagBlocks      uint64 `json:"state_lag_blocks"`
	EventToEvaluationMs int64  `json:"event_to_evaluation_ms"`
	Decision            string `json:"decision"`
	RejectReason        string `json:"reject_reason,omitempty"`
	Route               string `json:"route"`
	Rank                int    `json:"rank"`
	Token               string `json:"token,omitempty"`
	InputAmountWei      string `json:"input_amount_wei"`
	GrossProfitWei      string `json:"gross_profit_wei,omitempty"`
	NetProfitWei        string `json:"net_profit_wei,omitempty"`
	GasCostWei          string `json:"gas_cost_wei,omitempty"`
	Net1xWei            string `json:"net1x_wei,omitempty"`
	Net2xWei            string `json:"net2x_wei,omitempty"`
	Net3xWei            string `json:"net3x_wei,omitempty"`
	// 本次 token 组评估的吞吐指标（性能验收用）
	RpcCalls     int   `json:"rpc_calls,omitempty"`
	UniquePools  int   `json:"unique_pools,omitempty"`
	RouteCount   int   `json:"route_count,omitempty"`
	StateFetchMs int64 `json:"state_fetch_ms,omitempty"`
	LocalQuoteMs int64 `json:"local_quote_ms,omitempty"`
	TotalEvalMs  int64 `json:"total_eval_ms,omitempty"`
}

// eventRecord 每个 WETH Swap 事件一行（含被跳过的事件；评估结果由 candidate 行承载）。
type eventRecord struct {
	TS                  int64  `json:"ts"`
	Kind                string `json:"kind"` // "event"
	Block               uint64 `json:"block"`
	BlockHash           string `json:"block_hash"`
	TxHash              string `json:"tx_hash"`
	LogIndex            uint   `json:"log_index"`
	Pool                string `json:"pool"`
	Token               string `json:"token,omitempty"`
	Head                uint64 `json:"head"`
	StateLagBlocks      uint64 `json:"state_lag_blocks"`
	EventToEvaluationMs int64  `json:"event_to_evaluation_ms"`
	Skipped             string `json:"skipped,omitempty"` // "" | "stale" | "non_weth" | "discovery_pending" | "head_error" | "eval_error" | "eval_throttled"
	BestDecision        string `json:"best_decision,omitempty"`
}

// poolRecord 运行时发现的 WETH 池（研究侧建池集用）。
type poolRecord struct {
	TS     int64  `json:"ts"`
	Kind   string `json:"kind"` // "pool"
	Pool   string `json:"pool"`
	Token0 string `json:"token0"`
	Token1 string `json:"token1"`
	Fee    uint32 `json:"fee"`
}

type canary struct {
	cfg             *config.Config
	weth            common.Address
	httpCli         *ethclient.Client // Alchemy HTTP（状态读取 + 恢复）
	wss             *rpc.Client       // Alchemy WSS（日志订阅）
	streamURL       string
	adapter         *v3.Adapter
	reg             *dex.Registry
	graph           *dex.Graph
	searcher        *arbitrage.LocalSearcher
	engine          *arbitrage.Engine
	pmu             sync.Mutex // 保护 pool 集合（wethPools/nonWeth/notV3/discoverPending）
	wethPools       map[common.Address]struct{}
	nonWeth         map[common.Address]struct{} // 已确认非 WETH 池（过滤缓存）
	notV3           map[common.Address]struct{} // 已确认非本 Factory 池（过滤缓存）
	discoverPending map[common.Address]struct{} // 发现队列去重
	discoverCh      chan common.Address         // 异步发现队列（事件循环不阻塞 RPC）
	engineMu        sync.Mutex                  // 序列化 engine 评估与 reg/graph 写入
	outMu           sync.Mutex                  // JSONL 写入互斥（bufio.Writer 非线程安全）
	// 注：无全局评估节流。同 token 连续事件由 (headHash,pool) 快照缓存吸收——
	// 同一 head 的重复评估零 RPC（rpc_calls p50≈0），无需人为限速。
	cursor     chain.LogCursor
	logFilter  map[string]interface{} // eth_subscribe logs 参数（必须小写 key）
	metrics    *metrics
	out        *bufio.Writer
	outFile    *os.File
	maxObsLag  uint64
	staleSkip  uint64
	chunk      uint64
	maxEvals   int
	duration   time.Duration
	startedAt  time.Time
	startFrom  uint64        // 启动对齐：H0（首次恢复起点）
	headMu     sync.Mutex    // latestHead 由 newHeads 订阅 goroutine 更新
	headVal    uint64        // 最新 head（WSS 同流推送 → 与日志同延迟，lag 精确）
	headHeader *types.Header // 与 headVal 同锁更新的最新 header（评估 hint 用）
	publicRPC  string
	poolFile   string
	stopCh     chan struct{}
}

// pendingLog 等待评估的 WETH Swap 事件（token 组批处理单元）。
type pendingLog struct {
	l          types.Log
	head       uint64
	lag        uint64
	receivedAt time.Time
}

func main() {
	cfgPath := flag.String("config", "configs/robinhood.yaml", "config file")
	duration := flag.Duration("duration", 2*time.Hour, "max canary run duration")
	maxEvals := flag.Int("max-evals", 1000, "stop after N fresh evaluations (lag <= max-obs-lag)")
	chunk := flag.Uint64("recovery-chunk", 10, "max blocks per getLogs recovery query (Alchemy free limit)")
	staleSkip := flag.Uint64("stale-skip-lag", 10, "skip evaluation when state_lag > N (no signal, just noise)")
	outDir := flag.String("out-dir", "data/canary", "output directory for results jsonl")
	poolFile := flag.String("pools", "data/canary/weth-universe.jsonl", "WETH pool universe file (jsonl/json; PG restore takes precedence)")
	flag.Parse()

	telemetry.SetupLogging(slog.LevelInfo)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadMerged(*cfgPath, "configs/dexes.yaml", "configs/morpho.yaml")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}

	// ---- 前置检查：stream-first 需要 Alchemy 端点（免费注册即可获得）----
	streamURL := firstNonEmpty(cfg.RPC.Groups.Stream)
	alchemyURL := firstNonEmpty(cfg.RPC.Groups.Alchemy)
	if streamURL == "" && alchemyURL == "" {
		slog.Error("no Alchemy endpoint configured",
			"hint", "export RH_STREAM_RPC=wss://robinhood-mainnet.g.alchemy.com/v2/{API_KEY} "+
				"and RH_ALCHEMY_RPC=https://robinhood-mainnet.g.alchemy.com/v2/{API_KEY}")
		os.Exit(1)
	}
	if streamURL == "" {
		// 便捷回退：HTTP 与 WSS 同 key，仅 scheme 不同
		if strings.HasPrefix(alchemyURL, "https://") {
			streamURL = "wss://" + strings.TrimPrefix(alchemyURL, "https://")
			slog.Info("derived stream URL from alchemy HTTP", "url", maskKey(streamURL))
		} else {
			slog.Error("stream URL required (wss://...)")
			os.Exit(1)
		}
	}
	if !strings.HasPrefix(streamURL, "wss://") && !strings.HasPrefix(streamURL, "ws://") {
		slog.Error("stream URL must be websocket", "url", maskKey(streamURL))
		os.Exit(1)
	}
	if alchemyURL == "" {
		slog.Error("alchemy HTTP URL required (https://robinhood-mainnet.g.alchemy.com/v2/{API_KEY})")
		os.Exit(1)
	}
	if !strings.HasPrefix(alchemyURL, "https://") && !strings.HasPrefix(alchemyURL, "http://") {
		slog.Error("alchemy URL must be http(s)", "url", maskKey(alchemyURL))
		os.Exit(1)
	}
	publicRPC := firstNonEmpty(cfg.RPC.Groups.Archive)

	// ---- 池宇宙：PG 预载（可选）+ 运行时自发现 ----
	// 预载存在时直接作为 WETH 池集（过滤 + 建图）；缺失/为空时进入自发现模式：
	// 首个 Swap 事件对未知池做一次 token0/token1 校验（PoolByAddress），
	// WETH 池注册进图并缓存，之后按普通 WETH 池评估。旧池无需重扫——
	// 活跃池会持续产生事件，不活跃池对实时套利无意义。
	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	weth := common.HexToAddress(cfg.Chain.WETH)
	wethPools := make(map[common.Address]struct{})
	if cfg.Storage.PostgresURL != "" {
		sink, err := storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Warn("postgres unavailable; falling back to pool self-discovery", "err", err)
		} else {
			n, rerr := storage.RestorePools(ctx, sink, reg, graph, weth)
			sink.Close()
			if rerr != nil {
				slog.Warn("restore pools failed; falling back to pool self-discovery", "err", rerr)
			} else {
				for _, st := range reg.AllPools() {
					p := v3.UnwrapState(st)
					if p == nil {
						slog.Error("unsupported pool state", "type", fmt.Sprintf("%T", st))
						os.Exit(1)
					}
					wethPools[p.Address] = struct{}{}
				}
				slog.Info("WETH pool universe preloaded from db", "pools", n)
			}
		}
	}
	if len(wethPools) == 0 {
		// 文件宇宙回退（PG 不可用/被清）：一次性 bootstrap 产物，静态池保留。
		if uni, uerr := v3.LoadUniverse(*poolFile); uerr == nil && len(uni) > 0 {
			for _, u := range uni {
				addr := common.HexToAddress(u.Address)
				p := v3.NewPoolFromMetaWithCreated(addr, u.Exchange,
					common.HexToAddress(u.Token0), common.HexToAddress(u.Token1),
					u.Fee, u.TickSpacing, u.CreatedBlock,
					common.HexToHash(u.CreatedBlockHash), u.ProvenanceSource)
				reg.UpsertPool(v3.State(p))
				graph.AddPool(p.Pool(), addr)
				wethPools[addr] = struct{}{}
			}
			slog.Info("WETH pool universe loaded from file", "pools", len(uni), "file", *poolFile)
		} else {
			slog.Warn("no preloaded WETH pools; running in self-discovery mode",
				"hint", "run: rh-cli pools bootstrap --out "+*poolFile+" (PG was wiped; "+
					"self-discovery only sees pools with live Swap events)")
		}
	}

	// ---- 评估组件（local_only：零资金、不调合约）----
	httpCli, err := ethclient.Dial(alchemyURL)
	if err != nil {
		slog.Error("dial alchemy http", "err", err)
		os.Exit(1)
	}
	d := cfg.Dexes.V3[0]
	adapter, err := v3.NewAdapter(httpCli, d.Name,
		common.HexToAddress(d.Factory), common.HexToAddress(d.Router), d.RouterKind,
		common.HexToHash(d.InitCodeHash), d.FactoryBlock)
	if err != nil {
		slog.Error("v3 adapter", "err", err)
		os.Exit(1)
	}
	searcher := arbitrage.NewLocalSearcher(graph, reg, adapter, weth)
	if cfg.Arbitrage.MaxInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MaxInputWei, 10); ok {
			searcher.SetFunding(v, nil)
		}
	}
	if cfg.Arbitrage.MinInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MinInputWei, 10); ok {
			searcher.SetMinInput(v)
		}
	}
	maxObsLag := cfg.Arbitrage.MaxObservationLagBlocks
	if maxObsLag == 0 {
		maxObsLag = 2
	}
	// local_only 分支硬依赖 SafetyMarginWei（engine.go:326 net.Sub），缺失会 nil panic
	minProfit := big.NewInt(1e13)
	safetyMargin := big.NewInt(5e12)
	if cfg.Arbitrage.MinProfitWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MinProfitWei, 10); ok {
			minProfit = v
		}
	}
	if cfg.Arbitrage.SafetyMarginWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.SafetyMarginWei, 10); ok {
			safetyMargin = v
		}
	}
	engineCfg := arbitrage.Config{
		ChainID:                  cfg.Chain.ID,
		WETH:                     weth,
		MinProfitWei:             minProfit,
		SafetyMarginWei:          safetyMargin,
		MaxHops:                  2,
		TopK:                     cfg.Arbitrage.SimulationTopK,
		Mode:                     "shadow",
		SimulationMode:           "local_only",
		LocalGasUnits:            cfg.Arbitrage.LocalGasUnits,
		LocalGasStressMultiplier: cfg.Arbitrage.LocalGasStressMultiplier,
		MaxObservationLagBlocks:  maxObsLag,
	}
	engine := arbitrage.NewEngine(engineCfg, nil, searcher, nil, arbitrage.NewExecutor())

	// ---- 输出 ----
	startedAt := time.Now()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		slog.Error("mkdir out dir", "err", err)
		os.Exit(1)
	}
	outPath := fmt.Sprintf("%s/results-%s.jsonl", *outDir, startedAt.Format("20060102-150405"))
	outFile, err := os.Create(outPath)
	if err != nil {
		slog.Error("create results file", "err", err)
		os.Exit(1)
	}
	defer outFile.Close()
	out := bufio.NewWriter(outFile)

	c := &canary{
		cfg:             cfg,
		weth:            weth,
		httpCli:         httpCli,
		streamURL:       streamURL,
		adapter:         adapter,
		reg:             reg,
		graph:           graph,
		searcher:        searcher,
		engine:          engine,
		wethPools:       wethPools,
		nonWeth:         make(map[common.Address]struct{}),
		notV3:           make(map[common.Address]struct{}),
		discoverPending: make(map[common.Address]struct{}),
		discoverCh:      make(chan common.Address, 4096),

		// 订阅参数必须用小写 key map：go-ethereum 的 ethereum.FilterQuery 没有
		// json tag，直接传 rpc.EthSubscribe 会把字段序列化成大写（"Topics"），
		// Nitro 节点忽略未知 key → 订阅退化为全量日志。已验证格式：
		// {"topics":[[swapTopic]]}（与 ethclient.toFilterArg 的 key 一致）。
		logFilter: map[string]interface{}{
			"topics": [][]common.Hash{{v3.SwapTopic()}},
		},
		metrics:   &metrics{v: metricsView{startedAt: startedAt}},
		out:       out,
		outFile:   outFile,
		maxObsLag: maxObsLag,
		staleSkip: *staleSkip,
		chunk:     *chunk,
		maxEvals:  *maxEvals,
		duration:  *duration,
		startedAt: startedAt,
		publicRPC: publicRPC,
		poolFile:  *poolFile,
		stopCh:    make(chan struct{}),
	}

	// ---- 启动对齐：H0 = 当前链头，只处理启动后的新鲜窗口 ----
	h0, err := httpCli.BlockNumber(ctx)
	if err != nil {
		slog.Error("read startup head", "err", err)
		os.Exit(1)
	}
	c.startFrom = h0
	// 启动 head 的 header 不可得（HTTP 只返回高度）：headVal 直接设置，
	// header hint 由 newHeads 首帧接管。
	c.headMu.Lock()
	c.headVal = h0
	c.headMu.Unlock()
	slog.Info("canary starting",
		"stream", maskKey(streamURL), "alchemy", maskKey(alchemyURL),
		"start_head", h0, "pools", len(wethPools),
		"duration", c.duration, "max_fresh_evals", c.maxEvals,
		"recovery_chunk_blocks", c.chunk, "stale_skip_lag", c.staleSkip,
		"out", outPath)

	// 异步发现 worker：池发现不阻塞事件流（同步 RPC 会让消费者在 8 events/s 下落后）
	go c.discoveryWorker(ctx)

	// 监控：60s 统计 + 30s 公共 RPC 交叉校验
	go statsLoop(ctx, c)
	if c.publicRPC != "" {
		go crossCheckLoop(ctx, c)
	} else {
		slog.Warn("no public RPC configured; cross-check disabled")
	}

	// ---- 主循环：订阅 + 恢复 + 评估 ----
	runErr := c.run(ctx)

	// ---- 汇总 ----
	m := c.metrics.snapshot()
	summary(ctx, c, m, runErr)
}

// run 订阅主循环：断线自动重连，每次（重）连接后先做缺口恢复再消费订阅。
func (c *canary) run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		rc, err := rpc.DialContext(ctx, c.streamURL)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Warn("wss dial failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			if c.stopReached() {
				return nil
			}
			continue
		}
		c.wss = rc
		// newHeads 与 logs 同连接订阅：两者共享交付延迟，lag = latestHead - logBlock
		// 是真实的链相对新鲜度（不用 HTTP head，省 RPC 且不受缓存偏差影响）
		headsCh := make(chan *types.Header, 256)
		if hsub, herr := rc.EthSubscribe(ctx, headsCh, "newHeads"); herr == nil {
			go func() {
				for {
					select {
					case <-ctx.Done():
						hsub.Unsubscribe()
						return
					case h, ok := <-headsCh:
						if !ok {
							return
						}
						c.setHead(h)
					}
				}
			}()
		} else {
			slog.Warn("newHeads subscription failed; lag measured from stale head", "err", herr)
		}
		ch := make(chan types.Log, 4096)
		sub, err := rc.EthSubscribe(ctx, ch, "logs", c.logFilter)
		if err != nil {
			rc.Close()
			c.metrics.inc(func(m *metricsView) { m.disconnects++ })
			slog.Warn("eth_subscribe failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			if c.stopReached() {
				return nil
			}
			continue
		}
		slog.Info("logs subscription established", "url", maskKey(c.streamURL))
		// 缺口恢复（首次：从 H0；重连后：从游标）。订阅先建、恢复后跑：
		// 恢复期间到达的实时事件在 ch 缓冲，恢复处理后统一消费，LogCursor 去重。
		from := c.startFrom
		if c.cursor.Have {
			from = c.cursor.BlockNumber
		}
		if err := c.recoverAndProcess(ctx, from); err != nil {
			slog.Warn("gap recovery failed", "err", err)
		}
		// 消费订阅
		subErr := false
		for !subErr {
			select {
			case <-ctx.Done():
				sub.Unsubscribe()
				rc.Close()
				return nil
			case _, ok := <-sub.Err():
				if !ok {
					subErr = true
					break
				}
				subErr = true
			case l, ok := <-ch:
				if !ok {
					subErr = true
					break
				}
				if c.cursor.Seen(l) {
					continue
				}
				c.cursor.Advance(l)
				c.handleLog(ctx, l)
				if c.stopReached() {
					sub.Unsubscribe()
					rc.Close()
					return nil
				}
			case <-time.After(5 * time.Second):
				// 静默期（链上无事件）也检查结束条件，避免挂到 SIGINT
				if c.stopReached() {
					sub.Unsubscribe()
					rc.Close()
					return nil
				}
			}
		}
		sub.Unsubscribe()
		rc.Close()
		c.metrics.inc(func(m *metricsView) { m.disconnects++ })
		slog.Warn("logs subscription lost; reconnecting")
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(2 * time.Second):
		}
	}
}

// stopReached 结束条件：运行时长或 fresh 评估数达标。
func (c *canary) stopReached() bool {
	m := c.metrics.snapshot()
	if time.Since(c.startedAt) >= c.duration {
		return true
	}
	return c.maxEvals > 0 && m.eventsFresh >= uint64(c.maxEvals)
}

// recoverAndProcess 用 Alchemy HTTP getLogs 补齐 [from, head] 缺口，每次 ≤ chunk blocks。
// 与订阅共用 handleLog + LogCursor 去重。返回覆盖的区块数。
func (c *canary) recoverAndProcess(ctx context.Context, from uint64) error {
	head, err := c.httpCli.BlockNumber(ctx)
	if err != nil {
		return err
	}
	if from > head {
		return nil
	}
	c.metrics.inc(func(m *metricsView) { m.recoveries++ })
	for from <= head {
		to := from + c.chunk - 1
		if to > head {
			to = head
		}
		logs, err := c.getLogsRetry(ctx, from, to)
		if err != nil {
			return fmt.Errorf("getLogs %d..%d: %w", from, to, err)
		}
		for _, l := range logs {
			if c.cursor.Seen(l) {
				continue
			}
			c.cursor.Advance(l)
			c.handleLog(ctx, l)
			if c.stopReached() {
				return nil
			}
		}
		c.metrics.inc(func(m *metricsView) {
			m.recoveredBlocks += to - from + 1
			m.recoveredLogs += uint64(len(logs))
		})
		from = to + 1
	}
	return nil
}

// getLogsRetry getLogs 带 429 退避重试（Alchemy 免费版 10-block 上限由调用方保证）。
func (c *canary) getLogsRetry(ctx context.Context, from, to uint64) ([]types.Log, error) {
	// HTTP getLogs 走 ethclient.FilterLogs（toFilterArg 小写 key，过滤正确）
	q := ethereum.FilterQuery{
		Topics: [][]common.Hash{{v3.SwapTopic()}},
	}
	q.FromBlock = new(big.Int).SetUint64(from)
	q.ToBlock = new(big.Int).SetUint64(to)
	var lastErr error
	for attempt := 0; attempt < 6; attempt++ {
		logs, err := c.httpCli.FilterLogs(ctx, q)
		if err == nil {
			return logs, nil
		}
		lastErr = err
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "429") || strings.Contains(msg, "rate limit") {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
			continue
		}
		return nil, err
	}
	return nil, lastErr
}

// poolState 查询池的集合状态（0=未知 1=WETH 2=非WETH 3=非Factory）。
func (c *canary) poolState(addr common.Address) int {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	if _, ok := c.wethPools[addr]; ok {
		return 1
	}
	if _, ok := c.nonWeth[addr]; ok {
		return 2
	}
	if _, ok := c.notV3[addr]; ok {
		return 3
	}
	return 0
}

// enqueueDiscovery 未知池入发现队列（去重；队列满丢弃，下个事件重试）。
func (c *canary) enqueueDiscovery(addr common.Address) {
	c.pmu.Lock()
	defer c.pmu.Unlock()
	if _, ok := c.discoverPending[addr]; ok {
		return
	}
	c.discoverPending[addr] = struct{}{}
	select {
	case c.discoverCh <- addr:
	default:
		// 队列满：丢弃本次入队，事件下次出现时重试
	}
}

// discoveryWorker 异步池发现：事件循环不因 RPC 阻塞（8 events/s 下同步发现必落后）。
func (c *canary) discoveryWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case addr := <-c.discoverCh:
			c.resolvePool(ctx, addr)
		}
	}
}

// resolvePool 执行一次完整发现：廉价 token 预检（2 calls）→ WETH 池全量校验 → 注册进图。
// 基础设施错误不缓存（下次事件重试）；确定性失败（非 Factory / revert）缓存。
func (c *canary) resolvePool(ctx context.Context, addr common.Address) {
	if c.poolState(addr) != 0 {
		c.pmu.Lock()
		delete(c.discoverPending, addr)
		c.pmu.Unlock()
		return // 已被并发解决
	}
	t0, t1, err := c.tokenPair(ctx, addr)
	if err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "revert") || strings.Contains(msg, "invalid opcode") {
			// 确定性失败：地址不是 V3 池（无 token0()）→ 缓存，不再重试
			c.pmu.Lock()
			c.notV3[addr] = struct{}{}
			delete(c.discoverPending, addr)
			c.pmu.Unlock()
			return
		}
		c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
		slog.Debug("pool token check failed", "pool", addr.Hex(), "err", err)
		c.pmu.Lock()
		delete(c.discoverPending, addr)
		c.pmu.Unlock()
		return
	}
	if t0 != c.weth && t1 != c.weth {
		c.pmu.Lock()
		c.nonWeth[addr] = struct{}{}
		delete(c.discoverPending, addr)
		c.pmu.Unlock()
		return
	}
	pool, err := c.adapter.PoolByAddress(ctx, addr)
	if err != nil {
		if errors.Is(err, v3.ErrNotFactoryPool) {
			c.pmu.Lock()
			c.notV3[addr] = struct{}{}
			delete(c.discoverPending, addr)
			c.pmu.Unlock()
			return
		}
		c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
		slog.Debug("pool discovery failed", "pool", addr.Hex(), "err", err)
		c.pmu.Lock()
		delete(c.discoverPending, addr)
		c.pmu.Unlock()
		return
	}
	// 先注册图（engine 评估前必须可见），再标记 WETH（事件循环只在 WETH 后评估）
	c.engineMu.Lock()
	c.reg.UpsertPool(v3.State(pool))
	c.graph.AddPool(pool.Pool(), addr)
	c.engineMu.Unlock()
	c.pmu.Lock()
	c.wethPools[addr] = struct{}{}
	delete(c.discoverPending, addr)
	c.pmu.Unlock()
	c.metrics.inc(func(m *metricsView) { m.poolsDiscovered++ })
	slog.Info("WETH pool discovered", "pool", addr.Hex(),
		"token0", pool.Token0.Hex(), "token1", pool.Token1.Hex(), "fee", pool.Fee)
	c.writePool(pool)
}

// writePool WETH 池发现行落 JSONL。
func (c *canary) writePool(pool *v3.Pool) {
	raw, err := json.Marshal(poolRecord{
		TS:     time.Now().UnixMilli(),
		Kind:   "pool",
		Pool:   pool.Address.Hex(),
		Token0: pool.Token0.Hex(),
		Token1: pool.Token1.Hex(),
		Fee:    pool.Fee,
	})
	if err != nil {
		return
	}
	c.outMu.Lock()
	c.out.Write(raw)
	c.out.WriteByte('\n')
	c.out.Flush()
	c.outMu.Unlock()
}

// tokenPair 读取池的 token0/token1（2 次 eth_call；自发现廉价预检）。
func (c *canary) tokenPair(ctx context.Context, addr common.Address) (common.Address, common.Address, error) {
	t0raw, err := c.httpCli.CallContract(ctx, ethereum.CallMsg{
		To:   &addr,
		Data: crypto.Keccak256([]byte("token0()"))[:4],
	}, nil)
	if err != nil {
		return common.Address{}, common.Address{}, err
	}
	t1raw, err := c.httpCli.CallContract(ctx, ethereum.CallMsg{
		To:   &addr,
		Data: crypto.Keccak256([]byte("token1()"))[:4],
	}, nil)
	if err != nil {
		return common.Address{}, common.Address{}, err
	}
	if len(t0raw) < 32 || len(t1raw) < 32 {
		return common.Address{}, common.Address{}, fmt.Errorf("short token response")
	}
	return common.BytesToAddress(t0raw[12:32]), common.BytesToAddress(t1raw[12:32]), nil
}

// head 返回最新链头（由 newHeads 订阅 goroutine 维护，无 HTTP 调用）。
func (c *canary) head() uint64 {
	c.headMu.Lock()
	defer c.headMu.Unlock()
	return c.headVal
}

// setHead 更新最新链头与 header（newHeads 订阅 goroutine 调用；原子一致）。
func (c *canary) setHead(h *types.Header) {
	c.headMu.Lock()
	c.headVal = h.Number.Uint64()
	c.headHeader = h
	c.headMu.Unlock()
	c.searcher.SetHeaderHint(h)
}

// evalHeader 返回当前 head header（评估用；与 head() 同一锁，保证一致性）。
func (c *canary) evalHeader() *types.Header {
	c.headMu.Lock()
	defer c.headMu.Unlock()
	return c.headHeader
}

// headAndHeader 原子读取 (head, header)：评估的区块号与 hint 必须同源，
// 否则 hint 被订阅 goroutine 推进后 cachedHeaderAt 退化为 header RPC。
func (c *canary) headAndHeader() (uint64, *types.Header) {
	c.headMu.Lock()
	defer c.headMu.Unlock()
	return c.headVal, c.headHeader
}

// handleLog 单个 Swap 事件：记录全流量延迟 → WETH 池过滤（异步自发现）→ local_only 评估 → JSONL。
// state_lag 对全部事件采样（含非 WETH）：Alchemy WSS 的交付新鲜度是全流量问题，
// 策略评估只对已知 WETH 池执行。事件循环不做任何同步 RPC（head 缓存除外）——
// 8 events/s 下同步发现/评估会让消费者落后，lag 失真。
func (c *canary) handleLog(ctx context.Context, l types.Log) {
	c.metrics.inc(func(m *metricsView) { m.eventsTotal++ })
	// 防御：订阅过滤可能被节点忽略（曾发生：大写 key 导致全量日志）。
	// 非 Swap topic 直接丢弃，不污染延迟样本。
	if len(l.Topics) == 0 || l.Topics[0] != v3.SwapTopic() {
		c.metrics.inc(func(m *metricsView) { m.eventsNonWeth++ })
		return
	}
	head := c.head()
	if head == 0 {
		c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
		c.writeEvent(l, 0, 0, "head_error", "")
		return // newHeads 订阅未就绪
	}
	if head < l.BlockNumber {
		head = l.BlockNumber // 防御：同流推送不可能落后于日志区块
	}
	lag := head - l.BlockNumber
	c.metrics.recordLag(lag)
	switch c.poolState(l.Address) {
	case 2, 3: // 已确认非 WETH / 非 Factory：纯延迟样本
		c.metrics.inc(func(m *metricsView) { m.eventsNonWeth++ })
		c.writeEvent(l, head, lag, "non_weth", "")
		return
	case 0: // 未知池：异步发现，本事件跳过（下次同池事件可能已就绪）
		c.enqueueDiscovery(l.Address)
		c.writeEvent(l, head, lag, "discovery_pending", "")
		return
	}
	// 已知 WETH 池
	if lag > c.staleSkip {
		c.metrics.inc(func(m *metricsView) { m.eventsStaleSkip++ })
		c.writeEvent(l, head, lag, "stale", "")
		return // 陈旧事件无即时套利信号，评估只是噪音
	}
	// token-group 评估：同一 token 的连续事件由 (headHash,pool) 快照缓存吸收
	// （同 head 重复评估零 RPC），无需节流或合并窗口。
	token := c.tokenOf(l.Address)
	if token == (common.Address{}) {
		c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
		c.writeEvent(l, head, lag, "eval_error", "")
		return
	}
	c.evaluateTokenBatch(ctx, token, []pendingLog{{
		l: l, head: head, lag: lag, receivedAt: time.Now(),
	}})
}

// tokenOf 返回池的另一侧 token（非 WETH 侧）。未知/非 WETH 池返回零地址。
func (c *canary) tokenOf(pool common.Address) common.Address {
	c.engineMu.Lock()
	defer c.engineMu.Unlock()
	st := c.reg.Pool(pool)
	if st == nil {
		return common.Address{}
	}
	p := v3.UnwrapState(st)
	if p == nil {
		return common.Address{}
	}
	if p.Token0 == c.weth {
		return p.Token1
	}
	if p.Token1 == c.weth {
		return p.Token0
	}
	return common.Address{}
}

// evaluateTokenBatch 评估一个 token 的全部在途事件（token-group/head-batch）：
// 同一 Head H 下组内所有池状态只刷新一次，全部 route 本地报价。
func (c *canary) evaluateTokenBatch(ctx context.Context, token common.Address, batch []pendingLog) {
	if len(batch) == 0 {
		return
	}
	head, hdr := c.headAndHeader()
	if head == 0 {
		for _, pe := range batch {
			c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
			c.writeEvent(pe.l, pe.head, pe.lag, "head_error", "")
		}
		return // newHeads 订阅未就绪
	}
	// 固定本批的 header hint：head 与 hint 同锁读取，评估零 header RPC。
	// （订阅 goroutine 可能在评估期间推进 hint——下一次评估会重新固定。）
	if hdr != nil {
		c.searcher.SetHeaderHint(hdr)
	}
	triggerPools := make([]common.Address, 0, len(batch))
	seen := make(map[common.Address]struct{})
	for _, pe := range batch {
		if _, dup := seen[pe.l.Address]; dup {
			continue
		}
		seen[pe.l.Address] = struct{}{}
		triggerPools = append(triggerPools, pe.l.Address)
	}
	first := batch[0]
	t0 := time.Now()
	ev := arbitrage.SwapEvent{
		Pool:        first.l.Address,
		BlockNumber: first.l.BlockNumber,
		BlockHash:   first.l.BlockHash,
		TxHash:      first.l.TxHash,
		LogIndex:    first.l.Index,
		ReceivedAt:  first.receivedAt.UnixMilli(),
	}
	c.engineMu.Lock()
	res, err := c.engine.ProcessTokenGroupAt(ctx, ev, triggerPools, token, head)
	c.engineMu.Unlock()
	totalMs := time.Since(t0).Milliseconds()
	if err != nil {
		c.metrics.inc(func(m *metricsView) { m.evalErrors++ })
		slog.Warn("token group evaluation failed", "token", token.Hex(),
			"block", first.l.BlockNumber, "pools", len(triggerPools), "err", err)
		for _, pe := range batch {
			c.writeEvent(pe.l, pe.head, pe.lag, "eval_error", "")
		}
		return
	}
	freshCnt := 0
	for _, pe := range batch {
		if pe.lag <= c.maxObsLag {
			freshCnt++
		}
	}
	c.metrics.recordEvalMs(totalMs)
	c.metrics.recordEvalStat(evalStat{
		rpcCalls:     res.RpcCalls,
		uniquePools:  res.UniquePools,
		routeCount:   res.RouteCount,
		stateFetchMs: res.StateFetchMs,
		localQuoteMs: res.LocalQuoteMs,
		totalMs:      res.TotalEvalMs,
		fresh:        freshCnt > 0,
	})
	c.metrics.inc(func(m *metricsView) {
		m.eventsEvaluated += uint64(len(batch))
		m.eventsFresh += uint64(freshCnt)
	})
	best := ""
	for _, cand := range res.Candidates {
		if cand != nil && cand.Decision != "" {
			best = cand.Decision
			break
		}
	}
	for _, pe := range batch {
		c.writeEvent(pe.l, pe.head, pe.lag, "", best)
	}
	c.writeCandidates(first.l, head, first.lag, totalMs, res, token)
}

// writeEvent 事件行落 JSONL（跳过原因 + 延迟；评估结果在 candidate 行）。
func (c *canary) writeEvent(l types.Log, head, lag uint64, skipped, best string) {
	rec := eventRecord{
		TS:             time.Now().UnixMilli(),
		Kind:           "event",
		Block:          l.BlockNumber,
		BlockHash:      l.BlockHash.Hex(),
		TxHash:         l.TxHash.Hex(),
		LogIndex:       l.Index,
		Pool:           l.Address.Hex(),
		Head:           head,
		StateLagBlocks: lag,
		Skipped:        skipped,
		BestDecision:   best,
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return
	}
	c.outMu.Lock()
	c.out.Write(raw)
	c.out.WriteByte('\n')
	c.out.Flush()
	c.outMu.Unlock()
}

// writeCandidates 候选落 JSONL + 统计 gross/net 正数。
func (c *canary) writeCandidates(l types.Log, head, lag uint64, evalMs int64, res *arbitrage.BlockResult, token common.Address) {
	mult := c.cfg.Arbitrage.LocalGasStressMultiplier
	if mult <= 0 {
		mult = 2
	}
	routeStr := func(hops []arbitrage.Hop) string {
		parts := make([]string, 0, len(hops))
		for _, h := range hops {
			parts = append(parts, h.Pool.Hex())
		}
		return strings.Join(parts, ";")
	}
	for _, cand := range res.Candidates {
		if cand == nil || cand.Route == nil {
			continue
		}
		rec := candidateRecord{
			TS:                  time.Now().UnixMilli(),
			Block:               l.BlockNumber,
			BlockHash:           l.BlockHash.Hex(),
			TxHash:              l.TxHash.Hex(),
			LogIndex:            l.Index,
			Pool:                l.Address.Hex(),
			Head:                head,
			StateLagBlocks:      lag,
			EventToEvaluationMs: evalMs,
			Decision:            cand.Decision,
			RejectReason:        cand.RejectReason,
			Route:               routeStr(cand.Route),
			Rank:                cand.Rank,
			Token:               token.Hex(),
			RpcCalls:            res.RpcCalls,
			UniquePools:         res.UniquePools,
			RouteCount:          res.RouteCount,
			StateFetchMs:        res.StateFetchMs,
			LocalQuoteMs:        res.LocalQuoteMs,
			TotalEvalMs:         res.TotalEvalMs,
		}
		writeBig := func(s string) string {
			if s == "" {
				return ""
			}
			return s
		}
		rec.InputAmountWei = writeBig(bigStr(cand.InputAmount))
		rec.GrossProfitWei = writeBig(bigStr(cand.GrossProfit))
		rec.NetProfitWei = writeBig(bigStr(cand.ExpectedNetProfit))
		rec.GasCostWei = writeBig(bigStr(cand.GasCostWei))
		if cand.Decision == decisionProfitable && cand.GrossProfit != nil && cand.GrossProfit.Sign() > 0 {
			c.metrics.inc(func(m *metricsView) { m.grossPositive++ })
			// net_mx = gross - m × (gas_cost / stress_multiplier)（1x = 基础费一次）
			if cand.GasCostWei != nil && cand.GasCostWei.Sign() > 0 {
				cost1x := new(big.Int).Div(new(big.Int).Set(cand.GasCostWei), big.NewInt(int64(mult)))
				if cost1x.Sign() > 0 {
					for i := 1; i <= 3; i++ {
						net := new(big.Int).Sub(cand.GrossProfit, new(big.Int).Mul(cost1x, big.NewInt(int64(i))))
						switch i {
						case 1:
							rec.Net1xWei = net.String()
						case 2:
							rec.Net2xWei = net.String()
						case 3:
							rec.Net3xWei = net.String()
						}
						if net.Sign() > 0 {
							c.metrics.inc(func(m *metricsView) { m.netPos[i-1]++ })
						}
					}
				}
			}
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			slog.Warn("marshal record", "err", err)
			continue
		}
		c.outMu.Lock()
		c.out.Write(raw)
		c.out.WriteByte('\n')
		c.out.Flush()
		c.outMu.Unlock()
	}
}

func bigStr(v *big.Int) string {
	if v == nil {
		return ""
	}
	return v.String()
}

// statsLoop 每 60s 输出一次进度指标。
func statsLoop(ctx context.Context, c *canary) {
	t := time.NewTicker(60 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m := c.metrics.snapshot()
			lagP50 := pctUint64(m.lagSamples, 50)
			evalP50 := pctInt64(m.evalMsSamples, 50)
			slog.Info("canary progress",
				"elapsed", time.Since(m.startedAt).Round(time.Second),
				"events", m.eventsTotal, "non_weth", m.eventsNonWeth,
				"evaluated", m.eventsEvaluated, "fresh", m.eventsFresh,
				"stale_skipped", m.eventsStaleSkip, "eval_errors", m.evalErrors,
				"lag_p50", lagP50, "eval_ms_p50", evalP50,
				"gross_positive", m.grossPositive,
				"pools_discovered", m.poolsDiscovered,
				"disconnects", m.disconnects, "recoveries", m.recoveries,
				"recovered_blocks", m.recoveredBlocks)
		}
	}
}

// crossCheckLoop 每 30s 用公共 Robinhood RPC 对照 Alchemy head（secondary cross-check）。
func crossCheckLoop(ctx context.Context, c *canary) {
	pub, err := ethclient.Dial(c.publicRPC)
	if err != nil {
		slog.Warn("public rpc dial failed; cross-check disabled", "err", err)
		return
	}
	defer pub.Close()
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ah, err1 := c.httpCli.BlockNumber(ctx)
			ph, err2 := pub.BlockNumber(ctx)
			if err1 != nil || err2 != nil {
				slog.Warn("cross-check head read failed", "alchemy_err", err1, "public_err", err2)
				continue
			}
			mismatch := ah != ph
			c.metrics.inc(func(m *metricsView) {
				m.crossChecks++
				if mismatch {
					m.crossMismatches++
				}
			})
			if mismatch {
				slog.Warn("head mismatch", "alchemy", ah, "public", ph)
			} else {
				slog.Debug("head cross-check ok", "head", ah)
			}
		}
	}
}

// summary 结束汇总：Canary 门槛指标（p50/p95/p99）。
func summary(ctx context.Context, c *canary, m metricsView, runErr error) {
	elapsed := time.Since(m.startedAt)
	lag50, lag95, lag99 := pctUint64(m.lagSamples, 50), pctUint64(m.lagSamples, 95), pctUint64(m.lagSamples, 99)
	eval50, eval95 := pctInt64(m.evalMsSamples, 50), pctInt64(m.evalMsSamples, 95)
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format+"\n", args...) }
	w("=== rh-canary summary ===")
	w("run_duration: %s", elapsed.Round(time.Second))
	if runErr != nil {
		w("run_error: %v", runErr)
	}
	w("events_total: %d", m.eventsTotal)
	w("events_non_weth_filtered: %d", m.eventsNonWeth)
	w("pools_preloaded: %d", len(c.wethPools)-int(m.poolsDiscovered))
	w("pools_discovered: %d", m.poolsDiscovered)
	w("events_evaluated: %d", m.eventsEvaluated)
	w("events_fresh (lag<=%d): %d", c.maxObsLag, m.eventsFresh)
	w("events_stale_skipped (lag>%d): %d", c.staleSkip, m.eventsStaleSkip)
	w("eval_errors: %d", m.evalErrors)
	w("wss_disconnects: %d", m.disconnects)
	w("gap_recoveries: %d", m.recoveries)
	w("gap_recovered_blocks: %d", m.recoveredBlocks)
	w("gap_recovered_logs: %d", m.recoveredLogs)
	w("cross_checks: %d", m.crossChecks)
	w("cross_mismatches: %d", m.crossMismatches)
	w("state_lag_blocks p50/p95/p99: %d/%d/%d", lag50, lag95, lag99)
	w("event_to_evaluation_ms p50/p95: %d/%d", eval50, eval95)
	w("eval total_ms p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int64 { return st.totalMs }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int64 { return st.totalMs }, 95))
	w("eval state_fetch_ms p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int64 { return st.stateFetchMs }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int64 { return st.stateFetchMs }, 95))
	w("eval local_quote_ms p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int64 { return st.localQuoteMs }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int64 { return st.localQuoteMs }, 95))
	w("eval rpc_calls p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int { return st.rpcCalls }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int { return st.rpcCalls }, 95))
	w("eval unique_pools p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int { return st.uniquePools }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int { return st.uniquePools }, 95))
	w("eval route_count p50/p95: %d/%d", evalStatPct(m.evalStats, func(st evalStat) int { return st.routeCount }, 50),
		evalStatPct(m.evalStats, func(st evalStat) int { return st.routeCount }, 95))
	w("gross_positive_candidates: %d", m.grossPositive)
	w("net_positive 1x/2x/3x: %d/%d/%d", m.netPos[0], m.netPos[1], m.netPos[2])
	w("acceptance: lag p50<=0 p95<=1 p99<=2; eval total_ms p50<250ms p95<750ms; state_fetch p50<100ms")
	out := b.String()
	fmt.Print(out)
	// 同目录落一份 summary 便于留档
	sp := fmt.Sprintf("data/canary/summary-%s.txt", m.startedAt.Format("20060102-150405"))
	_ = os.MkdirAll("data/canary", 0o755)
	if err := os.WriteFile(sp, []byte(out), 0o644); err != nil {
		slog.Warn("write summary", "err", err)
	}
}

func pctUint64(sorted []uint64, p float64) uint64 {
	if len(sorted) == 0 {
		return 0
	}
	s := append([]uint64(nil), sorted...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[pctIndex(len(s), p)]
}

func pctInt64(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	s := append([]int64(nil), sorted...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[pctIndex(len(s), p)]
}

// evalStatPct 对 evalStats 的数值字段取分位数（int64 与 int 统一走 int64）。
func evalStatPct[T int64 | int](stats []evalStat, get func(evalStat) T, p float64) int64 {
	if len(stats) == 0 {
		return 0
	}
	vals := make([]int64, 0, len(stats))
	for _, st := range stats {
		vals = append(vals, int64(get(st)))
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	return vals[pctIndex(len(vals), p)]
}

func pctIndex(n int, p float64) int {
	idx := int(math.Ceil(p/100*float64(n))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

func firstNonEmpty(list []string) string {
	for _, s := range list {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

// maskKey 隐藏 URL 中的 API key（日志/汇总不泄密钥）。
func maskKey(u string) string {
	i := strings.Index(u, "/v2/")
	if i < 0 {
		return u
	}
	return u[:i+4] + "***"
}
