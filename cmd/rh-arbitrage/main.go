// rh-arbitrage: 套利引擎。默认 shadow 模式：发现、模拟、落盘，不发送交易。
// Robinhood 公共端点无 WS，默认轮询；配置 WS 端点时优先 WS。
package main

import (
	"context"
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
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/auren23/rh-searcher/internal/arbitrage"
	"github.com/auren23/rh-searcher/internal/chain"
	"github.com/auren23/rh-searcher/internal/config"
	"github.com/auren23/rh-searcher/internal/dex"
	"github.com/auren23/rh-searcher/internal/dex/v3"
	"github.com/auren23/rh-searcher/internal/simulation"
	"github.com/auren23/rh-searcher/internal/storage"
	"github.com/auren23/rh-searcher/internal/telemetry"
)

func main() {
	cfgPath := flag.String("config", "configs/robinhood.yaml", "config file")
	flag.Parse()

	telemetry.SetupLogging(slog.LevelInfo)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := config.LoadMerged(*cfgPath, "configs/dexes.yaml", "configs/morpho.yaml")
	if err != nil {
		slog.Error("load config", "err", err)
		os.Exit(1)
	}
	if len(cfg.Dexes.V3) == 0 {
		slog.Error("no v3 dex configured")
		os.Exit(1)
	}

	readURL := cfg.RPC.Groups.Read[0]
	var src chain.Source
	ws, wsErr := chain.NewWSClient(ctx, readURL)
	if wsErr != nil {
		slog.Warn("WS unavailable, falling back to polling", "url", readURL, "err", wsErr)
		httpCli, dialErr := ethclient.Dial(cfg.RPC.Groups.Archive[0])
		if dialErr != nil {
			slog.Error("dial http rpc", "err", dialErr)
			os.Exit(1)
		}
		src = chain.NewPollingSource(httpCli)
	} else {
		src = ws
	}

	// Adapter 用独立 HTTP Read RPC（连接池），不复用 WS 客户端
	readCli, dialErr := ethclient.Dial(cfg.RPC.Groups.Archive[0])
	if dialErr != nil {
		slog.Error("dial read rpc for adapter", "err", dialErr)
		os.Exit(1)
	}

	d := cfg.Dexes.V3[0]
	adapter, err := v3.NewAdapter(readCli, d.Name,
		common.HexToAddress(d.Factory), common.HexToAddress(d.Router), d.RouterKind,
		common.HexToHash(d.InitCodeHash), d.FactoryBlock)
	if err != nil {
		slog.Error("v3 adapter", "err", err)
		os.Exit(1)
	}

	reg := dex.NewRegistry()
	graph := dex.NewGraph()
	ckpt := storage.NewCheckpoint("deployments/checkpoint.json")
	heights := map[string]uint64{}
	var pgCkpt *storage.PGCheckpoint
	fromBlock := uint64(0)

	// 先初始化 DB（sink 必须可用，不允许静默丢失候选）；PG 是 checkpoint 与池恢复的主事实源
	var sink *storage.DB
	if cfg.Storage.PostgresURL != "" {
		sink, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Error("postgres unavailable, refusing to run without persistence", "err", err)
			os.Exit(1)
		}
		defer sink.Close()
		pgCkpt = storage.NewPGCheckpoint(sink)
		if pgHeights, err := pgCkpt.Load(ctx); err == nil && len(pgHeights) > 0 {
			heights = pgHeights
		}
		// 从数据库恢复全部池（重建 Registry/Graph），再补扫增量
		if _, err := storage.RestorePools(ctx, sink, reg, graph, common.HexToAddress(cfg.Chain.WETH)); err != nil {
			slog.Error("restore pools", "err", err)
			os.Exit(1)
		}
		// 旧数据 tick_spacing 为 0：补查（Registry 里是 stateAdapter 包装，
		// 必须 UnwrapState——裸断言会 panic）
		for _, st := range reg.AllPools() {
			pool := v3.UnwrapState(st)
			if pool == nil {
				slog.Error("unsupported pool state", "type", fmt.Sprintf("%T", st))
				os.Exit(1)
			}
			if pool.TickSpacing <= 0 {
				fresh, err := adapter.PoolByAddress(ctx, pool.Address)
				if err == nil {
					reg.UpsertPool(v3.State(fresh))
					graph.AddPool(fresh.Pool(), fresh.Address)
				} else {
					slog.Warn("backfill tickSpacing failed", "pool", pool.Address.Hex(), "err", err)
				}
			}
		}
	} else {
		if jh, err := ckpt.Load(); err == nil {
			heights = jh // JSON fallback（无 PG 的本地单进程开发）
		}
	}
	if fromBlock == 0 {
		fromBlock = heights[storage.CheckpointPools]
	}
	if fromBlock < d.FactoryBlock {
		fromBlock = d.FactoryBlock
	}

	// 启动时读链头再引导（PG pools checkpoint 续跑；未完成的补全到链头）
	headNum, err := readCli.BlockNumber(ctx)
	if err != nil {
		slog.Error("read chain head", "err", err)
		os.Exit(1)
	}
	lastPoolBlock, err := v3.Bootstrap(ctx, adapter, reg, graph, fromBlock, headNum, v3.BootstrapOptions{
		WETHOnly: common.HexToAddress(cfg.Chain.WETH),
	})
	if err != nil {
		slog.Error("bootstrap failed (pools may be missing)", "err", err)
		os.Exit(1)
	}
	// 全部已知池统一持久化（bootstrap 只写内存，重启依赖 PG 恢复）
	// 池批量保存与 pools checkpoint 在同一事务：任一池失败，pools 游标不推进
	if sink != nil {
		allPools := make([]storage.Pool, 0, len(reg.AllPools()))
		for _, st := range reg.AllPools() {
			sp := st.Pool()
			p3 := v3.UnwrapState(st)
			if p3 == nil {
				slog.Error("unsupported pool state", "type", fmt.Sprintf("%T", st))
				os.Exit(1)
			}
			// 来源不猜测：DiscoverPools 源头已写 pool_created_log；
			// 旧库/兜底保持原值（unknown 不被错误升级为权威）
			src := p3.ProvenanceSource
			allPools = append(allPools, storage.Pool{
				Address: sp.ID, Exchange: sp.Exchange, Protocol: "v3",
				Token0: sp.Token0.Hex(), Token1: sp.Token1.Hex(),
				Fee: sp.Fee, TickSpacing: p3.TickSpacing,
				// 创建溯源：bootstrap 的 DiscoverPools 直接从 PoolCreated 日志读取
				CreatedBlock: p3.CreatedBlock, CreatedBlockHash: p3.CreatedBlockHash.Hex(),
				ProvenanceSource: src,
			})
		}
		if err := sink.CommitPools(ctx, allPools, lastPoolBlock); err != nil {
			slog.Error("pool persistence failed, pools cursor not advanced", "err", err)
			os.Exit(1)
		}
	} else {
		_ = ckpt.Save(storage.CheckpointPools, lastPoolBlock)
	}

	// 评估模式：local_only（零资金，不要求 executor）/ latest_observe（需主网
	// executor，latest 状态对齐）/ historical_strict（需 archive）。显式配置，
	// 禁止模式间静默降级。
	simMode := cfg.Arbitrage.SimulationMode
	if simMode == "" {
		simMode = "local_only"
	}
	switch simMode {
	case "local_only", "latest_observe", "historical_strict":
	default:
		slog.Error("invalid simulation_mode", "mode", simMode,
			"want", "local_only | latest_observe | historical_strict")
		os.Exit(1)
	}
	var sim *simulation.ExecutorSimulator
	var contractAddr common.Address
	var contractBal *big.Int
	if simMode != "local_only" {
		// latest_observe / historical_strict 失败关闭：无执行合约 / 无 sim RPC /
		// 合约无代码 → 拒绝启动（不允许"以为在模拟其实没调合约"）
		if cfg.Executor.Contract == "" || cfg.Executor.Wallet == "" {
			slog.Error("executor.contract / executor.wallet not configured",
				"required for simulation_mode", simMode)
			os.Exit(1)
		}
		if len(cfg.RPC.Groups.Sim) == 0 {
			slog.Error("no sim RPC configured (required for simulation_mode)", "mode", simMode)
			os.Exit(1)
		}
		simCli, dialErr := ethclient.Dial(cfg.RPC.Groups.Sim[0])
		if dialErr != nil {
			slog.Error("sim rpc dial failed", "err", dialErr)
			os.Exit(1)
		}
		contractAddr = common.HexToAddress(cfg.Executor.Contract)
		code, codeErr := simCli.CodeAt(ctx, contractAddr, nil)
		if codeErr != nil || len(code) == 0 {
			slog.Error("executor contract has no code", "contract", cfg.Executor.Contract, "err", codeErr)
			os.Exit(1)
		}
		sim = simulation.NewExecutorSimulator(simCli, contractAddr,
			common.HexToAddress(cfg.Executor.Wallet), 5_000_000)
		// Executor 启动预检（wallet/weth/factory/paused/余额）→ 余额用于资金限制
		contractBal, err = preflightExecutor(ctx, simCli, contractAddr, cfg)
	}
	if err != nil {
		slog.Error("executor preflight failed", "err", err)
		os.Exit(1)
	}

	minProfit := big.NewInt(1e13)
	safetyMargin := big.NewInt(5e12)
	topK := 5
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
	if cfg.Arbitrage.SimulationTopK > 0 {
		topK = cfg.Arbitrage.SimulationTopK
	}
	var evaluator arbitrage.Evaluator
	if simMode != "local_only" {
		evaluator = simulation.NewSimulationEvaluator(sim, cfg.Chain.ID, safetyMargin)
	} else {
		// local_only：engine 的本地分支不调用 evaluator（零资金、无合约）
		evaluator = nil
	}
	slog.Info("evaluation mode", "simulation_mode", simMode,
		"contract", cfg.Executor.Contract,
		"min_profit_wei", minProfit.String(), "safety_margin_wei", safetyMargin.String(), "top_k", topK)

	weth := common.HexToAddress(cfg.Chain.WETH)

	searcher := arbitrage.NewLocalSearcher(graph, reg, adapter, weth)
	// 资金限制：max_input_wei / min_input_wei 与执行合约当前 WETH 余额（预检已读取）。
	// 逐块评估时会用该区块的历史余额重新 SetFunding（见 evaluatePending）。
	var maxInputWei *big.Int
	if cfg.Arbitrage.MaxInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MaxInputWei, 10); ok {
			maxInputWei = v
		}
	}
	var executorAddr common.Address
	if simMode != "local_only" {
		executorAddr = contractAddr
	}
	searcher.SetFunding(maxInputWei, contractBal)
	if cfg.Arbitrage.MinInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MinInputWei, 10); ok {
			searcher.SetMinInput(v)
		}
	}
	if cfg.Mode.Run == "live" {
		slog.Error("live mode not implemented (signer/nonce/broadcaster/pnl not wired)")
		os.Exit(1)
	}
	maxObsLag := cfg.Arbitrage.MaxObservationLagBlocks
	if maxObsLag == 0 {
		maxObsLag = 2
	}
	engineCfg := arbitrage.Config{
		ChainID:         cfg.Chain.ID,
		WETH:            weth,
		MinProfitWei:    minProfit,
		SafetyMarginWei: safetyMargin,
		MaxHops:         2,
		TopK:            topK,
		Mode:            cfg.Mode.Run,
		SimulationMode:  simMode,
		LocalGasUnits:   cfg.Arbitrage.LocalGasUnits,
		LocalGasStressMultiplier: cfg.Arbitrage.LocalGasStressMultiplier,
		MaxObservationLagBlocks: maxObsLag,
	}
	engine := arbitrage.NewEngine(
		engineCfg,
		nil, // Engine 不再直写数据库；结果经 BlockResult 由 CommitBlockResult 事务提交
		searcher,
		evaluator,
		arbitrage.NewExecutor(),
	)

	slog.Info("arbitrage engine started", "mode", cfg.Mode.Run, "pools", len(reg.AllPools()))
	// 区块原子摄取：head 驱动，日志以 HTTP FilterLogs 精确取整块（唯一事实源）。
	startHead, err := readCli.BlockNumber(ctx)
	if err != nil {
		slog.Error("read chain head", "err", err)
		os.Exit(1)
	}
	eventTopics := [][]common.Hash{{
		v3.SwapTopic(),
		common.HexToHash("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde"), // Mint
		common.HexToHash("0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c"), // Burn
	}}
	saveCheckpoint := func(strategy string, number uint64) error {
		if pgCkpt != nil {
			return pgCkpt.Save(ctx, strategy, number)
		}
		return ckpt.Save(strategy, number)
	}
	// 游标：高度 + 上次真正处理过的区块 hash（reorg 检测基准）
	var lastApplied uint64
	var lastAppliedHash common.Hash
	switch {
	case pgCkpt != nil:
		h, hh, err := pgCkpt.LoadWithHash(ctx, storage.CheckpointBlocks)
		switch {
		case err == nil:
			lastApplied = h
			lastAppliedHash = hh
		case errors.Is(err, pgx.ErrNoRows):
			// 空 PG 首次启动：从当前链头开始（绝不从区块 1 扫描），并立即落 checkpoint
			hdr, err := readCli.HeaderByNumber(ctx, nil)
			if err != nil {
				slog.Error("first header", "err", err)
				os.Exit(1)
			}
			if err := sink.InitializeBlockCheckpoint(ctx, storage.CheckpointBlocks,
				hdr.Number.Uint64(), hdr.Hash().Hex(), hdr.ParentHash.Hex()); err != nil {
				slog.Error("first checkpoint init failed", "err", err)
				os.Exit(1) // 失败关闭
			}
			lastApplied = hdr.Number.Uint64()
			lastAppliedHash = hdr.Hash()
		default:
			slog.Error("load checkpoint", "err", err)
			os.Exit(1)
		}
	default:
		if h, ok := heights[storage.CheckpointBlocks]; ok {
			lastApplied = h
		} else {
			// 首次实时 Shadow：从当前链头开始，只处理启动后的新块。
			lastApplied = startHead
			if hdr, err := readCli.HeaderByNumber(ctx, nil); err == nil {
				lastAppliedHash = hdr.Hash()
			}
			if err := saveCheckpoint(storage.CheckpointBlocks, startHead); err != nil {
				slog.Error("first checkpoint save failed", "err", err)
				os.Exit(1) // 失败关闭
			}
		}
	}
	// evaluate 游标：评估进度（≤ ingest 游标）。ingest 已提交但评估崩溃 →
	// 重启从这里重新评估，候选不丢失。
	lastEvaluated := lastApplied
	if pgCkpt != nil {
		eh, _, err := pgCkpt.LoadWithHash(ctx, storage.CheckpointEvaluate)
		switch {
		case err == nil:
			lastEvaluated = eh
		case errors.Is(err, pgx.ErrNoRows):
			// 0008 迁移未补种 evaluate 游标：静默回退会永久跳过未评估队列
			slog.Error("evaluate checkpoint missing: migration 0008 incomplete")
			os.Exit(1)
		default:
			slog.Error("load evaluate checkpoint", "err", err)
			os.Exit(1)
		}
	}
	if lastApplied > 0 && lastAppliedHash == (common.Hash{}) {
		// JSON checkpoint 无 hash：重读当前链该高度（离线 reorg 无法识别，保守处理）
		if hdr, err := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(lastApplied)); err == nil {
			lastAppliedHash = hdr.Hash()
		}
	}

	// 区块处理：日志只确定 affected pools / 新池，不做任何内存修改；
	// 评估完全走 RPC（RefreshRoute 固定 stateBlock）。
	// 数据库事务提交成功后才应用日志到内存（exactly-once，事务失败内存保持原状）。
	type pendingPool struct {
		pool   *v3.Pool
		isNew  bool
		applies []func()          // 成功后按序执行
		tickBounds [][2]int       // 成功后 ResyncMintBurn
	}
	// 回滚本区块临时注册的新池（事务失败/处理错误后调用；正式注册在 commit 成功后）
	rollbackTempPools := func(pending map[common.Address]*pendingPool) {
		for _, pp := range pending {
			if pp.isNew {
				reg.Remove(pp.pool.Address)
				graph.Remove(pp.pool.Address)
			}
		}
	}
	// processBlockWithLogs：日志由调用方提供（批量拉取按块分发，或单块拉取）。
	// evaluate=false 时只取日志不评估（补扫路径用，避免重复评估同一当前状态）。
	// skipPoolVerify=true（补扫）：bootstrap 已扫描到 head，补扫范围内不存在
	// 未发现的新池——未知池直接跳过，不做 PoolByAddress 验证（避免补扫期间
	// 每池 5 个 RPC 被限速打死）
	processBlockWithLogs := func(h chain.BlockEvent, logs []types.Log, evaluate, skipPoolVerify, skipHashCheck bool) (*arbitrage.BlockResult, map[common.Address]struct{}, map[common.Address]*pendingPool, error) {
		res := &arbitrage.BlockResult{Block: h.Number, BlockHash: h.Hash}
		if !skipHashCheck {
			for _, l := range logs {
				if l.BlockHash != h.Hash {
					return nil, nil, nil, fmt.Errorf("block %d hash mismatch: log=%s header=%s", h.Number, l.BlockHash.Hex(), h.Hash.Hex())
				}
			}
		}
		sort.Slice(logs, func(i, j int) bool {
			if logs[i].TxIndex != logs[j].TxIndex {
				return logs[i].TxIndex < logs[j].TxIndex
			}
			return logs[i].Index < logs[j].Index
		})
		pending := map[common.Address]*pendingPool{}
		newPoolMetas := []arbitrage.PoolMeta{}
		affected := map[common.Address]struct{}{}
		for _, l := range logs {
			pp, ok := pending[l.Address]
			if !ok {
				state := reg.Pool(l.Address)
				if state == nil && skipPoolVerify {
					// 补扫：bootstrap 已注册全部池；未知 = WETH 过滤跳过
					continue
				}
				if state == nil {
					// 启动后新创建的池：验证归属。非本 Factory 池 → 跳过；
					// RPC 类错误 → 禁止推进（不能永久跳过这个事件）。
					pool, derr := adapter.PoolByAddress(ctx, l.Address)
					if derr != nil {
						if errors.Is(derr, v3.ErrNotFactoryPool) {
							slog.Debug("not a factory pool, skipped", "addr", l.Address.Hex())
							continue
						}
						rollbackTempPools(pending)
						return nil, nil, nil, fmt.Errorf("block %d pool %s verify: %w", h.Number, l.Address.Hex(), derr)
					}
					// WETH-only 运行集：非 WETH 池不进入（两跳路线用不到）
					if pool.Token0 != weth && pool.Token1 != weth {
						slog.Debug("non-WETH pool skipped", "addr", l.Address.Hex())
						continue
					}
					if pool.TickSpacing > 0 {
						meta := arbitrage.PoolMeta{
							Address: pool.Address, Exchange: pool.Exchange,
							Token0: pool.Token0, Token1: pool.Token1,
							Fee: pool.Fee, TickSpacing: pool.TickSpacing,
						}
						// 创建溯源：按 PoolCreated(token0, token1, fee) 三个 indexed
						// 参数精确查询（节点端 topics 过滤，无需全量扫描）。
						// 失败降级观察块兜底（observed_swap_fallback）——内存与
						// 数据库必须一致（fallback 也同步 pool 对象，否则评估游标
						// 落后时未来池过滤失效）。
						if cb, ch, cerr := adapter.PoolCreatedByTokens(ctx, pool.Token0, pool.Token1, pool.Fee); cerr == nil {
							meta.CreatedBlock = cb
							meta.CreatedBlockHash = ch
							meta.ProvenanceSource = "pool_created_log"
							pool.CreatedBlock = cb
							pool.CreatedBlockHash = ch
							pool.ProvenanceSource = "pool_created_log"
						} else {
							meta.CreatedBlock = h.Number
							meta.CreatedBlockHash = h.Hash
							meta.ProvenanceSource = "observed_swap_fallback"
							pool.CreatedBlock = h.Number
							pool.CreatedBlockHash = h.Hash
							pool.ProvenanceSource = "observed_swap_fallback"
						}
						newPoolMetas = append(newPoolMetas, meta)
					}
					pp = &pendingPool{pool: pool, isNew: true}
					// P1: 新池先临时进 Registry/Graph——Engine 评估（RefreshRoute 查
					// registry + FindRoutes 查 graph）必须在事务前能看到它，否则
					// 新池的第一次 Swap 找不到任何路由。事务失败时池保持临时状态
					// （幽灵池，重启后从 PG 恢复即消失），DB 无写入，无数据污染。
					reg.UpsertPool(v3.State(pool))
					graph.AddPool(pool.Pool(), l.Address)
				} else {
					pool := v3.UnwrapState(state)
					if pool == nil {
						rollbackTempPools(pending)
						return nil, nil, nil, fmt.Errorf(
							"block %d pool %s unsupported state %T",
							h.Number, l.Address.Hex(), state)
					}
					pp = &pendingPool{pool: pool}
				}
				pending[l.Address] = pp
			}
			apply, derr := adapter.DecodeLog(pp.pool, l)
			if derr != nil {
				// 已知事件解码失败 → 禁止推进（不允许永久跳过）
				rollbackTempPools(pending)
				return nil, nil, nil, fmt.Errorf("block %d decode %s tx=%d log=%d: %w",
					h.Number, l.Address.Hex(), l.TxIndex, l.Index, derr)
			}
			if apply == nil {
				continue
			}
			pp.applies = append(pp.applies, apply)
			if l.Topics[0] == eventTopics[0][1] || l.Topics[0] == eventTopics[0][2] {
				if tl, tu, terr := v3.DecodeTickBounds(l); terr == nil {
					pp.tickBounds = append(pp.tickBounds, [2]int{tl, tu})
				}
			} else {
				affected[l.Address] = struct{}{} // 仅 Swap 触发评估
			}
		}
		res.NewPools = newPoolMetas
		pools := make([]common.Address, 0, len(affected))
		for p := range affected {
			pools = append(pools, p)
		}
		if evaluate && len(pools) > 0 {
			processed, err := engine.ProcessBlock(ctx, arbitrage.SwapEvent{
				BlockNumber: h.Number,
				BlockHash:   h.Hash,
				ReceivedAt:  time.Now().UnixMilli(),
			}, pools)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("block %d evaluate: %w", h.Number, err)
			}
			res.Candidates = processed.Candidates
		}
		return res, affected, pending, nil
	}


	// 事务成功后应用日志到内存（exactly-once）。Resync 失败仅该池滞后，
	// 下次 RefreshRoute 的 RPC 状态会自愈，不影响已提交数据。
	applyCommitted := func(pending map[common.Address]*pendingPool) {
		for addr, pp := range pending {
			for _, f := range pp.applies {
				f()
			}
			for _, tb := range pp.tickBounds {
				if err := adapter.ResyncMintBurn(ctx, pp.pool, tb[0], tb[1]); err != nil {
					slog.Warn("resync mint/burn", "pool", pp.pool.Address.Hex(), "err", err)
				}
			}
			reg.UpsertPool(v3.State(pp.pool))
			if pp.isNew {
				graph.AddPool(pp.pool.Pool(), addr)
			}
		}
	}

	// DB 提交（ingest：池 + 受影响池队列 + checkpoint + 历史）+ 成功后应用内存
	// + 推进 ingest 游标。提交失败 → 回滚本区块临时注册的新池（幽灵池不能留下：
	// 重试同一区块时会被误判为旧池而不再写入 dex_pools）
	commitResult := func(h chain.BlockEvent, res *arbitrage.BlockResult, pending map[common.Address]*pendingPool, affected map[common.Address]struct{}) error {
		if sink != nil {
			newPools := make([]storage.Pool, 0, len(res.NewPools))
			for _, np := range res.NewPools {
				newPools = append(newPools, storage.Pool{
					Address: np.Address.Hex(), Exchange: np.Exchange, Protocol: "v3",
					Token0: np.Token0.Hex(), Token1: np.Token1.Hex(),
					Fee: np.Fee, TickSpacing: np.TickSpacing,
					CreatedBlock: np.CreatedBlock, CreatedBlockHash: np.CreatedBlockHash.Hex(),
					ProvenanceSource: np.ProvenanceSource,
				})
			}
			affectedList := make([]string, 0, len(affected))
			for p := range affected {
				affectedList = append(affectedList, p.Hex())
			}
			if err := sink.CommitBlockIngest(ctx, h.Number, h.Hash.Hex(), h.Parent.Hex(), newPools, affectedList, h.ReceivedAtMs); err != nil {
				rollbackTempPools(pending)
				return fmt.Errorf("commit block %d: %w", h.Number, err)
			}
		} else {
			if err := saveCheckpoint(storage.CheckpointBlocks, h.Number); err != nil {
				rollbackTempPools(pending)
				return err
			}
		}
		applyCommitted(pending)
		lastApplied = h.Number
		lastAppliedHash = h.Hash
		return nil
	}

	// reorg 处理：新链与旧链在 lastApplied+1 断裂时，用 processed_blocks 历史
	// 按高度对比新旧 hash 找共同祖先（≤64 层）；RollbackToAncestor 单事务
	// （孤块标记 + checkpoint 回退）成功后，重建内存池状态，回退游标。
	// 任何一步失败 → 返回 error，由调用方失败关闭（不允许双规范数据运行）。
	handleReorg := func() error {
		rollback := lastApplied
		slog.Warn("reorg detected", "at", lastApplied,
			"expected", lastAppliedHash.Hex())
		ancestor := uint64(0)
		ancestorHash := ""
		found := false
		for d := uint64(0); d <= 64; d++ {
			bh := rollback - d
			var oldHash *string
			qerr := sink.QueryRow(ctx, `
				SELECT block_hash FROM processed_blocks
				WHERE strategy = $1 AND block_number = $2 AND canonical = TRUE`,
				storage.CheckpointBlocks, bh).Scan(&oldHash)
			if qerr != nil || oldHash == nil {
				break // 无更早历史：保守回退到最远已知层
			}
			hdr, herr := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(bh))
			if herr != nil {
				return fmt.Errorf("reorg ancestor header %d: %w", bh, herr)
			}
			if hdr.Hash().Hex() == *oldHash {
				ancestor = bh
				ancestorHash = *oldHash
				found = true
				break
			}
		}
		if !found {
			// 历史缺失或全被替换：无法确认共同祖先时禁止自动恢复
			// （盲回退 64 层不是祖先，深孤链数据会保持 canonical=true）
			return fmt.Errorf("reorg: no common ancestor found within %d blocks "+
				"(rollback needed at %d); manual recovery required", 64, rollback)
		}
		if ancestor < rollback {
			slog.Warn("reorg common ancestor", "rollback_to", ancestor)
		}
		var ancHdr *types.Header
		if found {
			var herr error
			ancHdr, herr = readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(ancestor))
			if herr != nil {
				return fmt.Errorf("reorg ancestor header: %w", herr)
			}
		}
		if err := sink.RollbackToAncestor(ctx, storage.CheckpointBlocks, ancestor,
			ancestorHash, ancParent(ancHdr)); err != nil {
			return fmt.Errorf("reorg rollback: %w", err)
		}
		// 数据库已回退：重建内存池状态（孤块 tick/swap 状态不能残留），
		// 失败关闭（内存与数据库不一致比崩溃更危险）
		reg.Reset()
		graph.Reset()
		if _, err := storage.RestorePools(ctx, sink, reg, graph, common.HexToAddress(cfg.Chain.WETH)); err != nil {
			return fmt.Errorf("reorg rebuild registry: %w", err)
		}
		lastApplied = ancestor
		lastAppliedHash = common.HexToHash(ancestorHash) // 重处理期间继续衔接校验
		// evaluate 只退不进（LEAST 语义，与事务一致）
		if lastEvaluated > ancestor {
			lastEvaluated = ancestor
		}
		return nil
	}

	heads, headErrs := src.SubscribeBlocks(ctx)
	go func() {
		for err := range headErrs {
			slog.Warn("head subscription error", "err", err)
		}
	}()

	// backfill：从 lastApplied+1 顺序补扫到 to（含）。逐块校验 parent 衔接
	// （离线/漏 head 的 reorg 在第一块即被发现），逐块提交 ingest（历史 +
	// 受影响池评估队列 + checkpoint）并应用内存状态，但不评估。
	// 评估由 evaluatePending 从数据库队列聚合执行——ingest 提交后进程崩溃，
	// 重启仍能重新评估（双游标，候选不丢失）。
	// 吞吐指标（每 60s 落库一次，见指标循环）
	metrics := &throughputMetrics{}
	// 机会衰减队列：正毛利机会在 T+1/2/4 块重报价（同 head 快照缓存零额外 RPC）
	type decayEntry struct {
		originBlock uint64
		route       []arbitrage.Hop
		amount      *big.Int
		delay       int
	}
	decayQueue := []decayEntry{}
	cloneRoute := func(hops []arbitrage.Hop) []arbitrage.Hop {
		out := make([]arbitrage.Hop, len(hops))
		copy(out, hops)
		return out
	}
	// 有预算评估（前置声明：backfill 定义在其前，闭包变量先声明后赋值）
	var evaluateBudgeted func(evalBudget int) error
	// 自适应批量拉日志：跨块一次 FilterLogs（合并 RPC 请求），按块分组；
	// 429/超大响应 → 范围二分（下限单块）；成功 → 扩大（上限 256）。
	// 数据库提交与 reorg 校验仍逐块 exactly-once（坏块只回滚自身，不拖累区间）。
	// 返回 (logs, 实际覆盖到的末块, err)。成功但未覆盖 to → 扩大重试；
	// 失败 → 二分缩小返回部分范围（调用方从 hi+1 继续）。绝不允许静默丢日志。
	batchLogs := func(from, to uint64) ([]types.Log, uint64, error) {
		size := uint64(256)
		for {
			hi := from + size - 1
			if hi > to {
				hi = to
			}
			metrics.getlogsReq++
			logs, err := readCli.FilterLogs(ctx, ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(from),
				ToBlock:   new(big.Int).SetUint64(hi),
				Topics:    eventTopics,
			})
			if err == nil {
				if hi < to {
					size *= 2 // 成功但未覆盖目标 → 扩大再拉
					if size > 256 {
						size = 256
					}
					continue
				}
				return logs, hi, nil
			}
			metrics.rpc429++
			if size <= 1 {
				return nil, hi, fmt.Errorf("batch logs %d..%d: %w", from, hi, err)
			}
			size /= 2 // 429/超大 → 二分缩小重试
		}
	}
	// headerAt 拉取 header（429/限速 → 2s 退避重试 ≤6 次；其余错误直接返回）
	headerAt := func(bn uint64) (*types.Header, error) {
		var lastErr error
		for attempt := 0; attempt < 6; attempt++ {
			hdr, err := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(bn))
			if err == nil {
				return hdr, nil
			}
			lastErr = err
			if strings.Contains(strings.ToLower(err.Error()), "429") ||
				strings.Contains(strings.ToLower(err.Error()), "rate limit") {
				metrics.rpc429++
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
	// 单块摄取（批量日志按块分发 + header 校验 + 逐块提交）
	ingestOne := func(bn uint64, header *types.Header, logs []types.Log) error {
		ev := chain.BlockEvent{Number: bn, Hash: header.Hash(), Parent: header.ParentHash,
			ReceivedAtMs: time.Now().UnixMilli()}
		res, a, pending, err := processBlockWithLogs(ev, logs, false, true, true)
		if err != nil {
			return fmt.Errorf("backfill block %d: %w", bn, err)
		}
		if err := commitResult(ev, res, pending, a); err != nil {
			return err
		}
		metrics.ingestBlocks++
		return nil
	}
	backfill := func(to uint64) error {
		for lastApplied < to {
			// chunk：批量日志起点 = 游标 + 1
			from := lastApplied + 1
			chunkTo := from + 256 - 1
			if chunkTo > to {
				chunkTo = to
			}
			logs, hi, err := batchLogs(from, chunkTo)
			if err != nil {
				return err
			}
			chunkTo = hi // 实际覆盖范围（可能因限速缩小；绝不小块丢日志）
			// 按块分组（块内按 txIndex/logIndex 排序由 processBlock 处理）
			byBlock := map[uint64][]types.Log{}
			for _, l := range logs {
				byBlock[l.BlockNumber] = append(byBlock[l.BlockNumber], l)
			}
			// 逐块处理：只对有日志的块拉 header + 校验 + 逐块提交；
			// 无日志块无状态副作用 → 只推进内存游标（不逐块提交，省 header RPC）。
			// chunk 末尾统一拉末块 header 作为游标 hash（reorg 检测在 chunk 边界）。
			// 崩溃后重扫无日志块 = 无操作（幂等）；有日志块仍逐块 exactly-once。
			for bn := from; bn <= chunkTo; bn++ {
				logs := byBlock[bn]
				if len(logs) == 0 {
					continue // 无日志块：无需 header/hash/提交
				}
				hdr, err := headerAt(bn)
				if err != nil {
					return fmt.Errorf("backfill header %d: %w", bn, err)
				}
				if lastAppliedHash != (common.Hash{}) && hdr.ParentHash != lastAppliedHash {
					// 伪 reorg 防护：公共 RPC 多节点负载均衡下 hash 可能短暂
					// 漂移——重拉同一块 2 次，hash 稳定不匹配才视为真 reorg
					mismatch := true
					for retry := 0; retry < 3; retry++ {
						time.Sleep(100 * time.Millisecond)
						h2, herr := headerAt(bn)
						if herr != nil {
							break
						}
						if h2.ParentHash == lastAppliedHash {
							hdr = h2
							mismatch = false
							break
						}
					}
					if mismatch {
						if err := handleReorg(); err != nil {
							return err
						}
						break // 回退后外层循环重新计算
					}
				}
				if err := ingestOne(bn, hdr, logs); err != nil {
					return err
				}
			}
			// chunk 末尾：锚点提交（processed_blocks + checkpoint 写 chunk 末块）。
			// 无日志块不逐块提交，但锚点保证 DB 历史链连续——reorg 祖先查找
			// 可以从任意 chunk 末块回溯（锚点间距 ≤256，64 层覆盖 16k 块）
			{
				hdr, err := headerAt(chunkTo)
				if err != nil {
					return fmt.Errorf("backfill chunk header %d: %w", chunkTo, err)
				}
				if sink != nil {
					if err := sink.CommitBlockIngest(ctx, chunkTo, hdr.Hash().Hex(),
						hdr.ParentHash.Hex(), nil, nil, time.Now().UnixMilli()); err != nil {
						return fmt.Errorf("backfill chunk anchor %d: %w", chunkTo, err)
					}
				}
				lastApplied = chunkTo
				lastAppliedHash = hdr.Hash()
				metrics.ingestBlocks += chunkTo - from + 1
			}
			// 补扫与评估交错（有预算）：每 chunk 评估 ≤8 块
			if err := evaluateBudgeted(8); err != nil {
				slog.Warn("interleaved evaluation", "err", err)
			}
		}
		return nil
	}
	// evaluatePendingCore：从 evaluate 游标之后逐块评估（固定 stateBlock = 队列区块）。
	// 每块独立事务（候选 + evaluate checkpoint）；失败 → 该块游标不前进，
	// 下次调用从同一块重试（不丢机会、无 look-ahead）。evalBudget 限制单次
	// 处理的块数（补扫交错用预算，不允许一次扫光积压导致 ingest 冻结）。
	evaluatePendingCore := func(ctx context.Context, evalBudget int, m *throughputMetrics) error {
		if sink == nil || evalBudget <= 0 {
			return nil
		}
		pending, err := sink.LoadPendingAffected(ctx, lastEvaluated)
		if err != nil {
			return fmt.Errorf("load pending affected: %w", err)
		}
		done := 0
		evalStart := time.Now().UnixMilli()
		// 一批评估共享同一 head H：快照缓存（headHash 键）全命中，
		// RPC 从每块 1 head + N 池降到 1 head + 池集一次
		var head uint64
		if simMode != "historical_strict" {
			hd, herr := readCli.HeaderByNumber(ctx, nil)
			if herr != nil {
				return fmt.Errorf("head for %s: %w", simMode, herr)
			}
			head = hd.Number.Uint64()
			cfgCopy := engineCfg
			cfgCopy.HeadAtSnapshot = head
			cfgCopy.HeadAtSnapshotMs = evalStart
			engine.SetConfig(cfgCopy)
		}
		for _, pb := range pending {
			if done >= evalBudget {
				break
			}
			done++
			// freshness-first：过期机会直接跳过（公共 RPC 无 archive，
			// 旧块无法还原——继续评估只是制造统计噪音）。
			// 显式审计 stale_skipped + 推进观察游标
			if simMode != "historical_strict" && head > pb.Block {
				lag := head - pb.Block
				if lag > maxObsLag {
					if err := sink.SkipStaleBlock(ctx, pb.Block, pb.Hash, lag, "stale_no_archive"); err != nil {
						return fmt.Errorf("skip stale block %d: %w", pb.Block, err)
					}
					lastEvaluated = pb.Block
					m.evaluateBlocks++
					continue
				}
			}
			addrs := make([]common.Address, 0, len(pb.Pools))
			for _, p := range pb.Pools {
				addrs = append(addrs, common.HexToAddress(p))
			}
			// 状态年龄基于 ingest 时持久化的原始接收时间（不允许恢复时重填 now）
			ageMs := int64(0)
			if pb.ReceivedAtMs > 0 {
				ageMs = evalStart - pb.ReceivedAtMs
				if ageMs < 0 {
					ageMs = 0
				}
			}
			if simMode != "historical_strict" {
				// local_only / latest_observe：快照与（latest_observe 的）
				// eth_call 都固定在本批 head H；local_only 不查 executor 余额
				ev := arbitrage.SwapEvent{
					BlockNumber: pb.Block,
					BlockHash:   common.HexToHash(pb.Hash),
					ReceivedAt:  pb.ReceivedAtMs, // 原始接收时间（age 基准）
				}
				processed, err := engine.ProcessBlockAt(ctx, ev, addrs, head)
				if err != nil {
					return fmt.Errorf("evaluate block %d (%s): %w", pb.Block, simMode, err)
				}
				// state_age_ms / state_lag_blocks 按持久化数据填充
				for _, c := range processed.Candidates {
					c.StateAgeMs = ageMs
					if head > pb.Block {
						c.StateLagBlocks = head - pb.Block
					}
					// 机会衰减：正毛利候选进入衰减队列（T+1/2/4 重报价）
					if c.Decision == "local_profitable_observed" && c.InputAmount.Sign() > 0 {
						decayQueue = append(decayQueue, decayEntry{
							originBlock: pb.Block,
							route:       cloneRoute(c.Route),
							amount:      new(big.Int).Set(c.InputAmount),
							delay:       0,
						})
					}
				}
				if err := sink.CommitEvaluation(ctx, pb.Block, pb.Hash, processed.Candidates); err != nil {
					return fmt.Errorf("evaluate block %d commit: %w", pb.Block, err)
				}
				lastEvaluated = pb.Block
				continue
			}
			// historical_strict：历史资金（执行合约在该区块的 WETH 余额）——
			// 优化器上限必须与历史状态一致；读取失败 = 基础设施（可重试）
			bal, err := wethBalanceAt(ctx, readCli, weth, executorAddr, pb.Block)
			if err != nil {
				return fmt.Errorf("balance at %d: %w", pb.Block, err)
			}
			searcher.SetFunding(maxInputWei, bal)
			processed, err := engine.ProcessBlock(ctx, arbitrage.SwapEvent{
				BlockNumber: pb.Block,
				BlockHash:   common.HexToHash(pb.Hash),
				ReceivedAt:  pb.ReceivedAtMs,
			}, addrs)
			if err != nil {
				// 基础设施错误：区块保持未评估，游标不前进（下轮重试整块）
				return fmt.Errorf("evaluate block %d: %w", pb.Block, err)
			}
			for _, c := range processed.Candidates {
				c.StateAgeMs = ageMs
			}
			if err := sink.CommitEvaluation(ctx, pb.Block, pb.Hash, processed.Candidates); err != nil {
				return fmt.Errorf("evaluate block %d commit: %w", pb.Block, err)
			}
			lastEvaluated = pb.Block
			m.evaluateBlocks++
		}
		// 衰减采样：对队列里的机会在当前 head 重报价（仅当 head 前进到
		// 目标延迟块；同一 head 快照缓存零额外 RPC）
		if simMode != "historical_strict" && len(decayQueue) > 0 && sink != nil {
			keep := decayQueue[:0]
			for _, d := range decayQueue {
				targetDelay := []int{0, 1, 2, 4}[minInt(d.delay, 3)]
				if d.delay < 0 {
					continue
				}
				if targetDelay > d.delay {
					// 未到采样时刻：等到 head 前进
					d.delay++
					keep = append(keep, d)
					continue
				}
				gross, qerr := engine.DecayQuote(ctx, arbitrage.Route{Hops: d.route},
					d.amount, head)
				if qerr != nil {
					// 基础设施错误：留待下批重试
					keep = append(keep, d)
					continue
				}
				// 离线重算 1x/2x/3x 净利（不重新采链）
				baseFee := big.NewInt(1e8)
				// gas 成本按当前配置的 units × baseFee × 倍率
				units := engineCfg.LocalGasUnits
				cost1x := new(big.Int).Mul(new(big.Int).SetUint64(units), baseFee)
				net1x := new(big.Int).Sub(gross, cost1x)
				net2x := new(big.Int).Sub(gross, new(big.Int).Mul(cost1x, big.NewInt(2)))
				net3x := new(big.Int).Sub(gross, new(big.Int).Mul(cost1x, big.NewInt(3)))
				_ = sink.SaveDecaySample(ctx, d.originBlock, d.delay,
					"", d.amount.String(), gross.String(),
					net1x.String(), net2x.String(), net3x.String())
				if d.delay < 4 {
					d.delay++
					keep = append(keep, d)
				}
			}
			decayQueue = keep
		}
		return nil
	}
	// 有预算包装（补扫交错用）
	evaluateBudgeted = func(budget int) error {
		return evaluatePendingCore(ctx, budget, metrics)
	}
	// evaluatePending 无预算包装（启动/实时用：一次清完）
	evaluatePending := func() error {
		return evaluatePendingCore(ctx, 1<<30, metrics)
	}

	// 指标采样循环（60s 周期）：落库吞吐/lag/限速/缓存命中率
	go func() {
		prevIngest := metrics.ingestBlocks
		prevEval := metrics.evaluateBlocks
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				nowIngest := metrics.ingestBlocks
				nowEval := metrics.evaluateBlocks
				ingestBps := float64(nowIngest-prevIngest) / 60.0
				evalBps := float64(nowEval-prevEval) / 60.0
				prevIngest, prevEval = nowIngest, nowEval
				hits, misses := searcher.SnapshotCacheStats()
				ratio := 0.0
				if hits+misses > 0 {
					ratio = float64(hits) / float64(hits+misses)
				}
				var ingestLag, evalLag uint64
				if lastApplied > 0 {
					head, herr := readCli.BlockNumber(ctx)
					if herr == nil {
						if head > lastApplied {
							ingestLag = head - lastApplied
						}
					}
				}
				if lastEvaluated > 0 && lastApplied > lastEvaluated {
					evalLag = lastApplied - lastEvaluated
				}
				slog.Info("throughput",
					"ingest_bps", round2(ingestBps), "evaluate_bps", round2(evalBps),
					"ingest_lag", ingestLag, "evaluate_lag", evalLag,
					"getlogs_reqs", metrics.getlogsReq, "rpc_429", metrics.rpc429,
					"cache_hit_ratio", round2(ratio))
				if sink != nil {
					if err := sink.SaveThroughputSample(ctx, ingestBps, evalBps,
						ingestLag, evalLag, metrics.getlogsReq, metrics.rpc429, ratio); err != nil {
						slog.Warn("throughput sample", "err", err)
					}
				}
			}
		}
	}()
	// 启动：先补 ingest 到链头，再评估未处理批次（崩溃恢复路径同此）
	if err := backfill(startHead); err != nil {
		slog.Error("startup backfill failed, cursor stays at", "block", lastApplied, "err", err)
		os.Exit(1)
	}
	if err := evaluatePending(); err != nil {
		slog.Error("startup evaluation failed (cursor stays; retried next head)", "err", err)
	}



	for {
		select {
		case <-ctx.Done():
			slog.Info("arbitrage stopped")
			return
		case h, ok := <-heads:
			if !ok {
				continue
			}
			if h.Number <= lastApplied {
				continue // 已处理（含轮询源补块重复）
			}
			// 顺序补扫到新 head（含 reorg 检测），随后评估未处理批次
			if err := backfill(h.Number); err != nil {
				slog.Error("backfill failed, cursor stays at", "block", lastApplied, "err", err)
				os.Exit(1) // 失败关闭：任一区块失败即停止，不跳块
			}
			if err := evaluatePending(); err != nil {
				slog.Error("evaluation commit failed (cursor stays; retried next head)", "err", err)
			}
		}
	}
}

// preflightExecutor 校验执行合约配置与链上状态一致（防错地址导致全部模拟 revert）。
// 返回执行合约的 WETH 余额（资金限制的一部分）。
func preflightExecutor(ctx context.Context, cli *ethclient.Client, contract common.Address, cfg *config.Config) (*big.Int, error) {
	read := func(to common.Address, sig string) ([]byte, error) {
		return cli.CallContract(ctx, ethereum.CallMsg{To: &to, Data: crypto.Keccak256([]byte(sig))[:4]}, nil)
	}
	addrOf := func(b []byte) (common.Address, error) {
		if len(b) < 32 {
			return common.Address{}, fmt.Errorf("short response %d", len(b))
		}
		return common.BytesToAddress(b[12:32]), nil
	}

	if b, err := read(contract, "executor()"); err != nil {
		return nil, fmt.Errorf("executor(): %w", err)
	} else if a, _ := addrOf(b); a != common.HexToAddress(cfg.Executor.Wallet) {
		return nil, fmt.Errorf("executor()=%s want %s", a.Hex(), cfg.Executor.Wallet)
	}
	if b, err := read(contract, "weth()"); err != nil {
		return nil, fmt.Errorf("weth(): %w", err)
	} else if a, _ := addrOf(b); a != common.HexToAddress(cfg.Chain.WETH) {
		return nil, fmt.Errorf("weth()=%s want %s", a.Hex(), cfg.Chain.WETH)
	}
	if b, err := read(contract, "factory()"); err != nil {
		return nil, fmt.Errorf("factory(): %w", err)
	} else if a, _ := addrOf(b); a != common.HexToAddress(cfg.Dexes.V3[0].Factory) {
		return nil, fmt.Errorf("factory()=%s want %s", a.Hex(), cfg.Dexes.V3[0].Factory)
	}
	if b, err := read(contract, "paused()"); err != nil {
		return nil, fmt.Errorf("paused(): %w", err)
	} else if len(b) >= 32 && b[31] != 0 {
		return nil, fmt.Errorf("executor is paused")
	}
	// 合约 WETH 余额：balanceOf(address) 需要 32 字节参数（ABI 左补零）
	addressType, err := abi.NewType("address", "", nil)
	if err != nil {
		return nil, err
	}
	argEnc, err := abi.Arguments{{Type: addressType}}.Pack(contract)
	if err != nil {
		return nil, err
	}
	weth := wethAddr(cfg)
	bal, err := cli.CallContract(ctx, ethereum.CallMsg{
		To:   &weth,
		Data: append(crypto.Keccak256([]byte("balanceOf(address)"))[:4], argEnc...),
	}, nil)
	if err != nil || len(bal) < 32 {
		return nil, fmt.Errorf("weth balanceOf: %v", err)
	}
	balance := new(big.Int).SetBytes(bal[:32])
	if balance.Sign() <= 0 {
		return nil, fmt.Errorf("executor WETH balance is zero (fund it before shadow)")
	}
	slog.Info("executor preflight ok", "contract", contract.Hex(), "weth_balance_wei", balance.String())
	return balance, nil
}

func wethAddr(cfg *config.Config) common.Address { return common.HexToAddress(cfg.Chain.WETH) }

// wethBalanceAt 读取执行合约在指定历史区块的 WETH 余额（eth_call 固定高度）。
// 历史资金限制：优化器上限必须与该区块一致（latest 余额会高估/低估历史机会）。
func wethBalanceAt(ctx context.Context, cli *ethclient.Client, weth common.Address, executor common.Address, block uint64) (*big.Int, error) {
	callData := crypto.Keccak256([]byte("balanceOf(address)"))[:4]
	args := append(append([]byte{}, callData...), common.LeftPadBytes(executor.Bytes(), 32)...)
	out, err := cli.CallContract(ctx, ethereum.CallMsg{To: &weth, Data: args}, new(big.Int).SetUint64(block))
	if err != nil {
		return nil, err
	}
	if len(out) < 32 {
		return nil, fmt.Errorf("short balance response %d", len(out))
	}
	return new(big.Int).SetBytes(out[0:32]), nil
}

// minInt 取小值（衰减延迟下标用）。
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// round2 保留两位小数（指标日志用）。
func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

// ancParent 返回祖先 header 的 parent hash（nil 时为空串；JSON fallback 模式无历史）。
func ancParent(h *types.Header) string {
	if h == nil {
		return ""
	}
	return h.ParentHash.Hex()
}

// throughputMetrics 吞吐与限速指标（60s 周期落库，Canary 判断用）。
type throughputMetrics struct {
	ingestBlocks    uint64
	evaluateBlocks  uint64
	getlogsReq      uint64
	rpc429          uint64
	snapshotHits    uint64
	snapshotMisses  uint64
	lastSample      time.Time
}
