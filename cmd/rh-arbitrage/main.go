// rh-arbitrage: 套利引擎。默认 shadow 模式：发现、模拟、落盘，不发送交易。
// Robinhood 公共端点无 WS，默认轮询；配置 WS 端点时优先 WS。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"os/signal"
	"sort"
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
		if _, err := storage.RestorePools(ctx, sink, reg, graph); err != nil {
			slog.Error("restore pools", "err", err)
			os.Exit(1)
		}
		// 旧数据 tick_spacing 为 0：补查
		for _, st := range reg.AllPools() {
			pool := st.(*v3.Pool)
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
	lastPoolBlock, err := v3.Bootstrap(ctx, adapter, reg, graph, fromBlock, headNum, v3.BootstrapOptions{})
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
			p3 := st.(*v3.Pool)
			allPools = append(allPools, storage.Pool{
				Address: sp.ID, Exchange: sp.Exchange, Protocol: "v3",
				Token0: sp.Token0.Hex(), Token1: sp.Token1.Hex(),
				Fee: sp.Fee, TickSpacing: p3.TickSpacing,
			})
		}
		if err := sink.CommitPools(ctx, allPools, lastPoolBlock); err != nil {
			slog.Error("pool persistence failed, pools cursor not advanced", "err", err)
			os.Exit(1)
		}
	} else {
		_ = ckpt.Save(storage.CheckpointPools, lastPoolBlock)
	}

	// 链上模拟器：真实 executeV3Cycle calldata + eth_call（sim RPC 组）。
	// shadow 模式失败关闭：无执行合约 / 无 sim RPC / 合约无代码 → 拒绝启动。
	if cfg.Executor.Contract == "" || cfg.Executor.Wallet == "" {
		slog.Error("executor.contract / executor.wallet not configured (required in shadow mode)")
		os.Exit(1)
	}
	if len(cfg.RPC.Groups.Sim) == 0 {
		slog.Error("no sim RPC configured (required in shadow mode)")
		os.Exit(1)
	}
	simCli, dialErr := ethclient.Dial(cfg.RPC.Groups.Sim[0])
	if dialErr != nil {
		slog.Error("sim rpc dial failed", "err", dialErr)
		os.Exit(1)
	}
	contractAddr := common.HexToAddress(cfg.Executor.Contract)
	code, codeErr := simCli.CodeAt(ctx, contractAddr, nil)
	if codeErr != nil || len(code) == 0 {
		slog.Error("executor contract has no code", "contract", cfg.Executor.Contract, "err", codeErr)
		os.Exit(1)
	}
	sim := simulation.NewExecutorSimulator(simCli, contractAddr,
		common.HexToAddress(cfg.Executor.Wallet), 5_000_000)

	// Executor 启动预检（wallet/weth/factory/paused/余额）→ 余额用于资金限制
	contractBal, err := preflightExecutor(ctx, simCli, contractAddr, cfg)
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
	evaluator := simulation.NewSimulationEvaluator(sim, cfg.Chain.ID, safetyMargin)
	slog.Info("simulation evaluator enabled", "contract", cfg.Executor.Contract,
		"min_profit_wei", minProfit.String(), "safety_margin_wei", safetyMargin.String(), "top_k", topK)

	weth := common.HexToAddress(cfg.Chain.WETH)

	searcher := arbitrage.NewLocalSearcher(graph, reg, adapter, weth)
	// 资金限制：max_input_wei / min_input_wei 与执行合约当前 WETH 余额（预检已读取）
	if cfg.Arbitrage.MaxInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MaxInputWei, 10); ok {
			searcher.SetFunding(v, contractBal)
		}
	} else {
		searcher.SetFunding(nil, contractBal)
	}
	if cfg.Arbitrage.MinInputWei != "" {
		if v, ok := new(big.Int).SetString(cfg.Arbitrage.MinInputWei, 10); ok {
			searcher.SetMinInput(v)
		}
	}
	if cfg.Mode.Run == "live" {
		slog.Error("live mode not implemented (signer/nonce/broadcaster/pnl not wired)")
		os.Exit(1)
	}
	engine := arbitrage.NewEngine(
		arbitrage.Config{
			ChainID:         cfg.Chain.ID,
			WETH:            weth,
			MinProfitWei:    minProfit,
			SafetyMarginWei: safetyMargin,
			MaxHops:         2,
			TopK:            topK,
			Mode:            cfg.Mode.Run,
		},
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
	// evaluate=false 时只取日志不评估（补扫路径用，避免重复评估同一当前状态）
	processBlock := func(h chain.BlockEvent, evaluate bool) (*arbitrage.BlockResult, map[common.Address]struct{}, map[common.Address]*pendingPool, error) {
		logs, err := readCli.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(h.Number),
			ToBlock:   new(big.Int).SetUint64(h.Number),
			Topics:    eventTopics,
		})
		if err != nil {
			return nil, nil, nil, fmt.Errorf("block %d getLogs: %w", h.Number, err)
		}
		for _, l := range logs {
			if l.BlockHash != h.Hash {
				return nil, nil, nil, fmt.Errorf("block %d hash mismatch: log=%s header=%s", h.Number, l.BlockHash.Hex(), h.Hash.Hex())
			}
		}
		res := &arbitrage.BlockResult{Block: h.Number, BlockHash: h.Hash}
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
				if state == nil {
					// 启动后新创建的池：验证归属。非本 Factory 池 → 跳过；
					// RPC 类错误 → 禁止推进（不能永久跳过这个事件）。
					pool, derr := adapter.PoolByAddress(ctx, l.Address)
					if derr != nil {
						if errors.Is(derr, v3.ErrNotFactoryPool) {
							slog.Debug("not a factory pool, skipped", "addr", l.Address.Hex())
							continue
						}
						return nil, nil, nil, fmt.Errorf("block %d pool %s verify: %w", h.Number, l.Address.Hex(), derr)
					}
					if pool.TickSpacing > 0 {
						newPoolMetas = append(newPoolMetas, arbitrage.PoolMeta{
							Address: pool.Address, Exchange: pool.Exchange,
							Token0: pool.Token0, Token1: pool.Token1,
							Fee: pool.Fee, TickSpacing: pool.TickSpacing,
						})
					}
					pp = &pendingPool{pool: pool, isNew: true}
					// P1: 新池先临时进 Registry/Graph——Engine 评估（RefreshRoute 查
					// registry + FindRoutes 查 graph）必须在事务前能看到它，否则
					// 新池的第一次 Swap 找不到任何路由。事务失败时池保持临时状态
					// （幽灵池，重启后从 PG 恢复即消失），DB 无写入，无数据污染。
					reg.UpsertPool(v3.State(pool))
					graph.AddPool(pool.Pool(), l.Address)
				} else {
					pp = &pendingPool{pool: state.(*v3.Pool)}
				}
				pending[l.Address] = pp
			}
			apply, derr := adapter.DecodeLog(pp.pool, l)
			if derr != nil {
				// 已知事件解码失败 → 禁止推进（不允许永久跳过）
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
			processed := engine.ProcessBlock(ctx, arbitrage.SwapEvent{
				BlockNumber: h.Number,
				BlockHash:   h.Hash,
				ReceivedAt:  time.Now().UnixMilli(),
			}, pools)
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

	// DB 提交 + 成功后应用内存 + 推进游标（processAndCommit 与补扫共用）
	commitResult := func(h chain.BlockEvent, res *arbitrage.BlockResult, pending map[common.Address]*pendingPool) error {
		if sink != nil {
			newPools := make([]storage.Pool, 0, len(res.NewPools))
			for _, np := range res.NewPools {
				newPools = append(newPools, storage.Pool{
					Address: np.Address.Hex(), Exchange: np.Exchange, Protocol: "v3",
					Token0: np.Token0.Hex(), Token1: np.Token1.Hex(),
					Fee: np.Fee, TickSpacing: np.TickSpacing,
				})
			}
			if err := sink.CommitBlockResult(ctx, h.Number, h.Hash.Hex(), h.Parent.Hex(), newPools, res.Candidates); err != nil {
				return fmt.Errorf("commit block %d: %w", h.Number, err)
			}
		} else {
			if err := saveCheckpoint(storage.CheckpointBlocks, h.Number); err != nil {
				return err
			}
		}
		applyCommitted(pending)
		lastApplied = h.Number
		lastAppliedHash = h.Hash
		return nil
	}

	// 单事务提交（pools + candidates + processed_blocks + checkpoint）；失败 → 游标不前进
	processAndCommit := func(h chain.BlockEvent) error {
		res, _, pending, err := processBlock(h, true)
		if err != nil {
			return err
		}
		return commitResult(h, res, pending)
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
			// 历史缺失或全被替换：保守回退 64 层重处理（checkpoint 回退到该层）
			ancestor = 0
			if rollback > 64 {
				ancestor = rollback - 64
			}
			slog.Warn("reorg: no common ancestor found, conservative rollback", "to", ancestor)
		} else if ancestor < rollback {
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
		if _, err := storage.RestorePools(ctx, sink, reg, graph); err != nil {
			return fmt.Errorf("reorg rebuild registry: %w", err)
		}
		lastApplied = ancestor
		lastAppliedHash = common.HexToHash(ancestorHash) // 重处理期间继续衔接校验
		return nil
	}

	heads, headErrs := src.SubscribeBlocks(ctx)
	go func() {
		for err := range headErrs {
			slog.Warn("head subscription error", "err", err)
		}
	}()

	// 启动窗口补扫：从 lastApplied+1 顺序处理；第一块校验父衔接（离线 reorg）。
	// 任一区块失败即停止推进（不跳块）。回退后循环条件重新计算（不会跳过新区块）。
	// 补扫只取日志 + 提交历史 + 应用内存状态，不逐块评估（避免对同一当前状态
	// 重复评估）；结束时对聚合的 affected pools 统一评估一次（P1-3）。
	backfillAffected := map[common.Address]struct{}{}
	for lastApplied < startHead {
		bn := lastApplied + 1
		hdr, err := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(bn))
		if err != nil {
			slog.Error("startup backfill header, stopping cursor", "block", bn, "err", err)
			break
		}
		if lastAppliedHash != (common.Hash{}) && hdr.ParentHash != lastAppliedHash {
			if err := handleReorg(); err != nil {
				slog.Error("startup reorg rollback failed, exiting", "err", err)
				os.Exit(1)
			}
			continue // 回退后重新计算 bn
		}
		res, affected, pending, err := processBlock(chain.BlockEvent{Number: bn, Hash: hdr.Hash(), Parent: hdr.ParentHash}, false)
		if err != nil {
			slog.Error("startup backfill failed, cursor stays at", "block", lastApplied, "err", err)
			break
		}
		if err := commitResult(chain.BlockEvent{Number: bn, Hash: hdr.Hash(), Parent: hdr.ParentHash}, res, pending); err != nil {
			slog.Error("startup backfill commit failed, cursor stays at", "block", lastApplied, "err", err)
			break
		}
		for p := range affected {
			backfillAffected[p] = struct{}{}
		}
	}
	// 补扫聚合评估：一个当前状态只评估一次（候选以补扫最后一块为观察区块）
	if len(backfillAffected) > 0 {
		pools := make([]common.Address, 0, len(backfillAffected))
		for p := range backfillAffected {
			pools = append(pools, p)
		}
		ev := chain.BlockEvent{Number: lastApplied, Hash: lastAppliedHash}
		processed := engine.ProcessBlock(ctx, arbitrage.SwapEvent{
			BlockNumber: ev.Number,
			BlockHash:   ev.Hash,
			ReceivedAt:  time.Now().UnixMilli(),
		}, pools)
		if len(processed.Candidates) > 0 && sink != nil {
			if err := sink.CommitBlockResult(ctx, ev.Number, ev.Hash.Hex(), "", nil, processed.Candidates); err != nil {
				slog.Error("backfill evaluation commit failed", "err", err)
			}
		}
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
			if lastAppliedHash != (common.Hash{}) && h.Number == lastApplied+1 && h.Parent != lastAppliedHash {
				if err := handleReorg(); err != nil {
					slog.Error("reorg rollback failed, exiting", "err", err)
					os.Exit(1) // 失败关闭：不允许双规范数据运行
				}
			}
			// 顺序处理 lastApplied+1 → h.Number；任一失败即停止推进（失败区块不被跳过）
			for bn := lastApplied + 1; bn <= h.Number; bn++ {
				var ev chain.BlockEvent
				if bn == h.Number {
					ev = h
				} else {
					hdr, err := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(bn))
					if err != nil {
						slog.Error("gap header, cursor stays at", "block", lastApplied, "err", err)
						break
					}
					ev = chain.BlockEvent{Number: bn, Hash: hdr.Hash(), Parent: hdr.ParentHash}
				}
				if err := processAndCommit(ev); err != nil {
					slog.Error("block processing failed, cursor stays at", "block", lastApplied, "err", err)
					break
				}
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

// ancParent 返回祖先 header 的 parent hash（nil 时为空串；JSON fallback 模式无历史）。
func ancParent(h *types.Header) string {
	if h == nil {
		return ""
	}
	return h.ParentHash.Hex()
}
