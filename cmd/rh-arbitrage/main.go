// rh-arbitrage: 套利引擎。默认 shadow 模式：发现、模拟、落盘，不发送交易。
// Robinhood 公共端点无 WS，默认轮询；配置 WS 端点时优先 WS。
package main

import (
	"context"
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
	"github.com/ethereum/go-ethereum/ethclient"

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
		fromBlock = heights["pools"]
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
		_ = ckpt.Save("pools", lastPoolBlock)
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
	// 游标：高度 + 区块 hash（reorg 检测：新 head 必须 parent 衔接）
	var lastApplied uint64
	var lastAppliedHash common.Hash
	if pgCkpt != nil {
		// 用 checkpoint 里存的 hash（上次真正处理过的区块），不重读当前链
		if h, hh, err := pgCkpt.LoadWithHash(ctx, storage.CheckpointBlocks); err == nil {
			lastApplied = h
			lastAppliedHash = hh
		}
	} else if h, ok := heights[storage.CheckpointBlocks]; ok {
		lastApplied = h
	} else {
		// 首次实时 Shadow：从当前链头开始，只处理启动后的新块。
		lastApplied = startHead
		if hdr, err := readCli.HeaderByNumber(ctx, nil); err == nil {
			lastAppliedHash = hdr.Hash()
		}
		if err := saveCheckpoint("blocks", startHead); err != nil {
			slog.Error("first checkpoint save failed", "err", err)
			os.Exit(1) // 失败关闭
		}
	}

	type appliedPool struct {
		pool  *v3.Pool
		isNew bool
	}
	processBlock := func(h chain.BlockEvent) (*arbitrage.BlockResult, map[common.Address]*appliedPool, error) {
		logs, err := readCli.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(h.Number),
			ToBlock:   new(big.Int).SetUint64(h.Number),
			Topics:    eventTopics,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("block %d getLogs: %w", h.Number, err)
		}
		for _, l := range logs {
			if l.BlockHash != h.Hash {
				return nil, nil, fmt.Errorf("block %d hash mismatch: log=%s header=%s", h.Number, l.BlockHash.Hex(), h.Hash.Hex())
			}
		}
		res := &arbitrage.BlockResult{Block: h.Number, BlockHash: h.Hash}
		sort.Slice(logs, func(i, j int) bool {
			if logs[i].TxIndex != logs[j].TxIndex {
				return logs[i].TxIndex < logs[j].TxIndex
			}
			return logs[i].Index < logs[j].Index
		})
		// 内存原子性：所有日志应用到池的克隆上；数据库事务提交成功后才写回 Registry。
		applied := map[common.Address]*appliedPool{}
		newPoolMetas := []arbitrage.PoolMeta{}
		affected := map[common.Address]struct{}{}
		for _, l := range logs {
			ap, ok := applied[l.Address]
			if !ok {
				state := reg.Pool(l.Address)
				if state == nil {
					// 启动后新创建的池：验证归属（暂不加入 Registry，commit 成功后再加入）
					pool, derr := adapter.PoolByAddress(ctx, l.Address)
					if derr != nil {
						slog.Debug("unknown pool skipped", "addr", l.Address.Hex(), "err", derr)
						continue
					}
					if pool.TickSpacing > 0 {
						newPoolMetas = append(newPoolMetas, arbitrage.PoolMeta{
							Address: pool.Address, Exchange: pool.Exchange,
							Token0: pool.Token0, Token1: pool.Token1,
							Fee: pool.Fee, TickSpacing: pool.TickSpacing,
						})
					}
					ap = &appliedPool{pool: pool, isNew: true}
				} else {
					ap = &appliedPool{pool: state.(*v3.Pool).Clone()}
				}
				applied[l.Address] = ap
			}
			apply, derr := adapter.DecodeLog(ap.pool, l)
			if derr != nil {
				slog.Warn("decode log", "err", derr)
				continue
			}
			if apply != nil {
				apply()
			}
			if l.Topics[0] == eventTopics[0][1] || l.Topics[0] == eventTopics[0][2] {
				if tl, tu, terr := v3.DecodeTickBounds(l); terr == nil {
					if err := adapter.ResyncMintBurn(ctx, ap.pool, tl, tu); err != nil {
						slog.Warn("resync mint/burn", "pool", ap.pool.Address.Hex(), "err", err)
					}
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
		if len(pools) > 0 {
			processed := engine.ProcessBlock(ctx, arbitrage.SwapEvent{
				BlockNumber: h.Number,
				BlockHash:   h.Hash,
				ReceivedAt:  time.Now().UnixMilli(),
			}, pools)
			res.Candidates = processed.Candidates
		}
		return res, applied, nil
	}

	// 单事务提交（pools + candidates + checkpoint 含 hash）；失败 → 游标不前进
	processAndCommit := func(h chain.BlockEvent) error {
		res, applied, err := processBlock(h)
		if err != nil {
			return err
		}
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
		// 事务成功：写回内存（幂等：新池注册、克隆状态替换）
		for addr, ap := range applied {
			reg.UpsertPool(v3.State(ap.pool))
			if ap.isNew {
				graph.AddPool(ap.pool.Pool(), addr)
			}
		}
		lastApplied = h.Number
		lastAppliedHash = h.Hash
		return nil
	}

	heads, headErrs := src.SubscribeBlocks(ctx)
	go func() {
		for err := range headErrs {
			slog.Warn("head subscription error", "err", err)
		}
	}()

	// 启动窗口补扫：从 lastApplied+1 顺序处理，任一区块失败即停止推进（不跳块）
	for bn := lastApplied + 1; bn <= startHead; bn++ {
		hdr, err := readCli.HeaderByNumber(ctx, new(big.Int).SetUint64(bn))
		if err != nil {
			slog.Error("startup backfill header, stopping cursor", "block", bn, "err", err)
			break
		}
		if err := processAndCommit(chain.BlockEvent{Number: bn, Hash: hdr.Hash()}); err != nil {
			slog.Error("startup backfill failed, cursor stays at", "block", lastApplied, "err", err)
			break
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
			// reorg 检测：新 head 必须与游标 parent 衔接；断裂 → 找共同祖先 → 标记孤块 → 回退重处理
			if lastAppliedHash != (common.Hash{}) && h.Number == lastApplied+1 && h.Parent != lastAppliedHash {
				rollback := lastApplied
				slog.Warn("reorg detected", "at", lastApplied,
					"parent", h.Parent.Hex(), "expected", lastAppliedHash.Hex())
				// 沿新链 parent 向后找共同祖先（最多 64 层）
				ancestor := rollback
				curHash := h.Parent
				for depth := 0; depth < 64; depth++ {
					if curHash == lastAppliedHash {
						ancestor = rollback - uint64(depth)
						break
					}
					hdr, err := readCli.HeaderByHash(ctx, curHash)
					if err != nil || hdr == nil {
						break
					}
					curHash = hdr.ParentHash
				}
				if ancestor < rollback {
					slog.Warn("reorg common ancestor", "rollback_to", ancestor)
				}
				// 孤块候选标记 canonical=false（保留数据；新链重处理时恢复）
				if sink != nil {
					if _, uerr := sink.Exec(ctx, `
						UPDATE opportunities SET canonical = FALSE
						WHERE observed_block > $1`,
						ancestor); uerr != nil {
						slog.Error("reorg mark failed", "err", uerr)
					}
				}
				lastApplied = ancestor // 从共同祖先重新处理（新链）
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
					ev = chain.BlockEvent{Number: bn, Hash: hdr.Hash()}
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
