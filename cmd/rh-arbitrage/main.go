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
	"github.com/ethereum/go-ethereum/core/types"
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

	// Adapter 一律使用独立 HTTP Read RPC（连接池），不复用 WS 客户端：
	// WS 重连会替换底层连接，Adapter 持有的旧指针会持续失败。
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
	heights, _ := ckpt.Load()
	fromBlock := heights["pools"]
	if fromBlock < d.FactoryBlock {
		fromBlock = d.FactoryBlock
	}
	// 启动时读链头再引导（已完成的从 checkpoint 续跑；未完成的补全到链头）
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
	_ = ckpt.Save("pools", lastPoolBlock)

	// 先初始化 DB，再创建 Engine（sink 必须可用，不允许静默丢失候选）
	var sink *storage.DB
	if cfg.Storage.PostgresURL != "" {
		sink, err = storage.New(ctx, cfg.Storage.PostgresURL)
		if err != nil {
			slog.Error("postgres unavailable, refusing to run without persistence", "err", err)
			os.Exit(1)
		}
		defer sink.Close()
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
	}

	weth := common.HexToAddress(cfg.Chain.WETH)

	// 链上模拟器：真实 executeV3Cycle calldata + eth_call（sim RPC 组）。
	// shadow 模式失败关闭：无执行合约 / 无 sim RPC / 合约无代码 → 拒绝启动，
	// 绝不静默降级成 LocalEvaluator（否则"accepted"只是本地数学，不是可执行验证）。
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

	// P0-5: Executor 启动预检（wallet/weth/factory/paused/余额）→ 余额用于资金限制
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
		sink,
		searcher,
		evaluator,
		arbitrage.NewExecutor(),
	)

	// 订阅 Swap（触发套利）+ Mint/Burn（仅更新状态）。
	// FromBlock = 启动时链头 + 1：初始化期间发生的 Swap 会被补回（封闭漏日志窗口）
	swapTopic := v3.SwapTopic()
	mintTopic := common.HexToHash("0x7a53080ba414158be7ec69b987b5fb7d07dee101fe85488f0853ae16239d0bde")
	burnTopic := common.HexToHash("0x0c396cd989a39f4459b5fa1aed6a9a8dcdbc45908acfd67e028cd568da98982c")
	startHead, err := readCli.BlockNumber(ctx)
	if err != nil {
		slog.Error("read chain head for log subscription", "err", err)
		os.Exit(1)
	}
	logs, logErrs := src.SubscribeLogs(ctx, ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(startHead) + 1),
		Topics:    [][]common.Hash{{swapTopic, mintTopic, burnTopic}},
	})
	go func() {
		for err := range logErrs {
			slog.Warn("log subscription error", "err", err)
		}
	}()

	slog.Info("arbitrage engine started", "mode", cfg.Mode.Run, "pools", len(reg.AllPools()))
	// 区块聚合：收集同一区块全部 Swap/Mint/Burn → 按 TxIndex/LogIndex 排序 → 应用完整区块状态
	// → 区块结束时汇总受影响池评估一次（消除交易中间状态机会）
	type pendingBlock struct {
		hash  common.Hash
		logs  []types.Log
	}
	pending := map[uint64]*pendingBlock{}
	var currentHead uint64

	flushBlock := func(number uint64) {
		pb, ok := pending[number]
		if !ok {
			return
		}
		delete(pending, number)
		// 按 (txIndex, logIndex) 排序，保证同区块内事件顺序正确
		sort.Slice(pb.logs, func(i, j int) bool {
			if pb.logs[i].TxIndex != pb.logs[j].TxIndex {
				return pb.logs[i].TxIndex < pb.logs[j].TxIndex
			}
			return pb.logs[i].Index < pb.logs[j].Index
		})
		affected := map[common.Address]struct{}{}
		for _, l := range pb.logs {
			state := reg.Pool(l.Address)
			if state == nil {
				continue
			}
			pool := state.(*v3.Pool)
			apply, derr := adapter.DecodeLog(pool, l)
			if derr != nil {
				slog.Warn("decode log", "err", derr)
				continue
			}
			if apply != nil {
				apply()
			}
			// Mint/Burn：重读链上 bitmap word 与 active liquidity（本地历史 gross 不可信）
			if l.Topics[0] == mintTopic || l.Topics[0] == burnTopic {
				if tl, tu, terr := v3.DecodeTickBounds(l); terr == nil {
					if err := adapter.ResyncMintBurn(ctx, pool, tl, tu); err != nil {
						slog.Warn("resync mint/burn", "pool", pool.Address.Hex(), "err", err)
					}
				}
			} else {
				affected[l.Address] = struct{}{} // 仅 Swap 触发评估
			}
			reg.UpsertPool(state)
		}
		pools := make([]common.Address, 0, len(affected))
		for p := range affected {
			pools = append(pools, p)
		}
		if len(pools) > 0 {
			engine.OnBlockBatch(ctx, arbitrage.SwapEvent{
				BlockNumber: number,
				BlockHash:   pb.hash,
				ReceivedAt:  time.Now().UnixMilli(),
			}, pools)
		}
	}

	for {
		select {
		case <-ctx.Done():
			slog.Info("arbitrage stopped")
			return
		case l, ok := <-logs:
			if !ok {
				continue
			}
			pb, exists := pending[l.BlockNumber]
			if !exists {
				pb = &pendingBlock{hash: l.BlockHash}
				pending[l.BlockNumber] = pb
			}
			pb.logs = append(pb.logs, l)
			// 区块完结判定：日志高度落后于当前处理头（或出现更高区块日志）
			if l.BlockNumber > currentHead {
				currentHead = l.BlockNumber
			}
			// 收到更高区块的日志时，flush 之前的区块（保证完整区块状态）
			for bn := range pending {
				if bn < currentHead {
					flushBlock(bn)
				}
			}
			// 链头推进检查由心跳完成；这里也处理同高度全部到达的情况
			if l.BlockNumber == currentHead && len(pending) == 1 {
				// 等待：同区块可能还有更多日志（由下一个区块触发 flush）
			}
		case <-time.After(3 * time.Second):
			// 心跳：flush 所有未完结区块（链头已推进但无新日志）
			for bn := range pending {
				flushBlock(bn)
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
