package simulation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/auren23/rh-searcher/internal/arbitrage"
)

// 分层决策：本地候选 → 模拟通过 → （live 才发送）
const (
	DecisionLocalCandidate  = "local_candidate"  // 仅本地数学模型，未过链上模拟
	DecisionSimulationOK    = "simulation_accepted"
	DecisionSimulationRejected = "simulation_rejected"
	// DecisionSimulationCostApprox 模拟成功但 gas 非 historical：
	// 记录利润，不得进入正式 Selected / 净 EV 统计
	DecisionSimulationCostApprox = "simulation_valid_cost_approx"
)

// ExecutorSimulator 用真实 executeV3Cycle calldata 做 eth_call 模拟。
// 模拟交易与最终发送交易逐字节相同（不允许"模拟专用简化路径"）。
type ExecutorSimulator struct {
	cli *ethclient.Client
	// histUnsupported: 节点明确不支持历史估算后缓存（整个运行期不再尝试）
	histUnsupported atomic.Bool
	contract  common.Address // ArbitrageExecutor 地址
	from      common.Address // 执行钱包（hot wallet）
	maxGas    uint64
}

func NewExecutorSimulator(cli *ethclient.Client, contract, from common.Address, maxGas uint64) *ExecutorSimulator {
	return &ExecutorSimulator{cli: cli, contract: contract, from: from, maxGas: maxGas}
}

// SimResult 一次链上模拟的结果。
// GasEstimateMode 标注 gas 成本来源：
// "latest_approximation" = EstimateGas 在最新状态估算（公共 RPC 无历史估算）
// "max_gas_fallback"     = 估算失败用上限（不得进入正式 EV 统计）
// "historical"           = 未来支持历史估算时使用
type GasEstimateMode string

const (
	GasEstimateLatest  GasEstimateMode = "latest_approximation"
	GasEstimateMax     GasEstimateMode = "max_gas_fallback"
	// GasEstimateComplete = gas used + gas price 均来自历史区块（正式 EV 唯一准入）
	GasEstimateComplete GasEstimateMode = "historical_complete"
)

type SimResult struct {
	Profit          *big.Int // 合约返回的 WETH 净利（未扣 gas）
	GasUsed         uint64
	GasPriceWei     *big.Int // 模拟时的链上 gas 价格
	CalldataHash    string   // keccak(calldata)（与最终发送逐字节相同）
	SimulationBlock uint64   // eth_call 时的链头
	// Arbitrum NodeInterface.gasEstimateL1Component 返回：(gasEstimateForL1, baseFee, l1BaseFeeEstimate)
	L1GasUnits         uint64   // gasEstimateForL1（仅分析用，不重复扣费）
	L2BaseFeeWei       *big.Int // baseFee
	L1BaseFeeEstimateWei *big.Int // l1BaseFeeEstimate
	RevertMsg          string
	GasEstimateMode   GasEstimateMode
}

// VerifyBlockHash 校验 sim RPC 在指定高度的区块 hash 与 read RPC 一致
// （状态与模拟必须在同一链上同一区块；不一致 → 整组拒绝）。
func (s *ExecutorSimulator) VerifyBlockHash(ctx context.Context, block uint64, want common.Hash) error {
	hdr, err := s.cli.HeaderByNumber(ctx, new(big.Int).SetUint64(block))
	if err != nil {
		return fmt.Errorf("sim header %d: %w", block, err)
	}
	if hdr.Hash() != want {
		return fmt.Errorf("block hash mismatch at %d: sim=%s read=%s", block, hdr.Hash().Hex(), want.Hex())
	}
	return nil
}

// isUnsupportedHistoricalEstimate 判断节点是否明确不支持历史估算
// （-32601 method not found / -32602 invalid params / not supported）。
// 只有这类错误允许 fallback latest；其余按基础设施或一致性错误处理。
func isUnsupportedHistoricalEstimate(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"method not found", "-32601", "invalid params", "-32602",
		"too many arguments", "wrong number of arguments",
		"not supported", "unsupported",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// isInfraError 判断是否为可重试的基础设施错误（RPC 超时/限流/断连/节点落后），
// 而非确定性的合约执行结果。基础设施错误必须让区块保持未评估（游标不前进），
// 不能落成 simulation_rejected 永久拒绝。
func isInfraError(err error) bool {
	if err == nil {
		return false
	}
	// typed 优先：context 超时 / 网络错误
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, s := range []string{
		"timeout", "deadline exceeded", "connection reset", "connection refused",
		"eof", "429", "rate limit", "too many requests", "server error",
		"service unavailable", "execution aborted", "no historical state",
		"header not found", "not found", "archive",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// isRevertError 判断 eth_call 错误是否为确定性的合约 revert（可安全落盘拒绝）。
func isRevertError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "execution reverted") ||
		strings.Contains(msg, "revert") ||
		strings.Contains(msg, "vm execution error") ||
		strings.Contains(msg, "insufficient funds")
}

// Simulate 构建并模拟 executeV3Cycle 调用。
// block 非 nil 时 eth_call 固定在该高度（与状态读取同一区块，区块原子性）。
// 返回 error = 基础设施错误（可重试，调用方不得推进游标）；
// RevertMsg 非空 = 真实 revert（确定性拒绝，可落盘）。
func (s *ExecutorSimulator) Simulate(ctx context.Context, c *arbitrage.Candidate, chainID uint64, block *big.Int) (*SimResult, error) {
	// 历史 header 只读一次：deadline 时间 + base fee 共用（P0-3：
	// 正式 historical 成本禁止用硬编码默认 gas price）
	var historicalHeader *types.Header
	var blockTime int64
	if block != nil {
		hdr, herr := s.cli.HeaderByNumber(ctx, block)
		if herr != nil {
			return nil, fmt.Errorf("header at %d: %w", block.Uint64(), herr)
		}
		historicalHeader = hdr
		blockTime = int64(hdr.Time)
	}
	calldata, err := BuildExecuteV3CycleCalldataAt(c, chainID, blockTime)
	if err != nil {
		return nil, err
	}
	msg := ethereum.CallMsg{
		From: s.from,
		To:   &s.contract,
		Gas:  s.maxGas,
		Data: calldata,
	}
	out, err := s.cli.CallContract(ctx, msg, block)
	if err != nil {
		// 分类顺序：先明确的基础设施错误（typed：DeadlineExceeded/net/429/5xx），
		// 再确定性 revert；未知错误保守按可重试处理（宁可重试，不可错杀）
		if isInfraError(err) {
			return nil, fmt.Errorf("sim eth_call (infra): %w", err)
		}
		if isRevertError(err) {
			return &SimResult{RevertMsg: err.Error(), CalldataHash: hashHex(calldata)}, nil
		}
		return nil, fmt.Errorf("sim eth_call (unknown): %w", err)
	}
	if len(out) < 32 {
		return &SimResult{RevertMsg: fmt.Sprintf("short return %d bytes", len(out)), CalldataHash: hashHex(calldata)}, nil
	}
	profit := new(big.Int).SetBytes(out[:32])
	// gasPrice：latest/近似路径用 SuggestGasPrice 或默认；
	// historical_complete 路径在后面用历史 header base fee 覆盖
	gasPrice := big.NewInt(1e8) // 0.1 gwei 保守默认（仅近似路径，不进入正式 EV）
	if block == nil {
		if g, gerr := s.cli.SuggestGasPrice(ctx); gerr == nil && g.Sign() > 0 {
			gasPrice = g
		}
	}
	// Gas 估算：historical 尝试必须用原始 msg（历史 deadline 的历史 calldata）——
	// 与保存的 calldata_hash 严格对应、重放确定。只有 latest fallback 才用
	// fresh-deadline estMsg（latest 状态下历史 deadline 必然 DeadlinePassed）。
	gasMode := GasEstimateLatest
	gas := uint64(0)
	var estMsg ethereum.CallMsg
	if block != nil {
		if estCalldata, cerr := BuildExecuteV3CycleCalldata(c, chainID); cerr == nil {
			estMsg = ethereum.CallMsg{From: s.from, To: &s.contract, Gas: s.maxGas, Data: estCalldata}
		}
		// 节点明确不支持后整个运行期不再尝试 historical（避免每次猜测/轰炸）
		if !s.histUnsupported.Load() {
			gas, err = estimateGasAt(ctx, s.cli, msg, block)
			switch {
			case err == nil:
				// 历史估算成功：gas price 必须来自同一历史 header 的 base fee，
				// 否则不算完整 historical（禁止默认价进入正式数据）
				if historicalHeader == nil || historicalHeader.BaseFee == nil || historicalHeader.BaseFee.Sign() <= 0 {
					return nil, fmt.Errorf("historical block %d missing base fee", block.Uint64())
				}
				gasPrice = new(big.Int).Set(historicalHeader.BaseFee)
				gasMode = GasEstimateComplete
			case isUnsupportedHistoricalEstimate(err):
				// 明确不支持（-32601/-32602/not supported）→ latest 近似，
				// 并缓存：后续区块直接走 latest
				slog.Debug("historical estimateGas unsupported, using latest approximation", "err", err)
				s.histUnsupported.Store(true)
				gas, err = s.cli.EstimateGas(ctx, estMsg)
				if err != nil {
					if isInfraError(err) {
						return nil, fmt.Errorf("estimateGas (infra): %w", err)
					}
					gas = s.maxGas
					gasMode = GasEstimateMax
				}
			case isInfraError(err):
				// 基础设施故障：区块保持未评估，由上层重试
				return nil, fmt.Errorf("historical estimateGas (infra): %w", err)
			default:
				// 未知/矛盾错误：宁可重试，不可误认证
				return nil, fmt.Errorf("historical estimateGas (inconsistent): %w", err)
			}
		} else {
			gas, err = s.cli.EstimateGas(ctx, estMsg)
			if err != nil {
				if isInfraError(err) {
					return nil, fmt.Errorf("estimateGas (infra): %w", err)
				}
				gas = s.maxGas
				gasMode = GasEstimateMax
			}
		}
	} else {
		gas, err = s.cli.EstimateGas(ctx, msg)
		if err != nil {
			if isInfraError(err) {
				return nil, fmt.Errorf("estimateGas (infra): %w", err)
			}
			gas = s.maxGas
			gasMode = GasEstimateMax
		}
	}
	// Gas 价格固定到历史块（Arbitrum L2 base fee，读 header.BaseFee at block）：
	// 成本必须与池状态/eth_call 同一区块，不能用调用时的 latest 污染历史利润。
	// 读取失败视为基础设施错误（可重试），不静默 fallback。

	// Arbitrum L1 data 费用：NodeInterface 虚拟合约 (0x...C8) 的 gasEstimateL1Component(to, contractCreation, data)
	// 返回 (gasEstimateForL1, baseFee, l1BaseFeeEstimate)；eth_gasEstimateL1Component RPC 方法不存在。
	var l1GasUnits uint64
	var l2BaseFee, l1BaseFeeEstimate *big.Int
	{
		// NodeInterface 虚拟合约（0x...C8）的 gasEstimateL1Component(to, contractCreation, data)
		nodeInterface := common.HexToAddress("0x00000000000000000000000000000000000000C8")
		intfABI := `[{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"contractCreation","type":"bool"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"gasEstimateL1Component","outputs":[{"internalType":"uint64","name":"gasEstimateForL1","type":"uint64"},{"internalType":"uint256","name":"baseFee","type":"uint256"},{"internalType":"uint256","name":"l1BaseFeeEstimate","type":"uint256"}],"stateMutability":"view","type":"function"}]`
		parsed, aerr := abi.JSON(strings.NewReader(intfABI))
		if aerr == nil {
			if callData, perr := parsed.Pack("gasEstimateL1Component", msg.To, false, msg.Data); perr == nil {
				if res, cerr := s.cli.CallContract(ctx, ethereum.CallMsg{To: &nodeInterface, Data: callData}, block); cerr == nil && len(res) >= 96 {
					l1GasUnits = new(big.Int).SetBytes(res[0:32]).Uint64()
					l2BaseFee = new(big.Int).SetBytes(res[32:64])
					l1BaseFeeEstimate = new(big.Int).SetBytes(res[64:96])
				}
			}
		}
	}
	// SimulationBlock = eth_call 实际使用的区块（block 非 nil 时就是它，不是调用完成时的 latest）
	var simBlock uint64
	if block != nil {
		simBlock = block.Uint64()
	} else {
		simBlock, _ = s.cli.BlockNumber(ctx)
	}
	return &SimResult{Profit: profit, GasUsed: gas, GasPriceWei: gasPrice,
		CalldataHash: hashHex(calldata), SimulationBlock: simBlock,
		L1GasUnits: l1GasUnits, L2BaseFeeWei: l2BaseFee, L1BaseFeeEstimateWei: l1BaseFeeEstimate,
		GasEstimateMode: gasMode}, nil
}

// estimateGasAt 用原始 JSON-RPC 调用 eth_estimateGas(callObj, block)——
// Geth/archive 节点支持固定历史区块估算。节点不支持时返回 error，
// 调用方回退 latest 近似。
func estimateGasAt(ctx context.Context, cli *ethclient.Client, msg ethereum.CallMsg, block *big.Int) (uint64, error) {
	raw, err := json.Marshal(map[string]any{
		"from":     msg.From.Hex(),
		"to":       msg.To.Hex(),
		"gas":      hexutil.EncodeUint64(msg.Gas),
		"data":     hexutil.Bytes(msg.Data),
		"value":    hexutil.EncodeBig(big.NewInt(0)),
	})
	if err != nil {
		return 0, err
	}
	var res hexutil.Uint64
	callErr := cli.Client().CallContext(ctx, &res, "eth_estimateGas", json.RawMessage(raw), hexutil.EncodeBig(block))
	if callErr != nil {
		return 0, callErr
	}
	return uint64(res), nil
}

// hashHex keccak256 十六进制（calldata 指纹）。
func hashHex(data []byte) string {
	h := crypto.Keccak256(data)
	return common.Bytes2Hex(h)
}

// BuildExecuteV3CycleCalldata 构建 ArbitrageExecutor.executeV3Cycle 的 ABI calldata。
// Hop 结构：struct Hop { address pool; address tokenIn; address tokenOut; uint24 fee; }
// BuildExecuteV3CycleCalldata 构建执行 calldata。deadline 默认用当前时间；
// BuildExecuteV3CycleCalldataAt 用历史区块时间（重放确定性，同一历史区块
// 每次评估产出相同 calldata hash / L1 成本）。
func BuildExecuteV3CycleCalldata(c *arbitrage.Candidate, chainID uint64) ([]byte, error) {
	return buildExecuteV3CycleCalldata(c, chainID, time.Now().Add(2*time.Minute).Unix())
}

func BuildExecuteV3CycleCalldataAt(c *arbitrage.Candidate, chainID uint64, blockTime int64) ([]byte, error) {
	return buildExecuteV3CycleCalldata(c, chainID, blockTime+120)
}

func buildExecuteV3CycleCalldata(c *arbitrage.Candidate, chainID uint64, deadlineUnix int64) ([]byte, error) {
	if len(c.Route) < 2 || len(c.Route) > 3 {
		return nil, errors.New("route must have 2-3 hops")
	}
	executeABI := `[{"inputs":[{"components":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"address","name":"tokenIn","type":"address"},{"internalType":"address","name":"tokenOut","type":"address"},{"internalType":"uint24","name":"fee","type":"uint24"}],"internalType":"struct ArbitrageExecutor.Hop[]","name":"hops","type":"tuple[]"},{"internalType":"uint256","name":"amountIn","type":"uint256"},{"internalType":"uint256","name":"minProfit","type":"uint256"},{"internalType":"uint256","name":"deadline","type":"uint256"}],"name":"executeV3Cycle","outputs":[{"internalType":"uint256","name":"profit","type":"uint256"}],"stateMutability":"nonpayable","type":"function"}]`
	parsed, err := abi.JSON(strings.NewReader(executeABI))
	if err != nil {
		return nil, fmt.Errorf("parse executor abi: %w", err)
	}
	type hop struct {
		Pool     common.Address
		TokenIn  common.Address
		TokenOut common.Address
		// ABI uint24 的 Go 类型是 *big.Int（非 8/16/32/64 的 uint 都映射到 big.Int）——
		// 用 uint32 打包会报 "cannot use uint32 as type ptr"
		Fee *big.Int
	}
	hops := make([]hop, 0, len(c.Route))
	for _, h := range c.Route {
		hops = append(hops, hop{Pool: h.Pool, TokenIn: h.TokenIn, TokenOut: h.TokenOut,
			Fee: new(big.Int).SetUint64(uint64(h.Fee))})
	}
	minProfit := big.NewInt(1) // 模拟时最小利润门槛：只要能盈利即可；真实门槛在 Evaluate 决策
	if c.ExpectedNetProfit != nil && c.ExpectedNetProfit.Sign() > 0 {
		minProfit = new(big.Int).Set(c.ExpectedNetProfit)
	}
	return parsed.Pack("executeV3Cycle", hops, c.InputAmount, minProfit, big.NewInt(deadlineUnix))
}
