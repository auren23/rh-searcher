package simulation

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

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
)

// ExecutorSimulator 用真实 executeV3Cycle calldata 做 eth_call 模拟。
// 模拟交易与最终发送交易逐字节相同（不允许"模拟专用简化路径"）。
type ExecutorSimulator struct {
	cli       *ethclient.Client
	contract  common.Address // ArbitrageExecutor 地址
	from      common.Address // 执行钱包（hot wallet）
	maxGas    uint64
}

func NewExecutorSimulator(cli *ethclient.Client, contract, from common.Address, maxGas uint64) *ExecutorSimulator {
	return &ExecutorSimulator{cli: cli, contract: contract, from: from, maxGas: maxGas}
}

// SimResult 一次链上模拟的结果。
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

// Simulate 构建并模拟 executeV3Cycle 调用。
// block 非 nil 时 eth_call 固定在该高度（与状态读取同一区块，区块原子性）。
func (s *ExecutorSimulator) Simulate(ctx context.Context, c *arbitrage.Candidate, chainID uint64, block *big.Int) (*SimResult, error) {
	calldata, err := BuildExecuteV3CycleCalldata(c, chainID)
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
		return &SimResult{RevertMsg: err.Error(), CalldataHash: hashHex(calldata)}, nil
	}
	if len(out) < 32 {
		return &SimResult{RevertMsg: fmt.Sprintf("short return %d bytes", len(out)), CalldataHash: hashHex(calldata)}, nil
	}
	profit := new(big.Int).SetBytes(out[:32])
	gas, err := s.cli.EstimateGas(ctx, msg)
	if err != nil {
		gas = s.maxGas // 估算失败用上限（保守）
	}
	// 动态 gas 价格（eth_gasPrice；失败时保守默认）
	gasPrice, err := s.cli.SuggestGasPrice(ctx)
	if err != nil || gasPrice.Sign() <= 0 {
		gasPrice = big.NewInt(1e8) // 0.1 gwei 兜底
	}
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
				if res, cerr := s.cli.CallContract(ctx, ethereum.CallMsg{To: &nodeInterface, Data: callData}, nil); cerr == nil && len(res) >= 96 {
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
		L1GasUnits: l1GasUnits, L2BaseFeeWei: l2BaseFee, L1BaseFeeEstimateWei: l1BaseFeeEstimate}, nil
}

// hashHex keccak256 十六进制（calldata 指纹）。
func hashHex(data []byte) string {
	h := crypto.Keccak256(data)
	return common.Bytes2Hex(h)
}

// BuildExecuteV3CycleCalldata 构建 ArbitrageExecutor.executeV3Cycle 的 ABI calldata。
// Hop 结构：struct Hop { address pool; address tokenIn; address tokenOut; uint24 fee; }
func BuildExecuteV3CycleCalldata(c *arbitrage.Candidate, chainID uint64) ([]byte, error) {
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
		Fee      uint32
	}
	hops := make([]hop, 0, len(c.Route))
	for _, h := range c.Route {
		hops = append(hops, hop{Pool: h.Pool, TokenIn: h.TokenIn, TokenOut: h.TokenOut, Fee: h.Fee})
	}
	minProfit := big.NewInt(1) // 模拟时最小利润门槛：只要能盈利即可；真实门槛在 Evaluate 决策
	if c.ExpectedNetProfit != nil && c.ExpectedNetProfit.Sign() > 0 {
		minProfit = new(big.Int).Set(c.ExpectedNetProfit)
	}
	deadline := big.NewInt(time.Now().Add(2 * time.Minute).Unix())
	return parsed.Pack("executeV3Cycle", hops, c.InputAmount, minProfit, deadline)
}
