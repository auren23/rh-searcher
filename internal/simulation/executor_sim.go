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
	Profit    *big.Int // 合约返回的 WETH 净利（未扣 gas）
	GasUsed   uint64
	RevertMsg string
}

// Simulate 构建并模拟 executeV3Cycle 调用。
func (s *ExecutorSimulator) Simulate(ctx context.Context, c *arbitrage.Candidate, chainID uint64) (*SimResult, error) {
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
	out, err := s.cli.CallContract(ctx, msg, nil)
	if err != nil {
		return &SimResult{RevertMsg: err.Error()}, nil
	}
	if len(out) < 32 {
		return &SimResult{RevertMsg: fmt.Sprintf("short return %d bytes", len(out))}, nil
	}
	profit := new(big.Int).SetBytes(out[:32])
	gas, err := s.cli.EstimateGas(ctx, msg)
	if err != nil {
		gas = s.maxGas // 估算失败用上限（保守）
	}
	return &SimResult{Profit: profit, GasUsed: gas}, nil
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
