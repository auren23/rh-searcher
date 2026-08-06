package simulation

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/common"

	"github.com/auren23/rh-searcher/internal/arbitrage"
)

// SimulationEvaluator 分层评估器：
//   local_candidate —— 仅本地数学结果
//   simulation_accepted / simulation_rejected —— 真实 executeV3Cycle eth_call 验证后
//
// 净利 = 合约返回 profit（WETH 余额差）− gas 成本 − 安全边际。
// 模拟交易与最终发送交易逐字节相同（同一 calldata）。
type SimulationEvaluator struct {
	sim            *ExecutorSimulator
	chainID        uint64
	safetyMarginWei *big.Int
}

func NewSimulationEvaluator(sim *ExecutorSimulator, chainID uint64, safetyMarginWei *big.Int) *SimulationEvaluator {
	return &SimulationEvaluator{sim: sim, chainID: chainID, safetyMarginWei: safetyMarginWei}
}

// VerifyBlockHash 转发到模拟器（Engine 通过接口断言调用；必须有此转发否则校验被静默跳过）。
func (e *SimulationEvaluator) VerifyBlockHash(ctx context.Context, block uint64, want common.Hash) error {
	return e.sim.VerifyBlockHash(ctx, block, want)
}

func (e *SimulationEvaluator) Evaluate(ctx context.Context, c *arbitrage.Candidate, cfg arbitrage.Config) (string, string, *big.Int, error) {
	// 第一层：本地评估（毛利润 > 0 才值得上链模拟）
	if c.GrossProfit == nil || c.GrossProfit.Sign() <= 0 {
		return DecisionLocalCandidate, "non-positive gross profit", big.NewInt(0), nil
	}

	// 第二层：真实链上模拟（固定到 cfg.StateBlock，与状态读取同一区块）
	res, err := e.sim.Simulate(ctx, c, e.chainID, cfg.StateBlock)
	if err != nil {
		// 基础设施错误（RPC 超时/限流/节点落后）：区块必须保持未评估，
		// 由上层重试——不能落成 simulation_rejected 永久拒绝
		return "", "", nil, fmt.Errorf("simulate: %w", err)
	}
	if res.RevertMsg != "" {
		return DecisionSimulationRejected, "revert: " + truncate(res.RevertMsg, 120), big.NewInt(0), nil
	}
	if res.Profit == nil {
		return DecisionSimulationRejected, "no profit returned", big.NewInt(0), nil
	}

	// 完整记录：profit / gasUsed / gasPrice / gasCost / calldata hash（供复盘与误差分析）
	c.SimulationResult = "eth_call ok"
	c.GasEstimate = new(big.Int).SetUint64(res.GasUsed)
	c.SwapCost = big.NewInt(0) // DEX 费已含在链上 profit 中
	c.SlippageCost = big.NewInt(0)
	c.SimulatedProfitWei = new(big.Int).Set(res.Profit)
	c.GasUsed = res.GasUsed
	c.GasPriceWei = new(big.Int).Set(res.GasPriceWei)
	gasCostWei := new(big.Int).Mul(new(big.Int).SetUint64(res.GasUsed), res.GasPriceWei)
	c.GasCostWei = new(big.Int).Set(gasCostWei)
	c.CalldataHash = res.CalldataHash
	c.SimulationBlock = res.SimulationBlock
	c.GasEstimateMode = string(res.GasEstimateMode)
	// Arbitrum L1 组件持久化（总费仍按 gasUsed×gasPrice 近似；避免重复扣费）
	c.L1GasUnits = res.L1GasUnits
	c.L2BaseFeeWei = res.L2BaseFeeWei
	c.L1BaseFeeEstimateWei = res.L1BaseFeeEstimateWei
	if res.L1GasUnits > 0 {
		slog.Debug("l1 component",
			"l1_gas_units", res.L1GasUnits,
			"l2_base_fee_wei", weiString(res.L2BaseFeeWei),
			"l1_base_fee_estimate_wei", weiString(res.L1BaseFeeEstimateWei),
			"calldata_hash", res.CalldataHash)
	}
	net := new(big.Int).Sub(res.Profit, gasCostWei)
	net.Sub(net, e.safetyMarginWei)
	c.ExpectedNetProfit = net
	// gas 成本非 historical（latest 近似或 maxGas 兜底）：
	// 利润数据保留，但不得判定为正式 accepted/selected
	if res.GasEstimateMode != GasEstimateComplete {
		return DecisionSimulationCostApprox, "gas cost approximate (" + string(res.GasEstimateMode) + ")", net, nil
	}
	slog.Debug("simulation result",
		"profit_wei", res.Profit.String(), "gas_used", res.GasUsed,
		"gas_price_wei", res.GasPriceWei.String(), "net_wei", net.String(),
		"calldata_hash", res.CalldataHash)

	if net.Cmp(cfg.MinProfitWei) < 0 {
		return DecisionSimulationRejected, "below min profit after gas", net, nil
	}
	return DecisionSimulationOK, "", net, nil
}

func weiString(v *big.Int) string {
	if v == nil {
		return ""
	}
	return v.String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
