package simulation

import (
	"context"
	"log/slog"
	"math/big"

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

func (e *SimulationEvaluator) Evaluate(ctx context.Context, c *arbitrage.Candidate, cfg arbitrage.Config) (string, string, *big.Int) {
	// 第一层：本地评估（毛利润 > 0 才值得上链模拟）
	if c.GrossProfit == nil || c.GrossProfit.Sign() <= 0 {
		return DecisionLocalCandidate, "non-positive gross profit", big.NewInt(0)
	}

	// 第二层：真实链上模拟
	res, err := e.sim.Simulate(ctx, c, e.chainID)
	if err != nil {
		slog.Warn("simulate error", "err", err)
		return DecisionSimulationRejected, "simulate error: " + err.Error(), big.NewInt(0)
	}
	if res.RevertMsg != "" {
		return DecisionSimulationRejected, "revert: " + truncate(res.RevertMsg, 120), big.NewInt(0)
	}
	c.SimulationResult = "eth_call ok"
	if res.Profit == nil {
		return DecisionSimulationRejected, "no profit returned", big.NewInt(0)
	}

	// gas 成本（WETH 计价，1:1 ETH）
	gasPrice := big.NewInt(1e8) // 保守默认 0.1 gwei；生产从 sim RPC 读 eth_gasPrice
	gasCostWei := new(big.Int).Mul(new(big.Int).SetUint64(res.GasUsed), gasPrice)
	net := new(big.Int).Sub(res.Profit, gasCostWei)
	net.Sub(net, e.safetyMarginWei)
	c.ExpectedNetProfit = net

	if net.Cmp(cfg.MinProfitWei) < 0 {
		return DecisionSimulationRejected, "below min profit after gas", net
	}
	return DecisionSimulationOK, "", net
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
