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
	if res.Profit == nil {
		return DecisionSimulationRejected, "no profit returned", big.NewInt(0)
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
	net := new(big.Int).Sub(res.Profit, gasCostWei)
	net.Sub(net, e.safetyMarginWei)
	c.ExpectedNetProfit = net
	slog.Debug("simulation result",
		"profit_wei", res.Profit.String(), "gas_used", res.GasUsed,
		"gas_price_wei", res.GasPriceWei.String(), "net_wei", net.String(),
		"calldata_hash", res.CalldataHash)

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
