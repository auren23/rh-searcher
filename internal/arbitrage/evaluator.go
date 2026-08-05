package arbitrage

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// LocalEvaluator 本地评估器：成本核算 + 门槛判定。
// 净收益 = 毛利润 - gas - DEX 费 - 滑点 - 安全边际。
type LocalEvaluator struct{}

func NewLocalEvaluator() *LocalEvaluator { return &LocalEvaluator{} }

// LocalExecutor 执行器（shadow 模式：只记录，不发送）。
type LocalExecutor struct{}

func NewExecutor() *LocalExecutor { return &LocalExecutor{} }

// Execute 实现 Executor 接口。MVP shadow：返回零哈希。
func (x *LocalExecutor) Execute(ctx context.Context, c *Candidate) (common.Hash, error) {
	return common.Hash{}, nil
}

func (e *LocalEvaluator) Evaluate(ctx context.Context, c *Candidate, cfg Config) (string, string, *big.Int) {
	if c.GrossProfit == nil || c.GrossProfit.Sign() <= 0 {
		return "rejected", "non-positive gross profit", big.NewInt(0)
	}
	totalCost := new(big.Int).Set(c.GasEstimate)
	totalCost.Add(totalCost, c.SwapCost)
	totalCost.Add(totalCost, c.SlippageCost)
	totalCost.Add(totalCost, cfg.SafetyMarginWei)
	net := new(big.Int).Sub(c.GrossProfit, totalCost)
	if net.Cmp(cfg.MinProfitWei) < 0 {
		return "rejected", "below min profit", net
	}
	return "accepted", "", net
}
