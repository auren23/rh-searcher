package liquidation

import "context"

// LocalEvaluator 简易评估器：MVP 阶段全部标记 pending。
type LocalEvaluator struct{}

func NewLocalEvaluator() *LocalEvaluator { return &LocalEvaluator{} }

func (e *LocalEvaluator) Evaluate(ctx context.Context, c *Candidate) (string, string) {
	return "pending", "M7: profit calculation not yet wired"
}
