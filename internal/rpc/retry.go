package rpc

import (
	"context"
	"time"
)

// RetryPolicy 指数退避重试策略。
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: 200 * time.Millisecond, MaxDelay: 5 * time.Second}
}

// Do 执行 fn 直到成功或达到最大尝试次数。fn 返回 (retryable, err)。
func Do[T any](ctx context.Context, p RetryPolicy, fn func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	delay := p.BaseDelay
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		v, err := fn(ctx)
		if err == nil {
			return v, nil
		}
		lastErr = err
		if attempt < p.MaxAttempts-1 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
			if delay > p.MaxDelay {
				delay = p.MaxDelay
			}
		}
	}
	return zero, lastErr
}
