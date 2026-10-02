package interceptor

import (
	"context"
	"fmt"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"golang.org/x/time/rate"
)

// Limiter abstracts a rate-limiting strategy, allowing local (in-memory) and
// distributed (e.g. Redis) backends to be used interchangeably.
type Limiter interface {
	// Wait blocks until the limiter permits execution or the context is canceled.
	Wait(ctx context.Context) error
}

// RateLimiter wraps tg.Invoker to throttle outgoing RPC calls before hitting Telegram servers.
type RateLimiter struct {
	limiter Limiter
}

// NewRateLimiter instantiates a new MTProto rate limiter middleware.
func NewRateLimiter(limiter Limiter) *RateLimiter {
	return &RateLimiter{limiter: limiter}
}

// Handle intercepts the execution pipeline and enforces the rate limit rules.
func (r *RateLimiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		if err := r.limiter.Wait(ctx); err != nil {
			return err
		}
		return next.Invoke(ctx, input, output)
	}
}

// InMemoryLimiter implements a local token bucket limiter using golang.org/x/time/rate.
type InMemoryLimiter struct {
	lim *rate.Limiter
}

// NewInMemoryLimiter creates a local token-bucket limiter that refills
// rateLimit tokens per second with the given maximum burst.
func NewInMemoryLimiter(rateLimit rate.Limit, burst int) *InMemoryLimiter {
	return &InMemoryLimiter{
		lim: rate.NewLimiter(rateLimit, burst),
	}
}

// Wait blocks until a token is available or the context is canceled.
func (l *InMemoryLimiter) Wait(ctx context.Context) error {
	if err := l.lim.Wait(ctx); err != nil {
		return fmt.Errorf("rate limiter wait: %w", err)
	}
	return nil
}
