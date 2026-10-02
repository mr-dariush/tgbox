package interceptor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/interceptor"
)

// mockRateInvoker isolates MTProto RPC simulation specifically for rate limiter testing.
type mockRateInvoker struct {
	calls int
	errs  []error
}

func (m *mockRateInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	// FIX: Always increment the call counter before returning.
	callIdx := m.calls
	m.calls++
	if callIdx >= len(m.errs) {
		return nil
	}
	return m.errs[callIdx]
}

type mockLimiter struct {
	err    error
	called bool
}

func (m *mockLimiter) Wait(_ context.Context) error {
	m.called = true
	return m.err
}

// TestRateLimiter tests the RateLimiter interceptor with various limiter implementations.
func TestRateLimiter(t *testing.T) {
	t.Run("InvokesWhenLimiterSucceeds", func(t *testing.T) {
		lim := &mockLimiter{}
		invoker := &mockRateInvoker{errs: []error{}}
		rl := interceptor.NewRateLimiter(lim)

		err := rl.Handle(invoker).Invoke(context.Background(), nil, nil)
		require.NoError(t, err)
		assert.True(t, lim.called)
		assert.Equal(t, 1, invoker.calls)
	})

	t.Run("BailsOutWhenLimiterFails", func(t *testing.T) {
		expectedErr := errors.New("rate limit exhaustion on cluster side")
		lim := &mockLimiter{err: expectedErr}
		invoker := &mockRateInvoker{errs: []error{}}
		rl := interceptor.NewRateLimiter(lim)

		err := rl.Handle(invoker).Invoke(context.Background(), nil, nil)
		require.ErrorIs(t, err, expectedErr)
		assert.True(t, lim.called)
		assert.Equal(t, 0, invoker.calls) // Strict assert: Invoker MUST NOT be triggered
	})

	t.Run("InMemoryLimiter_ContextCancellation", func(t *testing.T) {
		// Configure a local token bucket that generates zero tokens (permanent block)
		lim := interceptor.NewInMemoryLimiter(0, 0)
		rl := interceptor.NewRateLimiter(lim)
		invoker := &mockRateInvoker{errs: []error{}}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		defer cancel()

		err := rl.Handle(invoker).Invoke(ctx, nil, nil)
		require.Error(t, err)
		// FIX: Expecting the specific error from x/time/rate
		assert.Contains(t, err.Error(), "exceeds limiter's burst")
		assert.Equal(t, 0, invoker.calls) // Ensured that the request was safely discarded
	})

	t.Run("InMemoryLimiter_Throttling", func(t *testing.T) {
		// Allows 10 requests per second, with a maximum burst of 1 token
		lim := interceptor.NewInMemoryLimiter(10, 1)
		rl := interceptor.NewRateLimiter(lim)
		invoker := &mockRateInvoker{errs: []error{}}

		// First invocation acquires the single burst token instantly
		err1 := rl.Handle(invoker).Invoke(context.Background(), nil, nil)
		require.NoError(t, err1)
		assert.Equal(t, 1, invoker.calls)

		// Immediate second invocation must trigger throttling due to token deficit
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
		defer cancel()

		err2 := rl.Handle(invoker).Invoke(ctx, nil, nil)
		require.Error(t, err2)
		// FIX: Expecting the specific error from x/time/rate for context timeouts
		assert.Contains(t, err2.Error(), "would exceed context deadline")
		assert.Equal(t, 1, invoker.calls) // Assert that invoke count remains 1
	})
}
