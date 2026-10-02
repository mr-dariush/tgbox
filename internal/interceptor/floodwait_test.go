package interceptor_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gotd/neo"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/interceptor"
)

// mockInvoker simulates raw MTProto RPC calls inside the test harness.
type mockInvoker struct {
	calls int
	errs  []error
}

func (m *mockInvoker) Invoke(_ context.Context, _ bin.Encoder, _ bin.Decoder) error {
	// FIX: Always increment the call counter before returning.
	callIdx := m.calls
	m.calls++
	if callIdx >= len(m.errs) {
		return nil
	}
	return m.errs[callIdx]
}

// TestBoundedFloodWaiter tests the BoundedFloodWaiter retry and load shedding behavior.
func TestBoundedFloodWaiter(t *testing.T) {
	t.Run("SuccessOnFirstTry", func(t *testing.T) {
		invoker := &mockInvoker{errs: []error{}}
		waiter := interceptor.NewBoundedFloodWaiter(3, 10*time.Second)

		err := waiter.Handle(invoker).Invoke(context.Background(), nil, nil)
		require.NoError(t, err)
		// FIX: The invoker was actually called once successfully.
		assert.Equal(t, 1, invoker.calls)
	})

	t.Run("FastFailOnNonFloodError", func(t *testing.T) {
		expectedErr := errors.New("unauthorized or invalid request parameters")
		invoker := &mockInvoker{errs: []error{expectedErr}}
		waiter := interceptor.NewBoundedFloodWaiter(3, 10*time.Second)

		err := waiter.Handle(invoker).Invoke(context.Background(), nil, nil)
		require.ErrorIs(t, err, expectedErr)
		assert.Equal(t, 1, invoker.calls)
	})

	t.Run("RetryAndSucceedWithinLimits", func(t *testing.T) {
		ctx := t.Context()

		mockClock := neo.NewTime(time.Now())
		floodErr := tgerr.New(420, "FLOOD_WAIT_5") // 5 seconds wait required
		invoker := &mockInvoker{errs: []error{floodErr}}

		waiter := interceptor.NewBoundedFloodWaiter(3, 10*time.Second).WithClock(mockClock)

		done := make(chan error, 1)
		go func() {
			done <- waiter.Handle(invoker).Invoke(ctx, nil, nil)
		}()

		// FIX: Use neo.Observe() to wait strictly until the timer is registered
		<-mockClock.Observe()
		mockClock.Travel(6 * time.Second)

		err := <-done
		require.NoError(t, err)
		assert.Equal(t, 2, invoker.calls) // First failed, second call succeeded
	})

	t.Run("SheddingOnMaxRetriesExceeded", func(t *testing.T) {
		ctx := t.Context()

		mockClock := neo.NewTime(time.Now())
		floodErr := tgerr.New(420, "FLOOD_WAIT_5")
		invoker := &mockInvoker{errs: []error{floodErr, floodErr, floodErr, floodErr}}

		waiter := interceptor.NewBoundedFloodWaiter(2, 60*time.Second).WithClock(mockClock)

		done := make(chan error, 1)
		go func() {
			done <- waiter.Handle(invoker).Invoke(ctx, nil, nil)
		}()

		// Step 1: Wait for 1st timer, warp time
		<-mockClock.Observe()
		mockClock.Travel(6 * time.Second)

		// Step 2: Wait for 2nd timer, warp time
		<-mockClock.Observe()
		mockClock.Travel(6 * time.Second)

		err := <-done
		require.Error(t, err)
		require.ErrorIs(t, err, interceptor.ErrFloodShedding)
		assert.Equal(t, 3, invoker.calls) // 1 initial call, 2 permitted retries, then shed
	})

	t.Run("SheddingOnMaxWaitExceeded", func(t *testing.T) {
		ctx := t.Context()

		mockClock := neo.NewTime(time.Now())
		floodErr := tgerr.New(420, "FLOOD_WAIT_30") // 30s wait, exceeds 10s maximum limit
		invoker := &mockInvoker{errs: []error{floodErr}}

		waiter := interceptor.NewBoundedFloodWaiter(3, 10*time.Second).WithClock(mockClock)

		err := waiter.Handle(invoker).Invoke(ctx, nil, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, interceptor.ErrFloodShedding)
		assert.Equal(t, 1, invoker.calls) // Instantly dropped without sleep block
	})
}
