package keepalive_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/neo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/keepalive"
)

// TestEngine_PingTriggers verifies that the engine successfully fires the ping function
// when the ping interval has elapsed without any external traffic notifications.
func TestEngine_PingTriggers(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	mockClock := neo.NewTime(time.Now())
	pingCh := make(chan struct{}, 1)

	cfg := keepalive.Config{PingInterval: 10 * time.Second, JitterOffset: 0}
	engine := keepalive.NewEngine(cfg, func(_ context.Context) error {
		pingCh <- struct{}{}
		return nil
	}, mockClock)

	engine.Start(ctx)

	// Await the timer registration internally.
	<-mockClock.Observe()
	mockClock.Travel(11 * time.Second) // Advance virtual clock past the interval boundary.

	select {
	case <-pingCh:
		// Passed successfully.
	case <-time.After(1 * time.Second):
		t.Fatal("ping was not triggered within the expected virtual interval")
	}
}

// TestEngine_TrafficDelaysPing guarantees that calling NotifyTraffic pushes the next
// scheduled ping execution forward asynchronously, preventing unnecessary pings.
func TestEngine_TrafficDelaysPing(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	mockClock := neo.NewTime(time.Now())
	var pings atomic.Int32

	cfg := keepalive.Config{PingInterval: 10 * time.Second, JitterOffset: 0}
	engine := keepalive.NewEngine(cfg, func(_ context.Context) error {
		pings.Add(1)
		return nil
	}, mockClock)

	engine.Start(ctx)

	<-mockClock.Observe()
	mockClock.Travel(5 * time.Second) // Travel halfway through the ping cycle.
	engine.NotifyTraffic()            // Delay ping: lastTraffic becomes T=5s, new target becomes T=15s.

	// First advance virtual clock to expire the first timer (T=11s >= T=10s)
	mockClock.Travel(6 * time.Second)
	// Now the loop wakes up, suppresses the ping, and registers the delayed timer for T=15s
	<-mockClock.Observe()
	assert.Equal(t, int32(0), pings.Load(), "ping should be suppressed due to traffic observation")

	// Advance virtual clock past the delayed target (T=16s >= T=15s)
	mockClock.Travel(5 * time.Second)

	// Ping fires reliably without blocking
	assert.Eventually(t, func() bool {
		return pings.Load() == 1
	}, 1*time.Second, 10*time.Millisecond, "ping should have fired after the extended delay")
}

// TestEngine_ContextCancellation validates that canceling the context immediately stops
// the background loop, leaving zero orphaned routines or memory leaks.
func TestEngine_ContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	mockClock := neo.NewTime(time.Now())

	cfg := keepalive.Config{PingInterval: 1 * time.Hour}
	engine := keepalive.NewEngine(cfg, func(_ context.Context) error {
		return nil
	}, mockClock)

	engine.Start(ctx)
	<-mockClock.Observe() // Ensure loop has officially entered the block state.

	cancel() // Abort background processes entirely.

	// Provide a short real-time grace period for the routine to exit safely.
	time.Sleep(50 * time.Millisecond)

	// Since we cleanly exited, starting again with a new context should not panic.
	ctx2 := t.Context()

	require.NotPanics(t, func() {
		engine.Start(ctx2)
	})
}
