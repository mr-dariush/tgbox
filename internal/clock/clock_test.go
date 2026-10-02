package clock_test

import (
	"testing"
	"time"

	gotdclock "github.com/gotd/td/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/clock"
)

func TestInterceptor_Now(t *testing.T) {
	t.Parallel()

	interceptor := clock.NewInterceptor(gotdclock.System, time.Minute)
	before := time.Now()
	now := interceptor.Now()
	after := time.Now()

	assert.True(t, !now.Before(before) && !now.After(after), "Now() must return accurate current time")
}

func TestInterceptor_TimerInterception(t *testing.T) {
	t.Parallel()

	target := time.Minute
	interceptor := clock.NewInterceptor(gotdclock.System, target)

	t.Run("SuppressedTimerDoesNotTick", func(t *testing.T) {
		t.Parallel()

		timer := interceptor.Timer(target)
		require.NotNil(t, timer)

		select {
		case <-timer.C():
			t.Fatal("dormant timer channel must not produce ticks")
		case <-time.After(50 * time.Millisecond):
			// Passed: timer is completely dormant
		}

		assert.False(t, timer.Stop())
		timer.Reset(target)
	})

	t.Run("NormalTimerTicksCorrectly", func(t *testing.T) {
		t.Parallel()

		activeDuration := 10 * time.Millisecond
		timer := interceptor.Timer(activeDuration)
		require.NotNil(t, timer)

		select {
		case <-timer.C():
			// Passed: regular timers execute normally
		case <-time.After(500 * time.Millisecond):
			t.Fatal("normal timer failed to tick within expected duration")
		}
	})
}

func TestInterceptor_TickerInterception(t *testing.T) {
	t.Parallel()

	target := time.Minute
	interceptor := clock.NewInterceptor(gotdclock.System, target)

	t.Run("SuppressedTickerDoesNotTick", func(t *testing.T) {
		t.Parallel()

		ticker := interceptor.Ticker(target)
		require.NotNil(t, ticker)

		select {
		case <-ticker.C():
			t.Fatal("dormant ticker channel must not produce ticks")
		case <-time.After(50 * time.Millisecond):
			// Passed
		}

		ticker.Stop()
		ticker.Reset(target)
	})

	t.Run("NormalTickerTicksCorrectly", func(t *testing.T) {
		t.Parallel()

		activeDuration := 10 * time.Millisecond
		ticker := interceptor.Ticker(activeDuration)
		require.NotNil(t, ticker)
		defer ticker.Stop()

		select {
		case <-ticker.C():
			// Passed
		case <-time.After(500 * time.Millisecond):
			t.Fatal("normal ticker failed to tick")
		}
	})
}
