// Package clock provides a decorator around gotd/td/clock to control and suppress native timers safely.
package clock

import (
	"time"

	"github.com/gotd/td/clock"
)

// dormantTimer implements clock.Timer but its channel never yields a tick,
// safely neutralizing timed background loops without causing panics or deadlocks.
type dormantTimer struct {
	ch chan time.Time
}

func newDormantTimer() *dormantTimer {
	return &dormantTimer{
		ch: make(chan time.Time),
	}
}

// C returns the channel associated with the timer.
func (d *dormantTimer) C() <-chan time.Time {
	return d.ch
}

// Reset implements clock.Timer.
func (*dormantTimer) Reset(time.Duration) {}

// Stop implements clock.Timer.
func (*dormantTimer) Stop() bool {
	return false
}

// dormantTicker implements clock.Ticker but its channel never yields a tick.
type dormantTicker struct {
	ch chan time.Time
}

func newDormantTicker() *dormantTicker {
	return &dormantTicker{
		ch: make(chan time.Time),
	}
}

// C returns the channel associated with the ticker.
func (d *dormantTicker) C() <-chan time.Time {
	return d.ch
}

// Reset implements clock.Ticker.
func (*dormantTicker) Reset(time.Duration) {}

// Stop implements clock.Ticker.
func (*dormantTicker) Stop() {}

// Interceptor decorates a base clock.Clock, selectively neutralizing timers
// matching a specified duration (such as the default gotd ping loop interval).
type Interceptor struct {
	base           clock.Clock
	targetInterval time.Duration
}

// NewInterceptor constructs a new Interceptor. If base is nil, clock.System is used.
// If targetInterval is zero or negative, it defaults to 1 minute.
func NewInterceptor(base clock.Clock, targetInterval time.Duration) *Interceptor {
	if base == nil {
		base = clock.System
	}
	if targetInterval <= 0 {
		targetInterval = time.Minute
	}
	return &Interceptor{
		base:           base,
		targetInterval: targetInterval,
	}
}

// Now returns the current time from the underlying base clock.
func (i *Interceptor) Now() time.Time {
	return i.base.Now()
}

// Timer returns a dormant timer if d matches the target interval; otherwise, delegates to the base clock.
func (i *Interceptor) Timer(d time.Duration) clock.Timer {
	if d == i.targetInterval {
		return newDormantTimer()
	}
	return i.base.Timer(d)
}

// Ticker returns a dormant ticker if d matches the target interval; otherwise, delegates to the base clock.
func (i *Interceptor) Ticker(d time.Duration) clock.Ticker {
	if d == i.targetInterval {
		return newDormantTicker()
	}
	return i.base.Ticker(d)
}
