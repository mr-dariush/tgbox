// Package interceptor provides decorators for tg.Invoker to sniff, augment, and control MTProto operations.
package interceptor

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/clock"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// ErrFloodShedding is returned when an MTProto request is dropped due to excessive flood wait saturation.
var ErrFloodShedding = errors.New("tgbox: request dropped due to flood wait saturation (load shedding)")

// BoundedFloodWaiter retries requests that hit FLOOD_WAIT errors, but only
// up to a bounded number of retries and a bounded wait duration; beyond
// either threshold the request is dropped with ErrFloodShedding.
type BoundedFloodWaiter struct {
	clock      clock.Clock
	maxRetries uint
	maxWait    time.Duration
}

// NewBoundedFloodWaiter creates the flood-wait middleware. A zero maxRetries
// or maxWait disables that specific limit.
func NewBoundedFloodWaiter(maxRetries uint, maxWait time.Duration) *BoundedFloodWaiter {
	return &BoundedFloodWaiter{
		clock:      clock.System,
		maxRetries: maxRetries,
		maxWait:    maxWait,
	}
}

// Handle wraps the standard tg.Invoker with backoff and shedding capabilities.
func (w *BoundedFloodWaiter) Handle(next tg.Invoker) telegram.InvokeFunc {
	return func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
		var retries uint

		for {
			err := next.Invoke(ctx, input, output)
			if err == nil {
				return nil
			}

			d, ok := tgerr.AsFloodWait(err)
			if !ok {
				return err //nolint:wrapcheck // error passed through unwrapped to preserve tgerr type
			}

			retries++
			if w.maxRetries > 0 && retries > w.maxRetries {
				return errors.Join(ErrFloodShedding, err)
			}
			if w.maxWait > 0 && d > w.maxWait {
				return errors.Join(ErrFloodShedding, err)
			}

			// Add full jitter (0-1000ms) to the server-mandated wait duration to mitigate thundering herds.
			jitter := time.Duration(rand.IntN(1000)) * time.Millisecond //nolint:gosec // jitter does not need crypto randomness
			waitDuration := d + jitter

			timer := w.clock.Timer(waitDuration)
			select {
			case <-ctx.Done():
				clock.StopTimer(timer)
				return ctx.Err()
			case <-timer.C():
				// Wait elapsed with jitter; retry.
			}
		}
	}
}

// WithClock sets a custom clock for the BoundedFloodWaiter (essential for virtual clock testing).
func (w *BoundedFloodWaiter) WithClock(c clock.Clock) *BoundedFloodWaiter {
	w.clock = c
	return w
}
