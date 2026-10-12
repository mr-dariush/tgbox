package disruptor

import (
	"context"
	"time"
)

// SlotHandler defines the callback signature for processing an active slot.
type SlotHandler func(slot *RingSlot) error

// SingleConsumerPoller pulls committed events from the ring buffer in batches.
// Being the single consumer, it updates gatingSeq without atomic CAS contention.
type SingleConsumerPoller struct {
	ring        *RingBuffer
	idleTimer   *time.Timer
	idleWaitDur time.Duration
}

// NewSingleConsumerPoller constructs a poller bound to the specified RingBuffer.
func NewSingleConsumerPoller(ring *RingBuffer, idleWaitDur time.Duration) *SingleConsumerPoller {
	if idleWaitDur <= 0 {
		idleWaitDur = 50 * time.Microsecond
	}

	timer := time.NewTimer(idleWaitDur)
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}

	return &SingleConsumerPoller{
		ring:        ring,
		idleTimer:   timer,
		idleWaitDur: idleWaitDur,
	}
}

// Run executes the polling loop until the provided context is canceled.
func (p *SingleConsumerPoller) Run(ctx context.Context, handler SlotHandler) error {
	defer p.idleTimer.Stop()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		processed, err := p.pollBatch(handler)
		if err != nil {
			return err
		}

		if processed == 0 {
			p.wait(ctx)
		}
	}
}

// pollBatch drains all published and unconsumed updates in a single continuous pass.
func (p *SingleConsumerPoller) pollBatch(handler SlotHandler) (int, error) {
	currGating := p.ring.gatingSeq.Get()
	nextSeq := currGating + 1

	count := 0
	for {
		slot := p.ring.Slot(nextSeq)
		if slot.Sequence.Load() != nextSeq {
			break
		}

		if err := handler(slot); err != nil {
			p.ring.gatingSeq.Set(nextSeq - 1)
			return count, err
		}

		nextSeq++
		count++
	}

	if count > 0 {
		p.ring.gatingSeq.Set(nextSeq - 1)
	}
	return count, nil
}

// wait pauses execution for idleWaitDur using a pre-allocated timer to avoid heap thrashing.
func (p *SingleConsumerPoller) wait(ctx context.Context) {
	p.idleTimer.Reset(p.idleWaitDur)
	select {
	case <-ctx.Done():
		if !p.idleTimer.Stop() {
			select {
			case <-p.idleTimer.C:
			default:
			}
		}
	case <-p.idleTimer.C:
	}
}
