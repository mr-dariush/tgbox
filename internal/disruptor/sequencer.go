package disruptor

import (
	"context"
	"errors"
	"runtime"
	"time"

	"github.com/gotd/td/tg"
)

// ErrBufferSaturated indicates the ring buffer is at capacity during non-blocking acquisition attempts.
var ErrBufferSaturated = errors.New("disruptor: ring buffer saturated")

// SaturationCallback defines the function executed when the ring buffer reaches capacity during non-blocking probes.
type SaturationCallback func(droppedCount uint64)

// Sequencer coordinates reserving and publishing sequence numbers in the ring buffer.
type Sequencer struct {
	ring        *RingBuffer
	onSaturated SaturationCallback
}

// NewSequencer constructs a Sequencer wrapping the provided RingBuffer.
func NewSequencer(ring *RingBuffer, onSaturated SaturationCallback) *Sequencer {
	return &Sequencer{
		ring:        ring,
		onSaturated: onSaturated,
	}
}

// TryNext attempts to reserve the next sequence number in a non-blocking manner.
// If the buffer is full (next - gatingSeq > capacity), it returns ErrBufferSaturated immediately.
func (s *Sequencer) TryNext() (uint64, error) {
	curr := s.ring.cursor.Get()
	next := curr + 1
	gating := s.ring.gatingSeq.Get()

	if next-gating > s.ring.capacity {
		return 0, ErrBufferSaturated
	}

	if s.ring.cursor.CompareAndSwap(curr, next) {
		return next, nil
	}

	return 0, ErrBufferSaturated
}

// Next attempts to reserve the next sequence number with backpressure.
// It blocks the caller if the buffer is saturated, exerting TCP backpressure upstream.
func (s *Sequencer) Next(ctx context.Context) (uint64, error) {
	spins := 0
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}

		curr := s.ring.cursor.Get()
		next := curr + 1
		gating := s.ring.gatingSeq.Get()

		if next-gating > s.ring.capacity {
			spins++
			waitBackpressure(ctx, spins)
			continue
		}

		if s.ring.cursor.CompareAndSwap(curr, next) {
			return next, nil
		}

		runtime.Gosched()
	}
}

// waitBackpressure yields processor time or pauses adaptively during buffer saturation.
func waitBackpressure(ctx context.Context, spins int) {
	if spins <= 100 {
		runtime.Gosched()
		return
	}

	select {
	case <-ctx.Done():
	case <-time.After(50 * time.Microsecond):
	}
}

// EnqueueContext stores an incoming update into the next available ring buffer slot with backpressure.
// If the buffer is saturated, it blocks until a slot is freed or ctx is canceled.
func (s *Sequencer) EnqueueContext(ctx context.Context, raw tg.UpdatesClass, traceID [16]byte, clientID int64) error {
	seq, err := s.Next(ctx)
	if err != nil {
		if s.onSaturated != nil {
			s.onSaturated(1)
		}
		return err
	}

	slot := s.ring.Slot(seq)
	slot.RawClass = raw
	slot.TraceID = traceID
	slot.ClientID = clientID
	slot.Received = time.Now().UnixNano()

	// Atomic store acts as a memory store-barrier, making previous writes globally visible.
	slot.Sequence.Store(seq)
	return nil
}

// Enqueue stores an incoming update into the next available ring buffer slot, applying backpressure.
// Returns true if published successfully, or false if the context was canceled.
func (s *Sequencer) Enqueue(raw tg.UpdatesClass, traceID [16]byte, clientID int64) bool {
	return s.EnqueueContext(context.Background(), raw, traceID, clientID) == nil
}
