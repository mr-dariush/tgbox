package disruptor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/disruptor"
)

func TestPaddedSequence_Isolation(t *testing.T) {
	t.Parallel()

	seq := disruptor.NewPaddedSequence(0)
	assert.Equal(t, uint64(0), seq.Get())

	seq.Set(42)
	assert.Equal(t, uint64(42), seq.Get())

	swapped := seq.CompareAndSwap(42, 100)
	assert.True(t, swapped)
	assert.Equal(t, uint64(100), seq.Get())

	assert.Equal(t, uint64(105), seq.Add(5))
	assert.Equal(t, uint64(106), seq.Inc())
}

func TestRingBuffer_CapacityValidation(t *testing.T) {
	t.Parallel()

	_, err := disruptor.NewRingBuffer(3)
	require.ErrorIs(t, err, disruptor.ErrInvalidCapacity)

	ring, err := disruptor.NewRingBuffer(1024)
	require.NoError(t, err)
	assert.Equal(t, uint64(1024), ring.Capacity())
	assert.Equal(t, uint64(1023), ring.Mask())
}

func TestSequencer_WrapAround_MonotonicOrder(t *testing.T) {
	t.Parallel()

	// Tiny buffer capacity forces hundreds of full ring buffer laps.
	const capacity = 16
	const totalItems = 5000

	ring, err := disruptor.NewRingBuffer(capacity)
	require.NoError(t, err)

	seq := disruptor.NewSequencer(ring, nil)
	poller := disruptor.NewSingleConsumerPoller(ring, 10*time.Microsecond)

	ctx := t.Context()

	var lastConsumed atomic.Uint64
	var totalConsumed atomic.Int64

	go func() {
		_ = poller.Run(ctx, func(slot *disruptor.RingSlot) error {
			currentSeq := slot.Sequence.Load()
			expectedSeq := lastConsumed.Load() + 1

			assert.Equal(t, expectedSeq, currentSeq, "Out-of-order sequence detected during wrap-around")
			assert.Equal(t, int64(currentSeq), slot.ClientID, "Data torn read detected across wrap-around")

			lastConsumed.Store(currentSeq)
			totalConsumed.Inc()
			return nil
		})
	}()

	for i := 1; i <= totalItems; i++ {
		trace := [16]byte{byte(i)}
		ok := seq.Enqueue(&tg.Updates{}, trace, int64(i))
		require.True(t, ok)
	}

	assert.Eventually(t, func() bool {
		return totalConsumed.Load() == int64(totalItems)
	}, 3*time.Second, 10*time.Millisecond)
}

func TestSequencer_Saturation_Backpressure(t *testing.T) {
	t.Parallel()

	const capacity = 8
	ring, err := disruptor.NewRingBuffer(capacity)
	require.NoError(t, err)

	seq := disruptor.NewSequencer(ring, nil)

	// Fill buffer completely without consumer progress
	for i := range capacity {
		ok := seq.Enqueue(&tg.Updates{}, [16]byte{}, int64(i))
		require.True(t, ok)
	}

	// Buffer is saturated. TryNext must reject non-blocking reservations.
	_, err = seq.TryNext()
	require.ErrorIs(t, err, disruptor.ErrBufferSaturated)

	// Context-bounded EnqueueContext must apply backpressure and time out rather than shedding load.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err = seq.EnqueueContext(ctx, &tg.Updates{}, [16]byte{}, 99)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// Advance consumer gating sequence: slot becomes available and Enqueue unblocks cleanly.
	ring.GatingSequence().Set(1)
	ok := seq.Enqueue(&tg.Updates{}, [16]byte{}, 100)
	assert.True(t, ok)
}

func TestSequencer_ConcurrentProducers_Stress(t *testing.T) {
	t.Parallel()

	const capacity = 2048
	ring, err := disruptor.NewRingBuffer(capacity)
	require.NoError(t, err)

	seq := disruptor.NewSequencer(ring, nil)
	poller := disruptor.NewSingleConsumerPoller(ring, 10*time.Microsecond)

	ctx := t.Context()

	var totalConsumed atomic.Int64
	go func() {
		_ = poller.Run(ctx, func(slot *disruptor.RingSlot) error {
			assert.Positive(t, slot.Sequence.Load())
			assert.NotZero(t, slot.ClientID)
			totalConsumed.Inc()
			return nil
		})
	}()

	const producers = 20
	const itemsPerProducer = 1000
	var wg sync.WaitGroup
	wg.Add(producers)

	for p := 1; p <= producers; p++ {
		producerID := int64(p)
		go func() {
			defer wg.Done()
			for range itemsPerProducer {
				ok := seq.Enqueue(&tg.Updates{}, [16]byte{}, producerID)
				assert.True(t, ok)
			}
		}()
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return totalConsumed.Load() == int64(producers*itemsPerProducer)
	}, 5*time.Second, 20*time.Millisecond)
}

func TestPoller_HandlerError_HaltsProgress(t *testing.T) {
	t.Parallel()

	ring, err := disruptor.NewRingBuffer(16)
	require.NoError(t, err)

	seq := disruptor.NewSequencer(ring, nil)
	poller := disruptor.NewSingleConsumerPoller(ring, 10*time.Microsecond)

	for i := 1; i <= 5; i++ {
		ok := seq.Enqueue(&tg.Updates{}, [16]byte{}, int64(i))
		require.True(t, ok)
	}

	expectedErr := errors.New("handler fatal failure")
	runErr := poller.Run(context.Background(), func(slot *disruptor.RingSlot) error {
		if slot.Sequence.Load() == 3 {
			return expectedErr
		}
		return nil
	})

	require.ErrorIs(t, runErr, expectedErr)
	assert.Equal(t, uint64(2), ring.GatingSequence().Get())
}
