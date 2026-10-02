package dispatcher_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/dispatcher"
)

// TestShard_OrderPreservation verifies that updates are processed in strict FIFO order.
func TestShard_OrderPreservation(t *testing.T) {
	ctx := t.Context()

	var processed []string
	var mu sync.Mutex

	// Worker callback that records execution order
	handle := func(_ int, u string) {
		mu.Lock()
		processed = append(processed, u)
		mu.Unlock()
	}

	// Create a shard with a reasonable buffer capacity
	shard := dispatcher.NewShard[int, string](10, nil, handle)
	shard.Start(ctx)

	// Push updates to mailbox
	require.True(t, shard.Push(1, "message_A"))
	require.True(t, shard.Push(2, "message_B"))
	require.True(t, shard.Push(3, "message_C"))

	// Wait for background worker to consume all items
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(processed) == 3
	}, 1*time.Second, 10*time.Millisecond)

	assert.Equal(t, []string{"message_A", "message_B", "message_C"}, processed)
}

// TestShard_LoadShedding verifies that enqueuing past the buffer limit drops
// messages immediately and increments the dropped counter without blocking.
func TestShard_LoadShedding(t *testing.T) {
	ctx := t.Context()

	blockCh := make(chan struct{})
	var processedCount int32
	var mu sync.Mutex

	// This worker blocks on the first message, simulating high-latency processing
	handle := func(_ int, _ string) {
		mu.Lock()
		processedCount++
		mu.Unlock()
		<-blockCh
	}

	// Bounded queue with exactly 1 buffer slot
	shard := dispatcher.NewShard[int, string](1, nil, handle)
	shard.Start(ctx)

	// 1st message goes straight to the active worker and blocks it
	require.True(t, shard.Push(1, "blocking_msg"))

	// Wait briefly to guarantee the worker has taken the 1st message and is blocked
	time.Sleep(20 * time.Millisecond)

	// 2nd message fits cleanly into the 1-slot buffer channel
	require.True(t, shard.Push(2, "buffered_msg"))

	// 3rd & 4th messages must fail to enqueue and trigger load shedding (non-blocking)
	require.False(t, shard.Push(3, "dropped_msg_1"))
	require.False(t, shard.Push(4, "dropped_msg_2"))

	// Assertions on dropped stats
	assert.Equal(t, uint64(2), shard.DroppedCount())

	// Unblock the worker loop to perform clean teardown
	close(blockCh)
}
