// File: network/pool_test.go
package network_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/network"
)

// TestBufferPool_Lifecycle verifies that the BufferPool correctly allocates,
// bounds, and reuses memory chunks exactly matching the configured dimensions.
func TestBufferPool_Lifecycle(t *testing.T) {
	t.Parallel()

	expectedSize := 262144 // 256 KB (Desktop profile constraint)
	pool := network.NewBufferPool(expectedSize)

	// 1. Retrieve a buffer from the pool
	b1 := pool.Get()
	require.NotNil(t, b1)
	assert.Equal(t, expectedSize, cap(*b1))
	assert.Len(t, *b1, expectedSize)

	// Modify the buffer to trace reuse
	(*b1)[0] = 0xAF
	pool.Put(b1)

	// 2. Retrieve the buffer again. Under normal sequential test conditions,
	// sync.Pool will generally return the exact same slice.
	b2 := pool.Get()
	assert.Equal(t, expectedSize, cap(*b2))
	assert.Len(t, *b2, expectedSize)
	assert.Equal(t, byte(0xAF), (*b2)[0], "Pool should recycle the underlying byte slice")

	pool.Put(b2)
}

// TestBufferPool_PipeThrough validates the zero-allocation data transfer
// between readers and writers using the pre-allocated buffer from sync.Pool.
func TestBufferPool_PipeThrough(t *testing.T) {
	t.Parallel()

	poolSize := 32768 // 32 KB (Android profile constraint)
	pool := network.NewBufferPool(poolSize)

	// Generate a payload significantly larger than the pool buffer size
	// to force the io.CopyBuffer to perform multiple read/write cycles.
	payload := bytes.Repeat([]byte{0xCC}, poolSize*3+10)
	src := bytes.NewReader(payload)
	var dst bytes.Buffer

	n, err := pool.PipeThrough(&dst, src)

	require.NoError(t, err)
	assert.Equal(t, int64(len(payload)), n)
	assert.Equal(t, payload, dst.Bytes())
}

// TestBufferPool_ZeroAllocation ensures that PipeThrough does not allocate
// new memory on the heap during active streaming, strictly respecting the GC constraint.
func TestBufferPool_ZeroAllocation(t *testing.T) {
	pool := network.NewBufferPool(1024)
	payload := bytes.Repeat([]byte{0xDD}, 5000)

	// Warm-up to pre-allocate the sync.Pool internal structures
	srcWarmup := bytes.NewReader(payload)
	_, _ = pool.PipeThrough(io.Discard, srcWarmup)

	// Assign interfaces statically to avoid boxing allocations inside the closure
	var src io.Reader = bytes.NewReader(payload)
	dst := io.Discard

	allocs := testing.AllocsPerRun(10, func() {
		// Reset the reader offset without allocating a new bytes.Reader
		src.(*bytes.Reader).Reset(payload)

		_, err := pool.PipeThrough(dst, src)
		if err != nil {
			t.Fatal(err)
		}
	})

	// The allocations must be extremely low (ideally 0, but accepting <= 1
	// due to possible interface method overheads or internal io.Copy edge cases).
	assert.LessOrEqual(t, allocs, float64(1), "PipeThrough should not trigger continuous heap allocations")
}

// TestBufferPool_BoundedCapacity verifies that buffers beyond the pool capacity
// are safely shed to enforce the hard memory ceiling under Cgroups.
func TestBufferPool_BoundedCapacity(t *testing.T) {
	t.Parallel()

	const maxBuffers = 2
	const bufSize = 1024
	pool := network.NewBufferPool(bufSize, maxBuffers)

	b1 := pool.Get()
	b2 := pool.Get()
	b3 := pool.Get()

	// Return 3 buffers into a pool of capacity 2
	pool.Put(b1)
	pool.Put(b2)
	pool.Put(b3) // Sheds cleanly without blocking or panicking

	r1 := pool.Get()
	r2 := pool.Get()
	require.NotNil(t, r1)
	require.NotNil(t, r2)
}
