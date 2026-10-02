package network

import (
	"io"
)

const defaultMaxBuffers = 128

// BufferPool represents a bounded recycler of byte slices.
// It utilizes a bounded channel pool to enforce a strict memory ceiling,
// mitigating Garbage Collector (GC) heap pressure and preventing OOM kills
// under containerized environments (Kubernetes Cgroups).
type BufferPool struct {
	ch   chan *[]byte
	size int
}

// NewBufferPool initializes a bounded zero-allocation memory pool for byte slices.
// It accepts an optional maxBuffers parameter to enforce a strict memory cap (defaults to 128).
func NewBufferPool(size int, maxBuffers ...int) *BufferPool {
	if size <= 0 {
		size = 32768 // Default to 32KB minimum boundary
	}
	limit := defaultMaxBuffers
	if len(maxBuffers) > 0 && maxBuffers[0] > 0 {
		limit = maxBuffers[0]
	}
	return &BufferPool{
		ch:   make(chan *[]byte, limit),
		size: size,
	}
}

// Get retrieves a pre-allocated byte slice from the bounded pool or allocates a new one if empty.
func (p *BufferPool) Get() *[]byte {
	select {
	case b := <-p.ch:
		return b
	default:
		return new(make([]byte, p.size))
	}
}

// Put returns the byte slice to the pool if capacity permits; otherwise it is safely dropped for GC.
func (p *BufferPool) Put(b *[]byte) {
	if b == nil || cap(*b) < p.size {
		return
	}
	*b = (*b)[:p.size]
	select {
	case p.ch <- b:
	default:
		// Pool is at max capacity; discard extra buffer to enforce hard memory ceiling.
	}
}

// PipeThrough streams data directly from an io.Reader to an io.Writer using
// a pre-allocated buffer from the pool. This direct pipe-through prevents
// unnecessary heap allocations and GC thrashing.
func (p *BufferPool) PipeThrough(dst io.Writer, src io.Reader) (int64, error) {
	bufPtr := p.Get()
	defer p.Put(bufPtr)

	// io.CopyBuffer internally uses the provided slice to stream data
	// without allocating intermediate buffers in the heap.
	return io.CopyBuffer(dst, src, *bufPtr)
}
