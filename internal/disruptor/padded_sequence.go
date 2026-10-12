package disruptor

import (
	"go.uber.org/atomic"
)

// PaddedSequence represents an isolated atomic 64-bit sequence counter.
// It is surrounded by 56 bytes of padding before and after to ensure the
// active atomic value never shares a 64-byte or 128-byte cache line with
// adjacent memory addresses, avoiding CPU cache-coherency bus storms.
type PaddedSequence struct {
	_   [7]uint64
	val atomic.Uint64
	_   [7]uint64
}

// NewPaddedSequence instantiates a PaddedSequence initialized with initialValue.
func NewPaddedSequence(initialValue uint64) PaddedSequence {
	var seq PaddedSequence
	seq.val.Store(initialValue)
	return seq
}

// Get atomically loads the sequence value.
func (s *PaddedSequence) Get() uint64 {
	return s.val.Load()
}

// Set atomically stores the sequence value.
func (s *PaddedSequence) Set(v uint64) {
	s.val.Store(v)
}

// CompareAndSwap atomically updates the sequence if current equals expected.
func (s *PaddedSequence) CompareAndSwap(expected, newVal uint64) bool {
	return s.val.CompareAndSwap(expected, newVal)
}

// Add atomically increments the sequence by delta and returns the new value.
func (s *PaddedSequence) Add(delta uint64) uint64 {
	return s.val.Add(delta)
}

// Inc atomically increments the sequence by 1 and returns the new value.
func (s *PaddedSequence) Inc() uint64 {
	return s.val.Inc()
}
