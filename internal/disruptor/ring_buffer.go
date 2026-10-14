package disruptor

import (
	"errors"

	"github.com/gotd/td/tg"
	"go.uber.org/atomic"
)

// DefaultCapacity defines the baseline ring buffer size (65536 slots).
const DefaultCapacity uint64 = 1 << 16

// ErrInvalidCapacity is returned when the requested ring buffer capacity is not a power of two.
var ErrInvalidCapacity = errors.New("disruptor: ring buffer capacity must be a positive power of two")

// RingSlot represents an in-memory pre-allocated container for an update payload.
// Sequence is an atomic counter indicating the exact sequence committed into this slot.
type RingSlot struct {
	Sequence atomic.Uint64
	RawClass tg.UpdatesClass
	TraceID  [16]byte
	ClientID int64
	Received int64
}

// RingBuffer implements the memory storage layer of the LMAX Disruptor pattern.
// All sequence pointers are cache-line isolated to allow parallel execution.
type RingBuffer struct {
	_         [7]uint64
	capacity  uint64
	mask      uint64
	_         [7]uint64
	slots     []RingSlot
	_         [7]uint64
	cursor    PaddedSequence // Sequence claimed by producer(s).
	_         [7]uint64
	published PaddedSequence // Sequence fully published and ready for consumption.
	_         [7]uint64
	gatingSeq PaddedSequence // Minimum sequence processed by consumer(s).
}

// NewRingBuffer constructs an aligned RingBuffer with the specified power-of-two capacity.
// If capacity is 0, DefaultCapacity (65536) is applied.
func NewRingBuffer(capacity uint64) (*RingBuffer, error) {
	if capacity == 0 {
		capacity = DefaultCapacity
	}

	// Validate capacity is a strict power of two: (C & (C - 1)) == 0.
	if capacity&(capacity-1) != 0 {
		return nil, ErrInvalidCapacity
	}

	return &RingBuffer{
		capacity:  capacity,
		mask:      capacity - 1,
		slots:     make([]RingSlot, capacity),
		cursor:    NewPaddedSequence(0),
		published: NewPaddedSequence(0),
		gatingSeq: NewPaddedSequence(0),
	}, nil
}

// Capacity returns the total number of pre-allocated slots in the buffer.
func (r *RingBuffer) Capacity() uint64 {
	return r.capacity
}

// Mask returns the bitwise index mask (capacity - 1).
func (r *RingBuffer) Mask() uint64 {
	return r.mask
}

// Slot returns a direct mutable pointer to the pre-allocated slot for the sequence.
// Indexing is executed via bitwise AND with the power-of-two mask.
func (r *RingBuffer) Slot(seq uint64) *RingSlot {
	return &r.slots[seq&r.mask]
}

// Cursor returns the sequencer cursor pointing to the latest claimed sequence.
func (r *RingBuffer) Cursor() *PaddedSequence {
	return &r.cursor
}

// Published returns the pointer indicating updates committed and visible to readers.
func (r *RingBuffer) Published() *PaddedSequence {
	return &r.published
}

// GatingSequence returns the gating sequence tracker representing consumer progress.
func (r *RingBuffer) GatingSequence() *PaddedSequence {
	return &r.gatingSeq
}
