package syncstate

import (
	"context"
	"sync"
	"time"

	"go.uber.org/atomic"
)

// DefaultFlushInterval defines the periodic disk checkpoint duration (50ms parity with TDLib).
const DefaultFlushInterval = 50 * time.Millisecond

// DefaultDirtyThreshold defines the number of updates triggering an early asynchronous disk flush.
const DefaultDirtyThreshold uint64 = 500

// StateSnapshot captures an immutable point-in-time state of the sequence counters.
type StateSnapshot struct {
	PTS  int32
	QTS  int32
	Date int32
	Seq  int32
}

// StateStorage represents the persistence driver contract for committing vector checkpoints
// and channel state tracking, supporting multi-tenant isolation via clientID.
type StateStorage interface {
	SaveState(ctx context.Context, clientID int64, snapshot StateSnapshot) error
	LoadState(ctx context.Context, clientID int64) (snapshot StateSnapshot, found bool, err error)
	SaveChannelPTS(ctx context.Context, clientID, channelID int64, pts int32) error
	LoadChannelPTS(ctx context.Context, clientID, channelID int64) (pts int32, found bool, err error)
	ForEachChannelPTS(ctx context.Context, clientID int64, fn func(channelID int64, pts int32) error) error
	SetDialogNotLoaded(ctx context.Context, clientID, dialogID int64, notLoaded bool) error
	IsDialogNotLoaded(ctx context.Context, clientID, dialogID int64) (bool, error)
}

// StateVector encapsulates atomic sequence registers for MTProto state management.
type StateVector struct {
	pts  atomic.Int32
	qts  atomic.Int32
	date atomic.Int32
	seq  atomic.Int32
}

// NewStateVector instantiates a StateVector initialized from an initial snapshot.
func NewStateVector(initial StateSnapshot) *StateVector {
	v := &StateVector{}
	v.pts.Store(initial.PTS)
	v.qts.Store(initial.QTS)
	v.date.Store(initial.Date)
	v.seq.Store(initial.Seq)
	return v
}

// PTS returns the current local PTS counter.
func (s *StateVector) PTS() int32 {
	return s.pts.Load()
}

// SetPTS atomically updates the local PTS counter.
func (s *StateVector) SetPTS(val int32) {
	s.pts.Store(val)
}

// QTS returns the current local QTS counter.
func (s *StateVector) QTS() int32 {
	return s.qts.Load()
}

// SetQTS atomically updates the local QTS counter.
func (s *StateVector) SetQTS(val int32) {
	s.qts.Store(val)
}

// Date returns the current global date timestamp.
func (s *StateVector) Date() int32 {
	return s.date.Load()
}

// SetDate atomically sets the global date timestamp.
func (s *StateVector) SetDate(val int32) {
	s.date.Store(val)
}

// UpdateDate monotonically advances the date timestamp if val is strictly greater.
func (s *StateVector) UpdateDate(val int32) bool {
	for {
		curr := s.date.Load()
		if val <= curr {
			return false
		}
		if s.date.CompareAndSwap(curr, val) {
			return true
		}
	}
}

// Seq returns the current local sequence number.
func (s *StateVector) Seq() int32 {
	return s.seq.Load()
}

// SetSeq atomically updates the local sequence number.
func (s *StateVector) SetSeq(val int32) {
	s.seq.Store(val)
}

// Snapshot returns an immutable point-in-time copy of all four state counters.
func (s *StateVector) Snapshot() StateSnapshot {
	return StateSnapshot{
		PTS:  s.pts.Load(),
		QTS:  s.qts.Load(),
		Date: s.date.Load(),
		Seq:  s.seq.Load(),
	}
}

// CheckpointFlusher governs periodic and synchronous disk flushes to prevent fsync bottlenecks.
type CheckpointFlusher struct {
	clientID       int64
	vector         *StateVector
	sink           StateStorage
	interval       time.Duration
	dirtyThreshold uint64
	dirtyCounter   atomic.Uint64
	lastFlushed    StateSnapshot
	mu             sync.Mutex
	done           chan struct{}
}

// NewCheckpointFlusher constructs a flusher bound to a StateVector and StateStorage.
func NewCheckpointFlusher(clientID int64, vector *StateVector, sink StateStorage, interval time.Duration) *CheckpointFlusher {
	if interval <= 0 {
		interval = DefaultFlushInterval
	}
	return &CheckpointFlusher{
		clientID:       clientID,
		vector:         vector,
		sink:           sink,
		interval:       interval,
		dirtyThreshold: DefaultDirtyThreshold,
		lastFlushed:    vector.Snapshot(),
		done:           make(chan struct{}),
	}
}

// MarkDirty increments the count of uncommitted in-memory updates.
func (c *CheckpointFlusher) MarkDirty(count uint64) {
	c.dirtyCounter.Add(count)
}

// Flush synchronously writes the current vector snapshot to the persistence sink.
func (c *CheckpointFlusher) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	current := c.vector.Snapshot()
	if current == c.lastFlushed && c.dirtyCounter.Load() == 0 {
		return nil
	}

	if err := c.sink.SaveState(ctx, c.clientID, current); err != nil {
		return err
	}

	c.lastFlushed = current
	c.dirtyCounter.Store(0)
	return nil
}

// Start launches the background flusher loop until ctx is canceled or Close is called.
func (c *CheckpointFlusher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				_ = c.Flush(context.Background())
				return
			case <-c.done:
				_ = c.Flush(context.Background())
				return
			case <-ticker.C:
				if c.dirtyCounter.Load() > 0 {
					_ = c.Flush(ctx)
				}
			}
		}
	}()
}

// Close gracefully terminates the flusher background loop and forces a final flush.
func (c *CheckpointFlusher) Close() error {
	select {
	case <-c.done:
		return nil
	default:
		close(c.done)
	}
	return c.Flush(context.Background())
}
