package syncstate_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/syncstate"
)

type mockStorageSink struct {
	mu    sync.Mutex
	saves []syncstate.StateSnapshot
}

func (m *mockStorageSink) SaveState(_ context.Context, _ int64, snapshot syncstate.StateSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves = append(m.saves, snapshot)
	return nil
}

func (*mockStorageSink) LoadState(_ context.Context, _ int64) (syncstate.StateSnapshot, bool, error) {
	return syncstate.StateSnapshot{}, false, nil
}

func (*mockStorageSink) SaveChannelPTS(_ context.Context, _, _ int64, _ int32) error {
	return nil
}

func (*mockStorageSink) LoadChannelPTS(_ context.Context, _, _ int64) (int32, bool, error) {
	return 0, false, nil
}

func (*mockStorageSink) ForEachChannelPTS(_ context.Context, _ int64, _ func(channelID int64, pts int32) error) error {
	return nil
}

func (*mockStorageSink) SetDialogNotLoaded(_ context.Context, _, _ int64, _ bool) error { return nil }

func (*mockStorageSink) IsDialogNotLoaded(_ context.Context, _, _ int64) (bool, error) {
	return false, nil
}

func (m *mockStorageSink) LastSave() (syncstate.StateSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saves) == 0 {
		return syncstate.StateSnapshot{}, false
	}
	return m.saves[len(m.saves)-1], true
}

func TestStateVector_AtomicOperations(t *testing.T) {
	t.Parallel()

	initSnap := syncstate.StateSnapshot{PTS: 100, QTS: 50, Date: 1000, Seq: 10}
	vec := syncstate.NewStateVector(initSnap)

	assert.Equal(t, int32(100), vec.PTS())
	assert.Equal(t, int32(50), vec.QTS())
	assert.Equal(t, int32(1000), vec.Date())
	assert.Equal(t, int32(10), vec.Seq())

	vec.SetPTS(105)
	vec.SetQTS(55)
	vec.SetSeq(12)

	assert.False(t, vec.UpdateDate(999), "Older date must not update current date")
	assert.True(t, vec.UpdateDate(1050), "Newer date must update current date")

	snap := vec.Snapshot()
	assert.Equal(t, int32(105), snap.PTS)
	assert.Equal(t, int32(55), snap.QTS)
	assert.Equal(t, int32(1050), snap.Date)
	assert.Equal(t, int32(12), snap.Seq)
}

func TestCheckpointFlusher_SynchronousAndPeriodic(t *testing.T) {
	t.Parallel()

	sink := &mockStorageSink{}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 1})
	flusher := syncstate.NewCheckpointFlusher(1, vec, sink, 20*time.Millisecond)

	ctx := t.Context()
	flusher.Start(ctx)

	// In-memory update without manual flush
	vec.SetPTS(50)
	flusher.MarkDirty(1)

	// Periodic flusher should commit state automatically
	assert.Eventually(t, func() bool {
		last, ok := sink.LastSave()
		return ok && last.PTS == 50
	}, 1*time.Second, 10*time.Millisecond)

	// Synchronous flush at transaction boundary
	vec.SetPTS(100)
	flusher.MarkDirty(1)
	err := flusher.Flush(context.Background())
	require.NoError(t, err)

	last, ok := sink.LastSave()
	require.True(t, ok)
	assert.Equal(t, int32(100), last.PTS)

	require.NoError(t, flusher.Close())
}

func TestCheckGap_Matrix(t *testing.T) {
	t.Parallel()

	// 1. Direct apply: localPTS (10) + count (1) == remotePTS (11)
	assert.Equal(t, syncstate.GapApply, syncstate.CheckGap(10, 11, 1))

	// 2. Dummy update: remotePTS <= 0
	assert.Equal(t, syncstate.GapApply, syncstate.CheckGap(10, 0, 0))

	// 3. Stale update: remotePTS <= localPTS
	assert.Equal(t, syncstate.GapIgnore, syncstate.CheckGap(10, 10, 1))
	assert.Equal(t, syncstate.GapIgnore, syncstate.CheckGap(10, 9, 1))

	// 4. Gap detected: remotePTS (15) > localPTS (10) + count (1)
	assert.Equal(t, syncstate.GapRefetch, syncstate.CheckGap(10, 15, 1))
}

func TestGapBuffer_QueueingAndConsecutiveDrain(t *testing.T) {
	t.Parallel()

	var shortGapCalled atomic.Bool
	var timeoutCalled atomic.Bool

	buf := syncstate.NewGapBuffer(
		func(_ int32) { shortGapCalled.Store(true) },
		func() { timeoutCalled.Store(true) },
	)

	// Current PTS = 10. Update arrives with PTS = 12, count = 1 (single gap at 11).
	upd12 := syncstate.PendingUpdate{PTS: 12, PTSCount: 1, ReceiveTime: time.Now()}
	buf.Push(upd12, 10)
	assert.Equal(t, 1, buf.Len())

	// Consecutive drain cannot drain yet because 11 is missing
	drained, cursor := buf.PopConsecutive(10)
	assert.Empty(t, drained)
	assert.Equal(t, int32(10), cursor)

	// Missing update 11 arrives (PTS = 11, count = 1).
	upd11 := syncstate.PendingUpdate{PTS: 11, PTSCount: 1, ReceiveTime: time.Now()}
	buf.Push(upd11, 10)
	assert.Equal(t, 2, buf.Len())

	// Now both 11 and 12 can be drained consecutively
	drained, cursor = buf.PopConsecutive(10)
	require.Len(t, drained, 2)
	assert.Equal(t, int32(11), drained[0].PTS)
	assert.Equal(t, int32(12), drained[1].PTS)
	assert.Equal(t, int32(12), cursor)
	assert.Equal(t, 0, buf.Len())

	buf.Clear()
}

func TestGapBuffer_ThresholdTimeoutTrigger(t *testing.T) {
	t.Parallel()

	timeoutTriggered := make(chan struct{}, 1)
	buf := syncstate.NewGapBuffer(
		nil,
		func() {
			select {
			case timeoutTriggered <- struct{}{}:
			default:
			}
		},
	)

	// Push 20 out-of-order updates to hit threshold
	for i := int32(1); i <= int32(syncstate.MaxPendingUpdatesThreshold); i++ {
		buf.Push(syncstate.PendingUpdate{PTS: 100 + i, PTSCount: 1}, 10)
	}

	select {
	case <-timeoutTriggered:
		// Threshold reached, difference recovery triggered
	case <-time.After(1 * time.Second):
		t.Fatal("Expected gap threshold to trigger timeout callback immediately")
	}

	buf.Clear()
}
