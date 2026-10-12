package syncstate_test

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

	"github.com/mr-dariush/tgbox/internal/syncstate"
)

type mockDifferenceAPI struct {
	mu        sync.Mutex
	responses []tg.UpdatesDifferenceClass
	calls     int
	latency   time.Duration
}

func (m *mockDifferenceAPI) UpdatesGetDifference(_ context.Context, _ *tg.UpdatesGetDifferenceRequest) (tg.UpdatesDifferenceClass, error) {
	if m.latency > 0 {
		time.Sleep(m.latency)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.calls
	m.calls++
	if idx >= len(m.responses) {
		return &tg.UpdatesDifferenceEmpty{Date: 2000, Seq: 100}, nil
	}
	return m.responses[idx], nil
}

func (m *mockDifferenceAPI) Calls() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

type failingMockDifferenceAPI struct {
	mu        sync.Mutex
	responses []tg.UpdatesDifferenceClass
	calls     int
	failAfter int
}

func (m *failingMockDifferenceAPI) UpdatesGetDifference(_ context.Context, _ *tg.UpdatesGetDifferenceRequest) (tg.UpdatesDifferenceClass, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls >= m.failAfter {
		return nil, errors.New("simulated TCP connection drop mid-pagination")
	}
	idx := m.calls
	m.calls++
	return m.responses[idx], nil
}

type mockDifferenceSink struct {
	mu    sync.Mutex
	saves []syncstate.StateSnapshot
}

func (m *mockDifferenceSink) SaveState(_ context.Context, _ int64, snapshot syncstate.StateSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves = append(m.saves, snapshot)
	return nil
}

func (*mockDifferenceSink) LoadState(_ context.Context, _ int64) (syncstate.StateSnapshot, bool, error) {
	return syncstate.StateSnapshot{}, false, nil
}

func (*mockDifferenceSink) SaveChannelPTS(_ context.Context, _, _ int64, _ int32) error {
	return nil
}

func (*mockDifferenceSink) LoadChannelPTS(_ context.Context, _, _ int64) (int32, bool, error) {
	return 0, false, nil
}

func (*mockDifferenceSink) ForEachChannelPTS(_ context.Context, _ int64, _ func(channelID int64, pts int32) error) error {
	return nil
}

func (*mockDifferenceSink) SetDialogNotLoaded(_ context.Context, _, _ int64, _ bool) error {
	return nil
}

func (*mockDifferenceSink) IsDialogNotLoaded(_ context.Context, _, _ int64) (bool, error) {
	return false, nil
}

func (m *mockDifferenceSink) LastSave() (syncstate.StateSnapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.saves) == 0 {
		return syncstate.StateSnapshot{}, false
	}
	return m.saves[len(m.saves)-1], true
}

func (m *mockDifferenceSink) SaveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.saves)
}

func TestDifferenceFetcher_EmptyDifference(t *testing.T) {
	t.Parallel()

	api := &mockDifferenceAPI{
		responses: []tg.UpdatesDifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 1500, Seq: 42},
		},
	}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 10, Date: 1000})
	fetcher := syncstate.NewDifferenceFetcher(api, vec, nil, nil, nil)

	err := fetcher.FetchDifference(context.Background(), nil)
	require.NoError(t, err)

	assert.Equal(t, int32(1500), vec.Date())
	assert.Equal(t, int32(42), vec.Seq())
	assert.Equal(t, int32(10), vec.PTS())
}

func TestDifferenceFetcher_SlicePaginationAndCommit(t *testing.T) {
	t.Parallel()

	sink := &mockDifferenceSink{}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 10, QTS: 1})
	flusher := syncstate.NewCheckpointFlusher(1, vec, sink, 0)

	api := &mockDifferenceAPI{
		responses: []tg.UpdatesDifferenceClass{
			&tg.UpdatesDifferenceSlice{
				NewMessages: []tg.MessageClass{&tg.Message{ID: 101}},
				IntermediateState: tg.UpdatesState{
					Pts: 20, Qts: 5, Date: 1100, Seq: 50,
				},
			},
			&tg.UpdatesDifference{
				NewMessages: []tg.MessageClass{&tg.Message{ID: 102}},
				State: tg.UpdatesState{
					Pts: 30, Qts: 10, Date: 1200, Seq: 60,
				},
			},
		},
	}

	fetcher := syncstate.NewDifferenceFetcher(api, vec, flusher, nil, nil)

	var batchesReceived int
	err := fetcher.FetchDifference(context.Background(), func(batch syncstate.DifferenceBatch) error {
		batchesReceived++
		assert.Len(t, batch.Messages, 1)
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, batchesReceived, "Expected two batches to be processed sequentially")

	assert.Equal(t, int32(30), vec.PTS())
	assert.Equal(t, int32(10), vec.QTS())
	assert.Equal(t, int32(1200), vec.Date())

	// Streaming WAL guarantees that disk was flushed exactly ONCE per slice (2 times total)
	assert.Equal(t, 2, sink.SaveCount(), "Disk must be flushed once for the slice and once for the final block")
	lastSave, ok := sink.LastSave()
	require.True(t, ok)
	assert.Equal(t, int32(30), lastSave.PTS)
}

func TestDifferenceFetcher_NetworkDrop_PreservesIntermediateCheckpoint(t *testing.T) {
	t.Parallel()

	sink := &mockDifferenceSink{}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 10, QTS: 1})
	flusher := syncstate.NewCheckpointFlusher(1, vec, sink, 0)

	api := &failingMockDifferenceAPI{
		responses: []tg.UpdatesDifferenceClass{
			&tg.UpdatesDifferenceSlice{
				NewMessages: []tg.MessageClass{&tg.Message{ID: 101}},
				IntermediateState: tg.UpdatesState{
					Pts: 20, Qts: 5, Date: 1100, Seq: 50,
				},
			},
		},
		failAfter: 1, // Drops connection on second iteration before reaching final
	}

	fetcher := syncstate.NewDifferenceFetcher(api, vec, flusher, nil, nil)
	err := fetcher.FetchDifference(context.Background(), func(_ syncstate.DifferenceBatch) error { return nil })
	require.Error(t, err)

	// Memory vector and disk sink MUST advance to the intermediate checkpoint (PTS=20)
	assert.Equal(t, int32(20), vec.PTS(), "PTS in memory must advance and save progress up to the successful slice")
	assert.Equal(t, 1, sink.SaveCount(), "Disk sink must have exactly 1 commit representing the successful intermediate slice")
}

func TestDifferenceFetcher_DifferenceTooLong(t *testing.T) {
	t.Parallel()

	buf := syncstate.NewGapBuffer(nil, nil)
	buf.Push(syncstate.PendingUpdate{PTS: 15, PTSCount: 1}, 10)
	assert.Equal(t, 1, buf.Len())

	var tooLongTriggered atomic.Bool
	api := &mockDifferenceAPI{
		responses: []tg.UpdatesDifferenceClass{
			&tg.UpdatesDifferenceTooLong{Pts: 5000},
		},
	}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 10})
	fetcher := syncstate.NewDifferenceFetcher(api, vec, nil, buf, func() {
		tooLongTriggered.Store(true)
	})

	err := fetcher.FetchDifference(context.Background(), nil)
	require.NoError(t, err)

	assert.True(t, tooLongTriggered.Load(), "Callback onTooLong must be triggered")
	assert.Equal(t, int32(5000), vec.PTS(), "Local PTS must jump to remote PTS")
	assert.Equal(t, 0, buf.Len(), "Gap buffer must be wiped to prevent stale update collision")
}

func TestDifferenceFetcher_ConcurrentDeduplication(t *testing.T) {
	t.Parallel()

	api := &mockDifferenceAPI{
		latency: 20 * time.Millisecond,
		responses: []tg.UpdatesDifferenceClass{
			&tg.UpdatesDifferenceEmpty{Date: 100},
		},
	}
	vec := syncstate.NewStateVector(syncstate.StateSnapshot{PTS: 1})
	fetcher := syncstate.NewDifferenceFetcher(api, vec, nil, nil, nil)

	var wg sync.WaitGroup
	const callers = 10
	wg.Add(callers)

	for range callers {
		go func() {
			defer wg.Done()
			_ = fetcher.FetchDifference(context.Background(), nil)
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, api.Calls(), "Concurrent calls must be deduplicated to a single execution via singleflight")
}
