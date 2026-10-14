package channelpool_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/channelpool"
)

type mockChannelAPI struct {
	mu        sync.Mutex
	responses map[int64][]tg.UpdatesChannelDifferenceClass
	calls     atomic.Int64
}

func newMockChannelAPI() *mockChannelAPI {
	return &mockChannelAPI{
		responses: make(map[int64][]tg.UpdatesChannelDifferenceClass),
	}
}

func (m *mockChannelAPI) SetResponse(channelID int64, res ...tg.UpdatesChannelDifferenceClass) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[channelID] = res
}

func (m *mockChannelAPI) UpdatesGetChannelDifference(_ context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
	m.calls.Inc()
	m.mu.Lock()
	defer m.mu.Unlock()

	ch, ok := req.Channel.(*tg.InputChannel)
	if !ok {
		return nil, errors.New("invalid channel type")
	}

	resList := m.responses[ch.ChannelID]
	if len(resList) == 0 {
		return &tg.UpdatesChannelDifferenceEmpty{Pts: req.Pts, Final: true}, nil
	}

	resp := resList[0]
	m.responses[ch.ChannelID] = resList[1:]
	return resp, nil
}

func TestPriorityQueue_DynamicPromotionInPlace(t *testing.T) {
	t.Parallel()

	pq := channelpool.NewPriorityQueue()

	task := &channelpool.ChannelSyncTask{
		ChannelID: 555, LocalPTS: 10, RemotePTS: 12, Muted: true, Tier: channelpool.TierBacklog,
	}
	pq.PushOrUpdate(task)

	other := &channelpool.ChannelSyncTask{
		ChannelID: 777, LocalPTS: 10, RemotePTS: 20, Muted: false, Tier: channelpool.TierBacklog,
	}
	pq.PushOrUpdate(other)

	promoted := &channelpool.ChannelSyncTask{
		ChannelID: 555, LocalPTS: 10, RemotePTS: 500, Muted: false, Tier: channelpool.TierActive, Pinned: true,
	}
	pq.PushOrUpdate(promoted)

	assert.Equal(t, 2, pq.Len())

	top, ok := pq.Pop()
	require.True(t, ok)
	assert.Equal(t, int64(555), top.ChannelID)
	assert.Equal(t, int32(500), top.RemotePTS)

	next, ok := pq.Pop()
	require.True(t, ok)
	assert.Equal(t, int64(777), next.ChannelID)
}

func TestWorkerPool_ConcurrencyFloorAndCeiling(t *testing.T) {
	t.Parallel()

	api := newMockChannelAPI()

	// Zero or negative input must clamp to at least MinConcurrency (8) and at most MaxConcurrency (16)
	wpDefault := channelpool.NewWorkerPool(api, 0, nil, nil, nil)
	assert.GreaterOrEqual(t, wpDefault.Concurrency(), channelpool.MinConcurrency)
	assert.LessOrEqual(t, wpDefault.Concurrency(), channelpool.MaxConcurrency)

	// Sub-floor input (e.g. 2) must be clamped up to 8
	wpFloor := channelpool.NewWorkerPool(api, 2, nil, nil, nil)
	assert.Equal(t, channelpool.MinConcurrency, wpFloor.Concurrency(), "Worker pool must enforce floor of 8")

	// Excessive input (e.g. 100) must be clamped down to 16
	wpCeil := channelpool.NewWorkerPool(api, 100, nil, nil, nil)
	assert.Equal(t, channelpool.MaxConcurrency, wpCeil.Concurrency(), "Worker pool must enforce ceiling of 16")
}

func TestWorkerPool_ThunderingHerd_500ChannelsStress(t *testing.T) {
	t.Parallel()

	const maxWorkers = channelpool.MinConcurrency // 8
	const totalChannels = 500

	api := newMockChannelAPI()
	var activeWorkers atomic.Int32
	var maxObserved atomic.Int32
	var processedTotal atomic.Int64
	var activeProcessedFirst atomic.Int64

	// Pre-populate all responses thread-safely before starting workers to eliminate map race
	for i := 1; i <= totalChannels; i++ {
		api.SetResponse(int64(i), &tg.UpdatesChannelDifference{Pts: 100, Final: true})
	}

	wp := channelpool.NewWorkerPool(
		api,
		maxWorkers,
		func(batch channelpool.ChannelDifferenceBatch) error {
			curr := activeWorkers.Inc()
			if curr > maxObserved.Load() {
				maxObserved.Store(curr)
			}
			time.Sleep(1 * time.Millisecond)
			activeWorkers.Dec()

			if batch.ChannelID <= 50 {
				activeProcessedFirst.Inc()
			}
			processedTotal.Inc()
			return nil
		},
		nil,
		nil,
	)

	ctx := t.Context()
	wp.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(totalChannels)

	for i := 1; i <= totalChannels; i++ {
		chID := int64(i)
		go func() {
			defer wg.Done()
			tier := channelpool.TierBacklog
			muted := true
			if chID <= 50 {
				tier = channelpool.TierActive
				muted = false
			}
			wp.Submit(&channelpool.ChannelSyncTask{
				ChannelID: chID,
				LocalPTS:  10,
				RemotePTS: 100,
				Tier:      tier,
				Muted:     muted,
			})
		}()
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return processedTotal.Load() == totalChannels
	}, 5*time.Second, 20*time.Millisecond)

	assert.LessOrEqual(t, maxObserved.Load(), int32(maxWorkers), "Concurrency limit must never be violated")
	assert.Equal(t, int64(50), activeProcessedFirst.Load(), "All priority channels must be processed")
	wp.Stop()
}

func TestWorkerPool_MultiSlice_PaginationLoop(t *testing.T) {
	t.Parallel()

	api := newMockChannelAPI()
	const targetChannelID = int64(999)

	api.SetResponse(targetChannelID,
		&tg.UpdatesChannelDifference{
			Pts:   150,
			Final: false,
			NewMessages: []tg.MessageClass{
				&tg.Message{ID: 1},
			},
		},
		&tg.UpdatesChannelDifference{
			Pts:   200,
			Final: false,
			NewMessages: []tg.MessageClass{
				&tg.Message{ID: 2},
			},
		},
		&tg.UpdatesChannelDifference{
			Pts:   250,
			Final: true,
			NewMessages: []tg.MessageClass{
				&tg.Message{ID: 3},
			},
		},
	)

	var collectedPTS []int32
	var mu sync.Mutex

	wp := channelpool.NewWorkerPool(
		api,
		channelpool.MinConcurrency,
		func(batch channelpool.ChannelDifferenceBatch) error {
			mu.Lock()
			collectedPTS = append(collectedPTS, batch.NewPTS)
			mu.Unlock()
			return nil
		},
		nil,
		nil,
	)

	ctx := t.Context()
	wp.Start(ctx)

	wp.Submit(&channelpool.ChannelSyncTask{
		ChannelID: targetChannelID,
		LocalPTS:  100,
		RemotePTS: 250,
		Tier:      channelpool.TierActive,
	})

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(collectedPTS) == 3
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []int32{150, 200, 250}, collectedPTS, "Slices must be processed in consecutive progression")
	mu.Unlock()

	wp.Stop()
}

func TestWorkerPool_TooLong_HaltsLoop(t *testing.T) {
	t.Parallel()

	api := newMockChannelAPI()
	const targetChannelID = int64(404)

	dialog := &tg.Dialog{Pts: 88888}
	dialog.SetPts(88888)

	api.SetResponse(targetChannelID, &tg.UpdatesChannelDifferenceTooLong{
		Dialog: dialog,
	})

	var tooLongReported atomic.Int32
	wp := channelpool.NewWorkerPool(
		api,
		channelpool.MinConcurrency,
		nil,
		nil,
		func(channelID int64, newPTS int32) {
			if channelID == targetChannelID {
				tooLongReported.Store(newPTS)
			}
		},
	)

	ctx := t.Context()
	wp.Start(ctx)

	wp.Submit(&channelpool.ChannelSyncTask{
		ChannelID: targetChannelID,
		LocalPTS:  10,
		RemotePTS: 100,
	})

	assert.Eventually(t, func() bool {
		return tooLongReported.Load() == 88888
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, int64(1), api.calls.Load())
	wp.Stop()
}

func TestWorkerPool_Inaccessible_ErrorClassification(t *testing.T) {
	t.Parallel()

	errorCases := []string{"CHANNEL_PRIVATE", "CHANNEL_PUBLIC_GROUP_NA"}

	for _, errCode := range errorCases {
		t.Run(errCode, func(t *testing.T) {
			t.Parallel()

			var reported atomic.Int64
			failingAPI := &failingAPI{err: tgerr.New(400, errCode)}

			wp := channelpool.NewWorkerPool(
				failingAPI,
				channelpool.MinConcurrency,
				nil,
				func(channelID int64) {
					reported.Store(channelID)
				},
				nil,
			)

			ctx := t.Context()
			wp.Start(ctx)

			wp.Submit(&channelpool.ChannelSyncTask{ChannelID: 1234})

			assert.Eventually(t, func() bool {
				return reported.Load() == 1234
			}, 1*time.Second, 10*time.Millisecond)

			wp.Stop()
		})
	}
}

type failingAPI struct {
	err error
}

func (f *failingAPI) UpdatesGetChannelDifference(_ context.Context, _ *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
	return nil, f.err
}

func TestWorkerPool_GracefulShutdown(t *testing.T) {
	t.Parallel()

	api := newMockChannelAPI()
	wp := channelpool.NewWorkerPool(api, channelpool.MinConcurrency, nil, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	wp.Start(ctx)

	for i := 1; i <= 20; i++ {
		wp.Submit(&channelpool.ChannelSyncTask{ChannelID: int64(i)})
	}

	shutdownDone := make(chan struct{})
	go func() {
		cancel()
		wp.Stop()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		// Cleanly terminated without deadlocks or leaked workers
	case <-time.After(1 * time.Second):
		t.Fatal("Worker pool failed to shut down within deadline")
	}
}
