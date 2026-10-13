package tgbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/channelpool"
	"github.com/mr-dariush/tgbox/internal/dispatcher"
	"github.com/mr-dariush/tgbox/internal/disruptor"
	tgboxstorage "github.com/mr-dariush/tgbox/internal/storage"
	"github.com/mr-dariush/tgbox/internal/syncstate"
)

func TestPipeline_EndToEnd_ConcurrentChaos_Ordering(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "chaos_ordering")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	const numChats = 10
	const msgsPerChat = 500
	const totalMsgs = numChats * msgsPerChat

	var mu sync.Mutex
	chatProgress := make(map[int64]int)
	var totalConsumed atomic.Int64

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		require.NotNil(t, upd)
		require.NotNil(t, upd.Message)

		chatID := upd.ChatID()
		msgID := upd.Message.ID

		mu.Lock()
		expected := chatProgress[chatID] + 1
		assert.Equal(t, expected, msgID, "Chat %d received out-of-order message: expected %d, got %d", chatID, expected, msgID)
		chatProgress[chatID] = msgID
		mu.Unlock()

		totalConsumed.Inc()
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(numChats)

	for ch := 1; ch <= numChats; ch++ {
		chatID := int64(ch * 1000)
		go func(cid int64) {
			defer wg.Done()
			for m := 1; m <= msgsPerChat; m++ {
				upd := &tg.Updates{
					Updates: []tg.UpdateClass{
						&tg.UpdateNewMessage{
							Message: &tg.Message{
								ID:      m,
								Message: fmt.Sprintf("msg_%d", m),
								PeerID:  &tg.PeerChat{ChatID: cid},
								FromID:  &tg.PeerUser{UserID: 42},
							},
						},
					},
				}
				for !c.sequencer.Enqueue(upd, [16]byte{byte(cid), byte(m)}, cid) {
					time.Sleep(50 * time.Microsecond)
				}
			}
		}(chatID)
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return totalConsumed.Load() == int64(totalMsgs)
	}, 6*time.Second, 20*time.Millisecond)

	mu.Lock()
	for ch := 1; ch <= numChats; ch++ {
		cid := int64(ch * 1000)
		assert.Equal(t, msgsPerChat, chatProgress[cid], "Chat %d did not process all messages", cid)
	}
	mu.Unlock()
}

func TestPipeline_Disruptor_Backpressure(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "backpressure_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	// Overwrite ring buffer with tiny capacity 16 to trigger backpressure quickly
	ring, err := disruptor.NewRingBuffer(16)
	require.NoError(t, err)

	c.ringBuffer = ring
	c.sequencer = disruptor.NewSequencer(ring, nil)

	// Fill buffer completely (16 items) without active consumer progress
	for i := 1; i <= 16; i++ {
		ok := c.sequencer.Enqueue(&tg.Updates{}, [16]byte{}, int64(i))
		require.True(t, ok)
	}

	// Buffer is saturated. EnqueueContext with short deadline must block and expire, exerting TCP backpressure
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err = c.sequencer.EnqueueContext(ctx, &tg.Updates{}, [16]byte{}, 100)
	require.ErrorIs(t, err, context.DeadlineExceeded, "Sequencer must block and apply backpressure when full")

	// Once consumer makes progress (advancing gating sequence), Enqueue immediately succeeds without packet drop
	c.ringBuffer.GatingSequence().Set(1)
	ok := c.sequencer.Enqueue(&tg.Updates{}, [16]byte{}, 101)
	assert.True(t, ok, "Enqueue must succeed once buffer slot is released by consumer")
}

func TestPipeline_TornRead_PayloadIntegrity(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "torn_read_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	const iterations = 2000
	var verifiedCount atomic.Int64

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		require.NotNil(t, upd.Message)
		expectedPayload := fmt.Sprintf("payload_%d_client_%d", upd.Message.ID, upd.ChatID())
		assert.Equal(t, expectedPayload, upd.Message.Message, "Torn read / payload corruption detected")
		verifiedCount.Inc()
		return nil
	})

	for i := 1; i <= iterations; i++ {
		cid := int64(100 + (i % 5))
		upd := &tg.Updates{
			Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      i,
						Message: fmt.Sprintf("payload_%d_client_%d", i, cid),
						PeerID:  &tg.PeerChat{ChatID: cid},
						FromID:  &tg.PeerUser{UserID: 42},
					},
				},
			},
		}
		for !c.sequencer.Enqueue(upd, [16]byte{}, cid) {
			time.Sleep(10 * time.Microsecond)
		}
	}

	assert.Eventually(t, func() bool {
		return verifiedCount.Load() == int64(iterations)
	}, 5*time.Second, 15*time.Millisecond)
}

func TestPipeline_ChannelPool_ThunderingHerd_Prioritization(t *testing.T) {
	t.Parallel()

	api := &mockPipelineChannelAPI{
		responses: make(map[int64][]tg.UpdatesChannelDifferenceClass),
	}

	const maxWorkers = channelpool.MinConcurrency // Rigid floor: 8 workers
	const totalChannels = 100

	for i := 1; i <= totalChannels; i++ {
		api.setResponse(int64(i), &tg.UpdatesChannelDifference{Pts: 200, Final: true})
	}

	var activeObserved atomic.Int32
	var maxActive atomic.Int32
	var processedCount atomic.Int64
	var activeChannelsFirst atomic.Int64

	pool := channelpool.NewWorkerPool(
		api,
		maxWorkers,
		func(batch channelpool.ChannelDifferenceBatch) error {
			curr := activeObserved.Inc()
			if curr > maxActive.Load() {
				maxActive.Store(curr)
			}
			time.Sleep(2 * time.Millisecond)
			activeObserved.Dec()

			if batch.ChannelID <= 20 {
				activeChannelsFirst.Inc()
			}
			processedCount.Inc()
			return nil
		},
		nil,
		nil,
	)

	ctx := t.Context()
	pool.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(totalChannels)

	for i := 1; i <= totalChannels; i++ {
		chID := int64(i)
		go func() {
			defer wg.Done()
			tier := channelpool.TierBacklog
			muted := true
			pinned := false
			if chID <= 20 {
				tier = channelpool.TierActive
				muted = false
				pinned = true
			}
			pool.Submit(&channelpool.ChannelSyncTask{
				ChannelID: chID,
				LocalPTS:  10,
				RemotePTS: 200,
				Tier:      tier,
				Muted:     muted,
				Pinned:    pinned,
			})
		}()
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int64(totalChannels)
	}, 6*time.Second, 20*time.Millisecond)

	assert.LessOrEqual(t, maxActive.Load(), int32(maxWorkers), "Concurrency limit must never be exceeded")
	assert.Equal(t, int64(20), activeChannelsFirst.Load(), "All 20 high-priority active channels must complete")
	pool.Stop()
}

type mockPipelineChannelAPI struct {
	mu        sync.Mutex
	responses map[int64][]tg.UpdatesChannelDifferenceClass
}

func (m *mockPipelineChannelAPI) setResponse(channelID int64, res ...tg.UpdatesChannelDifferenceClass) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses[channelID] = res
}

func (m *mockPipelineChannelAPI) UpdatesGetChannelDifference(_ context.Context, req *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
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

func TestPipeline_StatelessFSM_ConcurrentInteraction(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "fsm_interaction")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	var step1Done atomic.Int64
	var step2Done atomic.Int64

	c.OnState("step_1", func(ctx *Context, upd *Update) error {
		step1Done.Inc()
		return c.State().SetState(ctx.Context(), upd.ChatID(), upd.SenderID(), "step_2", 10*time.Minute)
	})

	c.OnState("step_2", func(ctx *Context, upd *Update) error {
		step2Done.Inc()
		return c.State().ClearState(ctx.Context(), upd.ChatID(), upd.SenderID())
	})

	const users = 50
	for u := 1; u <= users; u++ {
		chatID := int64(u * 10)
		userID := int64(u * 10)

		err := c.State().SetState(ctx, chatID, userID, "step_1", 10*time.Minute)
		require.NoError(t, err)

		upd1 := &tg.Updates{
			Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      1,
						Message: "trigger step 1",
						PeerID:  &tg.PeerChat{ChatID: chatID},
						FromID:  &tg.PeerUser{UserID: userID},
					},
				},
			},
		}
		for !c.sequencer.Enqueue(upd1, [16]byte{}, chatID) {
			time.Sleep(10 * time.Microsecond)
		}
	}

	assert.Eventually(t, func() bool {
		return step1Done.Load() == int64(users)
	}, 5*time.Second, 15*time.Millisecond)

	for u := 1; u <= users; u++ {
		chatID := int64(u * 10)
		userID := int64(u * 10)

		upd2 := &tg.Updates{
			Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      2,
						Message: "trigger step 2",
						PeerID:  &tg.PeerChat{ChatID: chatID},
						FromID:  &tg.PeerUser{UserID: userID},
					},
				},
			},
		}
		for !c.sequencer.Enqueue(upd2, [16]byte{}, chatID) {
			time.Sleep(10 * time.Microsecond)
		}
	}

	assert.Eventually(t, func() bool {
		return step2Done.Load() == int64(users)
	}, 5*time.Second, 15*time.Millisecond)
}

func TestPipeline_ChatMigrateTo_FSM(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "migrate_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := context.Background()
	const oldChatID = int64(1001)
	const newChannelID = int64(2002)
	const userID = int64(3003)

	// 1. Establish conversational FSM state in the basic group
	err = c.State().SetState(ctx, oldChatID, userID, "step_checkout", 10*time.Minute)
	require.NoError(t, err)

	// 2. Simulate supergroup migration service message
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.MessageService{
					ID:     1,
					PeerID: &tg.PeerChat{ChatID: oldChatID},
					FromID: &tg.PeerUser{UserID: userID},
					Action: &tg.MessageActionChatMigrateTo{
						ChannelID: newChannelID,
					},
				},
			},
		},
	}

	pCtx := &Context{
		StdContext: ctx,
		Update:     &Update{Raw: upd.Updates[0], Kind: KindRaw},
		Logger:     c.logger,
	}
	c.dispatchUpdate(pCtx)

	// 3. Verify FSM state was transferred to new supergroup ID and cleaned from old ID
	newState, err := c.State().GetState(ctx, newChannelID, userID)
	require.NoError(t, err)
	assert.Equal(t, "step_checkout", newState, "FSM state must be migrated to new supergroup channel ID")

	oldState, err := c.State().GetState(ctx, oldChatID, userID)
	require.Error(t, err, "Old state lookup must fail")
	assert.Empty(t, oldState, "Old group FSM state must be cleared after migration")
}

func TestPipeline_ZeroAlloc_ContextPool(t *testing.T) {
	pool := NewContextPool()
	meta := FastMetadata{
		TraceID:  [16]byte{'t', 'r', 'a', 'c', 'e'},
		ClientID: 12345,
	}
	parent := context.Background()

	// 1. Acquire Context from pool
	ctx := pool.Acquire(parent, meta)

	// 2. Validate custom context values resolve functionally
	traceVal := ctx.Value(traceIDContextKey{})
	clientVal := ctx.Value(clientIDContextKey{})

	assert.Equal(t, "trace", traceVal, "TraceID must resolve correctly without falling back to parent")
	assert.Equal(t, int64(12345), clientVal, "ClientID must resolve correctly without falling back to parent")

	assert.Equal(t, "trace", TraceIDFromContext(ctx))
	assert.Equal(t, int64(12345), ClientIDFromContext(ctx))

	// 3. Release back to pool
	pool.Release(ctx)

	// Re-acquire to ensure clearing mechanism worked
	ctx2 := pool.Acquire(parent, FastMetadata{})
	assert.Empty(t, TraceIDFromContext(ctx2), "Released context must clear TraceID metadata")
	assert.Equal(t, int64(0), ClientIDFromContext(ctx2), "Released context must clear ClientID metadata")
	pool.Release(ctx2)
}

func TestPipeline_LivePTS_GapDetectionAndDrain(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "live_pts_gap_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	var mu sync.Mutex
	var receivedIDs []int
	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedIDs = append(receivedIDs, upd.Message.ID)
		}
		return nil
	})

	// 1. Establish baseline PTS = 10
	upd10 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:     10,
					PeerID: &tg.PeerChat{ChatID: 100},
				},
				Pts:      10,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd10)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 1 && receivedIDs[0] == 10
	}, 2*time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(10), c.stateVector.PTS())

	// 2. Out-of-order update arrives: PTS = 12, count = 1 (gap: 11 is missing)
	upd12 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:     12,
					PeerID: &tg.PeerChat{ChatID: 100},
				},
				Pts:      12,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd12)
	require.NoError(t, err)

	// Update 12 should be buffered in gapBuffer and NOT dispatched yet
	assert.Eventually(t, func() bool {
		return c.gapBuffer.Len() == 1
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Len(t, receivedIDs, 1, "Update 12 must be buffered in gapBuffer pending gap resolution")
	mu.Unlock()
	assert.Equal(t, int32(10), c.stateVector.PTS())

	// 3. Stale update arrives: PTS = 9, count = 1. Must be ignored (GapIgnore).
	upd9 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:     9,
					PeerID: &tg.PeerChat{ChatID: 100},
				},
				Pts:      9,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd9)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Len(t, receivedIDs, 1, "Stale update 9 must be ignored")
	mu.Unlock()

	// 4. Missing consecutive update arrives: PTS = 11, count = 1.
	upd11 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:     11,
					PeerID: &tg.PeerChat{ChatID: 100},
				},
				Pts:      11,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd11)
	require.NoError(t, err)

	// Now both 11 and buffered 12 should be drained and dispatched consecutively
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 3
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []int{10, 11, 12}, receivedIDs, "Updates must be dispatched in exact consecutive order")
	mu.Unlock()

	assert.Equal(t, 0, c.gapBuffer.Len())
	assert.Equal(t, int32(12), c.stateVector.PTS())
}

func TestPipeline_ChannelPool_LiveUpdateSubmission(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "channel_live_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	const channelID = int64(888)
	const accessHash = int64(999999)

	var mu sync.Mutex
	var receivedIDs []int
	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedIDs = append(receivedIDs, upd.Message.ID)
		}
		return nil
	})

	// 1. First channel update arrives with entities to learn AccessHash and establish baseline PTS = 50
	upd50 := &tg.Updates{
		Chats: []tg.ChatClass{
			&tg.Channel{ID: channelID, AccessHash: accessHash, Title: "TestChannel"},
		},
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message: &tg.Message{
					ID:     1,
					PeerID: &tg.PeerChannel{ChannelID: channelID},
				},
				Pts:      50,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd50)
	require.NoError(t, err)

	// Wait for async consumer to process upd50 and populate entities
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 1 && receivedIDs[0] == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Access hash must be learned and cached
	resolvedHash := c.resolveChannelAccessHash(ctx, channelID, nil)
	assert.Equal(t, accessHash, resolvedHash)

	val, ok := c.channelPTS.Load(channelID)
	require.True(t, ok)
	assert.Equal(t, int32(50), val.(int32))

	// 2. Consecutive channel update: PTS = 51, count = 1. Must be dispatched immediately.
	upd51 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message: &tg.Message{
					ID:     2,
					PeerID: &tg.PeerChannel{ChannelID: channelID},
				},
				Pts:      51,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd51)
	require.NoError(t, err)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 2 && receivedIDs[1] == 2
	}, 2*time.Second, 10*time.Millisecond)

	val, ok = c.channelPTS.Load(channelID)
	require.True(t, ok)
	assert.Equal(t, int32(51), val.(int32))

	// 3. Stale channel update: PTS = 50, count = 1. Must be discarded (GapIgnore).
	updStale := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message: &tg.Message{
					ID:     99,
					PeerID: &tg.PeerChannel{ChannelID: channelID},
				},
				Pts:      50,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, updStale)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Len(t, receivedIDs, 2, "Stale channel update must not be dispatched")
	mu.Unlock()

	// 4. Channel gap arrives: PTS = 70, PtsCount = 1 (expected 52, gap detected)
	upd70 := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message: &tg.Message{
					ID:     3,
					PeerID: &tg.PeerChannel{ChannelID: channelID},
				},
				Pts:      70,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd70)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Len(t, receivedIDs, 2, "Gapped channel update must be intercepted for worker pool sync")
	mu.Unlock()

	// 5. UpdateChannelTooLong: Must be intercepted and submitted for worker pool recovery
	updTooLong := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateChannelTooLong{
				ChannelID: channelID,
				Pts:       5000,
			},
		},
	}
	err = c.handleUpdates(ctx, updTooLong)
	require.NoError(t, err)
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Len(t, receivedIDs, 2, "UpdateChannelTooLong must not be dispatched as standard message")
	mu.Unlock()
}

func TestPipeline_ContextPool_Recycling_ZeroLeak(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "context_recycling_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	var dispatchedCount atomic.Int64
	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(ctx *Context, _ *Update) error {
		assert.NotEmpty(t, ctx.TraceID())
		dispatchedCount.Inc()
		return nil
	})

	const numUpdates = 100
	for i := 1; i <= numUpdates; i++ {
		upd := &tg.Updates{
			Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      i,
						Message: fmt.Sprintf("msg_%d", i),
						PeerID:  &tg.PeerChat{ChatID: 555},
					},
					Pts:      100 + i,
					PtsCount: 1,
				},
			},
		}
		err := c.handleUpdates(ctx, upd)
		require.NoError(t, err)
	}

	assert.Eventually(t, func() bool {
		return dispatchedCount.Load() == int64(numUpdates)
	}, 4*time.Second, 15*time.Millisecond)

	// Verify Context.Release recycling contract explicitly
	pool := NewContextPool()
	pCtx := pool.Acquire(context.Background(), FastMetadata{
		TraceID:  [16]byte{'r', 'e', 'c', 'y', 'c', 'l', 'e'},
		ClientID: 999,
	})
	userCtx := &Context{
		StdContext: pCtx,
		Params:     map[string]string{"foo": "bar"},
	}

	assert.Equal(t, "recycle", userCtx.TraceID())
	assert.Equal(t, int64(999), userCtx.ClientID())

	// Invoking userCtx.Release() must trigger pCtx.Release() and clear userCtx fields
	userCtx.Release()

	assert.Nil(t, userCtx.StdContext)
	assert.Nil(t, userCtx.Params)
	assert.Empty(t, userCtx.TraceID())

	// Re-acquiring from pool returns recycled instance with cleared state
	reacquired := pool.Acquire(context.Background(), FastMetadata{})
	assert.Empty(t, reacquired.Meta.TraceIDString())
	assert.Zero(t, reacquired.Meta.ClientID)
	pool.Release(reacquired)
}

type mockTenantStateSink struct {
	mu            sync.Mutex
	savedChannels map[string]int32
}

func (*mockTenantStateSink) Set(_ context.Context, _, _ string, _ time.Duration) error {
	return nil
}

func (*mockTenantStateSink) Get(_ context.Context, _ string) (string, error) {
	return "", nil
}

func (*mockTenantStateSink) Delete(_ context.Context, _ string) error {
	return nil
}

func (*mockTenantStateSink) Move(_ context.Context, _, _ string) error {
	return nil
}

func (*mockTenantStateSink) SaveState(_ context.Context, _ int64, _ syncstate.StateSnapshot) error {
	return nil
}

func (*mockTenantStateSink) LoadState(_ context.Context, _ int64) (syncstate.StateSnapshot, bool, error) {
	return syncstate.StateSnapshot{}, false, nil
}

func (m *mockTenantStateSink) SaveChannelPTS(_ context.Context, clientID, channelID int64, pts int32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.savedChannels == nil {
		m.savedChannels = make(map[string]int32)
	}
	m.savedChannels[fmt.Sprintf("%d:%d", clientID, channelID)] = pts
	return nil
}

func (*mockTenantStateSink) LoadChannelPTS(_ context.Context, _, _ int64) (int32, bool, error) {
	return 0, false, nil
}

func (*mockTenantStateSink) ForEachChannelPTS(_ context.Context, _ int64, _ func(int64, int32) error) error {
	return nil
}

func (*mockTenantStateSink) SetDialogNotLoaded(_ context.Context, _, _ int64, _ bool) error {
	return nil
}

func (*mockTenantStateSink) IsDialogNotLoaded(_ context.Context, _, _ int64) (bool, error) {
	return false, nil
}

func TestPipeline_PostponedUpdates_Integrity(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "postponed_integrity_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	var (
		mu          sync.Mutex
		receivedIDs []int
		messages    []string
	)

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedIDs = append(receivedIDs, upd.Message.ID)
			messages = append(messages, upd.Message.Message)
		}
		return nil
	})

	// 1. Simulate active difference fetch
	c.isFetchingDiff.Store(true)

	// 2. Incoming live message update arrives during diff fetch
	const expectedMsgID = 777
	const expectedText = "important live message during sync"
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:      expectedMsgID,
					Message: expectedText,
					PeerID:  &tg.PeerChat{ChatID: 555},
				},
				Pts:      100,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd)
	require.NoError(t, err)

	// Ensure it is buffered in postponedUpdates and not dispatched yet
	c.postponedMu.Lock()
	assert.Len(t, c.postponedUpdates, 1)
	c.postponedMu.Unlock()

	mu.Lock()
	assert.Empty(t, receivedIDs)
	mu.Unlock()

	// 3. Complete difference fetch and drain postponed updates atomically
	c.drainPostponedUpdates(ctx)

	// 4. Assert the message was dispatched intact with original ID and text (zero payload loss)
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 1 && receivedIDs[0] == expectedMsgID
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []int{expectedMsgID}, receivedIDs)
	assert.Equal(t, []string{expectedText}, messages)
	mu.Unlock()
}

func TestPipeline_MultiTenant_AccountIsolation(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "multitenant_test")
	c, err := New(dir,
		WithAppID(12345),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	// Unauthenticated fallback matches appID
	assert.Equal(t, int64(12345), c.accountID())

	// Authenticated client resolves to Telegram User ID
	const accountA = int64(999001)
	c.self.Store(&tg.User{ID: accountA})
	assert.Equal(t, accountA, c.accountID())

	sink := &mockTenantStateSink{}
	c.fsmStorage = sink
	c.stateStorage = sink

	const channelID = int64(8888)
	upd := &tg.UpdateNewChannelMessage{
		Message: &tg.Message{
			ID:     1,
			PeerID: &tg.PeerChannel{ChannelID: channelID},
		},
		Pts:      200,
		PtsCount: 1,
	}

	ok := c.handleChannelPtsUpdate(context.Background(), upd, nil, channelID, 200, 1)
	assert.True(t, ok)

	sink.mu.Lock()
	savedPTS, found := sink.savedChannels[fmt.Sprintf("%d:%d", accountA, channelID)]
	sink.mu.Unlock()

	require.True(t, found, "Channel PTS must be stored under authentic account ID")
	assert.Equal(t, int32(200), savedPTS)
}

type mockPipelineInvoker struct {
	mu      sync.Mutex
	dialogs tg.MessagesDialogsClass
	history tg.MessagesMessagesClass
	diff    tg.UpdatesDifferenceClass
	state   tg.UpdatesState
}

func (m *mockPipelineInvoker) Invoke(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch box := out.(type) {
	case *tg.MessagesDialogsBox:
		if m.dialogs != nil {
			box.Dialogs = m.dialogs
		} else {
			box.Dialogs = &tg.MessagesDialogs{}
		}
		return nil
	case *tg.MessagesMessagesBox:
		if m.history != nil {
			box.Messages = m.history
		} else {
			box.Messages = &tg.MessagesMessages{}
		}
		return nil
	case *tg.UpdatesDifferenceBox:
		if m.diff != nil {
			box.Difference = m.diff
		} else {
			box.Difference = &tg.UpdatesDifferenceEmpty{Date: 1700000000, Seq: 10}
		}
		return nil
	case *tg.UpdatesState:
		if m.state.Pts != 0 {
			*box = m.state
		} else {
			*box = tg.UpdatesState{
				Pts:  100,
				Qts:  10,
				Date: 1700000000,
				Seq:  10,
			}
		}
		return nil
	}
	return nil
}

func TestPipeline_DefaultConfig_InitStateSync(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "default_init_state")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	// 1. Verify default state storage is initialized and decoupled from fsmStorage
	require.NotNil(t, c.stateStorage, "Default client must instantiate persistent state storage")

	accountID := c.accountID()
	initialSnapshot := syncstate.StateSnapshot{
		PTS:  777,
		QTS:  50,
		Date: 1700000000,
		Seq:  10,
	}

	// 2. Pre-populate disk state to simulate offline recovery across restarts
	err = c.stateStorage.SaveState(ctx, accountID, initialSnapshot)
	require.NoError(t, err)

	err = c.stateStorage.SaveChannelPTS(ctx, accountID, 9999, 120)
	require.NoError(t, err)

	// Mock API invoker so background sync responds immediately without network deadlock
	c.api = tg.NewClient(&mockPipelineInvoker{})

	// 3. Execute initState: must succeed without early return or type-assertion failure
	err = c.initState(ctx)
	require.NoError(t, err)

	// 4. Assert that state vector and channel PTS were correctly restored into memory
	assert.Equal(t, int32(777), c.stateVector.PTS(), "Local PTS must match persistent disk state")
	assert.Equal(t, int32(50), c.stateVector.QTS(), "Local QTS must match persistent disk state")
	assert.Equal(t, int32(1700000000), c.stateVector.Date(), "Local Date must match persistent disk state")
	assert.Equal(t, int32(10), c.stateVector.Seq(), "Local Seq must match persistent disk state")

	val, found := c.channelPTS.Load(int64(9999))
	require.True(t, found, "Channel PTS must be restored from storage")
	assert.Equal(t, int32(120), val.(int32))
	require.NotNil(t, c.flusher, "CheckpointFlusher must be active and wired")
}

func TestPipeline_UpdatesTooLong_HistoryRecovery(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "updates_too_long_recovery")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)

	const expectedMsgID = 9999
	const expectedText = "recovered message after updatesTooLong"
	const channelID = int64(8888)

	var mu sync.Mutex
	var receivedIDs []int
	var receivedTexts []string

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedIDs = append(receivedIDs, upd.Message.ID)
			receivedTexts = append(receivedTexts, upd.Message.Message)
		}
		return nil
	})

	mockResp := &tg.MessagesDialogs{
		Dialogs: []tg.DialogClass{
			&tg.Dialog{
				Peer: &tg.PeerChannel{ChannelID: channelID},
				Pts:  350,
			},
		},
		Chats: []tg.ChatClass{
			&tg.Channel{
				ID:         channelID,
				AccessHash: 54321,
				Title:      "Recovered Channel",
			},
		},
	}

	mockHistory := &tg.MessagesChannelMessages{
		Messages: []tg.MessageClass{
			&tg.Message{
				ID:      expectedMsgID,
				Message: expectedText,
				PeerID:  &tg.PeerChannel{ChannelID: channelID},
			},
		},
		Chats: []tg.ChatClass{
			&tg.Channel{
				ID:         channelID,
				AccessHash: 54321,
				Title:      "Recovered Channel",
			},
		},
	}

	c.api = tg.NewClient(&mockPipelineInvoker{dialogs: mockResp, history: mockHistory})

	// Execute recoverHistoryViaGetHistory
	c.recoverHistoryViaGetHistory(ctx)

	// Verify that messages were dispatched and received by handler
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 1 && receivedIDs[0] == expectedMsgID
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []int{expectedMsgID}, receivedIDs)
	assert.Equal(t, []string{expectedText}, receivedTexts)
	mu.Unlock()

	// Verify channel PTS and access hash were updated
	val, ok := c.channelPTS.Load(channelID)
	require.True(t, ok)
	assert.Equal(t, int32(350), val.(int32))

	hash := c.resolveChannelAccessHash(ctx, channelID, nil)
	assert.Equal(t, int64(54321), hash)
}

type mockFloodDiffInvoker struct {
	mu           sync.Mutex
	calls        int
	floodErr     error
	diff         tg.UpdatesDifferenceClass
	dialogsCalls atomic.Int64
}

func (m *mockFloodDiffInvoker) Invoke(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch box := out.(type) {
	case *tg.UpdatesDifferenceBox:
		m.calls++
		if m.calls == 1 && m.floodErr != nil {
			return m.floodErr
		}
		if m.diff != nil {
			box.Difference = m.diff
		} else {
			box.Difference = &tg.UpdatesDifferenceEmpty{Date: 1700000000, Seq: 10}
		}
		return nil
	case *tg.MessagesDialogsBox:
		m.dialogsCalls.Inc()
		box.Dialogs = &tg.MessagesDialogs{}
		return nil
	case *tg.UpdatesState:
		*box = tg.UpdatesState{
			Pts:  100,
			Qts:  10,
			Date: 1700000000,
			Seq:  10,
		}
		return nil
	}
	return nil
}

func TestPipeline_Difference_FloodWaitRecovery(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "flood_wait_recovery")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	const expectedMsgID = 8888
	const expectedText = "critical message preserved across flood wait"

	var mu sync.Mutex
	var receivedIDs []int
	var receivedTexts []string

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedIDs = append(receivedIDs, upd.Message.ID)
			receivedTexts = append(receivedTexts, upd.Message.Message)
		}
		return nil
	})

	invoker := &mockFloodDiffInvoker{
		floodErr: tgerr.New(420, "FLOOD_WAIT_1"),
		diff:     &tg.UpdatesDifferenceEmpty{Date: 1700000000, Seq: 10},
	}
	c.api = tg.NewClient(invoker)

	c.diffFetcher = syncstate.NewDifferenceFetcher(
		c.api,
		c.stateVector,
		c.flusher,
		c.gapBuffer,
		func() {
			c.handleUpdatesTooLong(context.Background())
		},
	)

	// 1. Simulate active difference fetch.
	c.isFetchingDiff.Store(true)

	// 2. Incoming live message update arrives during diff fetch.
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:      expectedMsgID,
					Message: expectedText,
					PeerID:  &tg.PeerChat{ChatID: 777},
				},
				Pts:      100,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, upd)
	require.NoError(t, err)

	c.postponedMu.Lock()
	assert.Len(t, c.postponedUpdates, 1, "Update must be buffered in postponed queue during sync")
	c.postponedMu.Unlock()

	mu.Lock()
	assert.Empty(t, receivedIDs)
	mu.Unlock()

	// 3. Trigger resync which will encounter FLOOD_WAIT on first attempt.
	c.isFetchingDiff.Store(false)
	c.triggerResync()

	// Wait for retry loop: FLOOD_WAIT_1 backs off for 1s then retries and succeeds.
	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedIDs) == 1 && receivedIDs[0] == expectedMsgID
	}, 5*time.Second, 50*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []int{expectedMsgID}, receivedIDs)
	assert.Equal(t, []string{expectedText}, receivedTexts)
	mu.Unlock()

	// 4. Assert that handleUpdatesTooLong was NEVER called (zero dialogs sweep).
	assert.Zero(t, invoker.dialogsCalls.Load(), "handleUpdatesTooLong must not be invoked on FLOOD_WAIT")
}

func TestPipeline_PolymorphicUpdates_CoverageBooster(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "polymorphic_coverage")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	var (
		editedMsgCalled   atomic.Bool
		deletedMsgCalled  atomic.Bool
		callbackCalled    atomic.Bool
		inlineCalled      atomic.Bool
		chatMemberCalled  atomic.Bool
		joinRequestCalled atomic.Bool
	)

	c.OnEditedMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		editedMsgCalled.Store(true)
		return nil
	})
	c.OnDeletedMessages(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		deletedMsgCalled.Store(true)
		return nil
	})
	c.OnCallbackQuery(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		callbackCalled.Store(true)
		return nil
	})
	c.OnInlineQuery(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		inlineCalled.Store(true)
		return nil
	})
	c.OnChatMemberUpdated(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		chatMemberCalled.Store(true)
		return nil
	})
	c.OnChatJoinRequest(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		joinRequestCalled.Store(true)
		return nil
	})

	// 1. Establish baseline global PTS
	baselineUpd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message:  &tg.Message{ID: 100, PeerID: &tg.PeerChat{ChatID: 700}},
				Pts:      100,
				PtsCount: 1,
			},
		},
	}
	require.NoError(t, c.handleUpdates(ctx, baselineUpd))

	// 2. Exercise all global PTS update variants
	globalPtsUpdates := []tg.UpdateClass{
		&tg.UpdateEditMessage{
			Message:  &tg.Message{ID: 101, PeerID: &tg.PeerChat{ChatID: 700}},
			Pts:      101,
			PtsCount: 1,
		},
		&tg.UpdateDeleteMessages{
			Messages: []int{101},
			Pts:      102,
			PtsCount: 1,
		},
		&tg.UpdateReadHistoryInbox{
			Peer:     &tg.PeerChat{ChatID: 700},
			MaxID:    101,
			Pts:      103,
			PtsCount: 1,
		},
		&tg.UpdateReadHistoryOutbox{
			Peer:     &tg.PeerChat{ChatID: 700},
			MaxID:    101,
			Pts:      104,
			PtsCount: 1,
		},
		&tg.UpdatePinnedMessages{
			Peer:     &tg.PeerChat{ChatID: 700},
			Messages: []int{101},
			Pts:      105,
			PtsCount: 1,
		},
		&tg.UpdateReadMessagesContents{
			Messages: []int{101},
			Pts:      106,
			PtsCount: 1,
		},
		&tg.UpdateWebPage{
			Webpage:  &tg.WebPageEmpty{ID: 1},
			Pts:      107,
			PtsCount: 1,
		},
		&tg.UpdateFolderPeers{
			FolderPeers: []tg.FolderPeer{},
			Pts:         108,
			PtsCount:    1,
		},
	}
	for _, u := range globalPtsUpdates {
		wrapper := &tg.Updates{Updates: []tg.UpdateClass{u}}
		require.NoError(t, c.handleUpdates(ctx, wrapper))
	}

	// 3. Exercise QTS update variants
	qtsUpdates := []tg.UpdateClass{
		&tg.UpdateNewEncryptedMessage{Qts: 50},
		&tg.UpdateMessagePollVote{Qts: 51, PollID: 1, Peer: &tg.PeerUser{UserID: 2}},
		&tg.UpdateBotStopped{Qts: 52, UserID: 3, Stopped: true},
		&tg.UpdateChatParticipant{Qts: 53, ChatID: 700, UserID: 4},
		&tg.UpdateChannelParticipant{Qts: 54, ChannelID: 800, UserID: 5},
		&tg.UpdateBotChatInviteRequester{Qts: 55, UserID: 6},
		&tg.UpdateBotChatBoost{Qts: 56, Peer: &tg.PeerChat{ChatID: 700}},
		&tg.UpdateBotMessageReaction{Qts: 57, Peer: &tg.PeerChat{ChatID: 700}},
		&tg.UpdateBotMessageReactions{Qts: 58, Peer: &tg.PeerChat{ChatID: 700}},
		&tg.UpdateBotBusinessConnect{Qts: 59},
		&tg.UpdateBotNewBusinessMessage{Qts: 60},
		&tg.UpdateBotEditBusinessMessage{Qts: 61},
		&tg.UpdateBotDeleteBusinessMessage{Qts: 62},
		&tg.UpdateBotPurchasedPaidMedia{Qts: 63, UserID: 7},
		&tg.UpdateBotStarsSubscription{Qts: 64, UserID: 8},
	}
	for _, u := range qtsUpdates {
		wrapper := &tg.Updates{Updates: []tg.UpdateClass{u}}
		require.NoError(t, c.handleUpdates(ctx, wrapper))
	}

	// 4. Exercise Channel PTS variants
	const chID = int64(9999)
	initChUpd := &tg.Updates{
		Chats: []tg.ChatClass{
			&tg.Channel{ID: chID, AccessHash: 12345, Title: "Channel9999"},
		},
		Updates: []tg.UpdateClass{
			&tg.UpdateNewChannelMessage{
				Message:  &tg.Message{ID: 1, PeerID: &tg.PeerChannel{ChannelID: chID}},
				Pts:      200,
				PtsCount: 1,
			},
		},
	}
	require.NoError(t, c.handleUpdates(ctx, initChUpd))

	channelPtsUpdates := []tg.UpdateClass{
		&tg.UpdateEditChannelMessage{
			Message:  &tg.Message{ID: 2, PeerID: &tg.PeerChannel{ChannelID: chID}},
			Pts:      201,
			PtsCount: 1,
		},
		&tg.UpdateDeleteChannelMessages{
			ChannelID: chID,
			Messages:  []int{1},
			Pts:       202,
			PtsCount:  1,
		},
		&tg.UpdatePinnedChannelMessages{
			ChannelID: chID,
			Messages:  []int{2},
			Pts:       203,
			PtsCount:  1,
		},
	}
	for _, u := range channelPtsUpdates {
		wrapper := &tg.Updates{Updates: []tg.UpdateClass{u}}
		require.NoError(t, c.handleUpdates(ctx, wrapper))
	}

	// 5. Short updates and synthetic conversion pathways
	c.self.Store(&tg.User{ID: 42})
	c.sentMsgPeers.Store(55, &tg.PeerChat{ChatID: 700})

	require.NoError(t, c.handleUpdates(ctx, &tg.UpdateShortMessage{
		ID:        110,
		UserID:    12,
		Message:   "short user message",
		Pts:       109,
		PtsCount:  1,
		Date:      1700000001,
		Entities:  []tg.MessageEntityClass{&tg.MessageEntityBold{Length: 5}},
		ReplyTo:   &tg.MessageReplyHeader{ReplyToMsgID: 100},
		ViaBotID:  99,
		TTLPeriod: 60,
	}))

	require.NoError(t, c.handleUpdates(ctx, &tg.UpdateShortChatMessage{
		ID:        111,
		FromID:    12,
		ChatID:    700,
		Message:   "short chat message",
		Pts:       110,
		PtsCount:  1,
		Date:      1700000002,
		Entities:  []tg.MessageEntityClass{&tg.MessageEntityItalic{Length: 5}},
		ReplyTo:   &tg.MessageReplyHeader{ReplyToMsgID: 100},
		ViaBotID:  99,
		TTLPeriod: 60,
	}))

	require.NoError(t, c.handleUpdates(ctx, &tg.UpdateShortSentMessage{
		ID:       55,
		Pts:      111,
		PtsCount: 1,
		Date:     1700000003,
	}))

	require.NoError(t, c.handleUpdates(ctx, &tg.UpdateShort{
		Update: &tg.UpdateDeleteMessages{Messages: []int{110}, Pts: 112, PtsCount: 1},
		Date:   1700000004,
	}))

	require.NoError(t, c.handleUpdates(ctx, &tg.UpdatesCombined{
		Updates: []tg.UpdateClass{
			&tg.UpdateBotCallbackQuery{QueryID: 99, Peer: &tg.PeerChat{ChatID: 700}, UserID: 12},
			&tg.UpdateBotInlineQuery{QueryID: 100, UserID: 12, Query: "test"},
			&tg.UpdateChatParticipant{ChatID: 700, UserID: 12},
			&tg.UpdatePendingJoinRequests{Peer: &tg.PeerChat{ChatID: 700}},
		},
		Date: 1700000005,
		Seq:  10,
	}))

	assert.Eventually(t, func() bool {
		return editedMsgCalled.Load() &&
			deletedMsgCalled.Load() &&
			callbackCalled.Load() &&
			inlineCalled.Load() &&
			chatMemberCalled.Load() &&
			joinRequestCalled.Load()
	}, 4*time.Second, 20*time.Millisecond)

	assert.Equal(t, int32(112), c.stateVector.PTS())
	assert.Equal(t, int32(64), c.stateVector.QTS())
	val, ok := c.channelPTS.Load(chID)
	require.True(t, ok)
	assert.Equal(t, int32(203), val.(int32))
}

// [INSERT-AFTER] Place directly AFTER `TestPipeline_PolymorphicUpdates_CoverageBooster`.
// [ANCHOR] 	assert.Equal(t, int32(203), val.(int32))
// [ANCHOR] }
// [WHY] Validates fault injection pathways during sync: PERSISTENT_TIMESTAMP_INVALID purge, canceled context handling, and transient network error recovery.

type mockFaultDiffInvoker struct {
	mu           sync.Mutex
	calls        int
	injectedErr  error
	stateInvoked atomic.Bool
	dialogsCalls atomic.Int64
}

func (m *mockFaultDiffInvoker) Invoke(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch box := out.(type) {
	case *tg.UpdatesDifferenceBox:
		m.calls++
		if m.injectedErr != nil {
			return m.injectedErr
		}
		box.Difference = &tg.UpdatesDifferenceEmpty{Date: 1700000000, Seq: 10}
		return nil
	case *tg.UpdatesState:
		m.stateInvoked.Store(true)
		*box = tg.UpdatesState{
			Pts:  200,
			Qts:  20,
			Date: 1700000000,
			Seq:  10,
		}
		return nil
	case *tg.MessagesDialogsBox:
		m.dialogsCalls.Inc()
		box.Dialogs = &tg.MessagesDialogs{}
		return nil
	case *tg.MessagesMessagesBox:
		box.Messages = &tg.MessagesMessages{}
		return nil
	}
	return nil
}

func TestPipeline_FaultInjection_GetDifferenceErrors(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "fault_diff_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)

	// 1. Fault injection: PERSISTENT_TIMESTAMP_INVALID must purge postponed updates and trigger handleUpdatesTooLong
	faultInvoker := &mockFaultDiffInvoker{
		injectedErr: tgerr.New(400, "PERSISTENT_TIMESTAMP_INVALID"),
	}
	c.api = tg.NewClient(faultInvoker)

	c.diffFetcher = syncstate.NewDifferenceFetcher(
		c.api,
		c.stateVector,
		c.flusher,
		c.gapBuffer,
		func() {
			c.handleUpdatesTooLong(context.Background())
		},
	)

	c.postponedMu.Lock()
	c.postponedUpdates = append(c.postponedUpdates, &tg.Updates{})
	c.postponedMu.Unlock()

	c.triggerResync()

	assert.Eventually(t, func() bool {
		c.postponedMu.Lock()
		empty := len(c.postponedUpdates) == 0
		c.postponedMu.Unlock()
		return empty && faultInvoker.stateInvoked.Load()
	}, 4*time.Second, 20*time.Millisecond)

	// 2. Cancellation: Context cancellation during diff fetch must return without panic
	cancelCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err = c.diffFetcher.FetchDifference(cancelCtx, func(_ syncstate.DifferenceBatch) error { return nil })
	require.ErrorIs(t, err, context.Canceled)

	// 3. Transient network error: Schedules retry without state vector reset
	c.isFetchingDiff.Store(false)
	faultInvoker.mu.Lock()
	faultInvoker.injectedErr = errors.New("connection reset by peer")
	faultInvoker.mu.Unlock()

	c.triggerResync()
	assert.Eventually(t, func() bool {
		return !c.isFetchingDiff.Load()
	}, 2*time.Second, 10*time.Millisecond)
}

// [INSERT-AFTER] Place directly AFTER `TestPipeline_FaultInjection_GetDifferenceErrors`.
// [ANCHOR] 	assert.Eventually(t, func() bool {
// [ANCHOR] 		return !c.isFetchingDiff.Load()
// [ANCHOR] 	}, 2*time.Second, 10*time.Millisecond)
// [ANCHOR] }
// [WHY] Verifies recoverHistoryViaGetHistory restores user and basic group dialogs, parses MessagesDialogsSlice, and sets dialog recovery flags.

type mockMultiPeerDialogInvoker struct {
	mu           sync.Mutex
	userHistory  int
	chatHistory  int
	chHistory    int
	dialogsCalls int
}

func (m *mockMultiPeerDialogInvoker) Invoke(_ context.Context, in bin.Encoder, out bin.Decoder) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch box := out.(type) {
	case *tg.MessagesDialogsBox:
		m.dialogsCalls++
		box.Dialogs = &tg.MessagesDialogsSlice{
			Count: 3,
			Dialogs: []tg.DialogClass{
				&tg.Dialog{
					Peer: &tg.PeerUser{UserID: 1001},
					Pts:  50,
				},
				&tg.Dialog{
					Peer: &tg.PeerChat{ChatID: 2002},
					Pts:  60,
				},
				&tg.Dialog{
					Peer: &tg.PeerChannel{ChannelID: 3003},
					Pts:  70,
				},
				&tg.DialogFolder{}, // Ignored: !ok branch
				&tg.Dialog{
					Peer: &tg.PeerUser{UserID: 9999},
					Pts:  0, // Ignored: d.Pts == 0 branch
				},
			},
			Chats: []tg.ChatClass{
				&tg.Channel{ID: 3003, AccessHash: 9988, Title: "Channel3003"},
			},
		}
		return nil

	case *tg.MessagesMessagesBox:
		req, ok := in.(*tg.MessagesGetHistoryRequest)
		if ok {
			switch p := req.Peer.(type) {
			case *tg.InputPeerUser:
				m.userHistory++
				box.Messages = &tg.MessagesMessagesSlice{
					Messages: []tg.MessageClass{
						&tg.Message{ID: 11, Message: "user msg 1", PeerID: &tg.PeerUser{UserID: p.UserID}},
						&tg.Message{ID: 12, Message: "user msg 2", PeerID: &tg.PeerUser{UserID: p.UserID}},
					},
				}
				return nil
			case *tg.InputPeerChat:
				m.chatHistory++
				box.Messages = &tg.MessagesMessages{
					Messages: []tg.MessageClass{
						&tg.Message{ID: 21, Message: "chat msg 1", PeerID: &tg.PeerChat{ChatID: p.ChatID}},
					},
				}
				return nil
			case *tg.InputPeerChannel:
				m.chHistory++
				box.Messages = &tg.MessagesChannelMessages{
					Messages: []tg.MessageClass{
						&tg.Message{ID: 31, Message: "channel msg 1", PeerID: &tg.PeerChannel{ChannelID: p.ChannelID}},
					},
				}
				return nil
			}
		}
		box.Messages = &tg.MessagesMessages{}
		return nil
	}
	return nil
}

func TestPipeline_RecoverHistory_UserAndChatDialogs(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "multipeer_recovery")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)

	var (
		mu           sync.Mutex
		receivedMsgs []string
	)

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		mu.Lock()
		defer mu.Unlock()
		if upd != nil && upd.Message != nil {
			receivedMsgs = append(receivedMsgs, upd.Message.Message)
		}
		return nil
	})

	invoker := &mockMultiPeerDialogInvoker{}
	c.api = tg.NewClient(invoker)

	c.recoverHistoryViaGetHistory(ctx)

	assert.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(receivedMsgs) == 4
	}, 3*time.Second, 20*time.Millisecond)

	mu.Lock()
	assert.Contains(t, receivedMsgs, "user msg 1")
	assert.Contains(t, receivedMsgs, "chat msg 1")
	assert.Contains(t, receivedMsgs, "channel msg 1")
	mu.Unlock()

	// Verify all 3 peer dialogs were marked as incomplete/not loaded in storage
	clientID := c.accountID()
	userNotLoaded, err := c.stateStorage.IsDialogNotLoaded(ctx, clientID, 1001)
	require.NoError(t, err)
	assert.True(t, userNotLoaded)

	chatNotLoaded, err := c.stateStorage.IsDialogNotLoaded(ctx, clientID, 2002)
	require.NoError(t, err)
	assert.True(t, chatNotLoaded)

	chNotLoaded, err := c.stateStorage.IsDialogNotLoaded(ctx, clientID, 3003)
	require.NoError(t, err)
	assert.True(t, chNotLoaded)
}

func TestPipeline_ChannelInaccessible_And_PostponedSaturation(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "inaccessible_and_saturation")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	// 1. Guard check: Inaccessible channel with PTS == -1 must immediately return true
	const inaccessibleCh = int64(7777)
	c.channelPTS.Store(inaccessibleCh, int32(-1))

	upd := &tg.UpdateNewChannelMessage{
		Message:  &tg.Message{ID: 1, PeerID: &tg.PeerChannel{ChannelID: inaccessibleCh}},
		Pts:      50,
		PtsCount: 1,
	}
	applied := c.handleChannelPtsUpdate(ctx, upd, nil, inaccessibleCh, 50, 1)
	assert.True(t, applied, "Inaccessible channel must return true immediately to prevent sync loops")

	// 2. Postponed queue saturation in handleUpdates (>10000)
	c.isFetchingDiff.Store(true)
	c.postponedMu.Lock()
	c.postponedUpdates = make([]tg.UpdatesClass, 10000)
	c.postponedMu.Unlock()

	fakeUpd := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message:  &tg.Message{ID: 999, PeerID: &tg.PeerChat{ChatID: 100}},
				Pts:      300,
				PtsCount: 1,
			},
		},
	}
	err = c.handleUpdates(ctx, fakeUpd)
	require.NoError(t, err)

	c.postponedMu.Lock()
	assert.Nil(t, c.postponedUpdates, "Saturated queue must be purged")
	c.postponedMu.Unlock()
	assert.False(t, c.isFetchingDiff.Load(), "isFetchingDiff must be reset upon saturation purge")

	// 3. Postponed queue saturation in handleGlobalPtsUpdate (>10000)
	c.isFetchingDiff.Store(true)
	c.postponedMu.Lock()
	c.postponedUpdates = make([]tg.UpdatesClass, 10000)
	c.postponedMu.Unlock()

	handled := c.handleGlobalPtsUpdate(ctx, fakeUpd.Updates[0], nil, FastMetadata{}, 301, 1, false)
	assert.False(t, handled)

	c.postponedMu.Lock()
	assert.Nil(t, c.postponedUpdates, "handleGlobalPtsUpdate must purge queue on saturation")
	c.postponedMu.Unlock()
	assert.False(t, c.isFetchingDiff.Load())
}

func TestPipeline_Disruptor_MultiProducerSaturationStress(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "disruptor_saturation")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()
	c.dispatcher.Start(ctx)
	go c.startDisruptorConsumer(ctx)

	const producers = 20
	const msgsPerProducer = 200
	const expectedTotal = producers * msgsPerProducer

	var receivedCount atomic.Int64
	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		receivedCount.Inc()
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(producers)

	for p := range producers {
		producerID := int64(p + 1)
		go func(pid int64) {
			defer wg.Done()
			for m := range msgsPerProducer {
				upd := &tg.Updates{
					Updates: []tg.UpdateClass{
						&tg.UpdateNewMessage{
							Message: &tg.Message{
								ID:      m + 1,
								Message: fmt.Sprintf("p_%d_m_%d", pid, m),
								PeerID:  &tg.PeerChat{ChatID: 8000 + pid},
								FromID:  &tg.PeerUser{UserID: 42},
							},
						},
					},
				}
				for !c.sequencer.Enqueue(upd, [16]byte{byte(pid), byte(m)}, 8000+pid) {
					time.Sleep(20 * time.Microsecond)
				}
			}
		}(producerID)
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return receivedCount.Load() == int64(expectedTotal)
	}, 6*time.Second, 25*time.Millisecond)

	assert.Equal(t, int64(expectedTotal), receivedCount.Load(), "All concurrently published updates must be processed without loss")
}

func TestPipeline_ChannelPool_ConcurrentWorkerDrain(t *testing.T) {
	t.Parallel()

	api := &mockPipelineChannelAPI{
		responses: make(map[int64][]tg.UpdatesChannelDifferenceClass),
	}

	const totalChannels = 150
	for i := 1; i <= totalChannels; i++ {
		api.setResponse(int64(i), &tg.UpdatesChannelDifference{Pts: 500, Final: true})
	}

	var processedCount atomic.Int64
	var activeGoroutines atomic.Int32
	var maxActiveObserved atomic.Int32

	pool := channelpool.NewWorkerPool(
		api,
		channelpool.MinConcurrency,
		func(batch channelpool.ChannelDifferenceBatch) error {
			curr := activeGoroutines.Inc()
			if curr > maxActiveObserved.Load() {
				maxActiveObserved.Store(curr)
			}
			time.Sleep(1 * time.Millisecond)
			activeGoroutines.Dec()
			processedCount.Inc()
			return nil
		},
		nil,
		nil,
	)

	ctx := t.Context()
	pool.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(totalChannels)

	now := time.Now()
	for i := 1; i <= totalChannels; i++ {
		chID := int64(i)
		go func(id int64) {
			defer wg.Done()
			task := &channelpool.ChannelSyncTask{
				ChannelID: id,
				LocalPTS:  10,
				RemotePTS: 500,
				Tier:      channelpool.TierBacklog,
			}
			// Inject delayed retry for a subset to test PopReady wait calculations
			if id%10 == 0 {
				task.NextAllowedAt = now.Add(20 * time.Millisecond)
			}
			pool.Submit(task)
		}(chID)
	}

	wg.Wait()

	assert.Eventually(t, func() bool {
		return processedCount.Load() == int64(totalChannels)
	}, 6*time.Second, 20*time.Millisecond)

	assert.LessOrEqual(t, maxActiveObserved.Load(), int32(channelpool.MaxConcurrency))
	pool.Stop()
}

func TestPipeline_EdgeBranches_FileTransfer_And_Mentions(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "edge_branches_test")
	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)

	ctx := t.Context()

	// 1. Uninitialized engine guards on file transfer
	uninitializedClient := &Client{}
	_, errUpload := uninitializedClient.UploadFile(ctx, "test.bin", nil, 100)
	require.ErrorContains(t, errUpload, "engine not initialized")

	_, errDownload := uninitializedClient.DownloadFile(ctx, &tg.InputDocumentFileLocation{}, nil)
	require.ErrorContains(t, errDownload, "engine not initialized")

	// 2. Canceled context guards on file transfer
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, errUploadCanceled := c.UploadFile(canceledCtx, "test.bin", nil, 100)
	require.ErrorContains(t, errUploadCanceled, "upload file canceled")

	_, errDownloadCanceled := c.DownloadFile(canceledCtx, &tg.InputDocumentFileLocation{}, nil)
	require.ErrorContains(t, errDownloadCanceled, "download file canceled")

	// 3. Bot mention stripping coverage
	c.self.Store(&tg.User{Username: "testbot"})

	stripped, changed := c.stripBotMention("/start@testbot payload")
	assert.True(t, changed)
	assert.Equal(t, "/start payload", stripped)

	// Mismatched username mention must not strip
	unaltered, changedMismatch := c.stripBotMention("/start@otherbot payload")
	assert.False(t, changedMismatch)
	assert.Equal(t, "/start@otherbot payload", unaltered)

	// Non-command string must not strip
	plain, changedPlain := c.stripBotMention("regular text")
	assert.False(t, changedPlain)
	assert.Equal(t, "regular text", plain)

	// 4. OnChatMigrateTo registration contract
	assert.NotPanics(t, func() {
		c.OnChatMigrateTo(func(_ *Context, _ *Update) error {
			return nil
		})
	})

	// 5. Channel access hash resolution fallback via sync.Map and peer storage
	const knownChannelID = int64(445566)
	c.channelAccessHashes.Store(knownChannelID, int64(888999))
	resolvedHash := c.resolveChannelAccessHash(ctx, knownChannelID, nil)
	assert.Equal(t, int64(888999), resolvedHash)

	// 6. Close error joining verification
	require.NoError(t, c.Close())
}

func BenchmarkPipeline_Disruptor_IngressThroughput(b *testing.B) {
	ring, err := disruptor.NewRingBuffer(disruptor.DefaultCapacity)
	if err != nil {
		b.Fatal(err)
	}
	seq := disruptor.NewSequencer(ring, nil)

	var ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	poller := disruptor.NewSingleConsumerPoller(ring, 10*time.Microsecond)
	go func() {
		_ = poller.Run(ctx, func(_ *disruptor.RingSlot) error {
			return nil
		})
	}()

	rawUpdate := &tg.Updates{}
	trace := [16]byte{'b', 'e', 'n', 'c', 'h'}

	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		clientID := int64(1)
		for pb.Next() {
			for !seq.Enqueue(rawUpdate, trace, clientID) {
				runtime.Gosched()
			}
		}
	})
}

func BenchmarkPipeline_ContextPool_AcquireRelease(b *testing.B) {
	pool := NewContextPool()
	parent := context.Background()
	meta := FastMetadata{
		TraceID:  [16]byte{'b', 'e', 'n', 'c', 'h'},
		ClientID: 42,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ctx := pool.Acquire(parent, meta)
		ctx.Release()
	}
}

func BenchmarkPipeline_ChannelPool_PriorityHeap(b *testing.B) {
	pq := channelpool.NewPriorityQueue()
	now := time.Now()

	for i := 1; i <= 500; i++ {
		pq.PushOrUpdate(&channelpool.ChannelSyncTask{
			ChannelID:  int64(i),
			LocalPTS:   10,
			RemotePTS:  int32(10 + (i % 50)),
			Tier:       channelpool.TierBacklog,
			EnqueuedAt: now,
		})
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		taskID := int64((i % 1000) + 1)
		pq.PushOrUpdate(&channelpool.ChannelSyncTask{
			ChannelID:  taskID,
			LocalPTS:   10,
			RemotePTS:  int32(50 + (i % 200)),
			Tier:       channelpool.TierActive,
			EnqueuedAt: now,
		})
		if task, ok := pq.Pop(); ok && task != nil {
			_ = task.Score(now)
		}
	}
}

func BenchmarkPipeline_ShardedDispatcher_Routing(b *testing.B) {
	hashFunc := func(_ *Context, upd *Update) uint64 {
		return uint64(upd.ChatID())
	}
	kindFunc := func(_ *Context, upd *Update) int {
		return int(upd.Kind)
	}

	disp := dispatcher.NewDispatcher[*Context, *Update](
		8,
		1024,
		hashFunc,
		kindFunc,
		nil,
	)

	disp.AddRoute(int(KindMessage), func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error {
		return nil
	}, nil, 0)

	ctx := b.Context()
	disp.Start(ctx)

	upd := &Update{
		Kind:    KindMessage,
		Message: &tg.Message{ID: 1, PeerID: &tg.PeerChat{ChatID: 100}},
	}
	pCtx := &Context{Update: upd}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		disp.Handle(pCtx, upd)
	}
}

type dummyPlugin struct{}

func (dummyPlugin) Register(r Registrar) error {
	r.OnCommand("/test_plugin", func(_ *Context, _ *Update) error { return nil })
	return nil
}

type dummyController struct{}

func (dummyController) RegisterRoutes(r Registrar) error {
	r.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, _ *Update) error { return nil })
	return nil
}

func TestPipeline_ClientOptions_And_AuxiliaryCoverage(t *testing.T) {
	t.Parallel()

	dir := filepath.Join(t.TempDir(), "aux_coverage_client")
	var observedTraffic atomic.Bool
	var tooLongInvoked atomic.Bool

	c, err := New(dir,
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithWorkers(4),
		WithLogger(slog.Default()),
		WithAutoPeerCaching(false),
		WithTrafficObserver(func() { observedTraffic.Store(true) }),
		WithOnTooLong(func() { tooLongInvoked.Store(true) }),
		WithProxy("socks5://127.0.0.1:1080", false),
		WithSOCKS5("127.0.0.1:1080", "user", "pass", false),
		WithHTTPProxy("127.0.0.1:8080", "user", "pass", false),
	)
	require.NoError(t, err)
	defer func() { _ = c.Close() }()

	ctx := t.Context()

	// 1. Service singletons and accessors
	assert.NotNil(t, c.Chat())
	assert.NotNil(t, c.Admin())
	assert.NotNil(t, c.State())

	// 2. Plugins and Controller interfaces
	require.NoError(t, c.Install(dummyPlugin{}))
	require.NoError(t, c.RegisterController(dummyController{}))
	require.NoError(t, RegisterControllers(c, ControllerFunc(func(r Registrar) error {
		r.OnCommand("/controller_func", func(_ *Context, _ *Update) error { return nil })
		return nil
	})))

	// 3. Storage adapter direct tests (SetDialogNotLoaded & IsDialogNotLoaded)
	dbPath := filepath.Join(t.TempDir(), "storage_adapter_aux.db")
	stateStore, closer, err := tgboxstorage.NewDefaultStateStorage(dbPath)
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	const clientID = int64(777111)
	const dialogID = int64(888222)

	require.NoError(t, stateStore.SetDialogNotLoaded(ctx, clientID, dialogID, true))
	notLoaded, err := stateStore.IsDialogNotLoaded(ctx, clientID, dialogID)
	require.NoError(t, err)
	assert.True(t, notLoaded)

	require.NoError(t, stateStore.SetDialogNotLoaded(ctx, clientID, dialogID, false))
	notLoadedAfter, err := stateStore.IsDialogNotLoaded(ctx, clientID, dialogID)
	require.NoError(t, err)
	assert.False(t, notLoadedAfter)

	// 4. Traffic observer invocation
	c.opts.trafficObserver()
	assert.True(t, observedTraffic.Load())
}
