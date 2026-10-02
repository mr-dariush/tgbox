package tgbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/dispatcher"
	"github.com/mr-dariush/tgbox/internal/fsm"
	"github.com/mr-dariush/tgbox/internal/router"
	"github.com/mr-dariush/tgbox/network"
)

func TestContextHelpers(t *testing.T) {
	ctx := context.Background()

	ctx = WithTraceID(ctx, "mock_trace_id_123")
	assert.Equal(t, "mock_trace_id_123", TraceIDFromContext(ctx))

	ctx = WithClientID(ctx, 987654321)
	assert.Equal(t, int64(987654321), ClientIDFromContext(ctx))
}

func TestClient_IsFatalSessionError(t *testing.T) {
	c := &Client{}

	tests := []struct {
		err      error
		expected bool
	}{
		{errors.New("rpc error: AUTH_KEY_UNREGISTERED (401)"), true},
		{errors.New("connection failed: SESSION_REVOKED"), true},
		{errors.New("SESSION_EXPIRED - please log in again"), true},
		{errors.New("FLOOD_WAIT_300"), false},
		{errors.New("PEER_ID_INVALID"), false},
		{nil, false},
	}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("Case_%d", i), func(t *testing.T) {
			assert.Equal(t, tc.expected, c.isFatalSessionError(tc.err))
		})
	}
}

func TestClient_HandleUpdates_ContextPropagation(t *testing.T) {
	c := &Client{
		logger: slog.Default(),
	}
	c.self.Store(&tg.User{ID: 112233})

	hashFunc := func(_ *Context, _ *Update) uint64 { return 1 }
	kindFunc := func(_ *Context, upd *Update) int { return int(upd.Kind) }
	rawDisp := dispatcher.NewDispatcher[*Context, *Update](1, 10, hashFunc, kindFunc, slog.Default())
	c.dispatcher = &defaultDispatcher{inner: rawDisp}

	ctx := t.Context()
	c.dispatcher.Start(ctx)

	handlerCalled := make(chan struct{})
	var capturedCtx *Context

	c.dispatcher.AddRoute(
		int(KindMessage),
		func(_ *Context, _ *Update) bool { return true },
		func(ctx *Context, _ *Update) error {
			capturedCtx = ctx
			close(handlerCalled)
			return nil
		},
		nil,
		0,
	)

	rawUpdates := &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateNewMessage{
				Message: &tg.Message{
					ID:      999,
					Message: "testing distributed tracing propagation",
					PeerID:  &tg.PeerUser{UserID: 888},
				},
			},
		},
	}

	err := c.handleUpdates(context.Background(), rawUpdates)
	require.NoError(t, err)

	select {
	case <-handlerCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for dispatcher to process the update envelope")
	}

	assert.NotEmpty(t, capturedCtx.TraceID())
	assert.Equal(t, int64(112233), capturedCtx.ClientID())
	assert.NotNil(t, capturedCtx.Context())
	assert.Equal(t, capturedCtx.TraceID(), TraceIDFromContext(capturedCtx.Context()))
	assert.Equal(t, capturedCtx.ClientID(), ClientIDFromContext(capturedCtx.Context()))
}

func TestClient_StatelessFSM_Routing(t *testing.T) {
	ctx := t.Context()

	memStore := fsm.NewMemoryStorage(ctx, 10*time.Minute)
	defer func() { require.NoError(t, memStore.Close()) }()

	c := &Client{
		logger:     slog.Default(),
		fsmStorage: memStore,
	}
	c.stateRoutes.Store(new(map[string]stateRoute))
	c.commandRouter.Store(&defaultCommandRouter{inner: router.NewNode[commandRoute]()})

	hashFunc := func(_ *Context, _ *Update) uint64 { return 1 }
	kindFunc := func(_ *Context, upd *Update) int { return int(upd.Kind) }

	rawDisp := dispatcher.NewDispatcher[*Context, *Update](1, 10, hashFunc, kindFunc, slog.Default())
	c.dispatcher = &defaultDispatcher{inner: rawDisp}

	c.dispatcher.Start(ctx)
	c.registerBuiltinRoutes()

	stateHandled := make(chan string, 1)
	fallbackHandled := make(chan string, 1)

	c.OnState("step_1", func(ctx *Context, upd *Update) error {
		stateHandled <- upd.Message.Message
		return c.State().ClearState(ctx.Context(), upd.ChatID(), upd.SenderID())
	})

	c.OnMessage(func(_ *Context, _ *Update) bool { return true }, func(_ *Context, upd *Update) error {
		fallbackHandled <- upd.Message.Message
		return nil
	})

	chatID := int64(100)
	userID := int64(200)
	mockUpdate := func(text string) *tg.Updates {
		return &tg.Updates{
			Updates: []tg.UpdateClass{
				&tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      1,
						Message: text,
						PeerID:  &tg.PeerChat{ChatID: chatID},
						FromID:  &tg.PeerUser{UserID: userID},
					},
				},
			},
		}
	}

	err := c.handleUpdates(ctx, mockUpdate("hello fallback"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case msg := <-fallbackHandled:
			return msg == "hello fallback"
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)

	err = memStore.Set(ctx, fsmKey(chatID, userID), "step_1", 0)
	require.NoError(t, err)

	err = c.handleUpdates(ctx, mockUpdate("hello state"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case msg := <-stateHandled:
			return msg == "hello state"
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)

	err = c.handleUpdates(ctx, mockUpdate("hello again"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		select {
		case msg := <-fallbackHandled:
			return msg == "hello again"
		default:
			return false
		}
	}, 2*time.Second, 10*time.Millisecond)
}

func TestClient_LockFreeRouting_Concurrency(t *testing.T) {
	t.Parallel()

	c, err := New(filepath.Join(t.TempDir(), "test_lockfree_concurrent"),
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer c.Close()

	ctx := t.Context()

	const workers = 20
	const operations = 100
	var wg sync.WaitGroup
	wg.Add(workers * 2)

	// Concurrently register OnCommand and OnState (testing Copy-On-Write thread safety)
	for i := range workers {
		workerID := i
		go func() {
			defer wg.Done()
			for j := range operations {
				cmdPattern := fmt.Sprintf("/cmd_%d_%d", workerID, j)
				c.OnCommand(cmdPattern, func(_ *Context, _ *Update) error {
					return nil
				})

				stateName := fmt.Sprintf("state_%d_%d", workerID, j)
				c.OnState(stateName, func(_ *Context, _ *Update) error {
					return nil
				})
			}
		}()
	}

	// Concurrently read/route updates through the dispatcher (testing Wait-Free reading)
	for i := range workers {
		workerID := i
		go func() {
			defer wg.Done()
			for j := range operations {
				upd := &tg.Updates{
					Updates: []tg.UpdateClass{
						&tg.UpdateNewMessage{
							Message: &tg.Message{
								ID:      j + 1,
								Message: fmt.Sprintf("/cmd_%d_%d test_arg", workerID, j),
								PeerID:  &tg.PeerChat{ChatID: int64(1000 + workerID)},
								FromID:  &tg.PeerUser{UserID: int64(2000 + workerID)},
							},
						},
					},
				}
				_ = c.handleUpdates(ctx, upd)
			}
		}()
	}

	wg.Wait()
}

func TestClient_Suspension_NilGuard(t *testing.T) {
	c := &Client{}
	require.ErrorIs(t, c.Suspend(), ErrNetworkNotConfigured)
	require.ErrorIs(t, c.Resume(), ErrNetworkNotConfigured)
}

func TestClient_FuzzyPingInitialization(t *testing.T) {
	t.Parallel()

	t.Run("EnabledWhenProfileHasPingInterval", func(t *testing.T) {
		t.Parallel()

		profile := network.NewAndroidProfile()
		c, err := New(filepath.Join(t.TempDir(), "test_fuzzy_enabled"),
			WithAppID(123),
			WithAppHash("deadbeef"),
			WithAutoPeerCaching(false),
			WithNetworkProfile(profile),
		)
		require.NoError(t, err)
		defer c.Close()

		assert.NotNil(t, c.keepaliveEngine)
		require.NotNil(t, c.opts.trafficObserver)
		assert.NotPanics(t, func() {
			c.opts.trafficObserver()
		})
	})

	t.Run("DisabledWhenPingIntervalIsZero", func(t *testing.T) {
		t.Parallel()

		c, err := New(filepath.Join(t.TempDir(), "test_fuzzy_disabled"),
			WithAppID(123),
			WithAppHash("deadbeef"),
			WithAutoPeerCaching(false),
		)
		require.NoError(t, err)
		defer c.Close()

		assert.Nil(t, c.keepaliveEngine)
	})
}

func TestClient_FileTransfer_Cancellation(t *testing.T) {
	c, err := New("test_transfer", WithAppID(123), WithAppHash("deadbeef"))
	require.NoError(t, err)
	defer c.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("Upload Cancellation", func(t *testing.T) {
		dummyData := strings.NewReader("zero allocation test payload")
		_, errUpload := c.UploadFile(ctx, "test.txt", dummyData, int64(dummyData.Len()))

		require.Error(t, errUpload)
		assert.Contains(t, errUpload.Error(), "canceled")
	})

	t.Run("Download Cancellation", func(t *testing.T) {
		var dummyBuffer bytes.Buffer
		dummyLocation := &tg.InputDocumentFileLocation{ID: 12345}
		_, errDownload := c.DownloadFile(ctx, dummyLocation, &dummyBuffer)

		require.Error(t, errDownload)
		assert.Contains(t, errDownload.Error(), "canceled")
	})
}

func TestClient_PFS_Configuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		opts           []Option
		expectedPFS    bool
		expectedTTLSec int
	}{
		{
			name: "Default client without PFS",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
			},
			expectedPFS:    false,
			expectedTTLSec: 0,
		},
		{
			name: "Explicit WithPFS enabled with default TTL",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
				WithPFS(true),
			},
			expectedPFS:    true,
			expectedTTLSec: 86400,
		},
		{
			name: "Explicit WithPFS enabled with custom TTL",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
				WithPFS(true, 12*time.Hour),
			},
			expectedPFS:    true,
			expectedTTLSec: 43200,
		},
		{
			name: "Explicit WithPFS disabled",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
				WithPFS(false),
			},
			expectedPFS:    false,
			expectedTTLSec: 0,
		},
		{
			name: "Automatic PFS enablement via AndroidProfile",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
				WithNetworkProfile(network.NewAndroidProfile()),
			},
			expectedPFS:    true,
			expectedTTLSec: 86400,
		},
		{
			name: "Automatic PFS status via DesktopProfile",
			opts: []Option{
				WithAppID(123),
				WithAppHash("dummy_hash"),
				WithNetworkProfile(network.NewDesktopProfile()),
			},
			expectedPFS:    false,
			expectedTTLSec: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sessionDir := filepath.Join(t.TempDir(), "pfs_session")
			c, err := New(sessionDir, tc.opts...)
			require.NoError(t, err)
			defer c.Close()

			assert.Equal(t, tc.expectedPFS, c.opts.engineOpts.EnablePFS)
			assert.Equal(t, tc.expectedTTLSec, c.opts.engineOpts.TempKeyTTL)
		})
	}
}

type countingFSMStorage struct {
	fsm.Storage
	getCalls    atomic.Int64
	setCalls    atomic.Int64
	deleteCalls atomic.Int64
}

func (s *countingFSMStorage) Get(ctx context.Context, key string) (string, error) {
	s.getCalls.Inc()
	return s.Storage.Get(ctx, key)
}

func (s *countingFSMStorage) Set(ctx context.Context, key, state string, ttl time.Duration) error {
	s.setCalls.Inc()
	return s.Storage.Set(ctx, key, state, ttl)
}

func (s *countingFSMStorage) Delete(ctx context.Context, key string) error {
	s.deleteCalls.Inc()
	return s.Storage.Delete(ctx, key)
}

func TestStateManager_Operations(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	baseStore := fsm.NewMemoryStorage(ctx, 10*time.Minute)
	defer func() { require.NoError(t, baseStore.Close()) }()

	countingStore := &countingFSMStorage{Storage: baseStore}
	sm := NewStateManager(countingStore)

	chatID := int64(100)
	userID := int64(200)

	s0, err := sm.GetState(ctx, chatID, userID)
	require.ErrorIs(t, err, fsm.ErrNotFound)
	assert.Empty(t, s0)

	err = sm.SetState(ctx, chatID, userID, "step_initial", 5*time.Minute)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countingStore.setCalls.Load())

	s1, err := sm.GetState(ctx, chatID, userID)
	require.NoError(t, err)
	assert.Equal(t, "step_initial", s1)

	err = sm.ClearState(ctx, chatID, userID)
	require.NoError(t, err)
	assert.Equal(t, int64(1), countingStore.deleteCalls.Load())

	s2, err := sm.GetState(ctx, chatID, userID)
	require.ErrorIs(t, err, fsm.ErrNotFound)
	assert.Empty(t, s2)
}

func TestClient_ServiceContracts(t *testing.T) {
	t.Parallel()

	c, err := New(filepath.Join(t.TempDir(), "test_service_contracts"),
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer c.Close()

	assert.NotNil(t, c.Chat())
	assert.NotNil(t, c.Admin())
	assert.NotNil(t, c.State())
	assert.NotNil(t, c.Sender())

	_ = c.Chat()
	_ = c.Admin()
	_ = c.State()
}

func TestClient_SenderInitialization(t *testing.T) {
	t.Parallel()

	c, err := New(filepath.Join(t.TempDir(), "test_sender_singleton"),
		WithAppID(123),
		WithAppHash("deadbeef"),
		WithAutoPeerCaching(false),
	)
	require.NoError(t, err)
	defer c.Close()

	assert.NotNil(t, c.Sender())
}
