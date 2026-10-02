package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGotdEngineHandlerBridge(t *testing.T) {
	e := NewGotdEngine(123, "test-app-hash", telegram.Options{}).(*gotdEngine)
	require.NotNil(t, e.API())
	require.NotNil(t, e.Raw())
	update := &tg.Updates{Date: 42}
	require.NoError(t, e.bridgeHandler(t.Context(), update))
	failure := errors.New("handler failed")
	calls := 0
	e.SetUpdateHandler(telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
		calls++
		assert.Equal(t, t.Context(), ctx)
		assert.Same(t, update, u)
		return failure
	}))
	require.ErrorIs(t, e.bridgeHandler(t.Context(), update), failure)
	assert.Equal(t, 1, calls)
	e.SetUpdateHandler(nil)
	require.NoError(t, e.bridgeHandler(t.Context(), update))
	require.NoError(t, e.Disconnect())
}

func TestGotdEngineDisconnect(t *testing.T) {
	failure := errors.New("stop failed")
	calls := 0
	e := &gotdEngine{running: true, stop: func() error { calls++; return failure }}
	require.ErrorIs(t, e.Disconnect(), failure)
	assert.True(t, e.running)
	require.ErrorContains(t, e.Connect(t.Context()), "already running")
	failure = nil
	require.NoError(t, e.Disconnect())
	assert.False(t, e.running)
	require.NoError(t, e.Disconnect())
	assert.Equal(t, 2, calls)
}
