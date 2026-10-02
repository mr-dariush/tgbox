package tgbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/fsm"
)

func TestStateManagerLifecycle(t *testing.T) {
	storage := fsm.NewMemoryStorage(t.Context(), time.Hour)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	sm := NewStateManager(storage)
	require.NoError(t, sm.SetState(t.Context(), -10, 20, "waiting", 0))
	state, err := sm.GetState(t.Context(), -10, 20)
	require.NoError(t, err)
	assert.Equal(t, "waiting", state)
	_, err = sm.GetState(t.Context(), -10, 21)
	require.ErrorIs(t, err, fsm.ErrNotFound)
	require.NoError(t, sm.ClearState(t.Context(), -10, 20))
	_, err = sm.GetState(t.Context(), -10, 20)
	require.ErrorIs(t, err, fsm.ErrNotFound)
	require.ErrorIs(t, sm.WatchExpired(t.Context(), nil), ErrExpirationWatchNotSupported)
	for _, ids := range [][2]int64{{0, 20}, {10, 0}} {
		_, err = sm.GetState(t.Context(), ids[0], ids[1])
		require.ErrorIs(t, err, ErrInvalidFSMContext)
		require.ErrorIs(t, sm.SetState(t.Context(), ids[0], ids[1], "x", 0), ErrInvalidFSMContext)
		require.ErrorIs(t, sm.ClearState(t.Context(), ids[0], ids[1]), ErrInvalidFSMContext)
	}
	for _, missing := range []*defaultStateManager{nil, {}} {
		_, err = missing.GetState(t.Context(), 1, 2)
		require.ErrorIs(t, err, ErrFSMNotConfigured)
		require.ErrorIs(t, missing.SetState(t.Context(), 1, 2, "x", 0), ErrFSMNotConfigured)
		require.ErrorIs(t, missing.ClearState(t.Context(), 1, 2), ErrFSMNotConfigured)
		require.ErrorIs(t, missing.WatchExpired(t.Context(), nil), ErrFSMNotConfigured)
	}
}

type expirationStorage struct {
	fsm.Storage
	keys []string
	err  error
}

func (s expirationStorage) WatchExpired(_ context.Context, handler func(string)) error {
	for _, key := range s.keys {
		handler(key)
	}
	return s.err
}

func TestStateManagerExpiration(t *testing.T) {
	failure := errors.New("subscription closed")
	sm := NewStateManager(expirationStorage{keys: []string{"-10:20", "fsm:30:40", "bad", "x:2", "1:y", "1:2:3"}, err: failure})
	var expired [][2]int64
	err := sm.WatchExpired(t.Context(), func(chatID, userID int64) { expired = append(expired, [2]int64{chatID, userID}) })
	require.ErrorIs(t, err, failure)
	assert.Equal(t, [][2]int64{{-10, 20}, {30, 40}}, expired)
	require.ErrorIs(t, sm.WatchExpired(t.Context(), nil), failure)
}
