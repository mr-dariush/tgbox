package storage_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/storage"
	"github.com/mr-dariush/tgbox/internal/syncstate"
)

// TestBboltSession tests the bbolt-based session storage implementation.
func TestBboltSession(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test_session.db")
	sess, closer, err := storage.NewDefaultSessionStorage(path)
	require.NoError(t, err)
	defer closer.Close()

	_, err = sess.LoadSession(context.Background())
	t.Logf("LoadSession error: %v", err)
}

// TestBboltPeer tests the bbolt-based peer storage implementation.
func TestBboltPeer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test_peers.db")
	peerStore, closer, err := storage.NewDefaultPeerStorage(path)
	require.NoError(t, err)
	defer closer.Close()
	require.NotNil(t, peerStore)
}

// TestBboltStateStorage_Lifecycle validates state and channel PTS persistence across database reopen.
func TestBboltStateStorage_Lifecycle(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test_state.db")
	stateStore, closer, err := storage.NewDefaultStateStorage(dbPath)
	require.NoError(t, err)

	ctx := context.Background()
	const clientID = int64(123456)
	const channelID = int64(987654)

	snapshot := syncstate.StateSnapshot{
		PTS:  100,
		QTS:  50,
		Date: 1700000000,
		Seq:  25,
	}

	err = stateStore.SaveState(ctx, clientID, snapshot)
	require.NoError(t, err)

	err = stateStore.SaveChannelPTS(ctx, clientID, channelID, 500)
	require.NoError(t, err)

	require.NoError(t, closer.Close())

	// Reopen database to verify persistence on disk
	reopenedStore, reopenedCloser, err := storage.NewDefaultStateStorage(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopenedCloser.Close()) }()

	loadedSnap, found, err := reopenedStore.LoadState(ctx, clientID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, snapshot, loadedSnap)

	loadedPTS, ptsFound, err := reopenedStore.LoadChannelPTS(ctx, clientID, channelID)
	require.NoError(t, err)
	require.True(t, ptsFound)
	require.Equal(t, int32(500), loadedPTS)

	var iteratedChannels map[int64]int32
	err = reopenedStore.ForEachChannelPTS(ctx, clientID, func(chID int64, pts int32) error {
		if iteratedChannels == nil {
			iteratedChannels = make(map[int64]int32)
		}
		iteratedChannels[chID] = pts
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, map[int64]int32{channelID: 500}, iteratedChannels)
}

func TestBboltStateStorage_DialogRecovery_And_Hooks(t *testing.T) {
	t.Parallel()

	dbPath := filepath.Join(t.TempDir(), "test_recovery_flags.db")
	stateStore, closer, err := storage.NewDefaultStateStorage(dbPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()

	ctx := context.Background()
	const clientID = int64(111222)
	const dialogID = int64(333444)

	// 1. Verify initially not loaded
	notLoaded, err := stateStore.IsDialogNotLoaded(ctx, clientID, dialogID)
	require.NoError(t, err)
	require.False(t, notLoaded)

	// 2. Set not loaded to true
	err = stateStore.SetDialogNotLoaded(ctx, clientID, dialogID, true)
	require.NoError(t, err)

	notLoaded, err = stateStore.IsDialogNotLoaded(ctx, clientID, dialogID)
	require.NoError(t, err)
	require.True(t, notLoaded)

	// 3. Set not loaded to false (delete recovery flag)
	err = stateStore.SetDialogNotLoaded(ctx, clientID, dialogID, false)
	require.NoError(t, err)

	notLoaded, err = stateStore.IsDialogNotLoaded(ctx, clientID, dialogID)
	require.NoError(t, err)
	require.False(t, notLoaded)

	// 4. Exercise SetupUpdateHook
	peerPath := filepath.Join(t.TempDir(), "test_hook_peers.db")
	peerStore, peerCloser, err := storage.NewDefaultPeerStorage(peerPath)
	require.NoError(t, err)
	defer func() { require.NoError(t, peerCloser.Close()) }()

	called := false
	baseHandler := telegram.UpdateHandlerFunc(func(_ context.Context, _ tg.UpdatesClass) error {
		called = true
		return nil
	})
	hook := storage.SetupUpdateHook(baseHandler, peerStore)
	require.NotNil(t, hook)

	upd := &tg.Updates{
		Users: []tg.UserClass{
			&tg.User{ID: 10, AccessHash: 20, FirstName: "Alice"},
		},
	}
	err = hook.Handle(ctx, upd)
	require.NoError(t, err)
	require.True(t, called)
}
