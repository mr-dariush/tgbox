package storage_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/storage"
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
