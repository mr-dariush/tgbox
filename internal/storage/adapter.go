// Package storage provides storage implementations for sessions and peers.
package storage

import (
	"fmt"
	"io"

	"github.com/gotd/contrib/bbolt"
	"github.com/gotd/contrib/storage"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	bolt "go.etcd.io/bbolt"
)

// PeerStorage is an alias for the gotd/contrib peer storage interface.
type PeerStorage = storage.PeerStorage

// NewDefaultPeerStorage creates a bbolt-based peer storage at the given path.
// The returned closer releases the underlying database file.
func NewDefaultPeerStorage(path string) (PeerStorage, io.Closer, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open bbolt %q: %w", path, err)
	}
	return bbolt.NewPeerStorage(db, []byte("peers")), db, nil
}

// NewDefaultSessionStorage creates a bbolt-based session storage at the given
// path. The returned closer releases the underlying database file.
func NewDefaultSessionStorage(path string) (session.Storage, io.Closer, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open bbolt %q: %w", path, err)
	}

	// The bucket name must match gotd/contrib/bbolt's expectations.
	bucketName := []byte("sessions")

	err = db.Update(func(tx *bolt.Tx) error {
		if _, berr := tx.CreateBucketIfNotExists(bucketName); berr != nil {
			return fmt.Errorf("create bucket: %w", berr)
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("create session bucket: %w", err)
	}

	return bbolt.NewSessionStorage(db, string(bucketName), []byte("sessions")), db, nil
}

// SetupUpdateHook wraps an update handler so peers seen in updates are
// persisted to the peer storage.
func SetupUpdateHook(next telegram.UpdateHandler, p PeerStorage) telegram.UpdateHandler {
	return storage.UpdateHook(next, p)
}
