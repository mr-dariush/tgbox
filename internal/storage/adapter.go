// Package storage provides storage implementations for sessions and peers.
package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/gotd/contrib/bbolt"
	"github.com/gotd/contrib/storage"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	bolt "go.etcd.io/bbolt"

	"github.com/mr-dariush/tgbox/internal/syncstate"
)

// BboltStateStorage implements syncstate.StateStorage using bbolt for robust persistence.
type BboltStateStorage struct {
	db *bolt.DB
}

var _ syncstate.StateStorage = (*BboltStateStorage)(nil)

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

// NewBboltStateStorage creates a new state storage over a bolt database.
func NewBboltStateStorage(db *bolt.DB) (*BboltStateStorage, error) {
	err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("syncstate_global")); err != nil {
			return fmt.Errorf("create global bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("syncstate_channels")); err != nil {
			return fmt.Errorf("create channels bucket: %w", err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("syncstate_dialogs_recovery")); err != nil {
			return fmt.Errorf("create dialogs recovery bucket: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BboltStateStorage{db: db}, nil
}

// NewDefaultStateStorage creates a bbolt-based sync state storage at the given path.
// The returned closer releases the underlying database file.
func NewDefaultStateStorage(path string) (*BboltStateStorage, io.Closer, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("open bbolt %q: %w", path, err)
	}

	stateStorage, err := NewBboltStateStorage(db)
	if err != nil {
		_ = db.Close()
		return nil, nil, err
	}
	return stateStorage, db, nil
}

func (*BboltStateStorage) clientKey(clientID int64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(clientID))
	return key
}

func (*BboltStateStorage) channelKey(clientID, channelID int64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key[0:8], uint64(clientID))
	binary.BigEndian.PutUint64(key[8:16], uint64(channelID))
	return key
}

// SaveState stores the global state snapshot robustly.
func (b *BboltStateStorage) SaveState(_ context.Context, clientID int64, snapshot syncstate.StateSnapshot) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_global"))
		if bucket == nil {
			return errors.New("syncstate_global bucket not found")
		}
		data := make([]byte, 16)
		binary.BigEndian.PutUint32(data[0:4], uint32(snapshot.PTS))
		binary.BigEndian.PutUint32(data[4:8], uint32(snapshot.QTS))
		binary.BigEndian.PutUint32(data[8:12], uint32(snapshot.Date))
		binary.BigEndian.PutUint32(data[12:16], uint32(snapshot.Seq))
		return bucket.Put(b.clientKey(clientID), data)
	})
}

// LoadState retrieves the global state snapshot.
func (b *BboltStateStorage) LoadState(_ context.Context, clientID int64) (syncstate.StateSnapshot, bool, error) {
	var snapshot syncstate.StateSnapshot
	var found bool
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_global"))
		if bucket == nil {
			return errors.New("syncstate_global bucket not found")
		}
		data := bucket.Get(b.clientKey(clientID))
		if len(data) >= 16 {
			snapshot.PTS = int32(binary.BigEndian.Uint32(data[0:4]))
			snapshot.QTS = int32(binary.BigEndian.Uint32(data[4:8]))
			snapshot.Date = int32(binary.BigEndian.Uint32(data[8:12]))
			snapshot.Seq = int32(binary.BigEndian.Uint32(data[12:16]))
			found = true
		}
		return nil
	})
	return snapshot, found, err
}

// SaveChannelPTS stores the channel PTS atomically.
func (b *BboltStateStorage) SaveChannelPTS(_ context.Context, clientID, channelID int64, pts int32) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_channels"))
		if bucket == nil {
			return errors.New("syncstate_channels bucket not found")
		}
		data := make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(pts))
		return bucket.Put(b.channelKey(clientID, channelID), data)
	})
}

// LoadChannelPTS retrieves the channel PTS.
func (b *BboltStateStorage) LoadChannelPTS(_ context.Context, clientID, channelID int64) (int32, bool, error) {
	var pts int32
	var found bool
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_channels"))
		if bucket == nil {
			return errors.New("syncstate_channels bucket not found")
		}
		data := bucket.Get(b.channelKey(clientID, channelID))
		if len(data) >= 4 {
			pts = int32(binary.BigEndian.Uint32(data))
			found = true
		}
		return nil
	})
	return pts, found, err
}

// ForEachChannelPTS iterates over all channel PTS values for a client.
func (b *BboltStateStorage) ForEachChannelPTS(_ context.Context, clientID int64, fn func(channelID int64, pts int32) error) error {
	return b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_channels"))
		if bucket == nil {
			return errors.New("syncstate_channels bucket not found")
		}
		prefix := b.clientKey(clientID)
		cursor := bucket.Cursor()
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			if len(k) == 16 && len(v) >= 4 {
				channelID := int64(binary.BigEndian.Uint64(k[8:16]))
				pts := int32(binary.BigEndian.Uint32(v))
				if err := fn(channelID, pts); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (b *BboltStateStorage) SetDialogNotLoaded(_ context.Context, clientID, dialogID int64, notLoaded bool) error {
	return b.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_dialogs_recovery"))
		if bucket == nil {
			return errors.New("syncstate_dialogs_recovery bucket not found")
		}
		key := b.channelKey(clientID, dialogID)
		if notLoaded {
			return bucket.Put(key, []byte{1})
		}
		return bucket.Delete(key)
	})
}

// IsDialogNotLoaded retrieves the recovery flag for a dialog.
func (b *BboltStateStorage) IsDialogNotLoaded(_ context.Context, clientID, dialogID int64) (bool, error) {
	var notLoaded bool
	err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("syncstate_dialogs_recovery"))
		if bucket == nil {
			return errors.New("syncstate_dialogs_recovery bucket not found")
		}
		data := bucket.Get(b.channelKey(clientID, dialogID))
		notLoaded = len(data) > 0 && data[0] == 1
		return nil
	})
	return notLoaded, err
}
