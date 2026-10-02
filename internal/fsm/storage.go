// Package fsm implements a conversational Finite State Machine persistence layer.
package fsm

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned when a state key does not exist or has expired.
var ErrNotFound = errors.New("fsm: state not found")

// Storage defines the interface for persisting conversational states across restarts.
type Storage interface {
	// Set stores the state value for a session key with a specific TTL.
	// A TTL of 0 indicates no expiration.
	Set(ctx context.Context, key, state string, ttl time.Duration) error

	// Get retrieves the state value for a session key.
	// Returns ErrNotFound if the state is missing or expired.
	Get(ctx context.Context, key string) (string, error)

	// Delete removes the state associated with the session key.
	Delete(ctx context.Context, key string) error
}

// ExpirationWatcher defines an optional extension for storage drivers capable of
// observing and dispatching notifications when a key expires in distributed storage.
type ExpirationWatcher interface {
	// WatchExpired registers a reactive callback invoked whenever an FSM key expires.
	// It blocks until the context is canceled or a fatal subscription error occurs.
	WatchExpired(ctx context.Context, handler func(key string)) error
}
