package tgbox

import (
	"context"
	"time"
)

// Lock represents a handle to an acquired distributed lock.
// It must be released gracefully to allow other cluster replicas to manage the tenant.
type Lock interface {
	// Release unlocks the distributed resource.
	Release(ctx context.Context) error
}

// Locker defines the distributed locking contract required to prevent
// race conditions and session corruption during concurrent cluster logins.
type Locker interface {
	// Obtain attempts to acquire a lock for a given key with a strict TTL.
	// If the lock is already acquired by another node, implementations
	// return their held-lock sentinel (e.g. redis.ErrLockHeld).
	Obtain(ctx context.Context, key string, ttl time.Duration) (Lock, error)
}
