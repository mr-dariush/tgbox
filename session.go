package tgbox

import (
	"context"

	"github.com/gotd/td/session"
)

// SessionStorage is the contract for persisting a single MTProto session.
// It aliases gotd's session.Storage so implementations from gotd/contrib
// (file, bbolt, redis, ...) plug in directly.
type SessionStorage = session.Storage

// WithSessionStorage sets a custom session storage backend. When omitted,
// a bbolt database derived from the app name is created next to the binary.
func WithSessionStorage(s SessionStorage) Option {
	return fnOption(func(o *clientOptions) { o.engineOpts.SessionStorage = s })
}

// MultiTenantSessionStorage abstracts session management, allowing tgbox to dynamically map
// gotd session.Storage adapters based on unique tenant identifier keys (e.g., bot tokens, user IDs).
//
// The interface is defined in the consumer package (tgbox root) following Go's implicit
// interface convention. Concrete implementations (e.g., store/redis.Store) satisfy it
// without importing this package.
type MultiTenantSessionStorage interface {
	// GetSessionStorage returns a thread-safe gotd session.Storage bound to a specific tenant.
	GetSessionStorage(tenantKey string) SessionStorage

	// DeleteSession permanently purges the stored MTProto session bytes for a specific tenant.
	DeleteSession(ctx context.Context, tenantKey string) error

	// HasSession verifies if the session data exists for the given tenant without loading it.
	HasSession(ctx context.Context, tenantKey string) (bool, error)
}
