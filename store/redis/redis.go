package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gotd/td/session"
	goredis "github.com/redis/go-redis/v9"

	"github.com/mr-dariush/tgbox"
	"github.com/mr-dariush/tgbox/internal/fsm"
)

// Compile-time verification that Store satisfies the tgbox contracts.
var (
	_ tgbox.Locker                    = (*Store)(nil)
	_ tgbox.MultiTenantSessionStorage = (*Store)(nil)
	_ fsm.Storage                     = (*Store)(nil)
	_ fsm.ExpirationWatcher           = (*Store)(nil)
)

// ErrLockHeld indicates that the distributed lock is currently acquired by another cluster node.
var ErrLockHeld = errors.New("redis: distributed lock is already held")

// Store provides multi-tenant session management, distributed FSM state storage,
// and distributed locking backed by Redis (go-redis/v9).
type Store struct {
	client *goredis.Client
	prefix string
}

// NewStore instantiates a new Redis-backed clustered storage manager.
// The prefix parameter isolates keyspaces (e.g., "tgbox:prod") preventing collisions.
func NewStore(client *goredis.Client, prefix string) *Store {
	return &Store{
		client: client,
		prefix: prefix,
	}
}

type redisSessionStorage struct {
	client *goredis.Client
	key    string
}

// LoadSession fetches the raw MTProto session data, converting redis.Nil to session.ErrNotFound.
func (s *redisSessionStorage) LoadSession(ctx context.Context) ([]byte, error) {
	data, err := s.client.Get(ctx, s.key).Bytes()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, session.ErrNotFound
		}
		return nil, fmt.Errorf("load session %q: %w", s.key, err)
	}
	return data, nil
}

// StoreSession persists the raw MTProto session bytes without expiration.
func (s *redisSessionStorage) StoreSession(ctx context.Context, data []byte) error {
	if err := s.client.Set(ctx, s.key, data, 0).Err(); err != nil {
		return fmt.Errorf("store session %q: %w", s.key, err)
	}
	return nil
}

// GetSessionStorage dynamically maps and returns a thread-safe gotd session.Storage.
func (s *Store) GetSessionStorage(tenantKey string) session.Storage {
	fullKey := fmt.Sprintf("%s:session:%s", s.prefix, tenantKey)
	return &redisSessionStorage{
		client: s.client,
		key:    fullKey,
	}
}

// DeleteSession purges the session bytes from Redis for a specific tenant.
func (s *Store) DeleteSession(ctx context.Context, tenantKey string) error {
	fullKey := fmt.Sprintf("%s:session:%s", s.prefix, tenantKey)
	if err := s.client.Del(ctx, fullKey).Err(); err != nil {
		return fmt.Errorf("delete session %q: %w", fullKey, err)
	}
	return nil
}

// HasSession checks if the session data exists in Redis for the given tenant.
func (s *Store) HasSession(ctx context.Context, tenantKey string) (bool, error) {
	fullKey := fmt.Sprintf("%s:session:%s", s.prefix, tenantKey)
	count, err := s.client.Exists(ctx, fullKey).Result()
	if err != nil {
		return false, fmt.Errorf("check session %q: %w", fullKey, err)
	}
	return count > 0, nil
}

func (s *Store) fsmKey(key string) string {
	normalized := strings.TrimPrefix(key, "fsm:")
	return fmt.Sprintf("%s:fsm:%s", s.prefix, normalized)
}

// Set stores the conversational FSM state with an optional TTL.
// When ttl <= 0, the state is stored persistently without expiration.
func (s *Store) Set(ctx context.Context, key, state string, ttl time.Duration) error {
	fullKey := s.fsmKey(key)
	var expiration time.Duration
	if ttl > 0 {
		expiration = ttl
	}
	if err := s.client.Set(ctx, fullKey, state, expiration).Err(); err != nil {
		return fmt.Errorf("fsm set %q: %w", fullKey, err)
	}
	return nil
}

// Get retrieves the conversational state, cleanly mapping goredis.Nil to fsm.ErrNotFound.
func (s *Store) Get(ctx context.Context, key string) (string, error) {
	fullKey := s.fsmKey(key)
	val, err := s.client.Get(ctx, fullKey).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return "", fsm.ErrNotFound
		}
		return "", fmt.Errorf("fsm get %q: %w", fullKey, err)
	}
	return val, nil
}

// Delete removes the conversational state key immediately.
func (s *Store) Delete(ctx context.Context, key string) error {
	fullKey := s.fsmKey(key)
	if err := s.client.Del(ctx, fullKey).Err(); err != nil {
		return fmt.Errorf("fsm delete %q: %w", fullKey, err)
	}
	return nil
}

// WatchExpired subscribes to Redis keyspace expiration notifications (__keyevent@*__:expired)
// and invokes handler for every expired key belonging to this store's FSM namespace.
// It guarantees instant, leak-free termination when ctx is canceled by closing the pubsub socket.
func (s *Store) WatchExpired(ctx context.Context, handler func(key string)) error {
	if s.client == nil {
		return errors.New("tgbox/redis: redis client is nil")
	}

	// Attempt best-effort configuration enablement for keyspace expiration events.
	_ = s.client.ConfigSet(ctx, "notify-keyspace-events", "Ex").Err()

	pubsub := s.client.PSubscribe(ctx, "__keyevent@*__:expired")
	done := make(chan struct{})
	defer func() {
		close(done)
		_ = pubsub.Close()
	}()

	// Guarantee immediate unblocking of socket reads when context cancels
	go func() {
		select {
		case <-ctx.Done():
			_ = pubsub.Close()
		case <-done:
		}
	}()

	fsmPrefix := fmt.Sprintf("%s:fsm:", s.prefix)

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("tgbox/redis: receive keyspace message: %w", err)
		}

		if !strings.HasPrefix(msg.Payload, fsmPrefix) {
			continue
		}

		if handler != nil {
			handler(strings.TrimPrefix(msg.Payload, fsmPrefix))
		}
	}
}

var luaRelease = goredis.NewScript(`
	if redis.call("get", KEYS[1]) == ARGV[1] then
		return redis.call("del", KEYS[1])
	else
		return 0
	end
`)

// Lock represents an active distributed lock held by a specific node.
type Lock struct {
	client *goredis.Client
	key    string
	value  string
}

// Release unlocks the distributed resource atomically via luaRelease.
func (l *Lock) Release(ctx context.Context) error {
	if _, err := luaRelease.Run(ctx, l.client, []string{l.key}, l.value).Result(); err != nil {
		return fmt.Errorf("release lock %q: %w", l.key, err)
	}
	return nil
}

// Obtain attempts to acquire an exclusive distributed lock using SET NX PX.
// It returns ErrLockHeld if another node currently holds the lock.
func (s *Store) Obtain(ctx context.Context, key string, ttl time.Duration) (tgbox.Lock, error) {
	lockKey := fmt.Sprintf("%s:lock:%s", s.prefix, key)

	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("generate lock value: %w", err)
	}
	lockValue := hex.EncodeToString(buf)

	ok, err := s.client.SetNX(ctx, lockKey, lockValue, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire lock %q: %w", lockKey, err)
	}
	if !ok {
		return nil, ErrLockHeld
	}

	return &Lock{
		client: s.client,
		key:    lockKey,
		value:  lockValue,
	}, nil
}
