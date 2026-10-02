// Package pool manages a dynamic fleet of tgbox clients with LRU capacity
// bounds and idle garbage collection.
package pool

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.uber.org/atomic"
	"golang.org/x/sync/singleflight"

	"github.com/mr-dariush/tgbox"
)

// tokenSuffix returns the last few characters of a bot token for logging;
// tokens are credentials and must never be logged in full.
func tokenSuffix(token string) string {
	const keep = 6
	if len(token) <= keep {
		return token
	}
	return "…" + token[len(token)-keep:]
}

// ClientBuilder is the factory used to construct a new isolated bot client.
// The builder must wire keepAlive into the client's traffic observation
// (tgbox.WithTrafficObserver) so active clients are not reaped by the idle GC.
type ClientBuilder func(token string, keepAlive func()) (*tgbox.Client, error)

// managedClient represents a single active Telegram bot connection in the pool.
type managedClient struct {
	token      string
	client     *tgbox.Client
	lastActive atomic.Int64
	cancel     context.CancelFunc
}

// keepAlive refreshes the client's idle timer, deferring its garbage collection.
func (m *managedClient) keepAlive() {
	m.lastActive.Store(time.Now().UnixNano())
}

// Manager orchestrates a fleet of tgbox clients. Capacity is bounded by an
// LRU cache, concurrent Get calls for the same token are deduplicated with
// singleflight, and a background loop reaps idle connections.
type Manager struct {
	lru         *LRU[string, *managedClient]
	active      sync.Map
	sfg         singleflight.Group
	builder     ClientBuilder
	idleTimeout time.Duration
	done        chan struct{}
	closeOnce   sync.Once
	wg          sync.WaitGroup
}

// NewManager creates a pool manager. capacity must be positive. If
// idleTimeout is not positive, idle garbage collection is disabled and
// clients are only evicted by LRU capacity pressure.
func NewManager(capacity int, idleTimeout time.Duration, builder ClientBuilder) (*Manager, error) {
	if capacity <= 0 {
		return nil, errors.New("tgbox/pool: capacity must be positive")
	}
	if builder == nil {
		return nil, errors.New("tgbox/pool: builder must not be nil")
	}

	m := &Manager{
		builder:     builder,
		idleTimeout: idleTimeout,
		done:        make(chan struct{}),
	}

	m.lru = NewLRU[string, *managedClient](capacity, m.onEvict)

	if idleTimeout > 0 {
		m.wg.Add(1)
		go m.gcLoop()
	}

	return m, nil
}

// onEvict is invoked by the LRU on capacity eviction or explicit removal.
// Canceling the client context makes its Run loop return; the goroutine that
// started Run then releases the client's resources.
func (m *Manager) onEvict(token string, mc *managedClient) {
	m.active.Delete(token)
	mc.cancel()
}

// Get returns the client for the token, creating and starting it if needed.
func (m *Manager) Get(token string) (*tgbox.Client, error) {
	// Fast path: already pooled.
	if mc, ok := m.lru.Get(token); ok {
		mc.keepAlive()
		return mc.client, nil
	}

	// Singleflight collapses concurrent creations of the same token.
	res, err, _ := m.sfg.Do(token, func() (any, error) {
		if mc, ok := m.lru.Get(token); ok {
			return mc, nil
		}

		// Each pooled client instance maintains its own root cancellation context.
		clientCtx, clientCancel := context.WithCancel(context.Background())

		mc := &managedClient{
			token:  token,
			cancel: clientCancel,
		}
		mc.keepAlive()

		client, err := m.builder(token, mc.keepAlive)
		if err != nil {
			clientCancel()
			return nil, err
		}
		mc.client = client

		// Register before starting Run, so a client whose Run exits
		// immediately (e.g. invalid token) still removes itself below.
		m.active.Store(token, mc)
		m.lru.Put(token, mc)

		m.wg.Go(func() {
			if err := client.Run(clientCtx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("tgbox/pool: client run ended with error",
					slog.String("token_suffix", tokenSuffix(token)),
					slog.Any("error", err),
				)
			}
			// Run has ended (error, eviction, or shutdown): drop the pool
			// entry so Get does not hand out a dead client, and release the
			// client's storage resources.
			m.lru.Remove(token)
			_ = client.Close()
		})

		return mc, nil
	})

	if err != nil {
		return nil, fmt.Errorf("tgbox/pool: create client: %w", err)
	}

	mc := res.(*managedClient)
	mc.keepAlive()
	return mc.client, nil
}

// gcLoop periodically evicts clients that have been idle for idleTimeout.
func (m *Manager) gcLoop() {
	defer m.wg.Done()

	// Tick at half the timeout so an idle client is evicted at most 1.5x
	// idleTimeout after its last activity.
	interval := m.idleTimeout / 2
	if interval <= 0 {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.done: // CHANGED: Listen to the signal channel instead of stored context
			return
		case <-ticker.C:
			now := time.Now()
			m.active.Range(func(_, value any) bool {
				mc := value.(*managedClient)
				lastActive := time.Unix(0, mc.lastActive.Load())
				if now.Sub(lastActive) > m.idleTimeout {
					m.lru.Remove(mc.token)
				}
				return true
			})
		}
	}
}

// Close cleanly shuts down the manager and all active clients.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		close(m.done)

		m.active.Range(func(_, value any) bool {
			if mc, ok := value.(*managedClient); ok {
				mc.cancel()
			}
			return true
		})
	})

	m.wg.Wait()
}
