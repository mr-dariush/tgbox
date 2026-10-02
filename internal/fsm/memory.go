package fsm

import (
	"context"
	"sync"
	"time"
)

type memoryEntry struct {
	state     string
	expiresAt time.Time
}

// MemoryStorage implements fsm.Storage using an in-memory map.
// Expired entries are removed both lazily on Get and periodically by a
// background reaper goroutine.
type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string]memoryEntry

	closeOnce sync.Once
	done      chan struct{}
}

// NewMemoryStorage creates a MemoryStorage and starts its background reaper,
// which runs at reapInterval and stops when the provided context is canceled
// or Close is called.
func NewMemoryStorage(ctx context.Context, reapInterval time.Duration) *MemoryStorage {
	m := &MemoryStorage{
		data: make(map[string]memoryEntry),
		done: make(chan struct{}),
	}

	m.startReaper(ctx, reapInterval)
	return m
}

// Close stops the background reaper. It is safe to call multiple times.
func (m *MemoryStorage) Close() error {
	m.closeOnce.Do(func() { close(m.done) })
	return nil
}

// Set stores the conversational state for a key.
// If ttl is greater than 0, the state will expire after the given duration.
func (m *MemoryStorage) Set(_ context.Context, key, state string, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	m.data[key] = memoryEntry{
		state:     state,
		expiresAt: expiresAt,
	}
	return nil
}

// Get retrieves the state value for a session key.
// It applies Lazy Eviction: if the entry is expired, it is deleted instantly and ErrNotFound is returned.
func (m *MemoryStorage) Get(_ context.Context, key string) (string, error) {
	m.mu.RLock()
	entry, ok := m.data[key]
	m.mu.RUnlock()

	if !ok {
		return "", ErrNotFound
	}

	// Lazily evict the expired entry. Re-check under the write lock: a
	// concurrent Set may have refreshed the key between the two locks.
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		m.mu.Lock()
		if current, ok := m.data[key]; ok && current == entry {
			delete(m.data, key)
		}
		m.mu.Unlock()
		return "", ErrNotFound
	}

	return entry.state, nil
}

// Delete removes the state key from memory immediately.
func (m *MemoryStorage) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	delete(m.data, key)
	m.mu.Unlock()
	return nil
}

// startReaper launches the periodic background garbage collection routine.
func (m *MemoryStorage) startReaper(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-m.done:
				return
			case <-ticker.C:
				m.reap()
			}
		}
	}()
}

// reap clears all expired states. It acquires a write lock briefly.
func (m *MemoryStorage) reap() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for k, entry := range m.data {
		if !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			delete(m.data, k)
		}
	}
}
