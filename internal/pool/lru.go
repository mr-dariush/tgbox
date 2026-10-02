package pool

import (
	"container/list"
	"sync"
)

// EvictCallback is invoked when an entry is removed from the LRU cache,
// either by capacity pressure or an explicit Remove. It runs while the cache
// mutex is held, so it must not call back into the cache.
type EvictCallback[K comparable, V any] func(key K, value V)

// LRU is a thread-safe least-recently-used cache.
type LRU[K comparable, V any] struct {
	capacity int
	evict    EvictCallback[K, V]

	mu        sync.Mutex
	items     map[K]*list.Element
	evictList *list.List
}

// entry is the key-value pair stored in the linked list elements.
type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewLRU constructs an LRU cache with the given capacity. Capacity must be
// positive; the caller (NewManager) validates it.
func NewLRU[K comparable, V any](capacity int, onEvict EvictCallback[K, V]) *LRU[K, V] {
	if capacity <= 0 {
		panic("tgbox/pool: LRU capacity must be positive")
	}
	return &LRU[K, V]{
		capacity:  capacity,
		evict:     onEvict,
		items:     make(map[K]*list.Element),
		evictList: list.New(),
	}
}

// Put inserts or updates a value in the cache, promoting it to the front.
// Returns true if the capacity was exceeded and an eviction occurred.
func (c *LRU[K, V]) Put(key K, value V) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Update and promote existing entry
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		ent.Value.(*entry[K, V]).value = value
		return false
	}

	// 2. Insert new entry at the front
	ent := &entry[K, V]{key, value}
	element := c.evictList.PushFront(ent)
	c.items[key] = element

	// 3. Enforce capacity limit via LRU eviction
	evicted := false
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
		evicted = true
	}
	return evicted
}

// Get retrieves a key's value, promoting it as recently used.
// It achieves an O(1) retrieval leveraging the internal map.
func (c *LRU[K, V]) Get(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var zero V
	if ent, ok := c.items[key]; ok {
		c.evictList.MoveToFront(ent)
		return ent.Value.(*entry[K, V]).value, true
	}
	return zero, false
}

// Remove deletes the key from the cache, invoking the eviction callback.
func (c *LRU[K, V]) Remove(key K) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.items[key]; ok {
		c.removeElement(ent)
		return true
	}
	return false
}

// Len returns the current number of cached entries.
func (c *LRU[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.evictList.Len()
}

// removeOldest pops the tail element from the linked list.
// Caller must hold the mutex.
func (c *LRU[K, V]) removeOldest() {
	ent := c.evictList.Back()
	if ent != nil {
		c.removeElement(ent)
	}
}

// removeElement deletes a linked list element and its map reference, then
// runs the eviction callback. Caller must hold the mutex; running the
// callback inside the lock keeps eviction atomic with respect to an
// immediate re-add of the same key.
func (c *LRU[K, V]) removeElement(e *list.Element) {
	c.evictList.Remove(e)
	kv := e.Value.(*entry[K, V])
	delete(c.items, kv.key)

	if c.evict != nil {
		c.evict(kv.key, kv.value)
	}
}
