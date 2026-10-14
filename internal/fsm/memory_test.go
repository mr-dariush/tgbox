package fsm_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/fsm"
)

// TestMemoryStorage_BasicOperations verifies standard CRUD operations without expiration.
func TestMemoryStorage_BasicOperations(t *testing.T) {
	ctx := t.Context()

	// Initialize store with a long reaper cycle so it doesn't interfere
	store := fsm.NewMemoryStorage(ctx, 5*time.Minute)

	// Set state
	err := store.Set(ctx, "user:1234:state", "waiting_for_name", 0)
	require.NoError(t, err)

	// Get state
	val, err := store.Get(ctx, "user:1234:state")
	require.NoError(t, err)
	assert.Equal(t, "waiting_for_name", val)

	// Delete state
	err = store.Delete(ctx, "user:1234:state")
	require.NoError(t, err)

	// Get missing state
	_, err = store.Get(ctx, "user:1234:state")
	assert.ErrorIs(t, err, fsm.ErrNotFound)
}

// TestMemoryStorage_LazyEviction verifies that expired items are evicted during read.
func TestMemoryStorage_LazyEviction(t *testing.T) {
	ctx := t.Context()

	store := fsm.NewMemoryStorage(ctx, 5*time.Minute)

	// Set with short 10ms TTL
	err := store.Set(ctx, "user:5678:state", "waiting_for_age", 10*time.Millisecond)
	require.NoError(t, err)

	// Wait for TTL to expire
	time.Sleep(25 * time.Millisecond)

	// Get should trigger lazy eviction and return ErrNotFound
	_, err = store.Get(ctx, "user:5678:state")
	assert.ErrorIs(t, err, fsm.ErrNotFound)
}

// TestMemoryStorage_BackgroundReaper verifies that the background reaper cleans up
// expired items automatically even without read calls.
func TestMemoryStorage_BackgroundReaper(t *testing.T) {
	ctx := t.Context()

	// High frequency reaping: every 10ms
	store := fsm.NewMemoryStorage(ctx, 10*time.Millisecond)

	// Set with short 5ms TTL
	err := store.Set(ctx, "user:9999:state", "waiting_for_photo", 5*time.Millisecond)
	require.NoError(t, err)

	// Wait long enough for the reaper to tick and sweep the expired entry
	time.Sleep(30 * time.Millisecond)

	// Assert that the swept entry is gone
	_, err = store.Get(ctx, "user:9999:state")
	assert.ErrorIs(t, err, fsm.ErrNotFound)
}

func TestMemoryStorage_Move(t *testing.T) {
	ctx := t.Context()
	store := fsm.NewMemoryStorage(ctx, 5*time.Minute)
	defer func() { require.NoError(t, store.Close()) }()

	// 1. Move missing source key must return ErrNotFound
	err := store.Move(ctx, "non_existent_key", "target_key")
	require.ErrorIs(t, err, fsm.ErrNotFound)

	// 2. Move existing valid key
	err = store.Set(ctx, "chat:100:user:1", "state_checkout", time.Hour)
	require.NoError(t, err)

	err = store.Move(ctx, "chat:100:user:1", "channel:200:user:1")
	require.NoError(t, err)

	// Old key must be deleted
	_, err = store.Get(ctx, "chat:100:user:1")
	require.ErrorIs(t, err, fsm.ErrNotFound)

	// New key must hold state
	val, err := store.Get(ctx, "channel:200:user:1")
	require.NoError(t, err)
	assert.Equal(t, "state_checkout", val)

	// 3. Move expired key must trigger deletion and return ErrNotFound
	err = store.Set(ctx, "expiring_key", "state_transient", 5*time.Millisecond)
	require.NoError(t, err)

	time.Sleep(20 * time.Millisecond)

	err = store.Move(ctx, "expiring_key", "target_exp_key")
	require.ErrorIs(t, err, fsm.ErrNotFound)

	_, err = store.Get(ctx, "expiring_key")
	require.ErrorIs(t, err, fsm.ErrNotFound)
}
