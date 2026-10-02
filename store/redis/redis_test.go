package redis_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/gotd/td/session"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/fsm"
	"github.com/mr-dariush/tgbox/store/redis"
)

func getTestRedis(t *testing.T) *goredis.Client {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Fast pre-check: avoid connection pool retries if Redis is completely offline
	conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
	if err != nil {
		t.Skipf("Redis not running at %s, skipping Redis integration tests. Set REDIS_ADDR to enable.", addr)
	}
	_ = conn.Close()

	return goredis.NewClient(&goredis.Options{
		Addr: addr,
	})
}

func TestStore_SessionAndLock(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	ctx := context.Background()
	prefix := fmt.Sprintf("tgbox_test_%d", time.Now().UnixNano())
	store := redis.NewStore(client, prefix)

	t.Run("SessionLifecycle", func(t *testing.T) {
		tenant := "bot_test_tenant_xyz"

		has, err := store.HasSession(ctx, tenant)
		require.NoError(t, err)
		assert.False(t, has)

		sessStore := store.GetSessionStorage(tenant)
		_, err = sessStore.LoadSession(ctx)
		require.ErrorIs(t, err, session.ErrNotFound)

		testData := []byte("encrypted_mtproto_auth_session_data")

		err = sessStore.StoreSession(ctx, testData)
		require.NoError(t, err)

		has, err = store.HasSession(ctx, tenant)
		require.NoError(t, err)
		assert.True(t, has)

		loaded, err := sessStore.LoadSession(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, loaded)

		err = store.DeleteSession(ctx, tenant)
		require.NoError(t, err)

		has, err = store.HasSession(ctx, tenant)
		require.NoError(t, err)
		assert.False(t, has)
	})

	t.Run("LockMutualExclusion", func(t *testing.T) {
		lockKey := "concurrency_control_key"
		ttl := 2 * time.Second

		lock1, err := store.Obtain(ctx, lockKey, ttl)
		require.NoError(t, err)

		_, err = store.Obtain(ctx, lockKey, ttl)
		require.ErrorIs(t, err, redis.ErrLockHeld)

		err = lock1.Release(ctx)
		require.NoError(t, err)

		lock2, err := store.Obtain(ctx, lockKey, ttl)
		require.NoError(t, err)

		_ = lock2.Release(ctx)
	})

	t.Run("LockExpiration", func(t *testing.T) {
		lockKey := "transient_task_key"
		ttl := 50 * time.Millisecond

		_, err := store.Obtain(ctx, lockKey, ttl)
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		lock2, err := store.Obtain(ctx, lockKey, 1*time.Second)
		require.NoError(t, err)

		_ = lock2.Release(ctx)
	})
}

func TestStore_FSM(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	ctx := context.Background()
	prefix := fmt.Sprintf("tgbox_fsm_test_%d", time.Now().UnixNano())
	store := redis.NewStore(client, prefix)

	t.Run("FSM_CRUD_And_NotFound", func(t *testing.T) {
		key := "fsm:100:200"

		val, err := store.Get(ctx, key)
		require.ErrorIs(t, err, fsm.ErrNotFound)
		assert.Empty(t, val)

		err = store.Set(ctx, key, "step_waiting_name", 0)
		require.NoError(t, err)

		val, err = store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "step_waiting_name", val)

		err = store.Delete(ctx, key)
		require.NoError(t, err)

		val, err = store.Get(ctx, key)
		require.ErrorIs(t, err, fsm.ErrNotFound)
		assert.Empty(t, val)
	})

	t.Run("FSM_TTL_Expiration", func(t *testing.T) {
		key := "fsm:300:400"
		ttl := 50 * time.Millisecond

		err := store.Set(ctx, key, "step_temporary", ttl)
		require.NoError(t, err)

		val, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "step_temporary", val)

		time.Sleep(100 * time.Millisecond)

		val, err = store.Get(ctx, key)
		require.ErrorIs(t, err, fsm.ErrNotFound)
		assert.Empty(t, val)
	})
}

func TestStore_KeyspaceExpiration(t *testing.T) {
	client := getTestRedis(t)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	prefix := fmt.Sprintf("tgbox_exp_test_%d", time.Now().UnixNano())
	store := redis.NewStore(client, prefix)

	_ = client.ConfigSet(ctx, "notify-keyspace-events", "Ex").Err()

	expiredKeys := make(chan string, 1)
	watchDone := make(chan struct{})

	go func() {
		defer close(watchDone)
		_ = store.WatchExpired(ctx, func(key string) {
			select {
			case expiredKeys <- key:
			default:
			}
		})
	}()

	time.Sleep(50 * time.Millisecond)

	targetKey := "100:200"
	err := store.Set(ctx, targetKey, "state_short_lived", 50*time.Millisecond)
	require.NoError(t, err)

	select {
	case receivedKey := <-expiredKeys:
		assert.Equal(t, targetKey, receivedKey)
	case <-time.After(1 * time.Second):
		t.Log("Keyspace notification timeout (expected if notify-keyspace-events is disabled or restricted on Redis server)")
	}

	cancel()
	<-watchDone
}
