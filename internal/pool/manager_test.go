package pool

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox"
)

// mockEngine implements the engine.Engine interface specifically for pool tests,
// ensuring that the connection phase blocks indefinitely until the context is canceled.
type mockEngine struct{}

// Connect blocks until the context is canceled to prevent the client's Run loop from exiting.
func (*mockEngine) Connect(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Disconnect is a mock implementation of the Disconnect method.
func (*mockEngine) Disconnect() error {
	return nil
}

// API returns a nil tg.Client.
func (*mockEngine) API() *tg.Client {
	return nil
}

// Raw returns a nil telegram.Client.
func (*mockEngine) Raw() *telegram.Client {
	return nil
}

// SetUpdateHandler is a mock implementation of the SetUpdateHandler method.
func (*mockEngine) SetUpdateHandler(_ telegram.UpdateHandler) {}

// testBuilder returns a ClientBuilder that counts invocations and keeps the
// clients' storage files inside a per-test temporary directory.
func testBuilder(t *testing.T, buildCount *atomic.Int32) ClientBuilder {
	t.Helper()
	dir := t.TempDir()
	return func(token string, _ func()) (*tgbox.Client, error) {
		buildCount.Inc()

		mock := &mockEngine{}
		client, err := tgbox.New(filepath.Join(dir, "test_app_"+token),
			tgbox.WithAppID(123),
			tgbox.WithAppHash("hash"),
			tgbox.WithAutoPeerCaching(false),
			tgbox.WithEngine(mock),
		)
		if err != nil {
			return nil, err
		}

		return client, nil
	}
}

// TestManager_Singleflight verifies that concurrent Get requests for the
// same token execute the ClientBuilder exactly once.
func TestManager_Singleflight(t *testing.T) {
	var buildCount atomic.Int32
	mgr, err := NewManager(10, 5*time.Minute, testBuilder(t, &buildCount))
	require.NoError(t, err)
	defer mgr.Close()

	const concurrentRequests = 100
	var wg sync.WaitGroup
	wg.Add(concurrentRequests)

	// Spawn concurrent goroutines attempting to load the exact same bot token
	for range concurrentRequests {
		go func() {
			defer wg.Done()
			_, err := mgr.Get("bot_token_abc")
			assert.NoError(t, err)
		}()
	}

	wg.Wait()

	// Assert that singleflight grouped all 100 requests into exactly 1 build invocation
	assert.Equal(t, int32(1), buildCount.Load())
}

// TestManager_LRUEviction verifies that exceeding the pool capacity bounds
// cleanly evicts the oldest unused client from memory.
func TestManager_LRUEviction(t *testing.T) {
	var buildCount atomic.Int32

	// Bounded pool with a maximum capacity of exactly 2 bot clients
	mgr, err := NewManager(2, 5*time.Minute, testBuilder(t, &buildCount))
	require.NoError(t, err)
	defer mgr.Close()

	// 1. Load bot1 (buildCount = 1) -> Cache: [bot1]
	_, err = mgr.Get("bot1")
	require.NoError(t, err)

	// 2. Load bot2 (buildCount = 2) -> Cache: [bot2, bot1]
	_, err = mgr.Get("bot2")
	require.NoError(t, err)

	assert.Equal(t, int32(2), buildCount.Load())

	// 3. Load bot3 (buildCount = 3) -> Exceeds capacity! bot1 (oldest) must be evicted -> Cache: [bot3, bot2]
	_, err = mgr.Get("bot3")
	require.NoError(t, err)
	assert.Equal(t, int32(3), buildCount.Load())

	// 3.5. Access bot2 to promote it to the front of the LRU queue -> Cache: [bot2, bot3]
	_, err = mgr.Get("bot2")
	require.NoError(t, err)
	assert.Equal(t, int32(3), buildCount.Load())

	// 4. Request bot1 again. Since it was evicted, it must trigger a RE-BUILD (buildCount = 4)
	// Putting bot1 evicts the oldest remaining, which is now bot3 -> Cache: [bot1, bot2]
	_, err = mgr.Get("bot1")
	require.NoError(t, err)
	assert.Equal(t, int32(4), buildCount.Load())

	// 5. Request bot2 again. It is still recently used and cached, so buildCount must remain 4
	_, err = mgr.Get("bot2")
	require.NoError(t, err)
	assert.Equal(t, int32(4), buildCount.Load())
}

// TestManager_IdleGC verifies that the background sweeper loop terminates
// and evicts clients that have remained completely inactive for the timeout period.
func TestManager_IdleGC(t *testing.T) {
	var buildCount atomic.Int32

	// Configure an aggressive idle timeout of 50 milliseconds
	idleTimeout := 50 * time.Millisecond
	mgr, err := NewManager(10, idleTimeout, testBuilder(t, &buildCount))
	require.NoError(t, err)
	defer mgr.Close()

	// 1. Initial Load (buildCount = 1)
	_, err = mgr.Get("temporary_bot")
	require.NoError(t, err)
	assert.Equal(t, int32(1), buildCount.Load())

	// 2. Sleep to guarantee the background GC ticker sweep executes after inactivity
	time.Sleep(150 * time.Millisecond)

	// 3. Requesting the same bot again MUST trigger a rebuild because the GC cleared it
	_, err = mgr.Get("temporary_bot")
	require.NoError(t, err)
	assert.Equal(t, int32(2), buildCount.Load())
}
