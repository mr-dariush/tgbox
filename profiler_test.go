package tgbox_test

import (
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox"
)

func TestProfiler_Lifecycle(t *testing.T) {
	t.Parallel()

	cfg := tgbox.ProfilerConfig{
		Enabled:            true,
		HTTPAddr:           "127.0.0.1:0",
		ScanInterval:       50 * time.Millisecond,
		GoroutineThreshold: 10000,
	}

	profiler := tgbox.NewProfiler(cfg, nil)
	require.NotNil(t, profiler)
	defer func() { require.NoError(t, profiler.Close()) }()

	addr := profiler.Addr()
	require.NotEmpty(t, addr)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/debug/pprof/", addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "Types of profiles available:")
}

func TestProfiler_WatchdogThreshold(t *testing.T) {
	t.Parallel()

	var exceeded atomic.Int32
	cfg := tgbox.ProfilerConfig{
		Enabled:            true,
		ScanInterval:       10 * time.Millisecond,
		GoroutineThreshold: 1,
		OnThresholdExceeded: func(_ int) {
			exceeded.Inc()
		},
	}

	profiler := tgbox.NewProfiler(cfg, nil)
	require.NotNil(t, profiler)
	defer func() { require.NoError(t, profiler.Close()) }()

	assert.Eventually(t, func() bool {
		return exceeded.Load() > 0
	}, 2*time.Second, 20*time.Millisecond)

	assert.Positive(t, profiler.NumGoroutine())
}

func TestProfiler_GoroutineLeakEndpoint(t *testing.T) {
	t.Parallel()

	cfg := tgbox.ProfilerConfig{
		Enabled:  true,
		HTTPAddr: "127.0.0.1:0",
	}

	profiler := tgbox.NewProfiler(cfg, nil)
	require.NotNil(t, profiler)
	defer func() { require.NoError(t, profiler.Close()) }()

	addr := profiler.Addr()
	require.NotEmpty(t, addr)

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(fmt.Sprintf("http://%s/debug/pprof/goroutineleak", addr))
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.NotEmpty(t, string(body))
}

func TestProfiler_GoroutineLeakDetection(t *testing.T) {
	t.Parallel()

	var detected atomic.Int32
	cfg := tgbox.ProfilerConfig{
		Enabled:      true,
		ScanInterval: 10 * time.Millisecond,
		OnLeakDetected: func(count int) {
			detected.Add(int32(count))
		},
	}

	profiler := tgbox.NewProfiler(cfg, nil)
	require.NotNil(t, profiler)
	defer func() { require.NoError(t, profiler.Close()) }()

	leaks := profiler.LeakedGoroutines()
	assert.GreaterOrEqual(t, leaks, 0)
}

func TestClient_WithProfilerIntegration(t *testing.T) {
	t.Parallel()

	cfg := tgbox.ProfilerConfig{
		Enabled:            true,
		HTTPAddr:           "127.0.0.1:0",
		ScanInterval:       50 * time.Millisecond,
		GoroutineThreshold: 5000,
	}

	c, err := tgbox.New(filepath.Join(t.TempDir(), "test_profiler_client"),
		tgbox.WithAppID(123),
		tgbox.WithAppHash("deadbeef"),
		tgbox.WithAutoPeerCaching(false),
		tgbox.WithProfiler(cfg),
	)
	require.NoError(t, err)

	require.NoError(t, c.Close())
}
