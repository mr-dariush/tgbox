package telemetry_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/mr-dariush/tgbox/internal/telemetry"
)

func TestSyncMetrics_RecordingAndOutput(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewSyncMetrics()

	metrics.RecordRingBufferUtilization(0.45)
	metrics.RecordDroppedUpdates("buffer_full", 12)
	metrics.RecordDroppedUpdates("desync_drain", 3)
	metrics.RecordResyncEvent("user")
	metrics.RecordResyncEvent("channel")
	metrics.RecordResyncDuration(1250 * time.Millisecond)
	metrics.RecordPublishContention()

	var buf bytes.Buffer
	metrics.WritePrometheus(&buf)

	output := buf.String()
	assert.Contains(t, output, "tgbox_ringbuffer_capacity_used 0.4500")
	assert.Contains(t, output, `tgbox_updates_dropped_total{reason="buffer_full"} 12`)
	assert.Contains(t, output, `tgbox_updates_dropped_total{reason="desync_drain"} 3`)
	assert.Contains(t, output, `tgbox_resync_events_total{type="user"} 1`)
	assert.Contains(t, output, `tgbox_resync_events_total{type="channel"} 1`)
	assert.Contains(t, output, "tgbox_resync_duration_seconds 1.2500")
	assert.Contains(t, output, "tgbox_ringbuffer_publish_contention_total 1")
}

func TestSyncMetrics_HTTPHandler(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewSyncMetrics()
	metrics.RecordDroppedUpdates("buffer_full", 5)

	req := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	w := httptest.NewRecorder()

	metrics.Handler().ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/plain")
	assert.Contains(t, w.Body.String(), `tgbox_updates_dropped_total{reason="buffer_full"} 5`)
}

func TestSyncMetrics_ConcurrentRecording(t *testing.T) {
	t.Parallel()

	metrics := telemetry.NewSyncMetrics()
	const goroutines = 20
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				metrics.RecordRingBufferUtilization(0.5)
				metrics.RecordDroppedUpdates("buffer_full", 1)
				metrics.RecordPublishContention()
			}
		}()
	}

	wg.Wait()

	var buf bytes.Buffer
	metrics.WritePrometheus(&buf)
	assert.Contains(t, buf.String(), `tgbox_updates_dropped_total{reason="buffer_full"} 10000`)
	assert.Contains(t, buf.String(), "tgbox_ringbuffer_publish_contention_total 10000")
}
