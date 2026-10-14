package telemetry

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/atomic"
)

// SyncMetrics tracks high-throughput runtime telemetry for the Disruptor ring buffer and MTProto sync state.
type SyncMetrics struct {
	ringCapacityUsed        atomic.Float64
	droppedBufferFull       atomic.Uint64
	droppedDesyncDrain      atomic.Uint64
	resyncEventsUser        atomic.Uint64
	resyncEventsChannel     atomic.Uint64
	resyncDurationSeconds   atomic.Float64
	publishContentionTotal  atomic.Uint64
	shardQueueOverflowTotal atomic.Uint64
}

// NewSyncMetrics constructs a new atomic metrics tracker.
func NewSyncMetrics() *SyncMetrics {
	return &SyncMetrics{}
}

// RecordRingBufferUtilization records the percentage of ring buffer capacity currently occupied (0.0 to 1.0).
func (m *SyncMetrics) RecordRingBufferUtilization(ratio float64) {
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	m.ringCapacityUsed.Store(ratio)
}

// RecordDroppedUpdates increments the dropped updates counter with reason classification.
func (m *SyncMetrics) RecordDroppedUpdates(reason string, count uint64) {
	if reason == "desync_drain" {
		m.droppedDesyncDrain.Add(count)
		return
	}
	m.droppedBufferFull.Add(count)
}

// RecordResyncEvent increments the count of state resynchronization events.
func (m *SyncMetrics) RecordResyncEvent(resyncType string) {
	if resyncType == "channel" {
		m.resyncEventsChannel.Inc()
		return
	}
	m.resyncEventsUser.Inc()
}

// RecordResyncDuration updates the latest resynchronization duration in seconds.
func (m *SyncMetrics) RecordResyncDuration(d time.Duration) {
	m.resyncDurationSeconds.Store(d.Seconds())
}

// RecordPublishContention increments the counter of spin-waits experienced during sequence claiming.
func (m *SyncMetrics) RecordPublishContention() {
	m.publishContentionTotal.Inc()
}

// RecordShardOverflow increments the counter indicating updates spilling into the elastic shard queue.
func (m *SyncMetrics) RecordShardOverflow() {
	m.shardQueueOverflowTotal.Inc()
}

// WritePrometheus formats all tracked metrics into standard Prometheus exposition format.
func (m *SyncMetrics) WritePrometheus(w io.Writer) {
	_, _ = fmt.Fprint(w, "# HELP tgbox_ringbuffer_capacity_used Percentage of ring buffer slots currently occupied.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_ringbuffer_capacity_used gauge\n")
	_, _ = fmt.Fprintf(w, "tgbox_ringbuffer_capacity_used %.4f\n\n", m.ringCapacityUsed.Load())

	_, _ = fmt.Fprint(w, "# HELP tgbox_updates_dropped_total Total number of dropped updates due to saturation or desync.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_updates_dropped_total counter\n")
	_, _ = fmt.Fprintf(w, "tgbox_updates_dropped_total{reason=\"buffer_full\"} %d\n", m.droppedBufferFull.Load())
	_, _ = fmt.Fprintf(w, "tgbox_updates_dropped_total{reason=\"desync_drain\"} %d\n\n", m.droppedDesyncDrain.Load())

	_, _ = fmt.Fprint(w, "# HELP tgbox_resync_events_total Total number of state resynchronization events triggered.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_resync_events_total counter\n")
	_, _ = fmt.Fprintf(w, "tgbox_resync_events_total{type=\"user\"} %d\n", m.resyncEventsUser.Load())
	_, _ = fmt.Fprintf(w, "tgbox_resync_events_total{type=\"channel\"} %d\n\n", m.resyncEventsChannel.Load())

	_, _ = fmt.Fprint(w, "# HELP tgbox_resync_duration_seconds Duration of the latest state resynchronization in seconds.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_resync_duration_seconds gauge\n")
	_, _ = fmt.Fprintf(w, "tgbox_resync_duration_seconds %.4f\n\n", m.resyncDurationSeconds.Load())

	_, _ = fmt.Fprint(w, "# HELP tgbox_ringbuffer_publish_contention_total Total number of sequence claim contention spins.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_ringbuffer_publish_contention_total counter\n")
	_, _ = fmt.Fprintf(w, "tgbox_ringbuffer_publish_contention_total %d\n", m.publishContentionTotal.Load())
	_, _ = fmt.Fprint(w, "# HELP tgbox_shard_queue_overflow_total Total number of updates spilling into the elastic shard queue.\n")
	_, _ = fmt.Fprint(w, "# TYPE tgbox_shard_queue_overflow_total counter\n")
	_, _ = fmt.Fprintf(w, "tgbox_shard_queue_overflow_total %d\n", m.shardQueueOverflowTotal.Load())
}

// Handler returns an http.Handler that renders metrics for Prometheus scraping.
func (m *SyncMetrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.WritePrometheus(w)
	})
}
