package dispatcher

import (
	"context"
	"log/slog"
	"sync"

	"go.uber.org/atomic"
)

// MetricsReporter defines the contract for emitting shard queue metrics.
type MetricsReporter interface {
	RecordShardOverflow()
}

// Envelope wraps the context and update payloads to be processed sequentially within a shard.
type Envelope[C any, U any] struct {
	Ctx    C
	Update U
}

// DefaultMaxElasticQueue defines the default ceiling for the elastic overflow slice per shard.
const DefaultMaxElasticQueue = 10000

// Shard represents an isolated actor-like mailbox queue for a subset of chats.
// It processes updates sequentially in a FIFO order via a dedicated single worker loop.
type Shard[C any, U any] struct {
	queue              chan Envelope[C, U]
	elastic            []Envelope[C, U]
	notify             chan struct{}
	mu                 sync.Mutex
	dropped            atomic.Uint64
	logger             *slog.Logger
	handleFunc         func(C, U)
	metrics            MetricsReporter
	maxElasticCapacity int
}

// NewShard instantiates a generic Shard with a bounded buffer capacity and worker callback.
func NewShard[C, U any](capacity int, logger *slog.Logger, handleFunc func(C, U)) *Shard[C, U] {
	return &Shard[C, U]{
		queue:              make(chan Envelope[C, U], capacity),
		notify:             make(chan struct{}, 1),
		logger:             logger,
		handleFunc:         handleFunc,
		maxElasticCapacity: DefaultMaxElasticQueue,
	}
}

// SetMaxElasticCapacity overrides the default elastic overflow limit (10000).
// Setting limit <= 0 disables the elastic buffer, shedding load immediately when the primary queue is full.
func (s *Shard[C, U]) SetMaxElasticCapacity(limit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxElasticCapacity = limit
}

// SetMetrics binds a telemetry exporter to the shard.
func (s *Shard[C, U]) SetMetrics(m MetricsReporter) {
	s.metrics = m
}

// Start launches the dedicated background worker loop for the shard.
// It terminates gracefully when the provided context is canceled.
func (s *Shard[C, U]) Start(ctx context.Context) {
	// Elastic Feeder
	go func() {
		for {
			s.mu.Lock()
			if len(s.elastic) == 0 {
				s.elastic = s.elastic[:0] // PRESERVE CAPACITY
				s.mu.Unlock()
				select {
				case <-ctx.Done():
					return
				case <-s.notify:
				}
				continue
			}
			env := s.elastic[0]
			s.mu.Unlock()

			select {
			case <-ctx.Done():
				return
			case s.queue <- env:
				s.mu.Lock()
				s.elastic[0] = Envelope[C, U]{} // prevent leak of pointed memory
				s.elastic = s.elastic[1:]
				if len(s.elastic) == 0 {
					s.elastic = s.elastic[:0] // PRESERVE CAPACITY
				}
				s.mu.Unlock()
			}
		}
	}()

	// Worker Processor
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case env, ok := <-s.queue:
				if !ok {
					return
				}
				s.handleFunc(env.Ctx, env.Update)
			}
		}
	}()
}

// Push attempts to enqueue an update envelope into the shard's mailbox.
// It uses an elastic slice buffer if the primary channel is saturated, capped by maxElasticCapacity.
func (s *Shard[C, U]) Push(c C, u U) bool {
	s.mu.Lock()
	if len(s.elastic) > 0 {
		if s.maxElasticCapacity <= 0 || len(s.elastic) >= s.maxElasticCapacity {
			s.dropped.Inc()
			s.mu.Unlock()
			if releaser, ok := any(c).(interface{ Release() }); ok {
				releaser.Release()
			}
			return false
		}
		s.elastic = append(s.elastic, Envelope[C, U]{Ctx: c, Update: u})
		s.mu.Unlock()
		s.triggerFeeder()
		return true
	}
	s.mu.Unlock()

	select {
	case s.queue <- Envelope[C, U]{Ctx: c, Update: u}:
		return true
	default:
		s.mu.Lock()
		if s.maxElasticCapacity <= 0 || len(s.elastic) >= s.maxElasticCapacity {
			s.dropped.Inc()
			s.mu.Unlock()
			if releaser, ok := any(c).(interface{ Release() }); ok {
				releaser.Release()
			}
			return false
		}
		s.elastic = append(s.elastic, Envelope[C, U]{Ctx: c, Update: u})
		s.mu.Unlock()
		s.triggerFeeder()
		return true
	}
}

// PushWait delegates to Push, making it completely non-blocking to prevent Head-of-Line blocking.
func (s *Shard[C, U]) PushWait(ctx context.Context, c C, u U) bool {
	if ctx.Err() != nil {
		return false
	}
	return s.Push(c, u)
}

// triggerFeeder signals the elastic feeder to pump messages to the primary channel.
func (s *Shard[C, U]) triggerFeeder() {
	if s.metrics != nil {
		s.metrics.RecordShardOverflow()
	}
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// DroppedCount returns the aggregate number of updates discarded.
func (s *Shard[C, U]) DroppedCount() uint64 {
	return s.dropped.Load()
}
