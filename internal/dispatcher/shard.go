package dispatcher

import (
	"context"
	"log/slog"

	"go.uber.org/atomic"
)

// Envelope wraps the context and update payloads to be processed sequentially within a shard.
type Envelope[C any, U any] struct {
	Ctx    C
	Update U
}

// Shard represents an isolated actor-like mailbox queue for a subset of chats.
// It processes updates sequentially in a FIFO order via a dedicated single worker loop.
type Shard[C any, U any] struct {
	queue      chan Envelope[C, U]
	dropped    atomic.Uint64
	logger     *slog.Logger
	handleFunc func(C, U)
}

// NewShard instantiates a generic Shard with a bounded buffer capacity and worker callback.
func NewShard[C, U any](capacity int, logger *slog.Logger, handleFunc func(C, U)) *Shard[C, U] {
	return &Shard[C, U]{
		queue:      make(chan Envelope[C, U], capacity),
		logger:     logger,
		handleFunc: handleFunc,
	}
}

// Start launches the dedicated background worker loop for the shard.
// It terminates gracefully when the provided context is canceled.
func (s *Shard[C, U]) Start(ctx context.Context) {
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
// If the queue is saturated, it drops the update (Load Shedding), increments the counter,
// logs a warning, and returns false. This operation is non-blocking.
func (s *Shard[C, U]) Push(c C, u U) bool {
	select {
	case s.queue <- Envelope[C, U]{Ctx: c, Update: u}:
		return true
	default:
		dropped := s.dropped.Add(1)
		if s.logger != nil {
			s.logger.Warn("Load shedding triggered: Shard mailbox queue full. Update dropped.",
				slog.Uint64("dropped_count", dropped),
			)
		}
		return false
	}
}

// DroppedCount returns the aggregate number of updates discarded by this shard's load shedding.
func (s *Shard[C, U]) DroppedCount() uint64 {
	return s.dropped.Load()
}
