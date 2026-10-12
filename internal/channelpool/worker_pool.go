package channelpool

import (
	"context"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	// MinConcurrency defines the rigid lower bound of parallel channel sync workers (8).
	MinConcurrency = 8
	// MaxConcurrency defines the rigid upper bound of parallel channel sync workers (16).
	MaxConcurrency = 16
)

// ChannelDifferenceAPI abstracts the MTProto RPC method for channel difference retrieval.
type ChannelDifferenceAPI interface {
	UpdatesGetChannelDifference(ctx context.Context, request *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error)
}

// ChannelDifferenceBatch represents an atomic set of messages received from getChannelDifference.
type ChannelDifferenceBatch struct {
	ChannelID    int64
	NewMessages  []tg.MessageClass
	OtherUpdates []tg.UpdateClass
	Users        []tg.UserClass
	Chats        []tg.ChatClass
	Final        bool
	NewPTS       int32
}

// BatchHandler defines the consumer callback for processing channel updates.
type BatchHandler func(batch ChannelDifferenceBatch) error

// InaccessibleHandler is invoked when a channel is restricted or private.
type InaccessibleHandler func(channelID int64)

// TooLongHandler is invoked when channel updates overflow server buffer limits.
type TooLongHandler func(channelID int64, newPTS int32)

// WorkerPool coordinates parallel channel synchronization respecting server rate limits.
type WorkerPool struct {
	api            ChannelDifferenceAPI
	queue          *PriorityQueue
	concurrency    int
	onBatch        BatchHandler
	onInaccessible InaccessibleHandler
	onTooLong      TooLongHandler
	sem            chan struct{}
	notify         chan struct{}
	mu             sync.Mutex
	closed         bool
}

// NewWorkerPool constructs a worker pool bounded by strict concurrency limits (min 8, max 16).
func NewWorkerPool(
	api ChannelDifferenceAPI,
	concurrency int,
	onBatch BatchHandler,
	onInaccessible InaccessibleHandler,
	onTooLong TooLongHandler,
) *WorkerPool {
	if concurrency <= 0 {
		concurrency = runtime.NumCPU() * 2
	}
	if concurrency < MinConcurrency {
		concurrency = MinConcurrency
	}
	if concurrency > MaxConcurrency {
		concurrency = MaxConcurrency
	}

	return &WorkerPool{
		api:            api,
		queue:          NewPriorityQueue(),
		concurrency:    concurrency,
		onBatch:        onBatch,
		onInaccessible: onInaccessible,
		onTooLong:      onTooLong,
		sem:            make(chan struct{}, concurrency),
		notify:         make(chan struct{}, 1),
	}
}

// Concurrency returns the active bounded worker count of the pool.
func (wp *WorkerPool) Concurrency() int {
	return wp.concurrency
}

// Submit schedules a channel for difference synchronization and signals waiting workers.
func (wp *WorkerPool) Submit(task *ChannelSyncTask) {
	wp.queue.PushOrUpdate(task)
	select {
	case wp.notify <- struct{}{}:
	default:
	}
}

// Start launches worker goroutines that process queued tasks until ctx is canceled.
func (wp *WorkerPool) Start(ctx context.Context) {
	for i := 0; i < wp.concurrency; i++ {
		go wp.workerLoop(ctx)
	}
}

func (wp *WorkerPool) workerLoop(ctx context.Context) {
	for {
		task := wp.acquireNextTask(ctx)
		if task == nil {
			return
		}

		wp.executeTask(ctx, task)
	}
}

func (wp *WorkerPool) acquireNextTask(ctx context.Context) *ChannelSyncTask {
	for {
		task, waitDur, ok := wp.queue.PopReady(time.Now())
		if ok {
			return task
		}

		wp.mu.Lock()
		closed := wp.closed
		wp.mu.Unlock()

		if closed || ctx.Err() != nil {
			return nil
		}

		var timer <-chan time.Time
		if waitDur > 0 {
			if waitDur > time.Second {
				waitDur = time.Second
			}
			t := time.NewTimer(waitDur)
			timer = t.C
			defer t.Stop()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-wp.notify:
		case <-timer:
		}
	}
}

func (wp *WorkerPool) executeTask(ctx context.Context, task *ChannelSyncTask) {
	select {
	case wp.sem <- struct{}{}:
		defer func() { <-wp.sem }()
	case <-ctx.Done():
		return
	}

	req := &tg.UpdatesGetChannelDifferenceRequest{
		Channel: &tg.InputChannel{
			ChannelID:  task.ChannelID,
			AccessHash: task.AccessHash,
		},
		Filter: &tg.ChannelMessagesFilterEmpty{},
		Pts:    int(task.LocalPTS),
		Limit:  100,
	}

	res, err := wp.api.UpdatesGetChannelDifference(ctx, req)
	if err != nil {
		wp.handleError(task, err)
		return
	}

	wp.handleDifferenceResult(ctx, task, res)
}

func (wp *WorkerPool) handleError(task *ChannelSyncTask, err error) {
	if tgerr.Is(err, "FLOOD_WAIT") {
		d, _ := tgerr.AsFloodWait(err)
		jitter := time.Duration(rand.IntN(1000)) * time.Millisecond
		task.NextAllowedAt = time.Now().Add(d + jitter)
		wp.Submit(task)
		return
	}

	if tgerr.Is(err, "CHANNEL_PRIVATE", "CHANNEL_PUBLIC_GROUP_NA") {
		if wp.onInaccessible != nil {
			wp.onInaccessible(task.ChannelID)
		}
	}
}

func (wp *WorkerPool) handleDifferenceResult(
	_ context.Context,
	task *ChannelSyncTask,
	res tg.UpdatesChannelDifferenceClass,
) {
	switch d := res.(type) {
	case *tg.UpdatesChannelDifferenceEmpty:
		task.LocalPTS = int32(d.Pts)

	case *tg.UpdatesChannelDifferenceTooLong:
		if wp.onTooLong != nil {
			dialogPTS := task.RemotePTS
			if d.Dialog != nil {
				if dlg, ok := d.Dialog.(*tg.Dialog); ok {
					if pts, hasPts := dlg.GetPts(); hasPts {
						dialogPTS = int32(pts)
					} else if dlg.Pts > 0 {
						dialogPTS = int32(dlg.Pts)
					}
				}
			}
			wp.onTooLong(task.ChannelID, dialogPTS)
		}

	case *tg.UpdatesChannelDifference:
		batch := ChannelDifferenceBatch{
			ChannelID:    task.ChannelID,
			NewMessages:  d.NewMessages,
			OtherUpdates: d.OtherUpdates,
			Users:        d.Users,
			Chats:        d.Chats,
			Final:        d.Final,
			NewPTS:       int32(d.Pts),
		}
		if wp.onBatch != nil {
			_ = wp.onBatch(batch)
		}

		task.LocalPTS = int32(d.Pts)
		if !d.Final {
			wp.Submit(task)
		}
	}
}

// Stop signals all workers to terminate gracefully.
func (wp *WorkerPool) Stop() {
	wp.mu.Lock()
	wp.closed = true
	wp.mu.Unlock()

	for i := 0; i < wp.concurrency; i++ {
		select {
		case wp.notify <- struct{}{}:
		default:
		}
	}
}
