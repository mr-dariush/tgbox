package channelpool

import (
	"container/heap"
	"sync"
	"time"
)

// SyncTier categorizes channel priority into foreground active vs muted backlog.
type SyncTier int

const (
	// TierBacklog represents muted or idle channels synced in the background.
	TierBacklog SyncTier = iota
	// TierActive represents open, pinned, or frequently accessed channels.
	TierActive
)

// ChannelSyncTask encapsulates the state recovery parameters for a single channel.
type ChannelSyncTask struct {
	ChannelID     int64
	AccessHash    int64
	LocalPTS      int32
	RemotePTS     int32
	Tier          SyncTier
	Muted         bool
	Pinned        bool
	EnqueuedAt    time.Time
	NextAllowedAt time.Time
	index         int
}

// Score computes the dynamic priority score using a multi-variable weighting formula.
func (t *ChannelSyncTask) Score(now time.Time) int64 {
	if !t.NextAllowedAt.IsZero() && now.Before(t.NextAllowedAt) {
		return -1000000000 // Sink delayed tasks due to FLOOD_WAIT to the bottom of the max-heap
	}

	var score int64
	if t.Tier == TierActive || t.Pinned {
		score += 10000
	}

	gap := int64(t.RemotePTS - t.LocalPTS)
	if gap > 0 {
		score += gap * 10
	}

	waitSec := int64(now.Sub(t.EnqueuedAt).Seconds())
	score += waitSec * 5

	if t.Muted {
		score -= 4000
	}
	return score
}

type taskHeap []*ChannelSyncTask

func (h taskHeap) Len() int           { return len(h) }
func (h taskHeap) Less(i, j int) bool { return h[i].Score(time.Now()) > h[j].Score(time.Now()) }
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *taskHeap) Push(x any) {
	n := len(*h)
	item := x.(*ChannelSyncTask)
	item.index = n
	*h = append(*h, item)
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// PriorityQueue provides thread-safe access and deduplication for channel sync tasks.
type PriorityQueue struct {
	mu    sync.Mutex
	items taskHeap
	index map[int64]*ChannelSyncTask
}

// NewPriorityQueue constructs an initialized priority queue for channels.
func NewPriorityQueue() *PriorityQueue {
	pq := &PriorityQueue{
		items: make(taskHeap, 0),
		index: make(map[int64]*ChannelSyncTask),
	}
	heap.Init(&pq.items)
	return pq
}

// PushOrUpdate enqueues a new sync task or updates an existing pending task for the channel.
func (pq *PriorityQueue) PushOrUpdate(task *ChannelSyncTask) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if existing, found := pq.index[task.ChannelID]; found {
		if task.RemotePTS > existing.RemotePTS {
			existing.RemotePTS = task.RemotePTS
		}
		if task.Tier > existing.Tier {
			existing.Tier = task.Tier
		}
		if !task.NextAllowedAt.IsZero() {
			existing.NextAllowedAt = task.NextAllowedAt
		}
		heap.Fix(&pq.items, existing.index)
		return
	}

	task.EnqueuedAt = time.Now()
	pq.index[task.ChannelID] = task
	heap.Push(&pq.items, task)
}

// Pop removes and returns the highest priority sync task, or returns false if empty.
func (pq *PriorityQueue) Pop() (*ChannelSyncTask, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil, false
	}

	task := heap.Pop(&pq.items).(*ChannelSyncTask)
	delete(pq.index, task.ChannelID)
	return task, true
}

// PopReady removes and returns the highest priority ready sync task.
// If the top task is delayed due to FLOOD_WAIT, it returns the wait duration instead.
func (pq *PriorityQueue) PopReady(now time.Time) (*ChannelSyncTask, time.Duration, bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if len(pq.items) == 0 {
		return nil, 0, false
	}

	heap.Fix(&pq.items, 0)

	top := pq.items[0]
	if !top.NextAllowedAt.IsZero() && now.Before(top.NextAllowedAt) {
		return nil, top.NextAllowedAt.Sub(now), false
	}

	task := heap.Pop(&pq.items).(*ChannelSyncTask)
	delete(pq.index, task.ChannelID)
	return task, 0, true
}

// Len returns the count of tasks currently queued.
func (pq *PriorityQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.items)
}
