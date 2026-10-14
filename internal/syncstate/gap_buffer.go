package syncstate

import (
	"sort"
	"sync"
	"time"

	"github.com/gotd/td/tg"
)

// GapCheckResult defines the deterministic action required for an incoming update.
type GapCheckResult int

const (
	// GapApply indicates the update is consecutive and can be processed immediately.
	GapApply GapCheckResult = iota
	// GapIgnore indicates the update is stale or duplicate and must be discarded.
	GapIgnore
	// GapRefetch indicates a gap was detected, requiring buffering or difference recovery.
	GapRefetch
)

// CheckGap evaluates sequence mathematical invariants against local state.
func CheckGap(localPTS, remotePTS, ptsCount int32) GapCheckResult {
	if remotePTS <= 0 {
		return GapApply
	}
	if localPTS+ptsCount == remotePTS {
		return GapApply
	}
	if localPTS >= remotePTS || localPTS+ptsCount > remotePTS {
		return GapIgnore
	}
	return GapRefetch
}

// MinGapTimeout defines the short-gap speculative repair window (50ms).
const MinGapTimeout = 50 * time.Millisecond

// MaxGapTimeout defines the ceiling timeout after which getDifference is mandatory (700ms).
const MaxGapTimeout = 700 * time.Millisecond

// MaxPendingUpdatesThreshold defines the maximum unapplied out-of-order updates before forcing a sync.
const MaxPendingUpdatesThreshold = 20

// PendingUpdate encapsulates an out-of-order update awaiting gap resolution.
type PendingUpdate struct {
	PTS         int32
	PTSCount    int32
	ReceiveTime time.Time
	Raw         tg.UpdateClass
	TraceID     [16]byte
	ClientID    int64
}

// ShortGapCallback is invoked when a single missing update can be targeted specifically.
type ShortGapCallback func(missingPTS int32)

// GapTimeoutCallback is invoked when max gap duration or update count threshold is exceeded.
type GapTimeoutCallback func()

// GapBuffer manages unsequenced updates with micro-repair and escalation timers.
type GapBuffer struct {
	mu            sync.Mutex
	pending       []PendingUpdate
	minTimer      *time.Timer
	maxTimer      *time.Timer
	onShortGap    ShortGapCallback
	onTimeout     GapTimeoutCallback
	isShortActive bool
	isMaxActive   bool
}

// NewGapBuffer constructs a GapBuffer with short and maximum repair callback handlers.
func NewGapBuffer(onShortGap ShortGapCallback, onTimeout GapTimeoutCallback) *GapBuffer {
	return &GapBuffer{
		pending:    make([]PendingUpdate, 0, MaxPendingUpdatesThreshold),
		onShortGap: onShortGap,
		onTimeout:  onTimeout,
	}
}

// Push adds an out-of-order update to the buffer, sorting by PTS ascending and initiating timers.
func (b *GapBuffer) Push(upd PendingUpdate, currentPTS int32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.pending = append(b.pending, upd)
	sort.SliceStable(b.pending, func(i, j int) bool {
		if b.pending[i].PTS != b.pending[j].PTS {
			return b.pending[i].PTS < b.pending[j].PTS
		}
		return b.pending[i].PTSCount > b.pending[j].PTSCount
	})

	if len(b.pending) >= MaxPendingUpdatesThreshold {
		b.stopTimersLocked()
		if b.onTimeout != nil {
			go b.onTimeout()
		}
		return
	}

	// Single missing update check: remotePTS - ptsCount == currentPTS + 1.
	isSingleGap := (upd.PTS - upd.PTSCount) == (currentPTS + 1)
	if isSingleGap && !b.isShortActive {
		b.startShortTimerLocked(currentPTS + 1)
	}

	if !b.isMaxActive {
		b.startMaxTimerLocked()
	}
}

// PopConsecutive drains all consecutive updates from the buffer starting from currentPTS.
func (b *GapBuffer) PopConsecutive(currentPTS int32) ([]PendingUpdate, int32) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.pending) == 0 {
		return nil, currentPTS
	}

	var applied []PendingUpdate
	cursor := currentPTS
	idx := 0

	for idx < len(b.pending) {
		item := b.pending[idx]
		if cursor+item.PTSCount == item.PTS {
			applied = append(applied, item)
			cursor = item.PTS
			idx++
			continue
		}
		if cursor >= item.PTS {
			// Stale item in pending queue, advance over it.
			idx++
			continue
		}
		break
	}

	if idx > 0 {
		b.pending = append(b.pending[:0], b.pending[idx:]...)
	}

	if len(b.pending) == 0 {
		b.stopTimersLocked()
	}

	return applied, cursor
}

// Clear resets the buffer and halts any active timers immediately.
func (b *GapBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pending = b.pending[:0]
	b.stopTimersLocked()
}

// Len returns the count of updates currently held in the buffer.
func (b *GapBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.pending)
}

func (b *GapBuffer) startShortTimerLocked(missingPTS int32) {
	b.isShortActive = true
	b.minTimer = time.AfterFunc(MinGapTimeout, func() {
		b.mu.Lock()
		b.isShortActive = false
		b.mu.Unlock()
		if b.onShortGap != nil {
			b.onShortGap(missingPTS)
		}
	})
}

func (b *GapBuffer) startMaxTimerLocked() {
	b.isMaxActive = true
	b.maxTimer = time.AfterFunc(MaxGapTimeout, func() {
		b.mu.Lock()
		b.isMaxActive = false
		b.stopTimersLocked()
		b.mu.Unlock()
		if b.onTimeout != nil {
			b.onTimeout()
		}
	})
}

func (b *GapBuffer) stopTimersLocked() {
	if b.minTimer != nil {
		b.minTimer.Stop()
		b.minTimer = nil
		b.isShortActive = false
	}
	if b.maxTimer != nil {
		b.maxTimer.Stop()
		b.maxTimer = nil
		b.isMaxActive = false
	}
}
