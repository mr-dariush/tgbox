// Package keepalive provides a fuzzed, zero-allocation heartbeat engine to bypass DPI heuristics.
package keepalive

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/gotd/td/clock"
	"go.uber.org/atomic"
)

// Config defines the fuzzy timing boundaries for the keep-alive engine.
type Config struct {
	// PingInterval is the base duration between keep-alive heartbeats.
	PingInterval time.Duration
	// JitterOffset defines the maximum positive or negative deviation.
	JitterOffset time.Duration
}

// PingFunc defines the callback signature executed when a heartbeat is due.
type PingFunc func(ctx context.Context) error

// Engine manages fuzzed heartbeat execution natively preventing network-level bot heuristics.
type Engine struct {
	config      Config
	pingFunc    PingFunc
	clock       clock.Clock
	lastTraffic atomic.Int64
	mu          sync.Mutex
	running     bool
}

// NewEngine instantiates a new fuzzy keep-alive manager.
func NewEngine(cfg Config, pingFunc PingFunc, c clock.Clock) *Engine {
	if c == nil {
		c = clock.System
	}
	e := &Engine{
		config:   cfg,
		pingFunc: pingFunc,
		clock:    c,
	}
	e.NotifyTraffic() // Initialize baseline with the current clock
	return e
}

// NotifyTraffic updates the internal atomic tracker with the latest network transaction time.
// This is completely lock-free and invokes zero heap allocations, ensuring high-throughput safety.
func (e *Engine) NotifyTraffic() {
	e.lastTraffic.Store(e.clock.Now().UnixNano())
}

// Start spawns the background fuzzy ping loop.
func (e *Engine) Start(ctx context.Context) {
	e.mu.Lock()
	if e.running {
		e.mu.Unlock()
		return
	}
	e.running = true
	e.mu.Unlock()

	go e.loop(ctx)
}

func (e *Engine) loop(ctx context.Context) {
	// Safely reset running state upon loop termination to prevent phantom locks.
	defer func() {
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		// NextInterval = PingInterval ± RandomJitter
		var jitter time.Duration
		if e.config.JitterOffset > 0 {
			noise := rand.N(e.config.JitterOffset * 2)
			jitter = noise - e.config.JitterOffset
		}
		nextInterval := e.config.PingInterval + jitter

		// Safely unpack the last traffic timestamp.
		last := time.Unix(0, e.lastTraffic.Load())
		target := last.Add(nextInterval)

		now := e.clock.Now()
		sleepDuration := target.Sub(now)

		// If sleepDuration is exhausted, traffic has been dormant. Execute Ping.
		if sleepDuration <= 0 {
			pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			_ = e.pingFunc(pingCtx)
			cancel()

			// Reset traffic timer manually after firing to prevent spamming.
			e.NotifyTraffic()
			continue
		}

		// Sleep naturally. If NotifyTraffic is called externally during this sleep,
		// the target time will simply push forward in the next iteration cleanly.
		timer := e.clock.Timer(sleepDuration)
		select {
		case <-ctx.Done():
			clock.StopTimer(timer)
			return
		case <-timer.C():
			// Woke up normally; loop will recalculate boundaries to verify no traffic occurred.
		}
	}
}
