package tgbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	runtimepprof "runtime/pprof"
	"sync"
	"time"
)

const (
	defaultProfilerScanInterval       = 15 * time.Second
	defaultProfilerGoroutineThreshold = 5000
)

// ProfilerConfig configures the diagnostic profiling engine and goroutine leak watchdog.
type ProfilerConfig struct {
	// Enabled indicates whether the diagnostic engine should run.
	Enabled bool

	// HTTPAddr specifies the loopback address for the HTTP pprof endpoint (e.g., "127.0.0.1:6060" or "127.0.0.1:0").
	// If empty, no HTTP listener is started, but the background leak watchdog remains active.
	HTTPAddr string

	// ScanInterval defines the frequency of background goroutine checks. Default: 15 seconds.
	ScanInterval time.Duration

	// GoroutineThreshold specifies the limit of active goroutines before emitting a warning log. Default: 5000.
	GoroutineThreshold int

	// OnThresholdExceeded is an optional callback triggered when active goroutines exceed the threshold.
	OnThresholdExceeded func(count int)

	// OnLeakDetected is an optional callback triggered when GC-reachability analysis detects permanently leaked goroutines.
	OnLeakDetected func(count int)
}

// Profiler monitors runtime health, detects goroutine leaks, and exposes pprof endpoints.
// It implements io.Closer for clean teardown.
type Profiler struct {
	cfg       ProfilerConfig
	logger    *slog.Logger
	server    *http.Server
	listener  net.Listener
	done      chan struct{}
	closeOnce sync.Once
}

// NewProfiler instantiates and launches the profiler engine and background watchdog.
func NewProfiler(cfg ProfilerConfig, logger *slog.Logger) *Profiler {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.ScanInterval <= 0 {
		cfg.ScanInterval = defaultProfilerScanInterval
	}
	if cfg.GoroutineThreshold <= 0 {
		cfg.GoroutineThreshold = defaultProfilerGoroutineThreshold
	}

	p := &Profiler{
		cfg:    cfg,
		logger: logger,
		done:   make(chan struct{}),
	}

	if cfg.HTTPAddr != "" {
		if err := p.startHTTPServer(cfg.HTTPAddr); err != nil {
			logger.Error("failed to start diagnostic profiler http server", slog.Any("error", err))
		}
	}

	p.startWatchdog()
	return p
}

func (p *Profiler) startHTTPServer(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	mux.Handle("/debug/pprof/goroutineleak", pprof.Handler("goroutineleak"))

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tgbox/profiler: listen %q: %w", addr, err)
	}
	p.listener = listener

	p.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		if err := p.server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			p.logger.Error("tgbox/profiler: http server terminated unexpectedly", slog.Any("error", err))
		}
	}()

	p.logger.Info("diagnostic profiler endpoint active", slog.String("addr", listener.Addr().String()))
	return nil
}

func (p *Profiler) startWatchdog() {
	go func() {
		ticker := time.NewTicker(p.cfg.ScanInterval)
		defer ticker.Stop()

		for {
			select {
			case <-p.done:
				return
			case <-ticker.C:
				p.scanHealth()
			}
		}
	}()
}

func (p *Profiler) scanHealth() {
	count := runtime.NumGoroutine()
	if count > p.cfg.GoroutineThreshold {
		p.logger.Warn("high goroutine count detected (potential leak)",
			slog.Int("goroutines", count),
			slog.Int("threshold", p.cfg.GoroutineThreshold),
		)
		if p.cfg.OnThresholdExceeded != nil {
			p.cfg.OnThresholdExceeded(count)
		}
	}

	// Trigger GC reachability pass conditionally to avoid CPU and GC thrashing
	if p.cfg.OnLeakDetected != nil {
		if leakProf := runtimepprof.Lookup("goroutineleak"); leakProf != nil {
			if leaked := leakProf.Count(); leaked > 0 {
				p.logger.Warn("unreachable leaked goroutines detected by GC reachability graph",
					slog.Int("leaked_goroutines", leaked),
				)
				p.cfg.OnLeakDetected(leaked)
			}
		}
	}
}

// Addr returns the listening address of the diagnostic HTTP server, or empty string if disabled.
func (p *Profiler) Addr() string {
	if p.listener != nil {
		return p.listener.Addr().String()
	}
	return ""
}

// NumGoroutine returns the current number of active goroutines.
func (*Profiler) NumGoroutine() int {
	return runtime.NumGoroutine()
}

// LeakedGoroutines returns the count of permanently blocked goroutines detected via GC reachability.
// Returns 0 if the goroutineleak profile is unavailable or no leaks are detected.
func (*Profiler) LeakedGoroutines() int {
	if prof := runtimepprof.Lookup("goroutineleak"); prof != nil {
		return prof.Count()
	}
	return 0
}

// Close stops the background watchdog and terminates the diagnostic HTTP server gracefully.
func (p *Profiler) Close() error {
	var err error
	p.closeOnce.Do(func() {
		close(p.done)
		if p.server != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err = p.server.Shutdown(ctx)
		}
	})
	return err
}
