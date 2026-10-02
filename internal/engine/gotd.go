package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gotd/contrib/bg"
	gotdclock "github.com/gotd/td/clock"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"

	"github.com/mr-dariush/tgbox/internal/clock"
)

// gotdEngine implements the Engine interface using gotd/td.
type gotdEngine struct {
	client *telegram.Client
	raw    *tg.Client
	stop   bg.StopFunc

	mu      sync.RWMutex
	running bool
	handler telegram.UpdateHandler
}

// NewGotdEngine creates a new Engine based on gotd.
func NewGotdEngine(appID int, appHash string, opts telegram.Options) Engine {
	e := &gotdEngine{}

	// Set bridge handler in options
	opts.UpdateHandler = telegram.UpdateHandlerFunc(e.bridgeHandler)

	// Inject the Clock Interceptor to safely neutralize the default 1-minute ping loop
	// generated internally by gotd, ensuring it does not conflict with the fuzzy keep-alive engine.
	baseClock := opts.Clock
	if baseClock == nil {
		baseClock = gotdclock.System
	}
	opts.Clock = clock.NewInterceptor(baseClock, time.Minute)

	client := telegram.NewClient(appID, appHash, opts)
	e.client = client
	e.raw = tg.NewClient(client)

	return e
}

func (e *gotdEngine) bridgeHandler(ctx context.Context, u tg.UpdatesClass) error {
	e.mu.RLock()
	h := e.handler
	e.mu.RUnlock()

	if h != nil {
		if err := h.Handle(ctx, u); err != nil {
			return fmt.Errorf("update handler error: %w", err)
		}
	}
	return nil
}

// Connect establishes the connection.
func (e *gotdEngine) Connect(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return errors.New("engine is already running")
	}

	stop, err := bg.Connect(e.client, bg.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to connect engine: %w", err)
	}

	e.stop = stop
	e.running = true
	return nil
}

// Disconnect gracefully stops the background connection.
func (e *gotdEngine) Disconnect() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	if err := e.stop(); err != nil {
		return fmt.Errorf("failed to stop engine: %w", err)
	}

	e.running = false
	return nil
}

// API returns the high-level MTProto API client.
func (e *gotdEngine) API() *tg.Client {
	return e.raw
}

// Raw returns the underlying telegram client.
func (e *gotdEngine) Raw() *telegram.Client {
	return e.client
}

// SetUpdateHandler sets the callback for incoming updates.
func (e *gotdEngine) SetUpdateHandler(handler telegram.UpdateHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handler = handler
}
