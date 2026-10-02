// Package dispatcher routes updates to handlers through a sharded worker
// pool, so updates that share a routing key (e.g. a chat ID) are processed
// sequentially, while distinct keys are processed in parallel.
package dispatcher

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sort"
	"sync"

	"go.uber.org/atomic"
)

// Control-flow sentinel errors that handlers may return to influence group
// traversal. They are re-exported by the tgbox root package.
var (
	// ErrEndGroups stops evaluation of all remaining groups for this update.
	ErrEndGroups = errors.New("dispatcher: end group evaluation")
	// ErrContinueGroups keeps scanning the current group for further
	// matching routes instead of advancing to the next group.
	ErrContinueGroups = errors.New("dispatcher: continue group evaluation")
)

// Filter is a predicate for deciding if a handler should process an update.
type Filter[C any, U any] func(C, U) bool

// Handler is a function that processes an update.
type Handler[C any, U any] func(C, U) error

// Middleware wraps a handler to provide cross-cutting concerns.
type Middleware[C any, U any] func(Handler[C, U]) Handler[C, U]

type route[C any, U any] struct {
	kind        int
	filter      Filter[C, U]
	handler     Handler[C, U]
	middlewares []Middleware[C, U]
	group       int
}

type routesState[C any, U any] struct {
	routes          map[int][]route[C, U]
	groupOrder      []int
	wildcardKind    int
	hasWildcardKind bool
}

// Dispatcher delivers updates to registered routes via a fixed set of shards.
type Dispatcher[C any, U any] struct {
	state  atomic.Pointer[routesState[C, U]]
	shards []*Shard[C, U]

	// hashFunc calculates the shard routing key (e.g., ChatID).
	hashFunc func(C, U) uint64
	// kindFunc resolves the update category (e.g., Message, Callback).
	kindFunc func(C, U) int

	// interceptor is a pre-route hook (e.g., for conversational FSMs).
	// It runs on the Handle caller's goroutine, before the update is
	// enqueued to a shard; returning true aborts routing. It must not
	// block, otherwise it would stall update intake.
	interceptor func(C, U) bool

	logger  *slog.Logger
	writeMu sync.Mutex
}

// NewDispatcher creates a dispatcher backed by numShards workers, each with a
// bounded mailbox of bufferSize updates. numShards and bufferSize are clamped
// to a minimum of 1.
func NewDispatcher[C, U any](
	numShards int,
	bufferSize int,
	hashFunc func(C, U) uint64,
	kindFunc func(C, U) int,
	logger *slog.Logger,
) *Dispatcher[C, U] {
	if numShards < 1 {
		numShards = 1
	}
	if bufferSize < 1 {
		bufferSize = 1
	}

	d := &Dispatcher[C, U]{
		hashFunc: hashFunc,
		kindFunc: kindFunc,
		logger:   logger,
	}

	initialState := &routesState[C, U]{
		routes: make(map[int][]route[C, U]),
	}
	d.state.Store(initialState)

	d.shards = make([]*Shard[C, U], numShards)
	for i := 0; i < numShards; i++ {
		d.shards[i] = NewShard[C, U](bufferSize, logger, d.processEnvelope)
	}

	return d
}

// Start boots all shard workers in the background.
func (d *Dispatcher[C, U]) Start(ctx context.Context) {
	for _, shard := range d.shards {
		shard.Start(ctx)
	}
}

// SetInterceptor sets a hook executed on the Handle caller's goroutine before
// the update is enqueued to a shard. If it returns true, the update is
// considered consumed and never reaches the shards. This allows low-latency,
// non-blocking pre-processing (such as telemetry filters, metrics collection,
// or custom ingress short-circuiting) without stalling update ingestion.
func (d *Dispatcher[C, U]) SetInterceptor(f func(C, U) bool) {
	d.interceptor = f
}

// SetWildcardKind marks a kind whose routes match every update regardless of
// the update's resolved kind. Must be called before Start.
func (d *Dispatcher[C, U]) SetWildcardKind(kind int) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	curr := d.state.Load()
	newState := &routesState[C, U]{
		routes:          curr.routes,
		groupOrder:      curr.groupOrder,
		wildcardKind:    kind,
		hasWildcardKind: true,
	}
	d.state.Store(newState)
}

// AddRoute registers a new handler route with its filter and middlewares.
func (d *Dispatcher[C, U]) AddRoute(kind int, f Filter[C, U], h Handler[C, U], m []Middleware[C, U], group int) {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	curr := d.state.Load()

	// 1. Deep-copy existing route map and kind slices
	newRoutes := make(map[int][]route[C, U], len(curr.routes)+1)
	for k, v := range curr.routes {
		routesCopy := make([]route[C, U], len(v))
		copy(routesCopy, v)
		newRoutes[k] = routesCopy
	}

	// 2. Append new route to the target kind
	newRoutes[kind] = append(newRoutes[kind], route[C, U]{
		kind:        kind,
		filter:      f,
		handler:     h,
		middlewares: m,
		group:       group,
	})

	// 3. Clone and sort group order
	newGroupOrder := make([]int, len(curr.groupOrder))
	copy(newGroupOrder, curr.groupOrder)

	exists := slices.Contains(newGroupOrder, group)
	if !exists {
		newGroupOrder = append(newGroupOrder, group)
		sort.Ints(newGroupOrder)
	}

	// 4. Atomically publish the new snapshot
	d.state.Store(&routesState[C, U]{
		routes:          newRoutes,
		groupOrder:      newGroupOrder,
		wildcardKind:    curr.wildcardKind,
		hasWildcardKind: curr.hasWildcardKind,
	})
}

// Handle runs the interceptor and, unless the update was consumed, enqueues
// it to the shard derived from the routing hash. It never blocks: saturated
// shards drop the update (see Shard.Push).
func (d *Dispatcher[C, U]) Handle(c C, update U) {
	if d.interceptor != nil && d.interceptor(c, update) {
		return
	}
	h := d.hashFunc(c, update)
	d.shards[h%uint64(len(d.shards))].Push(c, update)
}

// processEnvelope runs inside a shard worker and executes matching routes,
// walking groups in ascending order. Within a group the first route whose
// filter matches is executed; the handler may return ErrContinueGroups to
// keep scanning the same group, or ErrEndGroups to stop entirely. Other
// handler errors are logged and evaluation moves to the next group.
func (d *Dispatcher[C, U]) processEnvelope(c C, update U) {
	defer func() {
		if r := recover(); r != nil {
			if d.logger != nil {
				d.logger.Error("panic recovered in shard worker", slog.Any("panic", r))
			}
		}
	}()

	kind := d.kindFunc(c, update)
	state := d.state.Load()

	kindRoutes := state.routes[kind]
	var wildcardRoutes []route[C, U]
	if state.hasWildcardKind && state.wildcardKind != kind {
		wildcardRoutes = state.routes[state.wildcardKind]
	}

	for _, group := range state.groupOrder {
		if d.runGroup(group, kindRoutes, wildcardRoutes, c, update) {
			return
		}
	}
}

// runGroup executes routes of one group and reports whether evaluation of
// all remaining groups should stop.
func (d *Dispatcher[C, U]) runGroup(group int, kindRoutes, wildcardRoutes []route[C, U], c C, update U) bool {
	for _, routes := range [2][]route[C, U]{kindRoutes, wildcardRoutes} {
		for i := range routes {
			r := &routes[i]
			if r.group != group || !r.filter(c, update) {
				continue
			}

			err := d.executePipeline(r, c, update)
			switch {
			case err == nil:
				return false // matched and handled; next group
			case errors.Is(err, ErrEndGroups):
				return true
			case errors.Is(err, ErrContinueGroups):
				continue // keep scanning this group
			default:
				if d.logger != nil {
					d.logger.Error("handler error",
						slog.Int("kind", r.kind),
						slog.Int("group", group),
						slog.Any("error", err),
					)
				}
				return false
			}
		}
	}
	return false
}

// executePipeline chains middlewares around the route handler and executes it.
func (*Dispatcher[C, U]) executePipeline(r *route[C, U], c C, update U) error {
	h := r.handler
	for _, v := range slices.Backward(r.middlewares) {
		h = v(h)
	}
	return h(c, update)
}
