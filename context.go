package tgbox

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/gotd/td/tg"
)

// FastMetadata carries fixed-size telemetry metadata without heap escapes.
type FastMetadata struct {
	TraceID  [16]byte
	ClientID int64
	SpanID   uint64
	UnixNano int64
}

// TraceIDString returns the non-empty prefix of TraceID as a string.
func (m FastMetadata) TraceIDString() string {
	end := 0
	for end < len(m.TraceID) && m.TraceID[end] != 0 {
		end++
	}
	return string(m.TraceID[:end])
}

// PipelineContext replaces dynamic standard context allocations in the update pipeline.
type PipelineContext struct {
	parent context.Context
	Meta   FastMetadata
	Update tg.UpdateClass
	pool   *ContextPool
}

var _ context.Context = (*PipelineContext)(nil)

// Deadline returns the parent deadline if available.
func (c *PipelineContext) Deadline() (deadline time.Time, ok bool) {
	if c.parent != nil {
		return c.parent.Deadline()
	}
	return time.Time{}, false
}

// Done returns the parent cancellation channel.
func (c *PipelineContext) Done() <-chan struct{} {
	if c.parent != nil {
		return c.parent.Done()
	}
	return nil
}

// Err returns the parent context error.
func (c *PipelineContext) Err() error {
	if c.parent != nil {
		return c.parent.Err()
	}
	return nil
}

// Value extracts values, resolving TraceID and ClientID directly without map lookups.
func (c *PipelineContext) Value(key any) any {
	switch key {
	case traceIDContextKey{}:
		return c.Meta.TraceIDString()
	case clientIDContextKey{}:
		return c.Meta.ClientID
	default:
		if c.parent != nil {
			return c.parent.Value(key)
		}
		return nil
	}
}

// Release resets and returns this PipelineContext back to its parent ContextPool.
func (c *PipelineContext) Release() {
	if c == nil || c.pool == nil {
		return
	}
	c.pool.Release(nil)
}

// ContextPool recycles Context and PipelineContext instances to guarantee zero heap allocations.
type ContextPool struct {
	pool sync.Pool
}

// NewContextPool constructs an initialized pool for Contexts with pre-allocated entity maps.
func NewContextPool() *ContextPool {
	return &ContextPool{
		pool: sync.Pool{
			New: func() any {
				return &Context{
					StdContext: &PipelineContext{},
					Update:     &Update{},
					Entities: &tg.Entities{
						Users:    make(map[int64]*tg.User, 16),
						Chats:    make(map[int64]*tg.Chat, 16),
						Channels: make(map[int64]*tg.Channel, 16),
					},
				}
			},
		},
	}
}

func (p *ContextPool) Acquire(parent context.Context, meta FastMetadata) *Context {
	if p == nil {
		return &Context{
			StdContext: &PipelineContext{parent: parent, Meta: meta},
			Update:     &Update{},
			Entities: &tg.Entities{
				Users:    make(map[int64]*tg.User, 16),
				Chats:    make(map[int64]*tg.Chat, 16),
				Channels: make(map[int64]*tg.Channel, 16),
			},
			Meta: meta,
		}
	}
	ctx := p.pool.Get().(*Context)
	pipeCtx, ok := ctx.StdContext.(*PipelineContext)
	if !ok {
		pipeCtx = &PipelineContext{}
		ctx.StdContext = pipeCtx
	}
	pipeCtx.parent = parent
	pipeCtx.Meta = meta
	pipeCtx.pool = p

	if ctx.Update == nil {
		ctx.Update = &Update{}
	}
	if ctx.Entities == nil {
		ctx.Entities = &tg.Entities{
			Users:    make(map[int64]*tg.User, 16),
			Chats:    make(map[int64]*tg.Chat, 16),
			Channels: make(map[int64]*tg.Channel, 16),
		}
	} else {
		clear(ctx.Entities.Users)
		clear(ctx.Entities.Chats)
		clear(ctx.Entities.Channels)
	}
	ctx.Meta = meta
	return ctx
}

// Release resets and returns a Context back to the pool.
func (p *ContextPool) Release(ctx *Context) {
	if p == nil || ctx == nil {
		return
	}
	if pipeCtx, ok := ctx.StdContext.(*PipelineContext); ok {
		pipeCtx.parent = nil
		pipeCtx.Update = nil
	}
	if ctx.Update != nil {
		*ctx.Update = Update{}
	}
	if ctx.Entities != nil {
		clear(ctx.Entities.Users)
		clear(ctx.Entities.Chats)
		clear(ctx.Entities.Channels)
	}
	ctx.Logger = nil
	ctx.Params = nil
	ctx.cmdRoute = nil
	ctx.Meta = FastMetadata{}
	p.pool.Put(ctx)
}

// Context is the request-scoped container passed to every update handler.
// It encapsulates the normalized update event, peer entities, route parameters,
// and cancellation context for a single update execution lifecycle.
type Context struct {
	// StdContext represents the underlying standard Go context for cancellation and deadlines.
	StdContext context.Context

	// Update contains the normalized and parsed update metadata for this request.
	Update *Update

	// Entities represents the cached peer metadata extracted from this update transaction.
	Entities *tg.Entities

	// Logger represents the structured transaction-scoped logger with request telemetry (trace_id, client_id).
	Logger *slog.Logger

	// Params contains route variables extracted from parametric Radix tree routing.
	Params map[string]string

	// cmdRoute caches the matched command route so handlers do not repeat the Radix tree lookup.
	cmdRoute *commandRoute

	// Meta caches zero-allocation telemetry metadata directly within the context struct.
	Meta FastMetadata
}

// Param retrieves a parsed route variable by its key.
// Returns an empty string if the key does not exist.
func (c *Context) Param(key string) string {
	if c == nil || c.Params == nil {
		return ""
	}
	return c.Params[key]
}

// Context returns the underlying standard library context.Context.
func (c *Context) Context() context.Context {
	if c != nil && c.StdContext != nil {
		return c.StdContext
	}
	return context.Background()
}

// Deadline implements the [context.Context] interface.
func (c *Context) Deadline() (time.Time, bool) {
	return c.Context().Deadline()
}

// Done implements the [context.Context] interface.
func (c *Context) Done() <-chan struct{} {
	return c.Context().Done()
}

// Err implements the [context.Context] interface.
func (c *Context) Err() error {
	return c.Context().Err()
}

// Value implements the [context.Context] interface.
func (c *Context) Value(key any) any {
	if c == nil {
		return nil
	}
	switch key {
	case traceIDContextKey{}:
		if c.Meta.TraceID[0] != 0 {
			return c.Meta.TraceIDString()
		}
	case clientIDContextKey{}:
		if c.Meta.ClientID != 0 {
			return c.Meta.ClientID
		}
	}
	return c.Context().Value(key)
}

// Release releases the pooled resources encapsulated by this Context back to their respective memory pools.
func (c *Context) Release() {
	if c == nil {
		return
	}
	if pipeCtx, ok := c.StdContext.(*PipelineContext); ok && pipeCtx.pool != nil {
		pipeCtx.pool.Release(c)
		return
	}
	c.StdContext = nil
	c.Update = nil
	c.Entities = nil
	c.Logger = nil
	c.Params = nil
	c.cmdRoute = nil
	c.Meta = FastMetadata{}
}

type (
	traceIDContextKey  struct{}
	clientIDContextKey struct{}
)

// WithTraceID embeds a unique sequential TraceID into the standard Go context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if p, ok := ctx.(*PipelineContext); ok {
		copy(p.Meta.TraceID[:], traceID)
		return p
	}
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

// TraceIDFromContext extracts the embedded TraceID from a standard context or PipelineContext.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if p, ok := ctx.(*PipelineContext); ok {
		return p.Meta.TraceIDString()
	}
	if val, ok := ctx.Value(traceIDContextKey{}).(string); ok {
		return val
	}
	return ""
}

// WithClientID embeds the active Bot/Userbot ID into the standard Go context.
func WithClientID(ctx context.Context, clientID int64) context.Context {
	if p, ok := ctx.(*PipelineContext); ok {
		p.Meta.ClientID = clientID
		return p
	}
	return context.WithValue(ctx, clientIDContextKey{}, clientID)
}

// ClientIDFromContext extracts the embedded ClientID from a standard context or PipelineContext.
func ClientIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	if p, ok := ctx.(*PipelineContext); ok {
		return p.Meta.ClientID
	}
	if val, ok := ctx.Value(clientIDContextKey{}).(int64); ok {
		return val
	}
	return 0
}

// TraceID returns the transaction's unique trace identifier directly from the active tgbox context.
func (c *Context) TraceID() string {
	if c == nil {
		return ""
	}
	if c.Meta.TraceID[0] != 0 {
		return c.Meta.TraceIDString()
	}
	return TraceIDFromContext(c.Context())
}

// ClientID returns the ID of the bot/userbot executing this transaction directly from the active tgbox context.
func (c *Context) ClientID() int64 {
	if c == nil {
		return 0
	}
	if c.Meta.ClientID != 0 {
		return c.Meta.ClientID
	}
	return ClientIDFromContext(c.Context())
}
