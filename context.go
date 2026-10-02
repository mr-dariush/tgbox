package tgbox

import (
	"context"
	"log/slog"
	"time"

	"github.com/gotd/td/tg"
)

// Context is the request-scoped container passed to every update handler.
// It encapsulates the normalized update event, peer entities, route parameters,
// and cancellation context for a single update execution lifecycle.
// It is intentionally decoupled from long-lived application services (RPC, Sender, Storage).
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
//
//nolint:wrapcheck // interface implementation must return the raw context error
func (c *Context) Err() error {
	return c.Context().Err()
}

// Value implements the [context.Context] interface.
func (c *Context) Value(key any) any {
	return c.Context().Value(key)
}

type (
	traceIDContextKey  struct{}
	clientIDContextKey struct{}
)

// WithTraceID embeds a unique sequential TraceID into the standard Go context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDContextKey{}, traceID)
}

// TraceIDFromContext extracts the embedded TraceID from a standard context.
func TraceIDFromContext(ctx context.Context) string {
	if val, ok := ctx.Value(traceIDContextKey{}).(string); ok {
		return val
	}
	return ""
}

// WithClientID embeds the active Bot/Userbot ID into the standard Go context.
func WithClientID(ctx context.Context, clientID int64) context.Context {
	return context.WithValue(ctx, clientIDContextKey{}, clientID)
}

// ClientIDFromContext extracts the embedded ClientID from a standard context.
func ClientIDFromContext(ctx context.Context) int64 {
	if val, ok := ctx.Value(clientIDContextKey{}).(int64); ok {
		return val
	}
	return 0
}

// TraceID returns the transaction's unique trace identifier directly from the active tgbox context.
func (c *Context) TraceID() string {
	return TraceIDFromContext(c.Context())
}

// ClientID returns the ID of the bot/userbot executing this transaction directly from the active tgbox context.
func (c *Context) ClientID() int64 {
	return ClientIDFromContext(c.Context())
}
