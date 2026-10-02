package tgbox

import (
	"fmt"
	"log/slog"
	"runtime/debug"
)

// Middleware is a function that wraps a Handler.
type Middleware func(Handler) Handler

// Recover is a middleware that recovers from panics in handlers and converts
// them into errors.
func Recover(h Handler) Handler {
	return func(ctx *Context, update *Update) (err error) {
		defer func() {
			if r := recover(); r != nil {
				ctx.Logger.Error("panic in handler",
					slog.Any("error", r),
					slog.String("stack", string(debug.Stack())),
				)
				err = fmt.Errorf("tgbox: panic in handler: %v", r)
			}
		}()
		return h(ctx, update)
	}
}

// Logger is a middleware that logs update processing details.
func Logger(h Handler) Handler {
	return func(ctx *Context, update *Update) error {
		ctx.Logger.Debug("processing update",
			slog.Int("kind", int(update.Kind)),
			slog.Int64("chat_id", update.ChatID()),
		)
		return h(ctx, update)
	}
}
