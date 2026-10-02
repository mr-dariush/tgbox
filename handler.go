package tgbox

import (
	"github.com/gotd/td/tg"

	"github.com/mr-dariush/tgbox/internal/dispatcher"
)

// Handler is the universal function signature for all update handlers.
type Handler func(ctx *Context, update *Update) error

// RawHandler is a lower-level handler for raw MTProto updates.
type RawHandler func(ctx *Context, entities *tg.Entities, raw tg.UpdateClass) error

// Plugin defines the interface for TGBox modules.
type Plugin interface {
	// Register is called by the client to initialize the plugin.
	Register(r Registrar) error
}

// Controller represents a cohesive group of route handlers configured via explicit dependency injection.
type Controller interface {
	// RegisterRoutes registers the controller's endpoints and handlers onto the provided Registrar.
	RegisterRoutes(r Registrar) error
}

// ControllerFunc allows plain functions to act as a Controller.
type ControllerFunc func(r Registrar) error

// RegisterRoutes implements Controller.
func (f ControllerFunc) RegisterRoutes(r Registrar) error {
	return f(r)
}

// Registrar abstracts the handler registration methods for plugins, controllers, and the client.
type Registrar interface {
	OnMessage(f Filter, h Handler, opts ...HandlerOption)
	OnEditedMessage(f Filter, h Handler, opts ...HandlerOption)
	OnCallbackQuery(f Filter, h Handler, opts ...HandlerOption)
	OnInlineQuery(f Filter, h Handler, opts ...HandlerOption)
	OnChatMemberUpdated(f Filter, h Handler, opts ...HandlerOption)
	OnChatJoinRequest(f Filter, h Handler, opts ...HandlerOption)
	OnDeletedMessages(f Filter, h Handler, opts ...HandlerOption)
	OnCommand(pattern string, h Handler, opts ...HandlerOption)
	OnState(state string, h Handler, opts ...HandlerOption)
	OnRawUpdate(h RawHandler, opts ...HandlerOption)
	API() *tg.Client
	Self() *tg.User
}

// RegisterControllers registers one or more controllers onto the provided Registrar.
func RegisterControllers(r Registrar, controllers ...Controller) error {
	for _, ctrl := range controllers {
		if err := ctrl.RegisterRoutes(r); err != nil {
			return err
		}
	}
	return nil
}

// Filter defines a predicate for routing updates.
type Filter func(ctx *Context, update *Update) bool

// HandlerOption defines optional configurations for a registered handler.
type HandlerOption interface {
	apply(*handlerConfig)
}

type fnHandlerOption func(*handlerConfig)

func (f fnHandlerOption) apply(c *handlerConfig) { f(c) }

type handlerConfig struct {
	group       int
	middlewares []Middleware
}

// InGroup assigns a priority group to the handler. Groups are evaluated in
// ascending order; within a group, the first handler whose filter matches
// runs, then evaluation moves to the next group.
func InGroup(group int) HandlerOption {
	return fnHandlerOption(func(cfg *handlerConfig) { cfg.group = group })
}

// WithLocalMiddlewares adds middlewares to the handler.
func WithLocalMiddlewares(m ...Middleware) HandlerOption {
	return fnHandlerOption(func(cfg *handlerConfig) {
		cfg.middlewares = append(cfg.middlewares, m...)
	})
}

// applyHandlerOptions extracts configuration from the variadic options.
func applyHandlerOptions(opts []HandlerOption) handlerConfig {
	var cfg handlerConfig
	for _, opt := range opts {
		opt.apply(&cfg)
	}
	return cfg
}

// Control-flow sentinel errors handlers may return to steer group traversal.
var (
	// ErrEndGroups stops evaluation of all remaining groups for this update.
	ErrEndGroups = dispatcher.ErrEndGroups
	// ErrContinueGroups keeps scanning the current group for further
	// matching handlers instead of advancing to the next group.
	ErrContinueGroups = dispatcher.ErrContinueGroups
)
