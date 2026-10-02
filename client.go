package tgbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/contrib/storage"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/dispatcher"
	"github.com/mr-dariush/tgbox/internal/engine"
	"github.com/mr-dariush/tgbox/internal/fsm"
	"github.com/mr-dariush/tgbox/internal/interceptor"
	"github.com/mr-dariush/tgbox/internal/keepalive"
	"github.com/mr-dariush/tgbox/internal/router"
	tgboxstorage "github.com/mr-dariush/tgbox/internal/storage"
	"github.com/mr-dariush/tgbox/network"
)

// Dispatcher defines the contract for routing updates to handlers.
type Dispatcher interface {
	Start(ctx context.Context)
	Handle(c *Context, update *Update)
	AddRoute(kind int, f Filter, h Handler, m []Middleware, group int)
	SetInterceptor(f func(*Context, *Update) bool)
	SetWildcardKind(kind int)
}

// CommandRouter defines the contract for the parametric command routing tree.
type CommandRouter interface {
	Insert(path string, handler commandRoute)
	Match(path string) (handler commandRoute, params map[string]string, ok bool)
}

const defaultShardBuffer = 100

type commandRoute struct {
	handler     Handler
	middlewares []Middleware
}

type registeredCommand struct {
	pattern     string
	handler     Handler
	middlewares []Middleware
}

type stateRoute struct {
	handler     Handler
	middlewares []Middleware
}

// Client is the main entry point for the tgbox framework.
type Client struct {
	engine     engine.Engine
	api        *tg.Client
	sender     *message.Sender
	dispatcher Dispatcher

	chatService  MessageService
	adminService AdminService
	stateManager StateManager

	commandRouter   atomic.Pointer[defaultCommandRouter]
	commandRoutesMu sync.Mutex
	commandEntries  []registeredCommand

	stateRoutes   atomic.Pointer[map[string]stateRoute]
	stateRoutesMu sync.Mutex

	peers      storage.PeerStorage
	session    session.Storage
	self       atomic.Pointer[tg.User]
	logger     *slog.Logger
	bufferPool *network.BufferPool

	fsmStorage fsm.Storage

	keepaliveEngine *keepalive.Engine
	traceCounter    atomic.Uint64
	closers         []io.Closer
	opts            clientOptions
}

type clientOptions struct {
	appID             int
	appHash           string
	botToken          string
	workers           int
	slogLogger        *slog.Logger
	peerStorage       storage.PeerStorage
	autoPeerCaching   bool
	engineOpts        telegram.Options
	fsmStorage        fsm.Storage
	trafficObserver   func()
	poolLimits        network.ConnectionPoolLimits
	keepaliveSettings network.KeepAliveSettings
	gatedDialer       *network.GatedDialer
	customEngine      engine.Engine
	profiler          *Profiler
}

// Option defines a functional configuration for the Client.
type Option interface {
	apply(*clientOptions)
}

type fnOption func(*clientOptions)

func (f fnOption) apply(o *clientOptions) { f(o) }

// WithAppID sets the Telegram Application ID.
func WithAppID(id int) Option { return fnOption(func(o *clientOptions) { o.appID = id }) }

// WithAppHash sets the Telegram Application Hash.
func WithAppHash(hash string) Option { return fnOption(func(o *clientOptions) { o.appHash = hash }) }

// WithBotToken sets the Telegram Bot token for authentication.
func WithBotToken(token string) Option {
	return fnOption(func(o *clientOptions) { o.botToken = token })
}

// WithoutUpdates disables the update manager in the underlying engine.
func WithoutUpdates() Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.NoUpdates = true
	})
}

// WithEngineOptions applies custom options directly to the underlying gotd engine options.
func WithEngineOptions(fn func(opts *telegram.Options)) Option {
	return fnOption(func(o *clientOptions) {
		fn(&o.engineOpts)
	})
}

// WithEngine provides a custom MTProto Engine implementation.
func WithEngine(e engine.Engine) Option {
	return fnOption(func(o *clientOptions) {
		o.customEngine = e
	})
}

// WithWorkers configures the number of parallel dispatcher worker shards.
func WithWorkers(w int) Option { return fnOption(func(o *clientOptions) { o.workers = w }) }

// WithLogger sets the structured slog logger for the client.
func WithLogger(l *slog.Logger) Option { return fnOption(func(o *clientOptions) { o.slogLogger = l }) }

// WithPeerStorage configures custom peer storage.
func WithPeerStorage(p storage.PeerStorage) Option {
	return fnOption(func(o *clientOptions) { o.peerStorage = p })
}

// WithAutoPeerCaching enables automatic background dialogs collection for peer cache warming.
func WithAutoPeerCaching(enabled bool) Option {
	return fnOption(func(o *clientOptions) { o.autoPeerCaching = enabled })
}

// WithFSMStorage configures a custom conversational FSM storage backend.
func WithFSMStorage(s fsm.Storage) Option {
	return fnOption(func(o *clientOptions) { o.fsmStorage = s })
}

// WithTrafficObserver attaches a traffic activity callback for keep-alive monitoring.
func WithTrafficObserver(cb func()) Option {
	return fnOption(func(o *clientOptions) {
		o.trafficObserver = cb
	})
}

// WithPFS configures Perfect Forward Secrecy (PFS) with optional temporary authorization key TTL.
func WithPFS(enabled bool, ttl ...time.Duration) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.EnablePFS = enabled
		if len(ttl) > 0 && ttl[0] > 0 {
			o.engineOpts.TempKeyTTL = int(ttl[0].Seconds())
			return
		}
		if enabled && o.engineOpts.TempKeyTTL == 0 {
			const defaultTempKeyTTL = 86400
			o.engineOpts.TempKeyTTL = defaultTempKeyTTL
		}
	})
}

// WithProfiler enables runtime diagnostic profiling and the goroutine leak watchdog.
func WithProfiler(cfg ProfilerConfig) Option {
	return fnOption(func(o *clientOptions) {
		cfg.Enabled = true
		o.profiler = NewProfiler(cfg, o.slogLogger)
	})
}

var (
	_ Dispatcher    = (*defaultDispatcher)(nil)
	_ CommandRouter = (*defaultCommandRouter)(nil)
)

type defaultDispatcher struct {
	inner *dispatcher.Dispatcher[*Context, *Update]
}

func (d *defaultDispatcher) Start(ctx context.Context) { d.inner.Start(ctx) }

func (d *defaultDispatcher) Handle(c *Context, update *Update) { d.inner.Handle(c, update) }

func (d *defaultDispatcher) AddRoute(kind int, f Filter, h Handler, m []Middleware, group int) {
	d.inner.AddRoute(kind,
		dispatcher.Filter[*Context, *Update](f),
		dispatcher.Handler[*Context, *Update](h),
		castMiddlewares(m), group)
}

func (d *defaultDispatcher) SetInterceptor(f func(*Context, *Update) bool) {
	d.inner.SetInterceptor(f)
}

func (d *defaultDispatcher) SetWildcardKind(kind int) { d.inner.SetWildcardKind(kind) }

type defaultCommandRouter struct {
	inner *router.Node[commandRoute]
}

func (r *defaultCommandRouter) Insert(path string, handler commandRoute) {
	r.inner.Insert(path, handler)
}

func (r *defaultCommandRouter) Match(path string) (commandRoute, map[string]string, bool) {
	return r.inner.Match(path)
}

// New creates a TGBox client.
func New(appName string, opts ...Option) (*Client, error) {
	config := clientOptions{
		slogLogger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	for _, opt := range opts {
		opt.apply(&config)
	}

	if config.appID == 0 {
		return nil, errors.New("tgbox: app ID is required (use WithAppID)")
	}
	if config.appHash == "" {
		return nil, errors.New("tgbox: app hash is required (use WithAppHash)")
	}
	if config.workers < 1 {
		config.workers = runtime.NumCPU() * 16
	}

	client := &Client{
		logger:     config.slogLogger,
		bufferPool: network.NewBufferPool(0),
	}

	emptyRoutes := make(map[string]stateRoute)
	client.stateRoutes.Store(&emptyRoutes)
	client.commandRouter.Store(&defaultCommandRouter{inner: router.NewNode[commandRoute]()})

	hashFunc := func(_ *Context, upd *Update) uint64 {
		cid := upd.ChatID()
		if cid == 0 {
			cid = upd.SenderID()
		}
		if cid < 0 {
			cid = -cid
		}
		return uint64(cid)
	}

	kindFunc := func(_ *Context, upd *Update) int {
		return int(upd.Kind)
	}

	disp := dispatcher.NewDispatcher[*Context, *Update](
		config.workers,
		defaultShardBuffer,
		hashFunc,
		kindFunc,
		config.slogLogger,
	)
	disp.SetWildcardKind(int(KindRaw))
	client.dispatcher = &defaultDispatcher{inner: disp}

	if config.profiler != nil {
		client.closers = append(client.closers, config.profiler)
	}

	if config.fsmStorage != nil {
		client.fsmStorage = config.fsmStorage
	} else {
		memStore := fsm.NewMemoryStorage(context.Background(), 15*time.Minute)
		client.fsmStorage = memStore
		client.closers = append(client.closers, memStore)
	}
	client.stateManager = NewStateManager(client.fsmStorage)

	if config.peerStorage != nil {
		client.peers = config.peerStorage
	} else {
		peers, closer, err := tgboxstorage.NewDefaultPeerStorage(appName + ".peers.db")
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("tgbox: create peer storage: %w", err)
		}
		client.peers = peers
		client.closers = append(client.closers, closer)
	}

	if config.engineOpts.SessionStorage == nil {
		sess, closer, err := tgboxstorage.NewDefaultSessionStorage(appName + ".session.db")
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("tgbox: create session storage: %w", err)
		}
		config.engineOpts.SessionStorage = sess
		client.closers = append(client.closers, closer)
	}
	client.session = config.engineOpts.SessionStorage

	if config.customEngine != nil {
		client.engine = config.customEngine
	} else {
		client.engine = engine.NewGotdEngine(config.appID, config.appHash, config.engineOpts)
	}
	client.opts = config

	if config.keepaliveSettings.PingInterval > 0 {
		client.keepaliveEngine = keepalive.NewEngine(
			keepalive.Config{
				PingInterval: config.keepaliveSettings.PingInterval,
				JitterOffset: config.keepaliveSettings.JitterOffset,
			},
			func(pingCtx context.Context) error {
				if client.engine == nil || client.engine.Raw() == nil {
					return nil
				}
				return client.engine.Raw().Ping(pingCtx)
			},
			config.engineOpts.Clock,
		)
	}

	originalObserver := config.trafficObserver
	config.trafficObserver = func() {
		if originalObserver != nil {
			originalObserver()
		}
		if client.keepaliveEngine != nil {
			client.keepaliveEngine.NotifyTraffic()
		}
	}
	client.opts = config

	var finalInvoker tg.Invoker
	if client.engine != nil && client.engine.Raw() != nil {
		sniffedInvoker := interceptor.NewOutgoingInterceptor(
			client.engine.Raw(),
			client.handleUpdates,
			func(ctx context.Context) (string, int64) {
				return TraceIDFromContext(ctx), ClientIDFromContext(ctx)
			},
		)

		finalInvoker = sniffedInvoker
		if config.trafficObserver != nil {
			finalInvoker = telegram.InvokeFunc(func(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
				err := sniffedInvoker.Invoke(ctx, input, output)
				if err == nil {
					config.trafficObserver()
				}
				return err
			})
		}
	}
	client.api = tg.NewClient(finalInvoker)
	client.sender = message.NewSender(client.api)
	client.chatService = NewChatService(client.sender, client.api, client.peers)
	client.adminService = NewAdminService(client.api)

	client.registerBuiltinRoutes()

	return client, nil
}

func fsmKey(chatID, userID int64) string {
	return fmt.Sprintf("fsm:%d:%d", chatID, userID)
}

func (c *Client) registerBuiltinRoutes() {
	c.dispatcher.AddRoute(
		int(KindMessage),
		func(ctx *Context, upd *Update) bool {
			if upd == nil || upd.Message == nil {
				return false
			}
			routerPtr := c.commandRouter.Load()
			if routerPtr == nil {
				return false
			}

			text := upd.Message.Message
			route, params, ok := routerPtr.Match(text)
			if !ok {
				stripped, changed := c.stripBotMention(text)
				if !changed {
					return false
				}
				if route, params, ok = routerPtr.Match(stripped); !ok {
					return false
				}
			}
			ctx.cmdRoute = &route
			ctx.Params = params
			return true
		},
		func(ctx *Context, upd *Update) error {
			route := ctx.cmdRoute
			if route == nil {
				return nil
			}
			return consume(wrap(route.handler, route.middlewares))(ctx, upd)
		},
		nil,
		groupCommands,
	)

	c.dispatcher.AddRoute(
		int(KindMessage),
		func(ctx *Context, upd *Update) bool {
			if upd == nil {
				return false
			}
			chatID := upd.ChatID()
			userID := upd.SenderID()
			if chatID == 0 || userID == 0 {
				return false
			}
			sm := c.State()
			if sm == nil {
				return false
			}
			state, err := sm.GetState(ctx.Context(), chatID, userID)
			if err != nil || state == "" {
				return false
			}
			routesPtr := c.stateRoutes.Load()
			if routesPtr == nil || *routesPtr == nil {
				return false
			}
			_, matched := (*routesPtr)[state]
			return matched
		},
		func(ctx *Context, upd *Update) error {
			if upd == nil {
				return ErrNilUpdate
			}
			chatID := upd.ChatID()
			userID := upd.SenderID()
			if chatID == 0 || userID == 0 {
				return ErrInvalidFSMContext
			}
			sm := c.State()
			if sm == nil {
				return ErrFSMNotConfigured
			}
			state, err := sm.GetState(ctx.Context(), chatID, userID)
			if err != nil {
				return fmt.Errorf("tgbox: read FSM state: %w", err)
			}
			routesPtr := c.stateRoutes.Load()
			if routesPtr == nil || *routesPtr == nil {
				return nil
			}
			route, ok := (*routesPtr)[state]
			if !ok {
				return nil
			}
			return consume(wrap(route.handler, route.middlewares))(ctx, upd)
		},
		nil,
		groupStates,
	)
}

func (c *Client) stripBotMention(text string) (string, bool) {
	if !strings.HasPrefix(text, "/") {
		return text, false
	}

	token := text
	rest := ""
	if i := strings.IndexAny(text, " \t\n"); i != -1 {
		token, rest = text[:i], text[i:]
	}

	at := strings.LastIndexByte(token, '@')
	if at == -1 {
		return text, false
	}

	mention := token[at+1:]
	if self := c.self.Load(); self != nil && self.Username != "" &&
		!strings.EqualFold(mention, self.Username) {
		return text, false
	}
	return token[:at] + rest, true
}

const (
	groupCommands = -100
	groupStates   = -90
)

func wrap(h Handler, middlewares []Middleware) Handler {
	for _, middleware := range slices.Backward(middlewares) {
		h = middleware(h)
	}
	return h
}

func consume(h Handler) Handler {
	return func(ctx *Context, upd *Update) error {
		err := h(ctx, upd)
		switch {
		case err == nil, errors.Is(err, ErrEndGroups):
			return ErrEndGroups
		case errors.Is(err, ErrContinueGroups):
			return err
		default:
			ctx.Logger.Error("handler error", slog.Any("error", err))
			return ErrEndGroups
		}
	}
}

// OnState registers a handler for conversational state transitions.
func (c *Client) OnState(state string, h Handler, opts ...HandlerOption) {
	cfg := applyHandlerOptions(opts)
	c.stateRoutesMu.Lock()
	defer c.stateRoutesMu.Unlock()

	curr := c.stateRoutes.Load()
	var newMap map[string]stateRoute
	if curr != nil && *curr != nil {
		newMap = make(map[string]stateRoute, len(*curr)+1)
		maps.Copy(newMap, *curr)
	} else {
		newMap = make(map[string]stateRoute, 1)
	}

	newMap[state] = stateRoute{
		handler:     h,
		middlewares: cfg.middlewares,
	}
	c.stateRoutes.Store(&newMap)
}

// FetchSelf retrieves and caches the current authenticated user/bot metadata.
func (c *Client) FetchSelf(ctx context.Context) error {
	self, err := c.engine.Raw().Self(ctx)
	if err != nil {
		return fmt.Errorf("get self: %w", err)
	}
	c.self.Store(self)
	c.logger.Info("tgbox client authenticated", slog.String("user", self.Username))
	return nil
}

// Run starts the MTProto connection, authenticates, and blocks until the context is canceled.
func (c *Client) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	c.dispatcher.Start(runCtx)

	if c.keepaliveEngine != nil {
		c.keepaliveEngine.Start(runCtx)
	}

	if err := c.engine.Connect(runCtx); err != nil {
		return fmt.Errorf("engine connect: %w", err)
	}

	handleFatal := func(err error) error {
		if c.isFatalSessionError(err) {
			c.logger.Error("fatal session error, invalidating stored session",
				slog.Any("error", err))
			storeCtx, storeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer storeCancel()
			if serr := c.session.StoreSession(storeCtx, nil); serr != nil {
				c.logger.Error("failed to invalidate session", slog.Any("error", serr))
			}
			cancel()
		}
		return err
	}

	if c.opts.botToken != "" {
		if _, err := c.engine.Raw().Auth().Bot(runCtx, c.opts.botToken); err != nil {
			return fmt.Errorf("bot auth: %w", handleFatal(err))
		}
	}

	status, err := c.engine.Raw().Auth().Status(runCtx)
	if err != nil {
		return fmt.Errorf("auth status: %w", handleFatal(err))
	}

	if status.Authorized {
		if err := c.FetchSelf(runCtx); err != nil {
			return fmt.Errorf("fetch self: %w", handleFatal(err))
		}
	} else {
		c.logger.Info("tgbox client is running (Unauthenticated). Awaiting interactive login...")
	}

	baseHandler := telegram.UpdateHandlerFunc(c.handleUpdates)
	c.engine.SetUpdateHandler(tgboxstorage.SetupUpdateHook(baseHandler, c.peers))

	if status.Authorized && c.opts.autoPeerCaching && c.peers != nil {
		go c.warmPeerStorage(runCtx)
	}

	<-runCtx.Done()
	return c.engine.Disconnect()
}

func (c *Client) warmPeerStorage(ctx context.Context) {
	c.logger.Info("warming up peer storage")
	collector := storage.CollectPeers(c.peers)
	iter := query.GetDialogs(c.api).Iter()
	if err := collector.Dialogs(ctx, iter); err != nil {
		c.logger.Error("peer storage warm-up failed", slog.Any("error", err))
		return
	}
	c.logger.Info("peer storage warm-up complete")
}

// Close gracefully terminates open background storage engines and profilers.
func (c *Client) Close() error {
	var errs []error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	c.closers = nil
	return errors.Join(errs...)
}

// Suspend pauses socket dialing and closes active connections if a gated dialer is configured.
func (c *Client) Suspend() error {
	if c.opts.gatedDialer == nil {
		return ErrNetworkNotConfigured
	}
	c.opts.gatedDialer.Suspend()
	return nil
}

// Resume unblocks socket dialing if a gated dialer is configured.
func (c *Client) Resume() error {
	if c.opts.gatedDialer == nil {
		return ErrNetworkNotConfigured
	}
	c.opts.gatedDialer.Resume()
	return nil
}

func (*Client) isFatalSessionError(err error) bool {
	if err == nil {
		return false
	}
	if tgerr.Is(err, "AUTH_KEY_UNREGISTERED", "SESSION_REVOKED", "SESSION_EXPIRED") {
		return true
	}
	errStr := err.Error()
	return strings.Contains(errStr, "AUTH_KEY_UNREGISTERED") ||
		strings.Contains(errStr, "SESSION_REVOKED") ||
		strings.Contains(errStr, "SESSION_EXPIRED")
}

func (c *Client) handleUpdates(ctx context.Context, u tg.UpdatesClass) error {
	if c.opts.trafficObserver != nil {
		c.opts.trafficObserver()
	}

	entities := &tg.Entities{
		Users:    make(map[int64]*tg.User),
		Chats:    make(map[int64]*tg.Chat),
		Channels: make(map[int64]*tg.Channel),
	}

	populate := func(users []tg.UserClass, chats []tg.ChatClass) {
		for _, uc := range users {
			if user, ok := uc.(*tg.User); ok {
				entities.Users[user.ID] = user
			}
		}
		for _, cc := range chats {
			switch ch := cc.(type) {
			case *tg.Chat:
				entities.Chats[ch.ID] = ch
			case *tg.Channel:
				entities.Channels[ch.ID] = ch
			}
		}
	}

	switch upds := u.(type) {
	case *tg.Updates:
		populate(upds.Users, upds.Chats)
	case *tg.UpdatesCombined:
		populate(upds.Users, upds.Chats)
	}

	traceID := strconv.FormatUint(c.traceCounter.Inc(), 36)
	isOutgoing := interceptor.IsOutgoing(ctx)

	var clientID int64
	if self := c.self.Load(); self != nil {
		clientID = self.ID
	}

	tracedCtx := WithTraceID(ctx, traceID)
	tracedCtx = WithClientID(tracedCtx, clientID)

	pCtx := &Context{
		StdContext: tracedCtx,
		Entities:   entities,
		Logger:     c.logger.With(slog.String("trace_id", traceID), slog.Int64("client_id", clientID)),
	}

	dispatchOne := func(raw tg.UpdateClass) {
		newCtx := *pCtx
		newCtx.Update = &Update{Raw: raw, Kind: KindRaw, IsOutgoing: isOutgoing}
		c.dispatchUpdate(&newCtx)
	}

	switch upds := u.(type) {
	case *tg.Updates:
		for _, upd := range upds.Updates {
			dispatchOne(upd)
		}
	case *tg.UpdateShort:
		dispatchOne(upds.Update)
	case *tg.UpdatesCombined:
		for _, upd := range upds.Updates {
			dispatchOne(upd)
		}
	}

	return nil
}

func (c *Client) dispatchUpdate(pCtx *Context) {
	upd := pCtx.Update

	switch m := upd.Raw.(type) {
	case *tg.UpdateNewMessage:
		if msg, ok := m.Message.(*tg.Message); ok {
			upd.Message = msg
			upd.Kind = KindMessage
		}
	case *tg.UpdateNewChannelMessage:
		if msg, ok := m.Message.(*tg.Message); ok {
			upd.Message = msg
			upd.Kind = KindMessage
		}
	case *tg.UpdateEditMessage:
		if msg, ok := m.Message.(*tg.Message); ok {
			upd.Message = msg
			upd.Kind = KindEditedMessage
		}
	case *tg.UpdateEditChannelMessage:
		if msg, ok := m.Message.(*tg.Message); ok {
			upd.Message = msg
			upd.Kind = KindEditedMessage
		}
	case *tg.UpdateBotCallbackQuery:
		upd.CallbackQuery = m
		upd.Kind = KindCallbackQuery
	case *tg.UpdateBotInlineQuery:
		upd.InlineQuery = m
		upd.Kind = KindInlineQuery
	case *tg.UpdateChatParticipant:
		upd.ChatMember = m
		upd.Kind = KindChatMemberUpdated
	case *tg.UpdateChannelParticipant:
		upd.ChannelMember = m
		upd.Kind = KindChatMemberUpdated
	case *tg.UpdatePendingJoinRequests:
		upd.JoinRequest = m
		upd.Kind = KindChatJoinRequest
	case *tg.UpdateDeleteMessages, *tg.UpdateDeleteChannelMessages:
		upd.Kind = KindDeletedMessages
	}

	c.dispatcher.Handle(pCtx, upd)
}

func (c *Client) on(kind UpdateKind, f Filter, h Handler, opts []HandlerOption) {
	cfg := applyHandlerOptions(opts)
	c.dispatcher.AddRoute(int(kind), f, h, cfg.middlewares, cfg.group)
}

// OnMessage registers an update handler for new incoming messages.
func (c *Client) OnMessage(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindMessage, f, h, opts)
}

// OnEditedMessage registers an update handler for edited messages.
func (c *Client) OnEditedMessage(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindEditedMessage, f, h, opts)
}

// OnCallbackQuery registers an update handler for inline button callbacks.
func (c *Client) OnCallbackQuery(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindCallbackQuery, f, h, opts)
}

// OnInlineQuery registers an update handler for inline queries.
func (c *Client) OnInlineQuery(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindInlineQuery, f, h, opts)
}

// OnChatMemberUpdated registers an update handler for chat membership updates.
func (c *Client) OnChatMemberUpdated(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindChatMemberUpdated, f, h, opts)
}

// OnChatJoinRequest registers an update handler for chat join requests.
func (c *Client) OnChatJoinRequest(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindChatJoinRequest, f, h, opts)
}

// OnDeletedMessages registers an update handler for message deletion events.
func (c *Client) OnDeletedMessages(f Filter, h Handler, opts ...HandlerOption) {
	c.on(KindDeletedMessages, f, h, opts)
}

// OnCommand registers a handler for command matching via the parametric Radix Trie.
func (c *Client) OnCommand(pattern string, h Handler, opts ...HandlerOption) {
	cfg := applyHandlerOptions(opts)
	if !strings.HasPrefix(pattern, "/") {
		pattern = "/" + pattern
	}

	c.commandRoutesMu.Lock()
	defer c.commandRoutesMu.Unlock()

	c.commandEntries = append(c.commandEntries, registeredCommand{
		pattern:     pattern,
		handler:     h,
		middlewares: cfg.middlewares,
	})

	currRouter := c.commandRouter.Load()
	var newTree *router.Node[commandRoute]
	if currRouter != nil && currRouter.inner != nil {
		newTree = currRouter.inner.Clone()
	} else {
		newTree = router.NewNode[commandRoute]()
	}

	newTree.Insert(pattern, commandRoute{
		handler:     h,
		middlewares: cfg.middlewares,
	})

	c.commandRouter.Store(&defaultCommandRouter{inner: newTree})
}

// OnRawUpdate registers a low-level handler for raw MTProto updates.
func (c *Client) OnRawUpdate(h RawHandler, opts ...HandlerOption) {
	c.on(KindRaw,
		func(_ *Context, _ *Update) bool { return true },
		func(ctx *Context, upd *Update) error {
			return h(ctx, ctx.Entities, upd.Raw)
		},
		opts)
}

func castMiddlewares(m []Middleware) []dispatcher.Middleware[*Context, *Update] {
	if len(m) == 0 {
		return nil
	}
	res := make([]dispatcher.Middleware[*Context, *Update], len(m))
	for i, mw := range m {
		res[i] = func(next dispatcher.Handler[*Context, *Update]) dispatcher.Handler[*Context, *Update] {
			return dispatcher.Handler[*Context, *Update](mw(Handler(next)))
		}
	}
	return res
}

// Install registers one or more plugins with the client.
func (c *Client) Install(plugins ...Plugin) error {
	for _, plugin := range plugins {
		if err := plugin.Register(c); err != nil {
			return fmt.Errorf("tgbox: register plugin: %w", err)
		}
	}
	return nil
}

// API returns the underlying raw MTProto API client.
func (c *Client) API() *tg.Client {
	return c.api
}

// Sender returns the application-scoped, concurrency-safe message.Sender bound to this client.
func (c *Client) Sender() *message.Sender {
	return c.sender
}

// Self returns the authenticated Telegram User object.
func (c *Client) Self() *tg.User {
	return c.self.Load()
}

// Chat returns the application-scoped MessageService for sending and modifying messages.
func (c *Client) Chat() MessageService {
	if c.chatService == nil && c.sender != nil && c.api != nil {
		c.chatService = NewChatService(c.sender, c.api, c.peers)
	}
	return c.chatService
}

// Admin returns the application-scoped AdminService for supergroup and channel moderation.
func (c *Client) Admin() AdminService {
	if c.adminService == nil && c.api != nil {
		c.adminService = NewAdminService(c.api)
	}
	return c.adminService
}

// State returns the application-scoped StateManager for conversational FSM management.
func (c *Client) State() StateManager {
	if c.stateManager == nil && c.fsmStorage != nil {
		c.stateManager = NewStateManager(c.fsmStorage)
	}
	return c.stateManager
}

// RegisterController registers one or more controllers with the client.
func (c *Client) RegisterController(controllers ...Controller) error {
	return RegisterControllers(c, controllers...)
}

// UploadFile streams and uploads a file to Telegram servers using the connection pool.
func (c *Client) UploadFile(ctx context.Context, name string, src io.Reader, size int64) (tg.InputFileClass, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("tgbox: upload file canceled: %w", err)
	}
	if c.engine == nil || c.engine.Raw() == nil {
		return nil, errors.New("tgbox: engine not initialized")
	}

	limit := max(c.opts.poolLimits.MaxUploadConnections, 1)

	poolInvoker, err := c.engine.Raw().Pool(limit)
	if err != nil {
		return nil, fmt.Errorf("tgbox: failed to allocate upload pool: %w", err)
	}
	defer func() { _ = poolInvoker.Close() }()

	poolClient := tg.NewClient(poolInvoker)
	up := uploader.NewUploader(poolClient).WithThreads(int(limit))

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	bufPool := c.bufferPool

	go func() {
		defer func() { _ = pw.Close() }()
		if _, copyErr := bufPool.PipeThrough(pw, src); copyErr != nil {
			_ = pw.CloseWithError(copyErr)
		}
	}()

	uploadObj := uploader.NewUpload(name, pr, size)
	inputFile, err := up.Upload(ctx, uploadObj)
	if err != nil {
		return nil, fmt.Errorf("tgbox: file upload failed: %w", err)
	}

	return inputFile, nil
}

// DownloadFile streams or downloads a file from Telegram servers into dst.
func (c *Client) DownloadFile(ctx context.Context, location tg.InputFileLocationClass, dst io.Writer) (tg.StorageFileTypeClass, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("tgbox: download file canceled: %w", err)
	}
	if c.engine == nil || c.engine.Raw() == nil {
		return nil, errors.New("tgbox: engine not initialized")
	}

	limit := max(c.opts.poolLimits.MaxDownloadConnections, 1)

	poolInvoker, err := c.engine.Raw().Pool(limit)
	if err != nil {
		return nil, fmt.Errorf("tgbox: failed to allocate download pool: %w", err)
	}
	defer func() { _ = poolInvoker.Close() }()

	poolClient := tg.NewClient(poolInvoker)
	dl := downloader.NewDownloader()
	builder := dl.Download(poolClient, location)

	if writerAt, ok := dst.(io.WriterAt); ok && limit > 1 {
		fileType, err := builder.WithThreads(int(limit)).Parallel(ctx, writerAt)
		if err != nil {
			return nil, fmt.Errorf("tgbox: parallel file download failed: %w", err)
		}
		return fileType, nil
	}

	pr, pw := io.Pipe()
	defer func() { _ = pr.Close() }()

	bufPool := c.bufferPool

	fileTypeChan := make(chan tg.StorageFileTypeClass, 1)
	errChan := make(chan error, 1)

	go func() {
		defer func() { _ = pw.Close() }()
		fileType, streamErr := builder.Stream(ctx, pw)
		if streamErr != nil {
			_ = pw.CloseWithError(streamErr)
			errChan <- streamErr
			return
		}
		fileTypeChan <- fileType
	}()

	if _, err = bufPool.PipeThrough(dst, pr); err != nil {
		return nil, fmt.Errorf("tgbox: streaming file download failed: %w", err)
	}

	select {
	case streamErr := <-errChan:
		return nil, fmt.Errorf("tgbox: download stream failed: %w", streamErr)
	case fileType := <-fileTypeChan:
		return fileType, nil
	}
}
