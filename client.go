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
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"go.uber.org/atomic"

	"github.com/mr-dariush/tgbox/internal/channelpool"
	"github.com/mr-dariush/tgbox/internal/dispatcher"
	"github.com/mr-dariush/tgbox/internal/disruptor"
	"github.com/mr-dariush/tgbox/internal/engine"
	"github.com/mr-dariush/tgbox/internal/fsm"
	"github.com/mr-dariush/tgbox/internal/interceptor"
	"github.com/mr-dariush/tgbox/internal/keepalive"
	"github.com/mr-dariush/tgbox/internal/router"
	tgboxstorage "github.com/mr-dariush/tgbox/internal/storage"
	"github.com/mr-dariush/tgbox/internal/syncstate"
	"github.com/mr-dariush/tgbox/network"
)

// Dispatcher defines the contract for routing updates to handlers.
type Dispatcher interface {
	Start(ctx context.Context)
	Handle(c *Context, update *Update)
	AddRoute(kind int, f Filter, h Handler, m []Middleware, group int)
	SetInterceptor(f func(*Context, *Update) bool)
	SetWildcardKind(kind int)
	SetMetrics(m dispatcher.MetricsReporter)
}

// CommandRouter defines the contract for the parametric command routing tree.
type CommandRouter interface {
	Insert(path string, handler commandRoute)
	Match(path string) (handler commandRoute, params map[string]string, ok bool)
}

const defaultShardBuffer = 1024

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

	keepaliveEngine     *keepalive.Engine
	stateStorage        syncstate.StateStorage
	traceCounter        atomic.Uint64
	closers             []io.Closer
	ringBuffer          *disruptor.RingBuffer
	sequencer           *disruptor.Sequencer
	poller              *disruptor.SingleConsumerPoller
	stateVector         *syncstate.StateVector
	flusher             *syncstate.CheckpointFlusher
	diffFetcher         *syncstate.DifferenceFetcher
	gapBuffer           *syncstate.GapBuffer
	channelPool         *channelpool.WorkerPool
	channelPTS          sync.Map
	channelAccessHashes sync.Map
	sentMsgPeers        sync.Map
	contextPool         *ContextPool
	opts                clientOptions

	postponedMu      sync.Mutex
	postponedUpdates []tg.UpdatesClass
	isFetchingDiff   atomic.Bool
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
	stateStorage      syncstate.StateStorage
	poolLimits        network.ConnectionPoolLimits
	keepaliveSettings network.KeepAliveSettings
	gatedDialer       *network.GatedDialer
	customEngine      engine.Engine
	profiler          *Profiler
	onTooLong         func()
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

// WithStateStorage configures a custom MTProto state vector persistence storage.
func WithStateStorage(s syncstate.StateStorage) Option {
	return fnOption(func(o *clientOptions) { o.stateStorage = s })
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

// WithOnTooLong sets a callback to be executed when the server buffer overflows (updatesTooLong).
func WithOnTooLong(cb func()) Option {
	return fnOption(func(o *clientOptions) { o.onTooLong = cb })
}

var (
	_ Dispatcher    = (*defaultDispatcher)(nil)
	_ CommandRouter = (*defaultCommandRouter)(nil)
)

type defaultDispatcher struct {
	inner *dispatcher.Dispatcher[*Context, *Update]
}

func (d *defaultDispatcher) Start(ctx context.Context) { d.inner.Start(ctx) }

func (d *defaultDispatcher) Handle(c *Context, update *Update) {
	if c != nil && c.StdContext != nil {
		_ = d.inner.HandleWait(c.StdContext, c, update)
		return
	}
	d.inner.Handle(c, update)
}

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

func (d *defaultDispatcher) SetMetrics(m dispatcher.MetricsReporter) { d.inner.SetMetrics(m) }

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
		logger:      config.slogLogger,
		bufferPool:  network.NewBufferPool(0),
		contextPool: NewContextPool(),
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
	if config.profiler != nil && config.profiler.Metrics() != nil {
		disp.SetMetrics(config.profiler.Metrics())
	}
	client.dispatcher = &defaultDispatcher{inner: disp}

	ring, err := disruptor.NewRingBuffer(disruptor.DefaultCapacity)
	if err != nil {
		return nil, fmt.Errorf("tgbox: init ring buffer: %w", err)
	}
	client.ringBuffer = ring
	client.poller = disruptor.NewSingleConsumerPoller(ring, 20*time.Microsecond)

	client.sequencer = disruptor.NewSequencer(ring, func(droppedCount uint64) {
		if client.opts.profiler != nil && client.opts.profiler.Metrics() != nil {
			client.opts.profiler.Metrics().RecordDroppedUpdates("buffer_full", droppedCount)
		}
		client.triggerResync()
	})

	client.stateVector = syncstate.NewStateVector(syncstate.StateSnapshot{})
	client.gapBuffer = syncstate.NewGapBuffer(
		func(missingPTS int32) {
			client.logger.Debug("short gap detected", slog.Int("missing_pts", int(missingPTS)))
		},
		func() {
			client.triggerResync()
		},
	)

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

	if config.stateStorage != nil {
		client.stateStorage = config.stateStorage
	} else if sStorage, ok := client.fsmStorage.(syncstate.StateStorage); ok {
		client.stateStorage = sStorage
	} else {
		stateStorage, closer, err := tgboxstorage.NewDefaultStateStorage(appName + ".state.db")
		if err != nil {
			_ = client.Close()
			return nil, fmt.Errorf("tgbox: create state storage: %w", err)
		}
		client.stateStorage = stateStorage
		client.closers = append(client.closers, closer)
	}

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

	floodWaiter := interceptor.NewBoundedFloodWaiter(5, 60*time.Second)
	config.engineOpts.Middlewares = append([]telegram.Middleware{floodWaiter}, config.engineOpts.Middlewares...)

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
		baseInvoker := client.engine.Raw()
		floodInvoker := floodWaiter.Handle(baseInvoker)

		sniffedInvoker := interceptor.NewOutgoingInterceptor(
			floodInvoker,
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

	client.diffFetcher = syncstate.NewDifferenceFetcher(
		client.api,
		client.stateVector,
		client.flusher,
		client.gapBuffer,
		func() {
			client.handleUpdatesTooLong(context.Background())
		},
	)

	client.channelPool = channelpool.NewWorkerPool(
		client.api,
		0,
		func(batch channelpool.ChannelDifferenceBatch) error {
			client.handleChannelBatch(batch)
			return nil
		},
		func(channelID int64) {
			client.logger.Info("channel inaccessible, sync permanently halted", slog.Int64("channel_id", channelID))
			client.channelPTS.Store(channelID, int32(-1))
			if sStorage := client.resolveStateStorage(); sStorage != nil {
				_ = sStorage.SaveChannelPTS(context.Background(), client.accountID(), channelID, -1)
			}
		},
		func(channelID int64, newPTS int32) {
			client.logger.Warn("channelDifferenceTooLong received", slog.Int64("channel_id", channelID), slog.Int("new_pts", int(newPTS)))
		},
	)

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

// OnChatMigrateTo registers an update handler for group migration to supergroup.
func (c *Client) OnChatMigrateTo(h Handler, opts ...HandlerOption) {
	c.on(KindChatMigrateTo, func(_ *Context, _ *Update) bool { return true }, h, opts)
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

func (c *Client) accountID() int64 {
	if self := c.self.Load(); self != nil && self.ID != 0 {
		return self.ID
	}
	return int64(c.opts.appID)
}

func (c *Client) resolveStateStorage() syncstate.StateStorage {
	if c.stateStorage != nil {
		return c.stateStorage
	}
	if sStorage, ok := c.fsmStorage.(syncstate.StateStorage); ok {
		return sStorage
	}
	return nil
}

func (c *Client) saveChannelPTS(ctx context.Context, clientID, channelID int64, pts int32) {
	if sStorage := c.resolveStateStorage(); sStorage != nil {
		_ = sStorage.SaveChannelPTS(ctx, clientID, channelID, pts)
	}
}

func (c *Client) initState(ctx context.Context) error {
	sStorage := c.resolveStateStorage()
	if sStorage == nil {
		return nil
	}
	clientID := c.accountID()

	snap, found, err := sStorage.LoadState(ctx, clientID)
	if err != nil {
		c.logger.Warn("failed to load state vector from storage", slog.Any("error", err))
	}

	if found && snap.PTS > 0 {
		c.stateVector = syncstate.NewStateVector(snap)
		c.logger.Info("restored state vector from storage", slog.Int("pts", int(snap.PTS)))

		_ = sStorage.ForEachChannelPTS(ctx, clientID, func(chID int64, pts int32) error {
			c.channelPTS.Store(chID, pts)
			return nil
		})

		c.flusher = syncstate.NewCheckpointFlusher(clientID, c.stateVector, sStorage, 50*time.Millisecond)
		c.flusher.Start(ctx)
		c.closers = append(c.closers, c.flusher)

		c.diffFetcher = syncstate.NewDifferenceFetcher(
			c.api,
			c.stateVector,
			c.flusher,
			c.gapBuffer,
			func() {
				c.handleUpdatesTooLong(context.Background())
			},
		)

		c.triggerResync()
		return nil
	}

	state, err := c.api.UpdatesGetState(ctx)
	if err != nil {
		c.logger.Warn("failed to fetch initial updates.getState from server", slog.Any("error", err))
		c.stateVector = syncstate.NewStateVector(syncstate.StateSnapshot{})
	} else {
		snap = syncstate.StateSnapshot{
			PTS:  int32(state.Pts),
			QTS:  int32(state.Qts),
			Date: int32(state.Date),
			Seq:  int32(state.Seq),
		}
		c.stateVector = syncstate.NewStateVector(snap)
		_ = sStorage.SaveState(ctx, clientID, snap)
		c.logger.Info("initialized state vector from server updates.getState", slog.Int("pts", int(snap.PTS)))
	}

	c.flusher = syncstate.NewCheckpointFlusher(clientID, c.stateVector, sStorage, 50*time.Millisecond)
	c.flusher.Start(ctx)
	c.closers = append(c.closers, c.flusher)

	c.diffFetcher = syncstate.NewDifferenceFetcher(
		c.api,
		c.stateVector,
		c.flusher,
		c.gapBuffer,
		func() {
			c.handleUpdatesTooLong(context.Background())
		},
	)
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

	if c.channelPool != nil {
		c.channelPool.Start(runCtx)
	}

	go c.startDisruptorConsumer(runCtx)

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
		if err := c.initState(runCtx); err != nil {
			c.logger.Error("failed to initialize sync state", slog.Any("error", err))
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
	if c.channelPool != nil {
		c.channelPool.Stop()
	}
	if c.flusher != nil {
		_ = c.flusher.Close()
	}

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

func formatTraceID(dst *[16]byte, n uint64) {
	if n == 0 {
		dst[0] = '0'
		return
	}
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	var buf [16]byte
	i := len(buf)
	for n > 0 && i > 0 {
		i--
		buf[i] = digits[n%36]
		n /= 36
	}
	copy(dst[:], buf[i:])
}

func (c *Client) enqueueUpdate(ctx context.Context, u tg.UpdatesClass) error {
	traceNum := c.traceCounter.Inc()
	var traceID [16]byte
	formatTraceID(&traceID, traceNum)

	var clientID int64
	if self := c.self.Load(); self != nil {
		clientID = self.ID
	}

	if c.sequencer != nil {
		c.sequencer.Enqueue(u, traceID, clientID)
		return nil
	}

	meta := FastMetadata{
		TraceID:  traceID,
		ClientID: clientID,
		UnixNano: time.Now().UnixNano(),
	}
	return c.processRawUpdates(ctx, u, meta, interceptor.IsOutgoing(ctx))
}

func (c *Client) handleUpdates(ctx context.Context, u tg.UpdatesClass) error {
	if c.opts.trafficObserver != nil {
		c.opts.trafficObserver()
	}

	// Postponed queue protection during active getDifference fetches
	if c.isFetchingDiff.Load() {
		c.postponedMu.Lock()
		if c.isFetchingDiff.Load() {
			if len(c.postponedUpdates) >= 10000 {
				c.logger.Warn("postponed updates queue saturated (>10000), aborting diff fetch and purging")
				c.postponedUpdates = nil
				c.isFetchingDiff.Store(false)
				c.postponedMu.Unlock()
				c.handleUpdatesTooLong(ctx)
				return nil
			}
			c.postponedUpdates = append(c.postponedUpdates, u)
			c.postponedMu.Unlock()
			return nil
		}
		c.postponedMu.Unlock()
	}

	return c.enqueueUpdate(ctx, u)
}

func (c *Client) startDisruptorConsumer(ctx context.Context) {
	if c.poller == nil {
		return
	}

	_ = c.poller.Run(ctx, func(slot *disruptor.RingSlot) error {
		if slot.RawClass == nil {
			return nil
		}

		meta := FastMetadata{
			TraceID:  slot.TraceID,
			ClientID: slot.ClientID,
			UnixNano: slot.Received,
		}
		_ = c.processRawUpdates(ctx, slot.RawClass, meta, false)
		slot.RawClass = nil

		if c.opts.profiler != nil && c.opts.profiler.Metrics() != nil {
			cursor := c.ringBuffer.Cursor().Get()
			gating := c.ringBuffer.GatingSequence().Get()
			capacity := c.ringBuffer.Capacity()
			if cursor > gating && capacity > 0 {
				ratio := float64(cursor-gating) / float64(capacity)
				c.opts.profiler.Metrics().RecordRingBufferUtilization(ratio)
			}
		}
		return nil
	})
}

func (c *Client) acquireContext(parent context.Context, meta FastMetadata) *Context {
	if c.contextPool == nil {
		return &Context{
			StdContext: &PipelineContext{parent: parent, Meta: meta},
			Update:     &Update{},
			Meta:       meta,
		}
	}
	return c.contextPool.Acquire(parent, meta)
}

func (*Client) cloneEntities(src *tg.Entities) *tg.Entities {
	if src == nil {
		return nil
	}
	return &tg.Entities{
		Users:    maps.Clone(src.Users),
		Chats:    maps.Clone(src.Chats),
		Channels: maps.Clone(src.Channels),
	}
}

func extractGlobalPTS(u tg.UpdateClass) (pts, ptsCount int32, ok bool) {
	switch upd := u.(type) {
	case *tg.UpdateNewMessage:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateEditMessage:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateDeleteMessages:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateReadHistoryInbox:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateReadHistoryOutbox:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdatePinnedMessages:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateReadMessagesContents:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateWebPage:
		return int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateFolderPeers:
		return int32(upd.Pts), int32(upd.PtsCount), true
	default:
		return 0, 0, false
	}
}

func extractQTS(u tg.UpdateClass) (qts int32, ok bool) {
	switch upd := u.(type) {
	case *tg.UpdateNewEncryptedMessage:
		return int32(upd.Qts), true
	case *tg.UpdateMessagePollVote:
		return int32(upd.Qts), true
	case *tg.UpdateBotStopped:
		return int32(upd.Qts), true
	case *tg.UpdateChatParticipant:
		return int32(upd.Qts), true
	case *tg.UpdateChannelParticipant:
		return int32(upd.Qts), true
	case *tg.UpdateBotChatInviteRequester:
		return int32(upd.Qts), true
	case *tg.UpdateBotChatBoost:
		return int32(upd.Qts), true
	case *tg.UpdateBotMessageReaction:
		return int32(upd.Qts), true
	case *tg.UpdateBotMessageReactions:
		return int32(upd.Qts), true
	case *tg.UpdateBotBusinessConnect:
		return int32(upd.Qts), true
	case *tg.UpdateBotNewBusinessMessage:
		return int32(upd.Qts), true
	case *tg.UpdateBotEditBusinessMessage:
		return int32(upd.Qts), true
	case *tg.UpdateBotDeleteBusinessMessage:
		return int32(upd.Qts), true
	case *tg.UpdateBotPurchasedPaidMedia:
		return int32(upd.Qts), true
	case *tg.UpdateBotStarsSubscription:
		return int32(upd.Qts), true
	default:
		return 0, false
	}
}

func channelIDFromMessage(msgClass tg.MessageClass) int64 {
	switch m := msgClass.(type) {
	case *tg.Message:
		if peer, ok := m.PeerID.(*tg.PeerChannel); ok {
			return peer.ChannelID
		}
	case *tg.MessageService:
		if peer, ok := m.PeerID.(*tg.PeerChannel); ok {
			return peer.ChannelID
		}
	}
	return 0
}

func extractChannelPTS(u tg.UpdateClass) (channelID int64, pts, ptsCount int32, ok bool) {
	switch upd := u.(type) {
	case *tg.UpdateNewChannelMessage:
		cid := channelIDFromMessage(upd.Message)
		if cid != 0 {
			return cid, int32(upd.Pts), int32(upd.PtsCount), true
		}
		return 0, 0, 0, false
	case *tg.UpdateEditChannelMessage:
		cid := channelIDFromMessage(upd.Message)
		if cid != 0 {
			return cid, int32(upd.Pts), int32(upd.PtsCount), true
		}
		return 0, 0, 0, false
	case *tg.UpdateDeleteChannelMessages:
		return upd.ChannelID, int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdatePinnedChannelMessages:
		return upd.ChannelID, int32(upd.Pts), int32(upd.PtsCount), true
	case *tg.UpdateChannelTooLong:
		var p int32
		if ptsVal, ok := upd.GetPts(); ok {
			p = int32(ptsVal)
		}
		return upd.ChannelID, p, 0, true
	default:
		return 0, 0, 0, false
	}
}

func (c *Client) resolveChannelAccessHash(ctx context.Context, channelID int64, entities *tg.Entities) int64 {
	if entities != nil && entities.Channels != nil {
		if ch, ok := entities.Channels[channelID]; ok && ch.AccessHash != 0 {
			c.channelAccessHashes.Store(channelID, ch.AccessHash)
			return ch.AccessHash
		}
	}
	if val, ok := c.channelAccessHashes.Load(channelID); ok {
		return val.(int64)
	}
	if c.peers != nil {
		if p, err := c.peers.Find(ctx, storage.PeerKey{Kind: dialogs.Channel, ID: channelID}); err == nil && p.Channel != nil && p.Channel.AccessHash != 0 {
			c.channelAccessHashes.Store(channelID, p.Channel.AccessHash)
			return p.Channel.AccessHash
		}
	}
	return 0
}

func (c *Client) handleChannelPtsUpdate(
	ctx context.Context,
	raw tg.UpdateClass,
	entities *tg.Entities,
	channelID int64,
	pts, ptsCount int32,
) bool {
	if c.channelPool == nil || channelID == 0 {
		return true
	}

	accessHash := c.resolveChannelAccessHash(ctx, channelID, entities)

	if _, isTooLong := raw.(*tg.UpdateChannelTooLong); isTooLong {
		c.channelPool.Submit(&channelpool.ChannelSyncTask{
			ChannelID:  channelID,
			AccessHash: accessHash,
			LocalPTS:   0,
			RemotePTS:  pts,
			Tier:       channelpool.TierActive,
		})
		return false
	}

	var localPTS int32
	if val, ok := c.channelPTS.Load(channelID); ok {
		localPTS = val.(int32)
	}

	// [NEW GUARD]: Abort synchronization completely if channel is marked as inaccessible
	if localPTS == -1 {
		return true
	}

	clientID := c.accountID()

	if localPTS == 0 && pts > 0 {
		c.channelPTS.Store(channelID, pts)
		c.saveChannelPTS(ctx, clientID, channelID, pts)
		return true
	}

	result := syncstate.CheckGap(localPTS, pts, ptsCount)
	switch result {
	case syncstate.GapApply:
		if pts > 0 {
			c.channelPTS.Store(channelID, pts)
			c.saveChannelPTS(ctx, clientID, channelID, pts)
		}
		return true

	case syncstate.GapIgnore:
		c.logger.Debug("discarding stale or duplicate channel update",
			slog.Int64("channel_id", channelID),
			slog.Int("remote_pts", int(pts)),
			slog.Int("local_pts", int(localPTS)),
		)
		return false

	case syncstate.GapRefetch:
		c.logger.Warn("channel sequence gap detected, submitting sync task to worker pool",
			slog.Int64("channel_id", channelID),
			slog.Int("remote_pts", int(pts)),
			slog.Int("pts_count", int(ptsCount)),
			slog.Int("local_pts", int(localPTS)),
		)
		c.channelPool.Submit(&channelpool.ChannelSyncTask{
			ChannelID:  channelID,
			AccessHash: accessHash,
			LocalPTS:   localPTS,
			RemotePTS:  pts,
			Tier:       channelpool.TierActive,
		})
		return false

	default:
		return true
	}
}

func (c *Client) handleGlobalPtsUpdate(
	ctx context.Context,
	raw tg.UpdateClass,
	entities *tg.Entities,
	meta FastMetadata,
	pts, ptsCount int32,
	_ bool,
) bool {
	if c.isFetchingDiff.Load() {
		c.postponedMu.Lock()
		if len(c.postponedUpdates) >= 10000 {
			c.logger.Warn("postponed updates buffer overflow, forcing too-long recovery", slog.Int("pts", int(pts)))
			c.postponedUpdates = nil
			c.isFetchingDiff.Store(false)
			c.postponedMu.Unlock()
			c.handleUpdatesTooLong(ctx)
			return false
		}

		wrapper := &tg.Updates{
			Updates: []tg.UpdateClass{raw},
		}
		if entities != nil {
			for _, user := range entities.Users {
				wrapper.Users = append(wrapper.Users, user)
			}
			for _, chat := range entities.Chats {
				wrapper.Chats = append(wrapper.Chats, chat)
			}
			for _, channel := range entities.Channels {
				wrapper.Chats = append(wrapper.Chats, channel)
			}
		}
		c.postponedUpdates = append(c.postponedUpdates, wrapper)
		c.postponedMu.Unlock()
		return false
	}

	if c.stateVector == nil || c.gapBuffer == nil {
		return true
	}

	currentPTS := c.stateVector.PTS()
	if currentPTS == 0 && pts > 0 {
		c.stateVector.SetPTS(pts)
		if c.flusher != nil {
			c.flusher.MarkDirty(1)
		}
		return true
	}

	result := syncstate.CheckGap(currentPTS, pts, ptsCount)
	switch result {
	case syncstate.GapApply:
		if pts > 0 {
			c.stateVector.SetPTS(pts)
			if c.flusher != nil {
				c.flusher.MarkDirty(1)
			}
		}
		return true

	case syncstate.GapIgnore:
		c.logger.Debug("discarding stale or duplicate update",
			slog.Int("remote_pts", int(pts)),
			slog.Int("local_pts", int(currentPTS)),
		)
		return false

	case syncstate.GapRefetch:
		c.logger.Warn("detected sequence gap in incoming update, buffering",
			slog.Int("remote_pts", int(pts)),
			slog.Int("pts_count", int(ptsCount)),
			slog.Int("local_pts", int(currentPTS)),
		)
		c.gapBuffer.Push(syncstate.PendingUpdate{
			PTS:         pts,
			PTSCount:    ptsCount,
			ReceiveTime: time.Now(),
			Raw:         raw,
			TraceID:     meta.TraceID,
			ClientID:    meta.ClientID,
		}, currentPTS)
		return false

	default:
		return true
	}
}

func (c *Client) drainGapBuffer(
	ctx context.Context,
	entities *tg.Entities,
	isOutgoing bool,
) {
	if c.gapBuffer == nil || c.stateVector == nil || c.gapBuffer.Len() == 0 {
		return
	}

	currentPTS := c.stateVector.PTS()
	drained, newPTS := c.gapBuffer.PopConsecutive(currentPTS)
	if len(drained) == 0 {
		return
	}

	c.stateVector.SetPTS(newPTS)
	if c.flusher != nil {
		c.flusher.MarkDirty(uint64(len(drained)))
	}

	for _, item := range drained {
		meta := FastMetadata{
			TraceID:  item.TraceID,
			ClientID: item.ClientID,
			UnixNano: item.ReceiveTime.UnixNano(),
		}
		pCtx := c.acquireContext(ctx, meta)
		pCtx.Entities = c.cloneEntities(entities)
		pCtx.Logger = c.logger.With(slog.String("trace_id", meta.TraceIDString()), slog.Int64("client_id", meta.ClientID))
		pCtx.Update.Raw = item.Raw
		pCtx.Update.Kind = KindRaw
		pCtx.Update.IsOutgoing = isOutgoing

		c.dispatchUpdate(pCtx)
	}
}

func (c *Client) drainPostponedUpdates(ctx context.Context) {
	for {
		c.postponedMu.Lock()
		if len(c.postponedUpdates) == 0 {
			c.isFetchingDiff.Store(false)
			c.postponedMu.Unlock()
			return
		}
		batch := c.postponedUpdates
		c.postponedUpdates = nil
		c.postponedMu.Unlock()

		for _, upd := range batch {
			_ = c.enqueueUpdate(ctx, upd)
		}
	}
}

func (c *Client) processRawUpdates(ctx context.Context, u tg.UpdatesClass, meta FastMetadata, isOutgoing bool) error {
	pCtx := c.acquireContext(ctx, meta)
	entities := pCtx.Entities
	if entities == nil {
		entities = &tg.Entities{
			Users:    make(map[int64]*tg.User, 16),
			Chats:    make(map[int64]*tg.Chat, 16),
			Channels: make(map[int64]*tg.Channel, 16),
		}
		pCtx.Entities = entities
	} else {
		clear(entities.Users)
		clear(entities.Chats)
		clear(entities.Channels)
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
				if ch.AccessHash != 0 {
					c.channelAccessHashes.Store(ch.ID, ch.AccessHash)
				}
			}
		}
	}

	switch upds := u.(type) {
	case *tg.Updates:
		populate(upds.Users, upds.Chats)
	case *tg.UpdatesCombined:
		populate(upds.Users, upds.Chats)
	}

	dispatchOne := func(raw tg.UpdateClass) {
		if pts, ptsCount, ok := extractGlobalPTS(raw); ok {
			if !c.handleGlobalPtsUpdate(ctx, raw, entities, meta, pts, ptsCount, isOutgoing) {
				return
			}
		}
		if channelID, pts, ptsCount, ok := extractChannelPTS(raw); ok {
			if !c.handleChannelPtsUpdate(ctx, raw, entities, channelID, pts, ptsCount) {
				return
			}
		}
		if qts, ok := extractQTS(raw); ok && qts > 0 && c.stateVector != nil {
			c.stateVector.SetQTS(qts)
			if c.flusher != nil {
				c.flusher.MarkDirty(1)
			}
		}

		itemCtx := c.acquireContext(ctx, meta)
		itemCtx.Entities = c.cloneEntities(entities)
		itemCtx.Logger = c.logger.With(slog.String("trace_id", meta.TraceIDString()), slog.Int64("client_id", meta.ClientID))
		itemCtx.Update.Raw = raw
		itemCtx.Update.Kind = KindRaw
		itemCtx.Update.IsOutgoing = isOutgoing

		c.dispatchUpdate(itemCtx)
		c.drainGapBuffer(ctx, entities, isOutgoing)
	}

	switch upds := u.(type) {
	case *tg.Updates:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		if upds.Seq > 0 && c.stateVector != nil {
			c.stateVector.SetSeq(int32(upds.Seq))
		}
		for _, upd := range upds.Updates {
			dispatchOne(upd)
		}
	case *tg.UpdateShort:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		dispatchOne(upds.Update)
	case *tg.UpdatesCombined:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		if upds.Seq > 0 && c.stateVector != nil {
			c.stateVector.SetSeq(int32(upds.Seq))
		}
		for _, upd := range upds.Updates {
			dispatchOne(upd)
		}
	case *tg.UpdateShortMessage:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		dispatchOne(c.convertShortMessage(upds))
	case *tg.UpdateShortChatMessage:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		dispatchOne(c.convertShortChatMessage(upds))
	case *tg.UpdateShortSentMessage:
		if upds.Date > 0 && c.stateVector != nil {
			c.stateVector.UpdateDate(int32(upds.Date))
		}
		dispatchOne(c.convertShortSentMessage(upds))
	case *tg.UpdatesTooLong:
		c.handleUpdatesTooLong(ctx)
	}

	pCtx.Release()
	return nil
}

func (c *Client) convertShortMessage(u *tg.UpdateShortMessage) *tg.UpdateNewMessage {
	msg := &tg.Message{
		ID:          u.ID,
		PeerID:      &tg.PeerUser{UserID: u.UserID},
		Message:     u.Message,
		Date:        u.Date,
		Out:         u.Out,
		Mentioned:   u.Mentioned,
		MediaUnread: u.MediaUnread,
		Silent:      u.Silent,
		Entities:    u.Entities,
	}
	if !u.Out {
		msg.FromID = &tg.PeerUser{UserID: u.UserID}
	} else if self := c.Self(); self != nil {
		msg.FromID = &tg.PeerUser{UserID: self.ID}
	}
	if fwd, ok := u.GetFwdFrom(); ok {
		msg.FwdFrom = fwd
	}
	if via, ok := u.GetViaBotID(); ok {
		msg.ViaBotID = via
	}
	if rep, ok := u.GetReplyTo(); ok {
		msg.ReplyTo = rep
	}
	if ttl, ok := u.GetTTLPeriod(); ok {
		msg.TTLPeriod = ttl
	}
	return &tg.UpdateNewMessage{
		Message:  msg,
		Pts:      u.Pts,
		PtsCount: u.PtsCount,
	}
}

func (*Client) convertShortChatMessage(u *tg.UpdateShortChatMessage) *tg.UpdateNewMessage {
	msg := &tg.Message{
		ID:          u.ID,
		PeerID:      &tg.PeerChat{ChatID: u.ChatID},
		FromID:      &tg.PeerUser{UserID: u.FromID},
		Message:     u.Message,
		Date:        u.Date,
		Out:         u.Out,
		Mentioned:   u.Mentioned,
		MediaUnread: u.MediaUnread,
		Silent:      u.Silent,
		Entities:    u.Entities,
	}
	if fwd, ok := u.GetFwdFrom(); ok {
		msg.FwdFrom = fwd
	}
	if via, ok := u.GetViaBotID(); ok {
		msg.ViaBotID = via
	}
	if rep, ok := u.GetReplyTo(); ok {
		msg.ReplyTo = rep
	}
	if ttl, ok := u.GetTTLPeriod(); ok {
		msg.TTLPeriod = ttl
	}
	return &tg.UpdateNewMessage{
		Message:  msg,
		Pts:      u.Pts,
		PtsCount: u.PtsCount,
	}
}

func (c *Client) convertShortSentMessage(u *tg.UpdateShortSentMessage) *tg.UpdateNewMessage {
	msg := &tg.Message{
		ID:       u.ID,
		Date:     u.Date,
		Out:      true,
		Entities: u.Entities,
		Media:    u.Media,
	}
	if ttl, ok := u.GetTTLPeriod(); ok {
		msg.TTLPeriod = ttl
	}
	if val, ok := c.sentMsgPeers.LoadAndDelete(u.ID); ok {
		if peer, valid := val.(tg.PeerClass); valid {
			msg.PeerID = peer
		}
	}
	return &tg.UpdateNewMessage{
		Message:  msg,
		Pts:      u.Pts,
		PtsCount: u.PtsCount,
	}
}

func (c *Client) handleUpdatesTooLong(_ context.Context) {
	c.logger.Warn("updatesTooLong received: jumping local state and recovering missing history via messages.getHistory")
	if c.opts.profiler != nil && c.opts.profiler.Metrics() != nil {
		c.opts.profiler.Metrics().RecordResyncEvent("user_too_long")
	}

	go func() {
		reqCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		state, err := c.api.UpdatesGetState(reqCtx)
		if err != nil {
			c.logger.Error("failed to get updates state after updatesTooLong", slog.Any("error", err))
			return
		}
		if c.stateVector != nil {
			c.stateVector.SetPTS(int32(state.Pts))
			c.stateVector.SetQTS(int32(state.Qts))
			c.stateVector.SetDate(int32(state.Date))
			c.stateVector.SetSeq(int32(state.Seq))
		}
		if c.gapBuffer != nil {
			c.gapBuffer.Clear()
		}

		c.postponedMu.Lock()
		c.postponedUpdates = nil
		c.postponedMu.Unlock()

		if c.flusher != nil {
			_ = c.flusher.Flush(reqCtx)
		}

		c.recoverHistoryViaGetHistory(reqCtx)

		if c.opts.onTooLong != nil {
			c.opts.onTooLong()
		}
	}()
}

// Instead of paginating through the entire history to bridge the PTS gap (which causes OOM and FLOOD_WAIT),
// it accepts the gap, flags the dialog as not fully loaded, and fetches ONLY the most recent N messages
// to populate the immediate UI context. Deeper history is deferred to explicit user-driven pagination.
func (c *Client) recoverHistoryViaGetHistory(ctx context.Context) {
	// 1. Fetch active dialogs to determine which peers we missed updates for.
	res, err := c.api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      100,
	})
	if err != nil {
		c.logger.Error("messages.getDialogs failed during recovery", slog.Any("error", err))
		return
	}

	var dialogs []tg.DialogClass
	var chats []tg.ChatClass
	switch d := res.(type) {
	case *tg.MessagesDialogs:
		dialogs = d.Dialogs
		chats = d.Chats
	case *tg.MessagesDialogsSlice:
		dialogs = d.Dialogs
		chats = d.Chats
	default:
		return
	}

	// Cache channel access hashes immediately.
	for _, cc := range chats {
		if ch, ok := cc.(*tg.Channel); ok && ch.AccessHash != 0 {
			c.channelAccessHashes.Store(ch.ID, ch.AccessHash)
		}
	}

	clientID := c.accountID()
	var wg sync.WaitGroup

	// 2. Iterate through each active dialog and recover ONLY the recent surface layer.
	for _, dlg := range dialogs {
		d, ok := dlg.(*tg.Dialog)
		if !ok || d.Pts == 0 {
			continue
		}

		var inputPeer tg.InputPeerClass
		var peerID int64
		switch p := d.Peer.(type) {
		case *tg.PeerUser:
			peerID = p.UserID
			inputPeer = &tg.InputPeerUser{UserID: p.UserID}
		case *tg.PeerChat:
			peerID = p.ChatID
			inputPeer = &tg.InputPeerChat{ChatID: p.ChatID}
		case *tg.PeerChannel:
			peerID = p.ChannelID
			accessHash := c.resolveChannelAccessHash(ctx, p.ChannelID, nil)
			inputPeer = &tg.InputPeerChannel{ChannelID: p.ChannelID, AccessHash: accessHash}
		}

		if inputPeer == nil {
			continue
		}

		remotePTS := int32(d.Pts)

		wg.Add(1)
		go func(targetPeer tg.InputPeerClass, id int64, rPts int32) {
			defer wg.Done()

			// 3. Flag dialog as not fully loaded at the bottom. The PTS gap is accepted as a structural void.
			// Deep history retrieval is strictly deferred to user-driven UI pagination.
			if sStorage := c.resolveStateStorage(); sStorage != nil {
				_ = sStorage.SetDialogNotLoaded(ctx, clientID, id, true)
			}

			// 4. Fetch ONLY the most recent N messages (e.g., 50) to populate the immediate UI view.
			const recentLimit = 50
			histRes, histErr := c.api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
				Peer:  targetPeer,
				Limit: recentLimit,
				// OffsetID is implicitly 0, fetching from the absolute latest message downwards.
			})
			if histErr != nil {
				c.logger.Error("getHistory (recent surface) failed during recovery", slog.Int64("peer_id", id), slog.Any("error", histErr))
				return
			}

			var collectedMsgs []tg.MessageClass
			var collectedUsers []tg.UserClass
			var collectedChats []tg.ChatClass

			switch h := histRes.(type) {
			case *tg.MessagesChannelMessages:
				collectedMsgs = h.Messages
				collectedUsers = h.Users
				collectedChats = h.Chats
			case *tg.MessagesMessagesSlice:
				collectedMsgs = h.Messages
				collectedUsers = h.Users
				collectedChats = h.Chats
			case *tg.MessagesMessages:
				collectedMsgs = h.Messages
				collectedUsers = h.Users
				collectedChats = h.Chats
			}

			// Update local PTS to the server's remote PTS to finalize the state jump and prevent infinite loops.
			c.channelPTS.Store(id, rPts)
			c.saveChannelPTS(ctx, clientID, id, rPts)

			if len(collectedMsgs) > 0 {
				// Reverse to process chronologically before dispatching to the dispatcher
				slices.Reverse(collectedMsgs)
				c.handleDifferenceBatch(syncstate.DifferenceBatch{
					Messages: collectedMsgs,
					Users:    collectedUsers,
					Chats:    collectedChats,
				})
			}
		}(inputPeer, peerID, remotePTS)
	}
	wg.Wait()
	c.logger.Info("messages.getHistory disaster recovery completed")
}

func (c *Client) triggerResync() {
	if c.diffFetcher == nil {
		return
	}
	if !c.isFetchingDiff.CompareAndSwap(false, true) {
		return
	}

	if c.opts.profiler != nil && c.opts.profiler.Metrics() != nil {
		c.opts.profiler.Metrics().RecordResyncEvent("user")
	}

	go func() {
		fetchCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		start := time.Now()

		err := c.diffFetcher.FetchDifference(fetchCtx, func(batch syncstate.DifferenceBatch) error {
			c.handleDifferenceBatch(batch)
			return nil
		})
		if err != nil {
			c.isFetchingDiff.Store(false)

			if errors.Is(err, context.Canceled) {
				c.logger.Debug("FetchDifference canceled")
				return
			}

			if tgerr.Is(err, "PERSISTENT_TIMESTAMP_INVALID") {
				c.logger.Error("FetchDifference reported PERSISTENT_TIMESTAMP_INVALID, initiating too-long recovery", slog.Any("error", err))
				c.postponedMu.Lock()
				c.postponedUpdates = nil
				c.postponedMu.Unlock()
				c.handleUpdatesTooLong(context.Background())
				return
			}

			if tgerr.Is(err, "FLOOD_WAIT") || errors.Is(err, interceptor.ErrFloodShedding) {
				waitDur, _ := tgerr.AsFloodWait(err)
				if waitDur <= 0 {
					waitDur = 5 * time.Second
				}
				c.logger.Warn("FetchDifference rate limited (FLOOD_WAIT), scheduling retry without resetting state",
					slog.Duration("wait", waitDur),
					slog.Any("error", err),
				)
				time.AfterFunc(waitDur, func() {
					c.triggerResync()
				})
				return
			}

			c.logger.Warn("FetchDifference transient failure, scheduling retry without resetting state",
				slog.Any("error", err),
			)
			time.AfterFunc(2*time.Second, func() {
				c.triggerResync()
			})
			return
		}

		if c.opts.profiler != nil && c.opts.profiler.Metrics() != nil {
			c.opts.profiler.Metrics().RecordResyncDuration(time.Since(start))
		}

		c.drainPostponedUpdates(context.Background())
	}()
}

func (c *Client) handleDifferenceBatch(batch syncstate.DifferenceBatch) {
	for _, msg := range batch.Messages {
		switch m := msg.(type) {
		case *tg.Message:
			pCtx := c.acquireContext(context.Background(), FastMetadata{})
			pCtx.Logger = c.logger
			pCtx.Update.Message = m
			pCtx.Update.Kind = KindMessage
			pCtx.Update.IsOutgoing = false
			c.dispatcher.Handle(pCtx, pCtx.Update)
		case *tg.MessageService:
			pCtx := c.acquireContext(context.Background(), FastMetadata{})
			pCtx.Logger = c.logger
			pCtx.Update.MessageService = m
			pCtx.Update.Kind = KindMessage
			pCtx.Update.IsOutgoing = false
			c.handleMessageService(pCtx, pCtx.Update, m)
			c.dispatcher.Handle(pCtx, pCtx.Update)
		}
	}

	for _, rawUpd := range batch.OtherUpdates {
		pCtx := c.acquireContext(context.Background(), FastMetadata{})
		pCtx.Logger = c.logger
		pCtx.Update.Raw = rawUpd
		pCtx.Update.Kind = KindRaw
		pCtx.Update.IsOutgoing = false
		c.dispatchUpdate(pCtx)
	}

	c.drainGapBuffer(context.Background(), nil, false)
}

func (c *Client) handleChannelBatch(batch channelpool.ChannelDifferenceBatch) {
	if batch.NewPTS > 0 {
		c.channelPTS.Store(batch.ChannelID, batch.NewPTS)
	}

	for _, msg := range batch.NewMessages {
		switch m := msg.(type) {
		case *tg.Message:
			pCtx := c.acquireContext(context.Background(), FastMetadata{})
			pCtx.Logger = c.logger
			pCtx.Update.Message = m
			pCtx.Update.Kind = KindMessage
			pCtx.Update.IsOutgoing = false
			c.dispatcher.Handle(pCtx, pCtx.Update)
		case *tg.MessageService:
			pCtx := c.acquireContext(context.Background(), FastMetadata{})
			pCtx.Logger = c.logger
			pCtx.Update.MessageService = m
			pCtx.Update.Kind = KindMessage
			pCtx.Update.IsOutgoing = false
			c.handleMessageService(pCtx, pCtx.Update, m)
			c.dispatcher.Handle(pCtx, pCtx.Update)
		}
	}

	for _, rawUpd := range batch.OtherUpdates {
		pCtx := c.acquireContext(context.Background(), FastMetadata{})
		pCtx.Logger = c.logger
		pCtx.Update.Raw = rawUpd
		pCtx.Update.Kind = KindRaw
		pCtx.Update.IsOutgoing = false
		c.dispatchUpdate(pCtx)
	}
}

func (c *Client) handleChatMigrateTo(ctx context.Context, oldChatID, newChannelID, senderID int64, entities *tg.Entities) {
	c.logger.Info("handling chat migration to supergroup",
		slog.Int64("old_chat_id", oldChatID),
		slog.Int64("new_channel_id", newChannelID),
	)

	if c.stateManager != nil && senderID != 0 {
		if err := c.stateManager.MoveState(ctx, oldChatID, senderID, newChannelID, senderID); err != nil && !errors.Is(err, fsm.ErrNotFound) {
			c.logger.Error("failed to move fsm state during chat migration", slog.Any("error", err))
		}
	}

	if c.channelPool != nil {
		accessHash := c.resolveChannelAccessHash(ctx, newChannelID, entities)
		c.channelPTS.Store(newChannelID, int32(1))
		c.channelPool.Submit(&channelpool.ChannelSyncTask{
			ChannelID:  newChannelID,
			AccessHash: accessHash,
			LocalPTS:   1,
			RemotePTS:  1,
			Tier:       channelpool.TierActive,
		})
	}
}

func (c *Client) handleMessageService(pCtx *Context, upd *Update, svc *tg.MessageService) {
	switch action := svc.Action.(type) {
	case *tg.MessageActionChatMigrateTo:
		oldChatID := peerID(svc.PeerID)
		newChannelID := action.ChannelID
		upd.Kind = KindChatMigrateTo
		upd.ChatMigrateTo = &ChatMigrateTo{
			OldChatID:    oldChatID,
			NewChannelID: newChannelID,
		}
		c.handleChatMigrateTo(pCtx.Context(), oldChatID, newChannelID, upd.SenderID(), pCtx.Entities)

	case *tg.MessageActionChannelMigrateFrom:
		oldChatID := action.ChatID
		newChannelID := peerID(svc.PeerID)
		upd.Kind = KindChatMigrateTo
		upd.ChatMigrateTo = &ChatMigrateTo{
			OldChatID:    oldChatID,
			NewChannelID: newChannelID,
		}
		c.handleChatMigrateTo(pCtx.Context(), oldChatID, newChannelID, upd.SenderID(), pCtx.Entities)
	}
}

func (c *Client) dispatchUpdate(pCtx *Context) {
	upd := pCtx.Update

	switch m := upd.Raw.(type) {
	case *tg.UpdateNewMessage:
		c.dispatchNewMessage(pCtx, upd, m.Message)
	case *tg.UpdateNewChannelMessage:
		c.dispatchNewMessage(pCtx, upd, m.Message)
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

func (c *Client) dispatchNewMessage(pCtx *Context, upd *Update, msgClass tg.MessageClass) {
	switch m := msgClass.(type) {
	case *tg.Message:
		upd.Message = m
		upd.Kind = KindMessage
	case *tg.MessageService:
		upd.MessageService = m
		c.handleMessageService(pCtx, upd, m)
	}
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
