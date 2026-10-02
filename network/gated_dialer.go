package network

import (
	"context"
	"errors"
	"net"
	"sync"
)

// ErrDialerSuspended is returned when a connection attempt is aborted due to transport suspension.
var ErrDialerSuspended = errors.New("network: dialer is currently suspended")

// DialerState defines the operational phases of the gated transport layer.
type DialerState int

const (
	// StateActive allows normal physical socket allocation.
	StateActive DialerState = iota
	// StateSuspended blocks socket allocation and buffers reconnect attempts.
	StateSuspended
)

// GatedDialer intercepts socket creation at the transport layer, enforcing absolute blockage during suspension phases.
type GatedDialer struct {
	mu         sync.Mutex
	state      DialerState
	resumeChan chan struct{}
	conns      map[net.Conn]struct{}
}

// NewGatedDialer instantiates a gated dialer in the Active state with an already unblocked resume channel.
func NewGatedDialer() *GatedDialer {
	ch := make(chan struct{})
	close(ch) // Pre-closed so reads in Active state never block
	return &GatedDialer{
		state:      StateActive,
		resumeChan: ch,
		conns:      make(map[net.Conn]struct{}),
	}
}

// DialContext handles physical TCP socket allocation, blocking safely if the gateway is suspended.
func (g *GatedDialer) DialContext(ctx context.Context, network, addr string, dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)) (net.Conn, error) {
	g.mu.Lock()
	if g.state == StateSuspended {
		resume := g.resumeChan
		g.mu.Unlock()

		select {
		case <-resume:
			// Unblocked, proceed with physical dialing
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		g.mu.Unlock()
	}

	// Execute physical dial via the provided standard or emulated dial function
	conn, err := dialFunc(ctx, network, addr)
	if err != nil {
		return nil, err
	}

	// Wrap connection to intercept Close() for self-cleaning connection tracking
	wrapped := &monitoredConn{
		Conn:   conn,
		dialer: g,
	}

	g.mu.Lock()
	// Post-dial verification to guard against concurrent Suspend() invocations
	if g.state == StateSuspended {
		g.mu.Unlock()
		_ = wrapped.Close()
		return nil, ErrDialerSuspended
	}
	g.conns[wrapped] = struct{}{}
	g.mu.Unlock()

	return wrapped, nil
}

// Suspend transitions the gateway to Suspended state and force-closes all active monitored connections.
func (g *GatedDialer) Suspend() {
	g.mu.Lock()
	if g.state == StateSuspended {
		g.mu.Unlock()
		return
	}
	g.state = StateSuspended
	// Initialize a new open channel to block subsequent DialContext invocations
	g.resumeChan = make(chan struct{})

	// Copy active connections to a local slice to avoid holding the mutex during socket I/O operations
	activeConns := make([]net.Conn, 0, len(g.conns))
	for c := range g.conns {
		activeConns = append(activeConns, c)
	}
	g.mu.Unlock()

	// Terminate active sockets. Their overridden Close() method will clean up their reference from the dialer.
	for _, c := range activeConns {
		_ = c.Close()
	}
}

// Resume transitions the gateway to Active state, signaling all blocked DialContext threads to proceed.
func (g *GatedDialer) Resume() {
	g.mu.Lock()
	if g.state == StateActive {
		g.mu.Unlock()
		return
	}
	g.state = StateActive
	close(g.resumeChan) // Broadcasting close safely unblocks all waiting DialContext channels
	g.mu.Unlock()
}

// ActiveConnectionsCount returns the current count of monitored active connections.
func (g *GatedDialer) ActiveConnectionsCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.conns)
}

func (g *GatedDialer) removeConn(c net.Conn) {
	g.mu.Lock()
	delete(g.conns, c)
	g.mu.Unlock()
}

// monitoredConn intercepts Close() calls to notify the parent GatedDialer to remove the closed socket.
type monitoredConn struct {
	net.Conn
	dialer *GatedDialer
	once   sync.Once
}

// Close closes the underlying network connection and safely removes it from GatedDialer tracking.
func (m *monitoredConn) Close() error {
	var err error
	m.once.Do(func() {
		err = m.Conn.Close()
		m.dialer.removeConn(m)
	})
	return err
}
