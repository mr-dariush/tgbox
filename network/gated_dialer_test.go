package network_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/network"
)

func TestGatedDialer_ActiveFlow(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	dialer := network.NewGatedDialer()
	baseDialer := (&net.Dialer{}).DialContext

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", l.Addr().String(), baseDialer)
	require.NoError(t, err)
	defer conn.Close()

	assert.Equal(t, 1, dialer.ActiveConnectionsCount())
}

func TestGatedDialer_SuspendedFlow(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	dialer := network.NewGatedDialer()
	baseDialer := (&net.Dialer{}).DialContext

	dialer.Suspend() // Block connection attempts

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	var dialConn net.Conn
	var dialErr error

	go func() {
		defer wg.Done()
		dialConn, dialErr = dialer.DialContext(ctx, "tcp", l.Addr().String(), baseDialer)
	}()

	// Ensure the dialing goroutine is currently blocked and hasn't returned
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 0, dialer.ActiveConnectionsCount())

	// Spin up TCP acceptor
	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	dialer.Resume() // Signal unblock
	wg.Wait()

	require.NoError(t, dialErr)
	assert.NotNil(t, dialConn)
	_ = dialConn.Close()
}

func TestGatedDialer_ConsecutiveToggles(t *testing.T) {
	t.Parallel()

	dialer := network.NewGatedDialer()

	// Direct sequential calls to Suspend or Resume should not panic or cause race conditions
	assert.NotPanics(t, func() {
		dialer.Suspend()
		dialer.Suspend()
		dialer.Resume()
		dialer.Resume()
		dialer.Suspend()
		dialer.Resume()
	})
}

func TestGatedDialer_ForceCloseOnSuspend(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		for {
			conn, acceptErr := l.Accept()
			if acceptErr != nil {
				return
			}
			go func(c net.Conn) {
				// Keep reading until closed
				buf := make([]byte, 1)
				_, _ = c.Read(buf)
				_ = c.Close()
			}(conn)
		}
	}()

	dialer := network.NewGatedDialer()
	baseDialer := (&net.Dialer{}).DialContext

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn1, err := dialer.DialContext(ctx, "tcp", l.Addr().String(), baseDialer)
	require.NoError(t, err)

	conn2, err := dialer.DialContext(ctx, "tcp", l.Addr().String(), baseDialer)
	require.NoError(t, err)

	assert.Equal(t, 2, dialer.ActiveConnectionsCount())

	dialer.Suspend() // Triggers force-closure of conn1 and conn2

	// Map must be completely wiped
	assert.Equal(t, 0, dialer.ActiveConnectionsCount())

	// Monitored connections should return read errors due to closed sockets
	buf := make([]byte, 1)
	_, err1 := conn1.Read(buf)
	require.Error(t, err1)

	_, err2 := conn2.Read(buf)
	require.Error(t, err2)
}

func TestGatedDialer_SelfCleaning(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	dialer := network.NewGatedDialer()
	baseDialer := (&net.Dialer{}).DialContext

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", l.Addr().String(), baseDialer)
	require.NoError(t, err)

	assert.Equal(t, 1, dialer.ActiveConnectionsCount())

	err = conn.Close() // Explicit close by application should trigger self-cleaning
	require.NoError(t, err)

	assert.Equal(t, 0, dialer.ActiveConnectionsCount())
}
