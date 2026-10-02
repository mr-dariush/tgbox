package network_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/network"
)

// TestEmulatedDialer_Segmentation verifies that the dialer correctly wraps the
// connection and applies a micro-delay only on the first large packet write.
func TestEmulatedDialer_Segmentation(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr == nil {
			_, _ = io.Copy(io.Discard, conn)
			_ = conn.Close()
		}
	}()

	params := network.SocketTuningParams{
		ReadBufferSize:  32768,
		WriteBufferSize: 65536,
		TCPKeepAlive:    15 * time.Minute,
	}

	dialer := network.EmulatedDialer(params, true /* applySegmentation */)
	conn, err := dialer(context.Background(), "tcp", l.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	payload := make([]byte, 500)

	// First write: should trigger segmentation and delay
	start := time.Now()
	n, err := conn.Write(payload)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 500, n)
	// Expecting at least ~14ms delay due to intentional sleep (allowing 1ms variance)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(14))

	// Second write: should bypass segmentation
	start = time.Now()
	n, err = conn.Write(payload)
	elapsed = time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 500, n)
	// Expecting immediate write execution
	assert.Less(t, elapsed.Milliseconds(), int64(10))
}

// TestEmulatedDialer_NoSegmentation verifies that setting the applySegmentation
// flag to false bypasses the custom logic entirely.
func TestEmulatedDialer_NoSegmentation(t *testing.T) {
	t.Parallel()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, acceptErr := l.Accept()
		if acceptErr == nil {
			_, _ = io.Copy(io.Discard, conn)
			_ = conn.Close()
		}
	}()

	params := network.SocketTuningParams{
		TCPKeepAlive: 60 * time.Second,
	}

	dialer := network.EmulatedDialer(params, false /* applySegmentation */)
	conn, err := dialer(context.Background(), "tcp", l.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	payload := make([]byte, 500)

	start := time.Now()
	n, err := conn.Write(payload)
	elapsed := time.Since(start)

	require.NoError(t, err)
	assert.Equal(t, 500, n)
	assert.Less(t, elapsed.Milliseconds(), int64(10))
}
