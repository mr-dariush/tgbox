package network

import (
	"context"
	"net"
	"time"
)

// segmentedConn wraps a net.Conn to enforce TCP segmentation on the first packet.
type segmentedConn struct {
	net.Conn
	segmented bool
}

// Write intercepts the network write to segment the initial payload (ClientHello)
// into two TCP segments separated by a micro-delay. This prevents the OS kernel
// from coalescing them, matching the specific footprint of the Desktop client.
func (c *segmentedConn) Write(b []byte) (int, error) {
	if !c.segmented && len(b) > 408 {
		c.segmented = true

		n1, err := c.Conn.Write(b[:408])
		if err != nil {
			return n1, err
		}

		// Micro-delay to ensure the first segment leaves the NIC buffer
		// before the second segment is pushed, avoiding TCP coalescing.
		time.Sleep(15 * time.Millisecond)

		n2, err := c.Conn.Write(b[408:])
		return n1 + n2, err
	}

	return c.Conn.Write(b)
}

// EmulatedDialer builds a custom dialer function that applies OS-level socket
// tuning parameters (buffer sizes, Nagle's algorithm) and optional TCP segmentation.
func EmulatedDialer(params SocketTuningParams, applySegmentation bool) func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			KeepAlive: params.TCPKeepAlive,
		}

		conn, err := dialer.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err //nolint:wrapcheck // keep original dialer error signature
		}

		if tcpConn, ok := conn.(*net.TCPConn); ok {
			// Disable Nagle's algorithm to allow micro-segmented writes
			_ = tcpConn.SetNoDelay(true)

			if params.ReadBufferSize > 0 {
				_ = tcpConn.SetReadBuffer(params.ReadBufferSize)
			}
			if params.WriteBufferSize > 0 {
				_ = tcpConn.SetWriteBuffer(params.WriteBufferSize)
			}
		}

		if applySegmentation {
			return &segmentedConn{Conn: conn}, nil
		}

		return conn, nil
	}
}
