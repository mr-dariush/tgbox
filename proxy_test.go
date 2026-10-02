package tgbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gotd/td/telegram/dcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startMockHTTPProxy spins up a lightweight raw TCP server acting as an HTTP CONNECT proxy.
func startMockHTTPProxy(t *testing.T, authUser, authPass string) (net.Listener, chan string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	reqChan := make(chan string, 1)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		reader := bufio.NewReader(conn)
		req, err := http.ReadRequest(reader)
		if err != nil {
			return
		}

		reqChan <- req.Method + " " + req.Host

		// Validate Proxy-Authorization header if authentication is configured.
		if authUser != "" {
			authHeader := req.Header.Get("Proxy-Authorization")
			expected := "Basic " + base64.StdEncoding.EncodeToString([]byte(authUser+":"+authPass))
			if authHeader != expected {
				resp := &http.Response{
					StatusCode: http.StatusProxyAuthRequired,
					ProtoMajor: 1,
					ProtoMinor: 1,
					Status:     "Proxy Authentication Required",
				}
				_ = resp.Write(conn)
				return
			}
		}

		resp := &http.Response{
			StatusCode: http.StatusOK,
			ProtoMajor: 1,
			ProtoMinor: 1,
			Status:     "OK",
		}
		_ = resp.Write(conn)

		// Hold the raw TCP tunnel open for verification.
		_, _ = io.Copy(io.Discard, conn)
	}()

	return l, reqChan
}

// TestHTTPProxyDialer_Success verifies the HTTP proxy dialer connects successfully with proper CONNECT handshake.
func TestHTTPProxyDialer_Success(t *testing.T) {
	t.Parallel()

	proxyServer, reqChan := startMockHTTPProxy(t, "admin", "securePass")
	defer proxyServer.Close()

	proxyURL, err := url.Parse(fmt.Sprintf("http://admin:securePass@%s", proxyServer.Addr().String()))
	require.NoError(t, err)

	dialer := newHTTPProxyDialer(proxyURL, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := dialer.DialContext(ctx, "tcp", "telegram.org:443")
	require.NoError(t, err)
	defer conn.Close()

	// Verify that the proxy parsed and processed the expected CONNECT payload.
	select {
	case reqLine := <-reqChan:
		assert.Equal(t, "CONNECT telegram.org:443", reqLine)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for proxy request")
	}
}

// TestHTTPProxyDialer_AuthFailure verifies the HTTP proxy dialer rejects invalid credentials.
func TestHTTPProxyDialer_AuthFailure(t *testing.T) {
	t.Parallel()

	proxyServer, _ := startMockHTTPProxy(t, "admin", "correctPassword")
	defer proxyServer.Close()

	// Attempt access using incorrect password.
	proxyURL, err := url.Parse(fmt.Sprintf("http://admin:wrongPassword@%s", proxyServer.Addr().String()))
	require.NoError(t, err)

	dialer := newHTTPProxyDialer(proxyURL, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err = dialer.DialContext(ctx, "tcp", "telegram.org:443")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "proxy connection refused")
}

// TestFallbackDialer verifies the fallback dialer degrades to direct dialing when the proxy fails.
func TestFallbackDialer(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	// Simulate a failing proxy dialer.
	badProxyDial := func(_ context.Context, _, _ string) (net.Conn, error) {
		return nil, errors.New("proxy offline")
	}

	// Mock a successful direct fallback dialer.
	mockConn := &net.TCPConn{}
	directDialCalled := false
	goodDirectDial := func(_ context.Context, _, _ string) (net.Conn, error) {
		directDialCalled = true
		return mockConn, nil
	}

	dialer := fallbackDialer(badProxyDial, goodDirectDial, logger)

	conn, err := dialer(context.Background(), "tcp", "telegram.org:443")
	require.NoError(t, err)
	assert.Equal(t, mockConn, conn)
	assert.True(t, directDialCalled)

	// Assert that a clear structural warn message was logged.
	assert.Contains(t, buf.String(), "proxy connection failed, initiating direct fallback")
	assert.Contains(t, buf.String(), "proxy offline")
}

// TestErrorResolver verifies the error resolver defers errors to connection time.
func TestErrorResolver(t *testing.T) {
	t.Parallel()

	dummyErr := errors.New("parse failed")
	resolver := errorResolver{err: dummyErr}

	_, err1 := resolver.Primary(context.Background(), 1, dcs.List{})
	require.ErrorIs(t, err1, dummyErr)

	_, err2 := resolver.MediaOnly(context.Background(), 1, dcs.List{})
	require.ErrorIs(t, err2, dummyErr)

	_, err3 := resolver.CDN(context.Background(), 1, dcs.List{})
	require.ErrorIs(t, err3, dummyErr)
}

// TestSOCKS5Dialer_URL_Parse verifies the SOCKS5 dialer parses URLs correctly and rejects invalid schemes.
func TestSOCKS5Dialer_URL_Parse(t *testing.T) {
	t.Parallel()

	// Verify socks5 dialer parses correctly.
	u, err := url.Parse("socks5://user:pass@127.0.0.1:1080")
	require.NoError(t, err)

	dialer, err := newSOCKS5Dialer(u, nil)
	require.NoError(t, err)
	assert.NotNil(t, dialer)

	// Ensure passing an unregistered proxy scheme (like http) to the SOCKS5 initializer strictly returns errors.
	badURL, err := url.Parse("http://127.0.0.1:1080")
	require.NoError(t, err)

	_, err = newSOCKS5Dialer(badURL, nil)
	assert.Error(t, err)
}
