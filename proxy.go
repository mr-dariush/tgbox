package tgbox

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/transport"
	"golang.org/x/net/proxy"
)

// dialContextFunc defines the standard context-aware dialer signature.
type dialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// fallbackDialer wraps a primary proxy dialer. If the proxy dialer fails, it logs
// the error and automatically gracefully degrades to the provided direct dialer.
func fallbackDialer(
	proxyDial dialContextFunc,
	directDial dialContextFunc,
	logger *slog.Logger,
) dialContextFunc {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := proxyDial(ctx, network, addr)
		if err == nil {
			return conn, nil
		}

		if logger != nil {
			logger.Warn("proxy connection failed, initiating direct fallback",
				slog.String("target_addr", addr),
				slog.String("network", network),
				slog.Any("error", err),
			)
		}

		return directDial(ctx, network, addr)
	}
}

// httpProxyDialer implements a context-aware HTTP CONNECT proxy dialer.
type httpProxyDialer struct {
	proxyURL *url.URL
	forward  *net.Dialer
}

// newHTTPProxyDialer creates a new HTTP CONNECT dialer.
func newHTTPProxyDialer(proxyURL *url.URL, forward *net.Dialer) *httpProxyDialer {
	if forward == nil {
		forward = &net.Dialer{}
	}
	return &httpProxyDialer{
		proxyURL: proxyURL,
		forward:  forward,
	}
}

// DialContext executes the HTTP CONNECT handshake over the defined context.
func (h *httpProxyDialer) DialContext(ctx context.Context, _, addr string) (net.Conn, error) {
	conn, err := h.forward.DialContext(ctx, "tcp", h.proxyURL.Host)
	if err != nil {
		return nil, fmt.Errorf("tgbox: failed to dial http proxy %s: %w", h.proxyURL.Host, err)
	}

	req := &http.Request{
		Method: http.MethodConnect,
		URL:    &url.URL{Opaque: addr},
		Host:   addr,
		Header: make(http.Header),
	}

	if h.proxyURL.User != nil {
		password, _ := h.proxyURL.User.Password()
		credentials := h.proxyURL.User.Username() + ":" + password
		basicAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(credentials))
		req.Header.Add("Proxy-Authorization", basicAuth)
	}

	req = req.WithContext(ctx)

	if writeErr := req.Write(conn); writeErr != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tgbox: failed to write http connect request: %w", writeErr)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tgbox: failed to read http connect response: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_ = conn.Close()
		return nil, fmt.Errorf("tgbox: proxy connection refused with status: %d %s", resp.StatusCode, resp.Status)
	}

	return conn, nil
}

// newSOCKS5Dialer parses the proxy URL and instantiates a context-aware SOCKS5 dialer.
func newSOCKS5Dialer(u *url.URL, forward *net.Dialer) (proxy.ContextDialer, error) {
	if forward == nil {
		forward = &net.Dialer{}
	}

	d, err := proxy.FromURL(u, forward)
	if err != nil {
		return nil, fmt.Errorf("tgbox: failed to parse socks5 proxy url: %w", err)
	}

	cd, ok := d.(proxy.ContextDialer)
	if !ok {
		return nil, errors.New("tgbox: resolved socks5 dialer does not support context propagation")
	}

	return cd, nil
}

// errorResolver is a deferred-error dcs.Resolver. It is loaded when the functional option
// parse phase fails. It defers the error to connection time, preventing silent failure.
type errorResolver struct {
	err error
}

func (e errorResolver) Primary(_ context.Context, _ int, _ dcs.List) (transport.Conn, error) {
	return nil, e.err
}

func (e errorResolver) MediaOnly(_ context.Context, _ int, _ dcs.List) (transport.Conn, error) {
	return nil, e.err
}

func (e errorResolver) CDN(_ context.Context, _ int, _ dcs.List) (transport.Conn, error) {
	return nil, e.err
}

// parseProxy parses a raw URL proxy string and constructs a fully resolved dcs.Resolver.
func parseProxy(rawURL string, fallback bool, logger *slog.Logger) (dcs.Resolver, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("tgbox: invalid proxy URL: %w", err)
	}

	var dialFunc dialContextFunc
	direct := &net.Dialer{}

	switch u.Scheme {
	case "socks5", "socks5h", "socks":
		cd, err := newSOCKS5Dialer(u, direct)
		if err != nil {
			return nil, err
		}
		dialFunc = cd.DialContext
	case "http", "https":
		dialer := newHTTPProxyDialer(u, direct)
		dialFunc = dialer.DialContext
	default:
		return nil, fmt.Errorf("tgbox: unsupported proxy protocol scheme %q", u.Scheme)
	}

	if fallback {
		dialFunc = fallbackDialer(dialFunc, direct.DialContext, logger)
	}

	return dcs.Plain(dcs.PlainOptions{
		Dial: dcs.DialFunc(dialFunc),
	}), nil
}

// WithProxy configures the client to route all network traffic through a proxy specified
// by a raw URL (e.g. "socks5://user:pass@127.0.0.1:1080" or "http://127.0.0.1:8080").
// If fallback is true, direct connection is attempted as a backup if the proxy goes offline.
func WithProxy(rawURL string, fallback bool) Option {
	return fnOption(func(o *clientOptions) {
		resolver, err := parseProxy(rawURL, fallback, o.slogLogger)
		if err != nil {
			o.engineOpts.Resolver = errorResolver{err: err}
			return
		}
		o.engineOpts.Resolver = resolver
	})
}

// WithSOCKS5 explicitly configures a SOCKS5 proxy using separate parameters.
func WithSOCKS5(addr, username, password string, fallback bool) Option {
	return fnOption(func(o *clientOptions) {
		var userInfo *url.Userinfo
		if username != "" {
			userInfo = url.UserPassword(username, password)
		}
		u := &url.URL{
			Scheme: "socks5",
			User:   userInfo,
			Host:   addr,
		}
		resolver, err := parseProxy(u.String(), fallback, o.slogLogger)
		if err != nil {
			o.engineOpts.Resolver = errorResolver{err: err}
			return
		}
		o.engineOpts.Resolver = resolver
	})
}

// WithHTTPProxy explicitly configures an HTTP CONNECT proxy using separate parameters.
func WithHTTPProxy(addr, username, password string, fallback bool) Option {
	return fnOption(func(o *clientOptions) {
		var userInfo *url.Userinfo
		if username != "" {
			userInfo = url.UserPassword(username, password)
		}
		u := &url.URL{
			Scheme: "http",
			User:   userInfo,
			Host:   addr,
		}
		resolver, err := parseProxy(u.String(), fallback, o.slogLogger)
		if err != nil {
			o.engineOpts.Resolver = errorResolver{err: err}
			return
		}
		o.engineOpts.Resolver = resolver
	})
}
