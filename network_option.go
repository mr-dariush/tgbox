// Package tgbox integrates the physical network simulation profile into the core MTProto engine.
package tgbox

import (
	"context"
	"net"

	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/transport"

	"github.com/mr-dariush/tgbox/network"
)

// WithNetworkProfile injects a strictly modeled Profile into the MTProto pipeline.
// It configures the physical transport protocol, OS-level TCP tuning (segmentation, buffers),
// Keep-Alive jitters, connection pool limits, and Perfect Forward Secrecy (PFS) temporary key parameters.
func WithNetworkProfile(profile network.Profile) Option {
	return fnOption(func(o *clientOptions) {
		if profile == nil {
			return
		}

		// 1. Resolve Transport Protocol Mapping
		var proto dcs.Protocol
		switch profile.GetTransportType() {
		case network.TransportAbridged:
			proto = transport.Abridged
		case network.TransportIntermediate:
			proto = transport.Intermediate
		case network.TransportPaddedIntermediate:
			proto = transport.PaddedIntermediate
		case network.TransportFakeTLS:
			// Gotd natively builds FakeTLS over the underlying protocol bytes.
			// Typically, Telegram's FakeTLS payload encloses PaddedIntermediate data.
			proto = transport.PaddedIntermediate
		default:
			proto = transport.PaddedIntermediate
		}

		// 2. Configure Custom Emulated Dialer with Segmentation
		// We fetch OS-level socket tunings and apply them dynamically.
		// Segmentation is governed strictly by the profile rather than the transport protocol.
		tuningParams := profile.GetSocketTuningParams()
		emulatedDialer := network.EmulatedDialer(tuningParams, profile.ApplySegmentation())

		// Instantiate GatedDialer and attach it to the client options
		gatedDialer := network.NewGatedDialer()
		o.gatedDialer = gatedDialer

		// 3. Inject to gotd's dcs.Resolver
		// We enforce Obfuscated2 natively. The EmulatedDialer handles the segmentation,
		// and gotd handles the obfuscation overlay. The GatedDialer controls suspension safely.
		o.engineOpts.Resolver = dcs.Plain(dcs.PlainOptions{
			Protocol: proto,
			Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
				return gatedDialer.DialContext(ctx, network, addr, emulatedDialer)
			},
			Obfuscated: true,
		})

		// 4. Store connection pool and keep-alive parameters internally so the Client
		// constructor can initialise the fuzzy ping engine and session pool manager.
		o.poolLimits = profile.GetConnectionPoolLimits()
		o.keepaliveSettings = profile.GetKeepAliveSettings()

		// 5. Configure Perfect Forward Secrecy (PFS) with temporary auth keys.
		// When enabled by the profile (mandatory for official Telegram Android emulation),
		// set the default 24-hour lifetime (86400s) if not already customized.
		if profile.EnablePFS() {
			o.engineOpts.EnablePFS = true
			if o.engineOpts.TempKeyTTL == 0 {
				const defaultTempKeyTTL = 86400 // 24 hours in seconds
				o.engineOpts.TempKeyTTL = defaultTempKeyTTL
			}
		}
	})
}
