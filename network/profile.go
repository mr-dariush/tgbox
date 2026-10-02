// Package network provides physical layer emulation boundaries and traffic tuning
// parameters to safely bypass DPI and mimic official Telegram clients.
package network

import "time"

// TransportType represents the MTProto transport encoding strategy.
type TransportType uint8

const (
	// TransportAbridged represents the Abridged protocol.
	TransportAbridged TransportType = iota
	// TransportIntermediate represents the Intermediate protocol.
	TransportIntermediate
	// TransportPaddedIntermediate represents the Padded Intermediate protocol.
	TransportPaddedIntermediate
	// TransportFakeTLS represents the FakeTLS protocol over an obfuscated connection.
	TransportFakeTLS
)

// SocketTuningParams contains operating system level socket buffer sizes
// and TCP keep-alive configurations.
type SocketTuningParams struct {
	// ReadBufferSize specifies the SO_RCVBUF size in bytes.
	ReadBufferSize int
	// WriteBufferSize specifies the SO_SNDBUF size in bytes.
	WriteBufferSize int
	// TCPKeepAlive defines the interval between TCP keep-alive probes.
	TCPKeepAlive time.Duration
}

// ConnectionPoolLimits dictates the maximum number of parallel sockets
// permitted for high-throughput media operations.
type ConnectionPoolLimits struct {
	// MaxUploadConnections defines the upload channel concurrency limit.
	MaxUploadConnections int64
	// MaxDownloadConnections defines the download channel concurrency limit.
	MaxDownloadConnections int64
}

// KeepAliveSettings contains timing configurations for background ping mechanisms.
// These parameters help mitigate DPI timing analysis attacks.
type KeepAliveSettings struct {
	// PingInterval defines the base frequency of application-level keep-alive pings.
	PingInterval time.Duration
	// JitterOffset defines the maximum random duration added/subtracted from the PingInterval.
	JitterOffset time.Duration
}

// Profile is the polymorphic interface contract representing a device's
// specific physical network fingerprint and connection behavior.
type Profile interface {
	// GetTransportType determines the base MTProto transport protocol.
	GetTransportType() TransportType

	// GetSocketTuningParams returns the OS-level TCP socket boundaries.
	GetSocketTuningParams() SocketTuningParams

	// GetConnectionPoolLimits returns the bounds for concurrent multi-DC media streams.
	GetConnectionPoolLimits() ConnectionPoolLimits

	// GetKeepAliveSettings returns the heartbeat interval and its jitter component.
	GetKeepAliveSettings() KeepAliveSettings

	// ApplySegmentation indicates whether TCP packet segmentation should be applied
	// to the initial connection packet to mimic specific client footprints.
	ApplySegmentation() bool

	// EnablePFS indicates whether Perfect Forward Secrecy (PFS) with temporary
	// authorization keys (authKeyTemp) should be enforced.
	EnablePFS() bool
}

// AndroidProfile implements Profile optimized for emulation of mobile Android devices.
type AndroidProfile struct{}

// NewAndroidProfile constructs an Android profile matching standard mobile properties.
func NewAndroidProfile() *AndroidProfile { return &AndroidProfile{} }

// GetTransportType implements Profile.
func (*AndroidProfile) GetTransportType() TransportType {
	return TransportAbridged
}

// GetSocketTuningParams implements Profile.
func (*AndroidProfile) GetSocketTuningParams() SocketTuningParams {
	return SocketTuningParams{
		ReadBufferSize:  0,                // 0 preserves OS dynamic auto-tuning (SO_RCVBUF)
		WriteBufferSize: 0,                // 0 preserves OS dynamic auto-tuning (SO_SNDBUF)
		TCPKeepAlive:    15 * time.Minute, // 900s battery saver keep-alive
	}
}

// GetConnectionPoolLimits implements Profile.
func (*AndroidProfile) GetConnectionPoolLimits() ConnectionPoolLimits {
	return ConnectionPoolLimits{
		MaxUploadConnections:   10,
		MaxDownloadConnections: 5,
	}
}

// GetKeepAliveSettings implements Profile.
func (*AndroidProfile) GetKeepAliveSettings() KeepAliveSettings {
	return KeepAliveSettings{
		PingInterval: 900 * time.Second,
		JitterOffset: 15 * time.Second,
	}
}

// ApplySegmentation implements Profile.
func (*AndroidProfile) ApplySegmentation() bool {
	return false
}

// EnablePFS implements Profile.
func (*AndroidProfile) EnablePFS() bool {
	return true
}

// AndroidFakeTLSProfile extends AndroidProfile but enforces FakeTLS encapsulation.
type AndroidFakeTLSProfile struct {
	AndroidProfile
}

// NewAndroidFakeTLSProfile constructs an Android profile wrapped inside FakeTLS boundaries.
func NewAndroidFakeTLSProfile() *AndroidFakeTLSProfile { return &AndroidFakeTLSProfile{} }

// GetTransportType implements Profile.
func (*AndroidFakeTLSProfile) GetTransportType() TransportType {
	return TransportFakeTLS
}

// DesktopProfile implements Profile optimized for Telegram Desktop (TDesktop).
type DesktopProfile struct{}

// NewDesktopProfile constructs a desktop profile matching broadband client specs.
func NewDesktopProfile() *DesktopProfile { return &DesktopProfile{} }

// GetTransportType implements Profile.
func (*DesktopProfile) GetTransportType() TransportType {
	return TransportAbridged
}

// GetSocketTuningParams implements Profile.
func (*DesktopProfile) GetSocketTuningParams() SocketTuningParams {
	return SocketTuningParams{
		ReadBufferSize:  262144,           // 256 KB broadband read window
		WriteBufferSize: 524288,           // 512 KB broadband write window
		TCPKeepAlive:    60 * time.Second, // 60s active state keep-alive
	}
}

// GetConnectionPoolLimits implements Profile.
func (*DesktopProfile) GetConnectionPoolLimits() ConnectionPoolLimits {
	return ConnectionPoolLimits{
		MaxUploadConnections:   4,
		MaxDownloadConnections: 2,
	}
}

// GetKeepAliveSettings implements Profile.
func (*DesktopProfile) GetKeepAliveSettings() KeepAliveSettings {
	return KeepAliveSettings{
		PingInterval: 60 * time.Second,
		JitterOffset: 5 * time.Second,
	}
}

// ApplySegmentation implements Profile.
func (*DesktopProfile) ApplySegmentation() bool {
	return true
}

// EnablePFS implements Profile.
func (*DesktopProfile) EnablePFS() bool {
	return false
}
