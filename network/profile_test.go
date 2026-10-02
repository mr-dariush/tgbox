package network_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/network"
)

// dummyProfile acts as an isolated mockup for Profile interface testing.
type dummyProfile struct{}

func (*dummyProfile) GetTransportType() network.TransportType {
	return network.TransportPaddedIntermediate
}

func (*dummyProfile) GetSocketTuningParams() network.SocketTuningParams {
	return network.SocketTuningParams{
		ReadBufferSize:  32768,
		WriteBufferSize: 65536,
		TCPKeepAlive:    15 * time.Minute,
	}
}

func (*dummyProfile) GetConnectionPoolLimits() network.ConnectionPoolLimits {
	return network.ConnectionPoolLimits{
		MaxUploadConnections:   10,
		MaxDownloadConnections: 5,
	}
}

func (*dummyProfile) GetKeepAliveSettings() network.KeepAliveSettings {
	return network.KeepAliveSettings{
		PingInterval: 30 * time.Second,
		JitterOffset: 5 * time.Second,
	}
}

func (*dummyProfile) ApplySegmentation() bool {
	return false
}

func (*dummyProfile) EnablePFS() bool {
	return false
}

// TestProfile_Contract verifies that the Profile interface
// is properly abstracting network topologies and behaves consistently.
func TestProfile_Contract(t *testing.T) {
	t.Parallel()

	// Compile-time check for interface satisfaction
	var profile network.Profile = &dummyProfile{}
	require.NotNil(t, profile)

	t.Run("TransportType", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, network.TransportPaddedIntermediate, profile.GetTransportType())
	})

	t.Run("SocketTuningParams", func(t *testing.T) {
		t.Parallel()
		params := profile.GetSocketTuningParams()
		assert.Equal(t, 32768, params.ReadBufferSize)
		assert.Equal(t, 65536, params.WriteBufferSize)
		assert.Equal(t, 15*time.Minute, params.TCPKeepAlive)
	})

	t.Run("ConnectionPoolLimits", func(t *testing.T) {
		t.Parallel()
		limits := profile.GetConnectionPoolLimits()
		assert.Equal(t, int64(10), limits.MaxUploadConnections)
		assert.Equal(t, int64(5), limits.MaxDownloadConnections)
	})

	t.Run("KeepAliveSettings", func(t *testing.T) {
		t.Parallel()
		ka := profile.GetKeepAliveSettings()
		assert.Equal(t, 30*time.Second, ka.PingInterval)
		assert.Equal(t, 5*time.Second, ka.JitterOffset)
	})
}

// TestAndroidProfile_Contract verifies that AndroidProfile adheres to official mobile specs.
func TestAndroidProfile_Contract(t *testing.T) {
	t.Parallel()

	profile := network.NewAndroidProfile()
	require.NotNil(t, profile)

	assert.Equal(t, network.TransportAbridged, profile.GetTransportType())
	assert.False(t, profile.ApplySegmentation(), "Android profile must not apply TCP segmentation")
	assert.True(t, profile.EnablePFS(), "Android profile must enforce PFS with temporary auth keys")

	params := profile.GetSocketTuningParams()
	assert.Equal(t, 0, params.ReadBufferSize, "ReadBufferSize must be 0 to allow OS auto-tuning")
	assert.Equal(t, 0, params.WriteBufferSize, "WriteBufferSize must be 0 to allow OS auto-tuning")
	assert.Equal(t, 15*time.Minute, params.TCPKeepAlive)
}
