package tgbox

import (
	"testing"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
)

// TestDeviceAndClientOptions exercises the exact mutation of config options
// into gotd specific structures without applying hardcoded library bypasses.
func TestDeviceAndClientOptions(t *testing.T) {
	t.Parallel()

	t.Run("TestRawDeviceOptions", func(t *testing.T) {
		t.Parallel()

		var o clientOptions

		// Apply raw connection configuration properties
		WithDeviceModel("CustomModel").apply(&o)
		WithSystemVersion("CustomSys").apply(&o)
		WithAppVersion("CustomApp").apply(&o)
		WithSystemLangCode("fa-IR").apply(&o)
		WithLangPack("tdesktop").apply(&o)
		WithLangCode("fa").apply(&o)

		assert.Equal(t, "CustomModel", o.engineOpts.Device.DeviceModel)
		assert.Equal(t, "CustomSys", o.engineOpts.Device.SystemVersion)
		assert.Equal(t, "CustomApp", o.engineOpts.Device.AppVersion)
		assert.Equal(t, "fa-IR", o.engineOpts.Device.SystemLangCode)
		assert.Equal(t, "tdesktop", o.engineOpts.Device.LangPack)
		assert.Equal(t, "fa", o.engineOpts.Device.LangCode)
	})

	t.Run("TestWithDeviceConfig", func(t *testing.T) {
		t.Parallel()

		var o clientOptions
		cfg := telegram.DeviceConfig{
			DeviceModel:    "DirectModel",
			SystemVersion:  "DirectSys",
			AppVersion:     "DirectApp",
			SystemLangCode: "en-US",
			LangPack:       "android",
			LangCode:       "en",
		}

		WithDeviceConfig(cfg).apply(&o)
		assert.Equal(t, cfg, o.engineOpts.Device)
	})

	t.Run("TestWithInitParams", func(t *testing.T) {
		t.Parallel()

		var o clientOptions
		params := &tg.JSONObject{
			Value: []tg.JSONObjectValue{
				{
					Key:   "tz_offset",
					Value: &tg.JSONNumber{Value: 12600},
				},
			},
		}

		WithInitParams(params).apply(&o)
		assert.Equal(t, params, o.engineOpts.Device.Params)
	})

	t.Run("TestWithoutUpdatesOption", func(t *testing.T) {
		t.Parallel()

		var o clientOptions
		assert.False(t, o.engineOpts.NoUpdates)

		WithoutUpdates().apply(&o)
		assert.True(t, o.engineOpts.NoUpdates)
	})
}
