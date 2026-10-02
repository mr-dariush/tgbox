package tgbox

import (
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// WithDeviceConfig completely overrides the underlying device configuration
// sent during the initConnection phase of the MTProto handshake.
func WithDeviceConfig(d telegram.DeviceConfig) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device = d
	})
}

// WithDeviceModel sets the raw device model string (device_model)
// reported to the Telegram servers during initialization.
func WithDeviceModel(model string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.DeviceModel = model
	})
}

// WithSystemVersion sets the raw operating system version string (system_version)
// reported to the Telegram servers during initialization.
func WithSystemVersion(v string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.SystemVersion = v
	})
}

// WithAppVersion sets the application version string (app_version)
// reported to the Telegram servers during initialization.
func WithAppVersion(v string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.AppVersion = v
	})
}

// WithSystemLangCode sets the system language code (system_lang_code)
// reported to the Telegram servers during initialization.
func WithSystemLangCode(code string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.SystemLangCode = code
	})
}

// WithLangPack sets the platform pack string (lang_pack),
// which indicates the target platform UI layer (e.g., "android", "tdesktop").
func WithLangPack(pack string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.LangPack = pack
	})
}

// WithLangCode sets either an ISO 639-1 language code or a language pack name (lang_code)
// obtained from a language pack link.
func WithLangCode(code string) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.LangCode = code
	})
}

// WithInitParams specifies custom, raw JSON parameters (params)
// sent to the Telegram servers during initialization. This allows complete
// and unrestricted flexibility to define properties such as timezone offsets.
func WithInitParams(v tg.JSONValueClass) Option {
	return fnOption(func(o *clientOptions) {
		o.engineOpts.Device.Params = v
	})
}
