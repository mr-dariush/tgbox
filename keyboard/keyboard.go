// Package keyboard provides fluent builders for Telegram inline and reply keyboards.
package keyboard

import "github.com/gotd/td/tg"

// ReplyMarkup defines the sealed interface for all Telegram keyboard markups.
type ReplyMarkup interface {
	isReplyMarkup()
}

// buttonStyle holds the visual and meta-configuration for Premium buttons.
type buttonStyle struct {
	primary bool
	danger  bool
	success bool
	iconID  int64
}

// ButtonStyleOption configures button visuals.
type ButtonStyleOption func(*buttonStyle)

// Primary returns a ButtonStyleOption that applies the primary style to a button.
func Primary() ButtonStyleOption { return func(s *buttonStyle) { s.primary = true } }

// Danger returns a ButtonStyleOption that applies the danger style to a button.
func Danger() ButtonStyleOption { return func(s *buttonStyle) { s.danger = true } }

// Success returns a ButtonStyleOption that applies the success style to a button.
func Success() ButtonStyleOption { return func(s *buttonStyle) { s.success = true } }

// Icon returns a ButtonStyleOption that sets an emoji icon for a button.
func Icon(emojiID int64) ButtonStyleOption { return func(s *buttonStyle) { s.iconID = emojiID } }

func applyStyle(opts []ButtonStyleOption) buttonStyle {
	var style buttonStyle
	for _, opt := range opts {
		opt(&style)
	}
	return style
}

// Row is a generic helper to construct a single row of buttons.
func Row[T any](buttons ...T) []T {
	return buttons
}

// BoolPtr is a helper to pass boolean pointers to options.
//
//go:fix inline
func BoolPtr(b bool) *bool { return new(b) }

// --- Inline Keyboard Definitions ---

// InlineButton represents a single button in an inline keyboard.
type InlineButton struct {
	Text                         string
	CallbackData                 string
	URL                          string
	WebAppURL                    string
	SwitchInlineQuery            *string
	SwitchInlineQueryCurrentChat *string
	Pay                          bool
	Style                        buttonStyle
}

// Callback creates an inline button that sends callback data when pressed.
func Callback(text, data string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, CallbackData: data, Style: applyStyle(opts)}
}

// URL creates an inline button that opens a URL.
func URL(text, url string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, URL: url, Style: applyStyle(opts)}
}

// WebApp creates an inline button that opens a web app.
func WebApp(text, url string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, WebAppURL: url, Style: applyStyle(opts)}
}

// SwitchInline creates an inline button that inserts text into the current chat's query.
func SwitchInline(text, query string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, SwitchInlineQuery: &query, Style: applyStyle(opts)}
}

// SwitchInlineCurrentChat creates an inline button that inserts text into the current inline query.
func SwitchInlineCurrentChat(text, query string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, SwitchInlineQueryCurrentChat: &query, Style: applyStyle(opts)}
}

// Pay creates an inline button that initiates a payment.
func Pay(text string, opts ...ButtonStyleOption) InlineButton {
	return InlineButton{Text: text, Pay: true, Style: applyStyle(opts)}
}

// InlineKeyboard represents an inline keyboard markup with rows of buttons.
type InlineKeyboard struct {
	Rows [][]InlineButton
}

func (InlineKeyboard) isReplyMarkup() {}

// NewInlineKeyboard creates an inline keyboard from rows of buttons.
func NewInlineKeyboard(rows ...[]InlineButton) InlineKeyboard {
	return InlineKeyboard{Rows: rows}
}

// InlineBuilder is a fluent builder for constructing inline keyboards.
type InlineBuilder struct {
	columns int
	buttons []InlineButton
}

// NewInlineBuilder creates a new inline keyboard builder with the specified number of columns.
func NewInlineBuilder(columns int) *InlineBuilder {
	if columns < 1 {
		columns = 1
	}
	return &InlineBuilder{columns: columns, buttons: make([]InlineButton, 0, 8)}
}

// Add appends buttons to the inline builder and returns the builder for chaining.
func (b *InlineBuilder) Add(buttons ...InlineButton) *InlineBuilder {
	b.buttons = append(b.buttons, buttons...)
	return b
}

// Build constructs the final InlineKeyboard from the accumulated buttons.
func (b *InlineBuilder) Build() InlineKeyboard {
	if len(b.buttons) == 0 {
		return InlineKeyboard{}
	}
	var rows [][]InlineButton
	var currentRow []InlineButton

	for _, btn := range b.buttons {
		currentRow = append(currentRow, btn)
		if len(currentRow) == b.columns {
			rows = append(rows, currentRow)
			currentRow = nil
		}
	}
	if len(currentRow) > 0 {
		rows = append(rows, currentRow)
	}
	return InlineKeyboard{Rows: rows}
}

// --- Reply Keyboard Definitions ---

// RequestUsersOptions specifies filters for requesting users.
type RequestUsersOptions struct {
	MaxQuantity int
	IsBot       *bool
	IsPremium   *bool
}

// RequestGroupOptions specifies filters for requesting groups.
type RequestGroupOptions struct {
	IsCreator       bool
	BotIsMember     bool
	HasUsername     *bool
	IsForum         *bool
	UserAdminRights *tg.ChatAdminRights
	BotAdminRights  *tg.ChatAdminRights
}

// RequestChannelOptions specifies filters for requesting channels.
type RequestChannelOptions struct {
	IsCreator       bool
	HasUsername     *bool
	UserAdminRights *tg.ChatAdminRights
	BotAdminRights  *tg.ChatAdminRights
}

// ReplyButton represents a single button in a reply keyboard.
type ReplyButton struct {
	Text            string
	RequestContact  bool
	RequestLocation bool
	WebAppURL       *string
	RequestPoll     *bool
	UserProfileID   *int64

	RequestPeerButtonID *int
	RequestUsers        *RequestUsersOptions
	RequestGroup        *RequestGroupOptions
	RequestChannel      *RequestChannelOptions

	Style buttonStyle
}

// Text creates a reply button with plain text.
func Text(text string, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, Style: applyStyle(opts)}
}

// RequestContact creates a reply button that requests the user's phone number.
func RequestContact(text string, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestContact: true, Style: applyStyle(opts)}
}

// RequestLocation creates a reply button that requests the user's location.
func RequestLocation(text string, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestLocation: true, Style: applyStyle(opts)}
}

// ReplyWebApp creates a reply button that opens a web app.
func ReplyWebApp(text, url string, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, WebAppURL: &url, Style: applyStyle(opts)}
}

// RequestPoll creates a reply button that requests a poll or quiz.
func RequestPoll(text string, quizOnly bool, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestPoll: &quizOnly, Style: applyStyle(opts)}
}

// UserProfile creates a reply button that requests a user profile.
func UserProfile(text string, userID int64, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, UserProfileID: &userID, Style: applyStyle(opts)}
}

// RequestUsers creates a reply button that requests users.
func RequestUsers(text string, buttonID int, reqOpts RequestUsersOptions, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestPeerButtonID: &buttonID, RequestUsers: &reqOpts, Style: applyStyle(opts)}
}

// RequestGroup creates a reply button that requests a group.
func RequestGroup(text string, buttonID int, reqOpts RequestGroupOptions, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestPeerButtonID: &buttonID, RequestGroup: &reqOpts, Style: applyStyle(opts)}
}

// RequestChannel creates a reply button that requests a channel.
func RequestChannel(text string, buttonID int, reqOpts RequestChannelOptions, opts ...ButtonStyleOption) ReplyButton {
	return ReplyButton{Text: text, RequestPeerButtonID: &buttonID, RequestChannel: &reqOpts, Style: applyStyle(opts)}
}

// ReplyKeyboard represents a reply keyboard markup with rows of buttons.
type ReplyKeyboard struct {
	Rows            [][]ReplyButton
	Resize          bool
	OneTime         bool
	Selective       bool
	PlaceholderText string
}

func (ReplyKeyboard) isReplyMarkup() {}

// NewReplyKeyboard creates a reply keyboard from rows of buttons.
func NewReplyKeyboard(rows ...[]ReplyButton) ReplyKeyboard {
	return ReplyKeyboard{Rows: rows}
}

// WithResize returns a copy of the reply keyboard with the resize flag set.
func (k ReplyKeyboard) WithResize() ReplyKeyboard { k.Resize = true; return k }

// WithOneTime returns a copy of the reply keyboard with the one-time flag set.
func (k ReplyKeyboard) WithOneTime() ReplyKeyboard { k.OneTime = true; return k }

// WithSelective returns a copy of the reply keyboard with the selective flag set.
func (k ReplyKeyboard) WithSelective() ReplyKeyboard { k.Selective = true; return k }

// WithPlaceholder returns a copy of the reply keyboard with the given placeholder text.
func (k ReplyKeyboard) WithPlaceholder(text string) ReplyKeyboard { k.PlaceholderText = text; return k }

// ReplyBuilder is a fluent builder for constructing reply keyboards.
type ReplyBuilder struct {
	columns         int
	buttons         []ReplyButton
	resize          bool
	oneTime         bool
	selective       bool
	placeholderText string
}

// NewReplyBuilder creates a new reply keyboard builder with the specified number of columns.
func NewReplyBuilder(columns int) *ReplyBuilder {
	if columns < 1 {
		columns = 1
	}
	return &ReplyBuilder{columns: columns, buttons: make([]ReplyButton, 0, 8)}
}

// Add appends buttons to the reply builder and returns the builder for chaining.
func (b *ReplyBuilder) Add(buttons ...ReplyButton) *ReplyBuilder {
	b.buttons = append(b.buttons, buttons...)
	return b
}

// WithResize sets the resize flag and returns the builder for chaining.
func (b *ReplyBuilder) WithResize() *ReplyBuilder { b.resize = true; return b }

// WithOneTime sets the one-time flag and returns the builder for chaining.
func (b *ReplyBuilder) WithOneTime() *ReplyBuilder { b.oneTime = true; return b }

// WithSelective sets the selective flag and returns the builder for chaining.
func (b *ReplyBuilder) WithSelective() *ReplyBuilder { b.selective = true; return b }

// WithPlaceholder sets the placeholder text and returns the builder for chaining.
func (b *ReplyBuilder) WithPlaceholder(text string) *ReplyBuilder { b.placeholderText = text; return b }

// Build constructs the final ReplyKeyboard from the accumulated buttons and settings.
func (b *ReplyBuilder) Build() ReplyKeyboard {
	if len(b.buttons) == 0 {
		return ReplyKeyboard{Resize: b.resize, OneTime: b.oneTime, Selective: b.selective, PlaceholderText: b.placeholderText}
	}
	var rows [][]ReplyButton
	var currentRow []ReplyButton

	for _, btn := range b.buttons {
		currentRow = append(currentRow, btn)
		if len(currentRow) == b.columns {
			rows = append(rows, currentRow)
			currentRow = nil
		}
	}
	if len(currentRow) > 0 {
		rows = append(rows, currentRow)
	}
	return ReplyKeyboard{Rows: rows, Resize: b.resize, OneTime: b.oneTime, Selective: b.selective, PlaceholderText: b.placeholderText}
}

// RemoveKeyboard represents a request to remove the keyboard markup from the screen.
type RemoveKeyboard struct {
	Selective bool
}

func (RemoveKeyboard) isReplyMarkup() {}

// NewRemoveKeyboard creates a remove keyboard markup with the given selective flag.
func NewRemoveKeyboard(selective bool) RemoveKeyboard {
	return RemoveKeyboard{Selective: selective}
}
