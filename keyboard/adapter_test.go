package keyboard

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeyboardAdapter_TranslateInline verifies translation of inline keyboards to Telegram ReplyInlineMarkup.
func TestKeyboardAdapter_TranslateInline(t *testing.T) {
	kb := NewInlineKeyboard(
		Row(
			Callback("Action", "data_1", Danger()),
			URL("Link", "https://google.com", Icon(12345)),
		),
	)

	translated, err := TranslateReplyMarkup(kb)
	require.NoError(t, err)

	inlineMarkup, ok := translated.(*tg.ReplyInlineMarkup)
	require.True(t, ok)
	require.Len(t, inlineMarkup.Rows, 1)

	// Verify Callback Button & Danger style
	btn1 := inlineMarkup.Rows[0].Buttons[0]
	assert.Equal(t, "Action", btn1.Text)
	cbType, ok := btn1.Type.(*tg.InlineButtonTypeCallback)
	require.True(t, ok)
	assert.Equal(t, []byte("data_1"), cbType.Data)
	assert.True(t, btn1.Style.BgDanger)

	// Verify URL Button & Icon style
	btn2 := inlineMarkup.Rows[0].Buttons[1]
	assert.Equal(t, "Link", btn2.Text)
	urlType, ok := btn2.Type.(*tg.InlineButtonTypeURL)
	require.True(t, ok)
	assert.Equal(t, "https://google.com", urlType.URL)
	iconID, ok := btn2.Style.GetIcon()
	assert.True(t, ok)
	assert.Equal(t, int64(12345), iconID)
}

// TestKeyboardAdapter_TranslateReply verifies translation of reply keyboards to Telegram ReplyKeyboardMarkup.
func TestKeyboardAdapter_TranslateReply(t *testing.T) {
	kb := NewReplyKeyboard(
		Row(Text("Hello", Primary()), RequestContact("Send")),
	).WithResize().WithPlaceholder("Waiting...")

	translated, err := TranslateReplyMarkup(kb)
	require.NoError(t, err)

	replyMarkup, ok := translated.(*tg.ReplyKeyboardMarkup)
	require.True(t, ok)
	assert.True(t, replyMarkup.Resize)

	placeholder, hasPlaceholder := replyMarkup.GetPlaceholder()
	assert.True(t, hasPlaceholder)
	assert.Equal(t, "Waiting...", placeholder)

	require.Len(t, replyMarkup.Rows, 1)

	// Verify Text Button & Primary style
	btn1 := replyMarkup.Rows[0].Buttons[0]
	assert.Equal(t, "Hello", btn1.Text)
	assert.True(t, btn1.Style.BgPrimary)
	assert.IsType(t, &tg.ButtonTypeDefault{}, btn1.Type)

	// Verify Contact Button (No explicit style)
	btn2 := replyMarkup.Rows[0].Buttons[1]
	assert.Equal(t, "Send", btn2.Text)
	assert.IsType(t, &tg.ButtonTypeRequestPhone{}, btn2.Type)
}

// TestKeyboardAdapter_TranslateRemove verifies translation of remove keyboards to Telegram ReplyKeyboardHide.
func TestKeyboardAdapter_TranslateRemove(t *testing.T) {
	kb := NewRemoveKeyboard(true)

	translated, err := TranslateReplyMarkup(kb)
	require.NoError(t, err)

	hideMarkup, ok := translated.(*tg.ReplyKeyboardHide)
	require.True(t, ok)
	assert.True(t, hideMarkup.Selective)
}

// TestKeyboardAdapter_Validation verifies that invalid inline keyboard buttons are properly rejected.
func TestKeyboardAdapter_Validation(t *testing.T) {
	invalidKB := NewInlineKeyboard(Row(InlineButton{Text: "Empty"}))

	translated, err := TranslateReplyMarkup(invalidKB)
	require.Error(t, err)
	assert.Nil(t, translated)
	assert.Contains(t, err.Error(), "must specify an action")
}
