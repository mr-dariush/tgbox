package keyboard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKeyboard_InlineBuilders verifies inline keyboard construction with style modifiers.
func TestKeyboard_InlineBuilders(t *testing.T) {
	t.Run("Construct Basic Inline Keyboard", func(t *testing.T) {
		kb := NewInlineKeyboard(
			Row(
				Callback("Action 1", "data_1", Primary()),
				URL("Link 1", "https://google.com", Danger()),
			),
			Row(
				Callback("Action 2", "data_2", Icon(987654)),
			),
		)

		require.Len(t, kb.Rows, 2)
		require.Len(t, kb.Rows[0], 2)
		require.Len(t, kb.Rows[1], 1)

		btn1 := kb.Rows[0][0]
		require.Equal(t, "Action 1", btn1.Text)
		require.Equal(t, "data_1", btn1.CallbackData)
		require.True(t, btn1.Style.primary)
		require.False(t, btn1.Style.danger)

		btn2 := kb.Rows[0][1]
		require.Equal(t, "https://google.com", btn2.URL)
		require.True(t, btn2.Style.danger)

		btn3 := kb.Rows[1][0]
		require.Equal(t, int64(987654), btn3.Style.iconID)
	})
}

// TestKeyboard_ReplyBuilders verifies reply keyboard construction and immutability of builder methods.
func TestKeyboard_ReplyBuilders(t *testing.T) {
	t.Run("Construct Reply Keyboard With Immutability", func(t *testing.T) {
		baseKB := NewReplyKeyboard(
			Row(
				Text("Button 1", Success()),
				RequestContact("Send Phone"),
			),
		)

		// Create a derived keyboard. The base instance must remain unmodified.
		derivedKB := baseKB.
			WithResize().
			WithOneTime().
			WithSelective().
			WithPlaceholder("Type here...")

		require.False(t, baseKB.Resize)
		require.Empty(t, baseKB.PlaceholderText)

		require.True(t, derivedKB.Resize)
		require.True(t, derivedKB.OneTime)
		require.True(t, derivedKB.Selective)
		require.Equal(t, "Type here...", derivedKB.PlaceholderText)

		require.Len(t, derivedKB.Rows, 1)
		require.True(t, derivedKB.Rows[0][1].RequestContact)
	})
}

// TestKeyboard_InterfaceSatisfaction verifies that keyboard types satisfy the ReplyMarkup interface.
func TestKeyboard_InterfaceSatisfaction(t *testing.T) {
	t.Run("Ensure Sealed Interface Compliance", func(_ *testing.T) {
		var _ ReplyMarkup = NewInlineKeyboard()
		var _ ReplyMarkup = NewReplyKeyboard()
		var _ ReplyMarkup = NewRemoveKeyboard(false)
	})
}
