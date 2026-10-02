package keyboard

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuilderRows(t *testing.T) {
	for _, tc := range []struct {
		name    string
		columns int
		lengths []int
	}{
		{"zero", 0, []int{1, 1, 1}}, {"negative", -1, []int{1, 1, 1}}, {"partial row", 2, []int{2, 1}}, {"full row", 3, []int{3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inline := NewInlineBuilder(tc.columns)
			assert.Empty(t, inline.Build().Rows)
			buttons := []InlineButton{Callback("A", "a"), Callback("B", "b"), Callback("C", "c")}
			kb := inline.Add(buttons[:1]...).Add(buttons[1:]...).Build()
			require.Len(t, kb.Rows, len(tc.lengths))
			var flat []InlineButton
			for i, n := range tc.lengths {
				assert.Len(t, kb.Rows[i], n)
				flat = append(flat, kb.Rows[i]...)
			}
			assert.Equal(t, buttons, flat)
			reply := NewReplyBuilder(tc.columns).WithResize().WithOneTime().WithSelective().WithPlaceholder("choose")
			empty := reply.Build()
			assert.Empty(t, empty.Rows)
			assert.True(t, empty.Resize)
			assert.True(t, empty.OneTime)
			assert.True(t, empty.Selective)
			assert.Equal(t, "choose", empty.PlaceholderText)
			replyButtons := []ReplyButton{Text("A"), Text("B"), Text("C")}
			result := reply.Add(replyButtons...).Build()
			require.Len(t, result.Rows, len(tc.lengths))
			var flatReply []ReplyButton
			for i, n := range tc.lengths {
				assert.Len(t, result.Rows[i], n)
				flatReply = append(flatReply, result.Rows[i]...)
			}
			assert.Equal(t, replyButtons, flatReply)
			assert.True(t, result.Resize)
			assert.True(t, result.OneTime)
			assert.True(t, result.Selective)
			assert.Equal(t, "choose", result.PlaceholderText)
		})
	}
}

func TestInlineActions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		button InlineButton
		action tg.InlineButtonTypeClass
	}{
		{"web app", WebApp("open", "https://example.com"), &tg.InlineButtonTypeWebView{URL: "https://example.com"}},
		{"switch", SwitchInline("search", ""), &tg.InlineButtonTypeSwitchInline{Query: ""}},
		{"current chat", SwitchInlineCurrentChat("search", "query"), &tg.InlineButtonTypeSwitchInline{Query: "query", SamePeer: true}},
		{"pay", Pay("buy", Success()), &tg.InlineButtonTypeBuy{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			markup, err := TranslateReplyMarkup(NewInlineKeyboard(Row(tc.button)))
			require.NoError(t, err)
			rows := markup.(*tg.ReplyInlineMarkup).Rows
			require.Len(t, rows, 1)
			require.Len(t, rows[0].Buttons, 1)
			assert.Equal(t, tc.button.Text, rows[0].Buttons[0].Text)
			assert.Equal(t, tc.action, rows[0].Buttons[0].Type)
			if tc.name == "pay" {
				assert.True(t, rows[0].Buttons[0].Style.BgSuccess)
			}
		})
	}
}

func TestReplyActions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		button ReplyButton
		action tg.ButtonTypeClass
	}{
		{"location", RequestLocation("where"), &tg.ButtonTypeRequestGeoLocation{}},
		{"web app", ReplyWebApp("open", "https://example.com"), &tg.ButtonTypeSimpleWebView{URL: "https://example.com"}},
		{"poll", RequestPoll("quiz", true), &tg.ButtonTypeRequestPoll{Quiz: true}},
	} {
		t.Run(tc.name, func(t *testing.T) { assert.Equal(t, tc.action, translateReplyButton(tc.button).Type) })
	}
	t.Run("users", func(t *testing.T) {
		btn := RequestUsers("users", 3, RequestUsersOptions{IsBot: new(false), IsPremium: new(true)})
		req := translateReplyButton(btn).Type.(*tg.ButtonTypeRequestPeer)
		assert.Equal(t, 3, req.ButtonID)
		assert.Equal(t, 1, req.MaxQuantity)
		typ := req.PeerType.(*tg.RequestPeerTypeUser)
		bot, ok := typ.GetBot()
		assert.True(t, ok)
		assert.False(t, bot)
		premium, ok := typ.GetPremium()
		assert.True(t, ok)
		assert.True(t, premium)
		req = translateReplyButton(RequestUsers("users", 4, RequestUsersOptions{MaxQuantity: 5})).Type.(*tg.ButtonTypeRequestPeer)
		assert.Equal(t, 5, req.MaxQuantity)
	})
	rights := &tg.ChatAdminRights{DeleteMessages: true}
	t.Run("group", func(t *testing.T) {
		req := translateReplyButton(RequestGroup("group", 5, RequestGroupOptions{IsCreator: true, BotIsMember: true, HasUsername: new(false), IsForum: new(true), UserAdminRights: rights, BotAdminRights: rights})).Type.(*tg.ButtonTypeRequestPeer)
		assert.Equal(t, 5, req.ButtonID)
		typ := req.PeerType.(*tg.RequestPeerTypeChat)
		assert.True(t, typ.Creator)
		assert.True(t, typ.BotParticipant)
		forum, ok := typ.GetForum()
		assert.True(t, ok)
		assert.True(t, forum)
		username, ok := typ.GetHasUsername()
		assert.True(t, ok)
		assert.False(t, username)
		userRights, ok := typ.GetUserAdminRights()
		assert.True(t, ok)
		assert.Equal(t, *rights, userRights)
		botRights, ok := typ.GetBotAdminRights()
		assert.True(t, ok)
		assert.Equal(t, *rights, botRights)
	})
	t.Run("channel", func(t *testing.T) {
		req := translateReplyButton(RequestChannel("channel", 6, RequestChannelOptions{IsCreator: true, HasUsername: new(true), UserAdminRights: rights, BotAdminRights: rights})).Type.(*tg.ButtonTypeRequestPeer)
		assert.Equal(t, 6, req.ButtonID)
		typ := req.PeerType.(*tg.RequestPeerTypeBroadcast)
		assert.True(t, typ.Creator)
		username, ok := typ.GetHasUsername()
		assert.True(t, ok)
		assert.True(t, username)
		userRights, ok := typ.GetUserAdminRights()
		assert.True(t, ok)
		assert.Equal(t, *rights, userRights)
		botRights, ok := typ.GetBotAdminRights()
		assert.True(t, ok)
		assert.Equal(t, *rights, botRights)
	})
}
