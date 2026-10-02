package tgbox

import (
	"testing"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
)

func TestUpdateIdentity(t *testing.T) {
	for _, tc := range []struct {
		name             string
		update           Update
		chatID, senderID int64
	}{
		{name: "empty"},
		{name: "private incoming", update: Update{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 7}}}, chatID: 7, senderID: 7},
		{name: "private outgoing", update: Update{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 7}, Out: true}}, chatID: 7},
		{name: "group sender", update: Update{Message: &tg.Message{PeerID: &tg.PeerChat{ChatID: 8}, FromID: &tg.PeerUser{UserID: 7}}}, chatID: 8, senderID: 7},
		{name: "channel sender", update: Update{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 9}, FromID: &tg.PeerChannel{ChannelID: 9}}}, chatID: 9},
		{name: "callback", update: Update{CallbackQuery: &tg.UpdateBotCallbackQuery{Peer: &tg.PeerChat{ChatID: 8}, UserID: 7}}, chatID: 8, senderID: 7},
		{name: "inline", update: Update{InlineQuery: &tg.UpdateBotInlineQuery{UserID: 7}}, senderID: 7},
		{name: "chat member", update: Update{ChatMember: &tg.UpdateChatParticipant{ChatID: 8, UserID: 7}}, chatID: 8, senderID: 7},
		{name: "channel member", update: Update{ChannelMember: &tg.UpdateChannelParticipant{ChannelID: 9, UserID: 7}}, chatID: 9, senderID: 7},
		{name: "join request", update: Update{JoinRequest: &tg.UpdatePendingJoinRequests{Peer: &tg.PeerChannel{ChannelID: 9}}}, chatID: 9},
		{name: "missing peer", update: Update{Message: &tg.Message{}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.chatID, tc.update.ChatID())
			assert.Equal(t, tc.senderID, tc.update.SenderID())
			if tc.update.Message != nil {
				assert.Same(t, tc.update.Message, tc.update.GetMessage())
			} else {
				assert.Nil(t, tc.update.GetMessage())
			}
		})
	}
}
