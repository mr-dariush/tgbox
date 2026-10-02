package tgbox

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminServiceRequests(t *testing.T) {
	channel := &tg.InputChannel{ChannelID: 1, AccessHash: 2}
	user := &tg.InputUser{UserID: 3, AccessHash: 4}
	peer := &tg.InputPeerUser{UserID: 3, AccessHash: 4}
	var request bin.Encoder
	var failure error
	api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		request = in
		if failure != nil {
			return failure
		}
		out.(*tg.UpdatesBox).Updates = &tg.Updates{}
		return nil
	}))
	service := NewAdminService(api)
	require.NoError(t, service.Promote(t.Context(), channel, user))
	promote, ok := request.(*tg.ChannelsEditAdminRequest)
	require.True(t, ok)
	assert.Equal(t, channel, promote.Channel)
	assert.Equal(t, user, promote.UserID)
	assert.Equal(t, tg.ChatAdminRights{AddAdmins: true, PostMessages: true, EditMessages: true, DeleteMessages: true, BanUsers: true, PinMessages: true, InviteUsers: true}, promote.AdminRights)
	require.NoError(t, service.Ban(t.Context(), channel, peer))
	ban, ok := request.(*tg.ChannelsEditBannedRequest)
	require.True(t, ok)
	assert.Equal(t, channel, ban.Channel)
	assert.Equal(t, peer, ban.Participant)
	assert.Equal(t, tg.ChatBannedRights{ViewMessages: true, EmbedLinks: true, SendMessages: true, SendMedia: true, SendStickers: true, SendGifs: true, SendGames: true, SendInline: true}, ban.BannedRights)
	failure = errors.New("permission denied")
	require.ErrorIs(t, service.Promote(t.Context(), channel, user), failure)
	require.ErrorIs(t, service.Ban(t.Context(), channel, peer), failure)
}
