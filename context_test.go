package tgbox

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextCancellationAndValues(t *testing.T) {
	var empty *Context
	assert.Empty(t, empty.Param("id"))
	assert.Empty(t, empty.TraceID())
	assert.Zero(t, empty.ClientID())
	assert.Nil(t, empty.Done())
	require.NoError(t, empty.Err())
	_, ok := empty.Deadline()
	assert.False(t, ok)
	deadline := time.Now().Add(time.Minute)
	base, cancel := context.WithDeadline(WithClientID(WithTraceID(t.Context(), "trace"), 42), deadline)
	defer cancel()
	ctx := &Context{StdContext: base, Params: map[string]string{"id": "7"}}
	assert.Equal(t, "7", ctx.Param("id"))
	assert.Empty(t, ctx.Param("unknown"))
	assert.Equal(t, "trace", ctx.TraceID())
	assert.Equal(t, int64(42), ctx.ClientID())
	assert.Equal(t, "trace", ctx.Value(traceIDContextKey{}))
	got, ok := ctx.Deadline()
	assert.True(t, ok)
	assert.Equal(t, deadline, got)
	cancel()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cancellation not propagated")
	}
}

func TestContextPeerResolution(t *testing.T) {
	entities := &tg.Entities{Users: map[int64]*tg.User{7: {ID: 7, AccessHash: 70}}, Chats: map[int64]*tg.Chat{8: {ID: 8}}, Channels: map[int64]*tg.Channel{9: {ID: 9, AccessHash: 90}}}
	for _, tc := range []struct {
		name   string
		update *Update
		want   tg.InputPeerClass
	}{
		{"message", &Update{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 7}}}, &tg.InputPeerUser{UserID: 7, AccessHash: 70}},
		{"callback", &Update{CallbackQuery: &tg.UpdateBotCallbackQuery{Peer: &tg.PeerChat{ChatID: 8}}}, &tg.InputPeerChat{ChatID: 8}},
		{"raw message", &Update{Raw: &tg.UpdateNewMessage{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 7}}}}, &tg.InputPeerUser{UserID: 7, AccessHash: 70}},
		{"raw channel", &Update{Raw: &tg.UpdateNewChannelMessage{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 9}}}}, &tg.InputPeerChannel{ChannelID: 9, AccessHash: 90}},
		{"raw callback", &Update{Raw: &tg.UpdateBotCallbackQuery{Peer: &tg.PeerUser{UserID: 7}}}, &tg.InputPeerUser{UserID: 7, AccessHash: 70}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := &Context{Update: tc.update, Entities: entities}
			got, err := ctx.CurrentPeer()
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
			promised, err := ctx.PeerPromise()(t.Context())
			require.NoError(t, err)
			assert.Equal(t, got, promised)
		})
	}
	var missing *Context
	_, err := missing.CurrentPeer()
	require.ErrorIs(t, err, ErrNilUpdate)
	_, err = (&Context{}).CurrentPeer()
	require.ErrorIs(t, err, ErrNilUpdate)
	_, err = (&Context{Update: &Update{}}).CurrentPeer()
	require.ErrorIs(t, err, ErrNoPeer)
	for _, upd := range []*Update{
		{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 99}}},
		{CallbackQuery: &tg.UpdateBotCallbackQuery{Peer: &tg.PeerChannel{ChannelID: 99}}},
		{Raw: &tg.UpdateNewMessage{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 99}}}},
		{Raw: &tg.UpdateNewChannelMessage{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 99}}}},
		{Raw: &tg.UpdateBotCallbackQuery{Peer: &tg.PeerChannel{ChannelID: 99}}},
	} {
		_, err = (&Context{Update: upd}).CurrentPeer()
		require.Error(t, err)
	}
	user, err := (&Context{Entities: entities}).ResolveUser(7)
	require.NoError(t, err)
	assert.Equal(t, &tg.InputUser{UserID: 7, AccessHash: 70}, user)
	user, err = missing.ResolveUser(10)
	require.NoError(t, err)
	assert.Equal(t, &tg.InputUser{UserID: 10}, user)
}

func TestActiveChannelAndSender(t *testing.T) {
	ctx := &Context{Update: &Update{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 9}, FromID: &tg.PeerUser{UserID: 7}}}, Entities: &tg.Entities{Channels: map[int64]*tg.Channel{9: {ID: 9, AccessHash: 90}}, Users: map[int64]*tg.User{7: {ID: 7, AccessHash: 70}}}}
	channel, user, err := ctx.ActiveChannelAndSender()
	require.NoError(t, err)
	assert.Equal(t, &tg.InputChannel{ChannelID: 9, AccessHash: 90}, channel)
	assert.Equal(t, &tg.InputUser{UserID: 7, AccessHash: 70}, user)
	ctx.Update.Message.FromID = nil
	_, _, err = ctx.ActiveChannelAndSender()
	require.ErrorContains(t, err, "no identifiable sender")
	ctx.Entities.Chats = map[int64]*tg.Chat{8: {ID: 8}}
	ctx.Update.Message.PeerID = &tg.PeerChat{ChatID: 8}
	_, _, err = ctx.ActiveChannelAndSender()
	require.ErrorContains(t, err, "not a channel")
	ctx.Update = &Update{}
	_, _, err = ctx.ActiveChannelAndSender()
	require.ErrorIs(t, err, ErrNoPeer)
	ctx.Update = nil
	_, _, err = ctx.ActiveChannelAndSender()
	require.ErrorIs(t, err, ErrNilUpdate)
}
