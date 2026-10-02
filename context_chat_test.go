package tgbox

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/keyboard"
)

func TestChatServiceMessages(t *testing.T) {
	peer := &tg.InputPeerUser{UserID: 42, AccessHash: 99}
	rich := &tg.InputRichMessageMarkdown{Markdown: "hello"}
	for _, tc := range []struct {
		name                      string
		call                      func(MessageService, ...SendOption) (tg.UpdatesClass, error)
		reply, edit, styled, rich bool
	}{
		{name: "text", call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.SendText(t.Context(), peer, "hello", o...)
		}},
		{name: "html", styled: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.SendHTML(t.Context(), peer, "<b>hello</b>", o...)
		}},
		{name: "markdown", styled: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.SendMarkdown(t.Context(), peer, "**hello**", o...)
		}},
		{name: "rich markdown", rich: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.SendRichMarkdown(t.Context(), peer, "hello", o...)
		}},
		{name: "rich", rich: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.SendRich(t.Context(), peer, rich, o...)
		}},
		{name: "reply text", reply: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.ReplyText(t.Context(), peer, 17, "hello", o...)
		}},
		{name: "reply html", reply: true, styled: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.ReplyHTML(t.Context(), peer, 17, "<b>hello</b>", o...)
		}},
		{name: "reply markdown", reply: true, styled: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.ReplyMarkdown(t.Context(), peer, 17, "**hello**", o...)
		}},
		{name: "reply rich markdown", reply: true, rich: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.ReplyRichMarkdown(t.Context(), peer, 17, "hello", o...)
		}},
		{name: "reply rich", reply: true, rich: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.ReplyRich(t.Context(), peer, 17, rich, o...)
		}},
		{name: "edit text", edit: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.EditActive(t.Context(), peer, 17, "hello", o...)
		}},
		{name: "edit rich", edit: true, rich: true, call: func(s MessageService, o ...SendOption) (tg.UpdatesClass, error) {
			return s.EditRich(t.Context(), peer, 17, rich, o...)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var requests []bin.Encoder
			rpcErr := errors.New("RPC failed")
			var failure error
			updates := &tg.Updates{Date: 123}
			api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				requests = append(requests, input)
				if failure != nil {
					return failure
				}
				out, ok := output.(*tg.UpdatesBox)
				require.True(t, ok)
				out.Updates = updates
				return nil
			}))
			service := NewChatService(message.NewSender(api), api, nil)
			markup := keyboard.NewInlineKeyboard(keyboard.Row(keyboard.Callback("OK", "ok")))
			got, err := tc.call(service, WithMarkup(nil), WithMarkup(markup))
			require.NoError(t, err)
			assert.Same(t, updates, got)
			require.Len(t, requests, 1)
			var text string
			var entities []tg.MessageEntityClass
			var richMessage tg.InputRichMessageClass
			if tc.edit {
				req, ok := requests[0].(*tg.MessagesEditMessageRequest)
				require.True(t, ok)
				assert.Equal(t, peer, req.Peer)
				assert.Equal(t, 17, req.ID)
				assert.IsType(t, &tg.ReplyInlineMarkup{}, req.ReplyMarkup)
				text, entities, richMessage = req.Message, req.Entities, req.RichMessage
			} else {
				req, ok := requests[0].(*tg.MessagesSendMessageRequest)
				require.True(t, ok)
				assert.Equal(t, peer, req.Peer)
				assert.IsType(t, &tg.ReplyInlineMarkup{}, req.ReplyMarkup)
				if tc.reply {
					reply, ok := req.ReplyTo.(*tg.InputReplyToMessage)
					require.True(t, ok)
					assert.Equal(t, 17, reply.ReplyToMsgID)
				} else {
					assert.Nil(t, req.ReplyTo)
				}
				text, entities, richMessage = req.Message, req.Entities, req.RichMessage
			}
			if tc.rich {
				assert.Equal(t, rich, richMessage)
			} else {
				assert.Equal(t, "hello", text)
			}
			if tc.styled {
				require.Len(t, entities, 1)
				assert.Equal(t, &tg.MessageEntityBold{Length: 5}, entities[0])
			}
			failure = rpcErr
			got, err = tc.call(service)
			require.ErrorIs(t, err, rpcErr)
			assert.Nil(t, got)
			require.Len(t, requests, 2)
			optionErr := errors.New("option failed")
			got, err = tc.call(service, func(_ *message.Builder) error { return optionErr }, func(_ *message.Builder) error { t.Fatal("options did not stop on error"); return nil })
			require.ErrorIs(t, err, optionErr)
			assert.Nil(t, got)
			assert.Len(t, requests, 2, "invalid options must not send RPCs")
		})
	}
}

func TestChatServiceMarkupAndCallback(t *testing.T) {
	var request bin.Encoder
	var failure error
	api := tg.NewClient(telegram.InvokeFunc(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		request = in
		if failure != nil {
			return failure
		}
		if box, ok := out.(*tg.UpdatesBox); ok {
			box.Updates = &tg.Updates{}
		}
		return nil
	}))
	service := NewChatService(message.NewSender(api), api, nil)
	peer := &tg.InputPeerSelf{}
	_, err := service.EditReplyMarkup(t.Context(), peer, 7, keyboard.NewRemoveKeyboard(true))
	require.NoError(t, err)
	req, ok := request.(*tg.MessagesEditMessageRequest)
	require.True(t, ok)
	assert.Equal(t, 7, req.ID)
	assert.Equal(t, peer, req.Peer)
	assert.Equal(t, &tg.ReplyKeyboardHide{Selective: true}, req.ReplyMarkup)
	_, err = service.EditReplyMarkup(t.Context(), peer, 7, nil)
	require.NoError(t, err)
	req = request.(*tg.MessagesEditMessageRequest)
	assert.Nil(t, req.ReplyMarkup)
	_, err = service.EditReplyMarkup(t.Context(), peer, 7, keyboard.NewInlineKeyboard(keyboard.Row(keyboard.InlineButton{Text: "invalid"})))
	require.ErrorContains(t, err, "translate reply markup")
	b := message.NewSender(api).To(peer)
	require.Error(t, WithMarkup(keyboard.NewInlineKeyboard(keyboard.Row(keyboard.InlineButton{})))(&b.Builder))
	for _, text := range []string{"", "notice"} {
		require.NoError(t, service.AnswerCallback(t.Context(), 123, true, text))
		callback, ok := request.(*tg.MessagesSetBotCallbackAnswerRequest)
		require.True(t, ok)
		assert.Equal(t, int64(123), callback.QueryID)
		assert.True(t, callback.Alert)
		assert.Equal(t, text, callback.Message)
	}
	failure = errors.New("RPC failure")
	require.ErrorIs(t, service.AnswerCallback(t.Context(), 1, false, ""), failure)
	_, err = service.EditReplyMarkup(t.Context(), peer, 7, nil)
	require.ErrorIs(t, err, failure)
}
