package interceptor_test

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox/internal/interceptor"
)

func TestOutgoingInterceptor(t *testing.T) {
	for _, tc := range []struct {
		name     string
		request  bin.Encoder
		output   bin.Decoder
		dispatch bool
	}{
		{"text", &tg.MessagesSendMessageRequest{}, &tg.Updates{}, true},
		{"media", &tg.MessagesSendMediaRequest{}, &tg.Updates{}, true},
		{"album", &tg.MessagesSendMultiMediaRequest{}, &tg.Updates{}, true},
		{"forward", &tg.MessagesForwardMessagesRequest{}, &tg.Updates{}, true},
		{"unrelated", &tg.UsersGetUsersRequest{}, &tg.Updates{}, false},
		{"non-update result", &tg.MessagesSendMessageRequest{}, &tg.User{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls, dispatches, extractions int
			var failure error
			next := telegram.InvokeFunc(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
				calls++
				assert.Same(t, tc.request, in)
				assert.Same(t, tc.output, out)
				return failure
			})
			invoker := interceptor.NewOutgoingInterceptor(next, func(ctx context.Context, updates tg.UpdatesClass) error {
				dispatches++
				assert.True(t, interceptor.IsOutgoing(ctx))
				assert.Same(t, tc.output, updates)
				return errors.New("dispatch errors do not fail successful RPCs")
			}, func(_ context.Context) (string, int64) { extractions++; return "trace", 42 })
			assert.False(t, interceptor.IsOutgoing(t.Context()))
			require.NoError(t, invoker.Invoke(t.Context(), tc.request, tc.output))
			want := 0
			if tc.dispatch {
				want = 1
			}
			assert.Equal(t, want, dispatches)
			failure = errors.New("RPC failed")
			require.ErrorIs(t, invoker.Invoke(t.Context(), tc.request, tc.output), failure)
			assert.Equal(t, want, dispatches, "failed RPC must not dispatch")
			assert.Equal(t, 2, calls)
			assert.Equal(t, 2, extractions)
		})
	}
}
