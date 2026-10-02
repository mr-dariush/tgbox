package auth_test

import (
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tgboxauth "github.com/mr-dariush/tgbox/auth"
)

// mockInvoker provides a highly customizable MTProto invocation simulation layer.
type mockInvoker struct {
	invokeFunc func(ctx context.Context, input bin.Encoder, output bin.Decoder) error
}

func (m *mockInvoker) Invoke(ctx context.Context, input bin.Encoder, output bin.Decoder) error {
	if m.invokeFunc != nil {
		return m.invokeFunc(ctx, input, output)
	}
	return nil
}

// TestHeadlessFSM_Authenticate exercises the FSM authentication flow with mocked Telegram API responses.
func TestHeadlessFSM_Authenticate(t *testing.T) {
	tests := []struct {
		name           string
		phone          string
		timeout        time.Duration
		preFeedActions []tgboxauth.Action
		mockBehavior   func(t *testing.T, step *int) func(ctx context.Context, input bin.Encoder, output bin.Decoder) error
		expectErr      string
		expectedEvents []tgboxauth.EventType
	}{
		{
			name:    "Success on first try",
			phone:   "+12345678900",
			timeout: 500 * time.Millisecond,
			preFeedActions: []tgboxauth.Action{
				{SessionID: "sess-1", Value: "12345"}, // OTP Code
			},
			mockBehavior: func(t *testing.T, step *int) func(context.Context, bin.Encoder, bin.Decoder) error {
				return func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
					*step++
					switch *step {
					case 1:
						require.IsType(t, &tg.AuthSendCodeRequest{}, input)
						setAuthSentCode(output, "hash-123")
						return nil
					case 2:
						req := input.(*tg.AuthSignInRequest)
						assert.Equal(t, "12345", req.PhoneCode)
						setAuthAuthorization(output, 1)
						return nil
					}
					return nil
				}
			},
			expectedEvents: []tgboxauth.EventType{tgboxauth.EventTypeNeedCode, tgboxauth.EventTypeSuccess},
		},
		{
			name:    "Retry on invalid code then success",
			phone:   "+12345678900",
			timeout: 500 * time.Millisecond,
			preFeedActions: []tgboxauth.Action{
				{SessionID: "sess-2", Value: "wrong"}, // 1st try
				{SessionID: "sess-2", Value: "12345"}, // 2nd try
			},
			mockBehavior: func(t *testing.T, step *int) func(context.Context, bin.Encoder, bin.Decoder) error {
				return func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
					*step++
					switch *step {
					case 1:
						setAuthSentCode(output, "hash-123")
						return nil
					case 2:
						req := input.(*tg.AuthSignInRequest)
						assert.Equal(t, "wrong", req.PhoneCode)
						return tgerr.New(400, "PHONE_CODE_INVALID")
					case 3:
						req := input.(*tg.AuthSignInRequest)
						assert.Equal(t, "12345", req.PhoneCode)
						setAuthAuthorization(output, 1)
						return nil
					}
					return nil
				}
			},
			expectedEvents: []tgboxauth.EventType{tgboxauth.EventTypeNeedCode, tgboxauth.EventTypeInvalidCode, tgboxauth.EventTypeSuccess},
		},
		{
			name:           "Timeout waiting for code",
			phone:          "+12345678900",
			timeout:        50 * time.Millisecond,
			preFeedActions: []tgboxauth.Action{},
			mockBehavior: func(_ *testing.T, _ *int) func(context.Context, bin.Encoder, bin.Decoder) error {
				return func(_ context.Context, _ bin.Encoder, output bin.Decoder) error {
					setAuthSentCode(output, "hash-123")
					return nil
				}
			},
			expectErr:      "timeout exceeded",
			expectedEvents: []tgboxauth.EventType{tgboxauth.EventTypeNeedCode, tgboxauth.EventTypeError},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			eventsCh := make(chan tgboxauth.Event, 10)
			actionsCh := make(chan tgboxauth.Action, 10)

			for _, act := range tc.preFeedActions {
				actionsCh <- act
			}

			invoker := &mockInvoker{
				invokeFunc: tc.mockBehavior(t, new(0)),
			}
			tgClient := tg.NewClient(invoker)
			authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")

			sessID := "default-session"
			if len(tc.preFeedActions) > 0 {
				sessID = tc.preFeedActions[0].SessionID
			}

			fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh, tgboxauth.WithTimeout(tc.timeout))

			err := fsm.Authenticate(context.Background(), authClient, tc.phone)
			if tc.expectErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectErr)
			} else {
				require.NoError(t, err)
			}

			close(eventsCh)
			var capturedEvents []tgboxauth.EventType
			for ev := range eventsCh {
				capturedEvents = append(capturedEvents, ev.Type)
			}

			assert.Equal(t, tc.expectedEvents, capturedEvents)
		})
	}
}

func setAuthSentCode(decoder bin.Decoder, hash string) {
	switch out := decoder.(type) {
	case *tg.AuthSentCode:
		*out = tg.AuthSentCode{PhoneCodeHash: hash}
	case *tg.AuthSentCodeBox:
		out.SentCode = &tg.AuthSentCode{PhoneCodeHash: hash}
	}
}

func setAuthAuthorization(decoder bin.Decoder, userID int64) {
	switch out := decoder.(type) {
	case *tg.AuthAuthorization:
		*out = tg.AuthAuthorization{User: &tg.User{ID: userID}}
	case *tg.AuthAuthorizationBox:
		out.Authorization = &tg.AuthAuthorization{User: &tg.User{ID: userID}}
	}
}
