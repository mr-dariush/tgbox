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

func setAuthSentCodeSuccess(decoder bin.Decoder, userID int64) {
	if out, ok := decoder.(*tg.AuthSentCodeBox); ok {
		out.SentCode = &tg.AuthSentCodeSuccess{
			Authorization: &tg.AuthAuthorization{
				User: &tg.User{ID: userID},
			},
		}
	}
}

func setAuthSignUpRequired(decoder bin.Decoder, tosText string) {
	if out, ok := decoder.(*tg.AuthAuthorizationBox); ok {
		out.Authorization = &tg.AuthAuthorizationSignUpRequired{
			TermsOfService: tg.HelpTermsOfService{
				ID:   tg.DataJSON{Data: "tos_data"},
				Text: tosText,
			},
		}
	}
}

func TestHeadlessFSM_2FA_Password_Lifecycle(t *testing.T) {
	t.Run("Password retry then fail on max retries", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-pwd"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "12345"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "wrong-pass-1"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "wrong-pass-2"}

		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch input.(type) {
				case *tg.AuthSendCodeRequest:
					setAuthSentCode(output, "hash-123")
					return nil
				case *tg.AuthSignInRequest:
					return tgerr.New(400, "SESSION_PASSWORD_NEEDED")
				default:
					return tgerr.New(400, "PASSWORD_HASH_INVALID")
				}
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
			tgboxauth.WithMaxRetries(2),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.ErrorIs(t, err, tgboxauth.ErrMaxRetriesPassword)
	})

	t.Run("Password fatal failure", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-fatal-pwd"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "12345"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "some-pass"}

		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch input.(type) {
				case *tg.AuthSendCodeRequest:
					setAuthSentCode(output, "hash-123")
					return nil
				case *tg.AuthSignInRequest:
					return tgerr.New(400, "SESSION_PASSWORD_NEEDED")
				default:
					return tgerr.New(500, "INTERNAL_RPC_ERROR")
				}
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
			tgboxauth.WithMaxRetries(2),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "password verification failed")
	})
}

func TestHeadlessFSM_EmptyPhone_And_CodeSuccess(t *testing.T) {
	t.Run("Empty phone asks and resolves", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-empty-phone"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "+1987654321"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "54321"}

		var sentPhone string
		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch req := input.(type) {
				case *tg.AuthSendCodeRequest:
					sentPhone = req.PhoneNumber
					setAuthSentCode(output, "hash-empty")
					return nil
				case *tg.AuthSignInRequest:
					setAuthAuthorization(output, 555)
					return nil
				}
				return nil
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
		)

		err := fsm.Authenticate(context.Background(), authClient, "")
		require.NoError(t, err)
		assert.Equal(t, "+1987654321", sentPhone)
	})

	t.Run("AuthSentCodeSuccess bypasses OTP", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-code-success"
		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				if _, ok := input.(*tg.AuthSendCodeRequest); ok {
					setAuthSentCodeSuccess(output, 777)
					return nil
				}
				return nil
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.NoError(t, err)

		close(eventsCh)
		var eventTypes []tgboxauth.EventType
		for ev := range eventsCh {
			eventTypes = append(eventTypes, ev.Type)
		}
		assert.Contains(t, eventTypes, tgboxauth.EventTypeSuccess)
	})
}

func TestHeadlessFSM_SignUp_And_TOS_Lifecycle(t *testing.T) {
	t.Run("Decline TOS aborts flow", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-tos-decline"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "12345"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, AcceptTOS: false}

		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch input.(type) {
				case *tg.AuthSendCodeRequest:
					setAuthSentCode(output, "hash-tos")
					return nil
				case *tg.AuthSignInRequest:
					setAuthSignUpRequired(output, "Mandatory Terms")
					return nil
				}
				return nil
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.ErrorIs(t, err, tgboxauth.ErrTOSDeclined)
	})

	t.Run("Missing sign up first name", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-missing-name"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "12345"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, AcceptTOS: true}
		actionsCh <- tgboxauth.Action{SessionID: sessID, SignUp: &tgboxauth.SignUpInfo{FirstName: ""}}

		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch input.(type) {
				case *tg.AuthSendCodeRequest:
					setAuthSentCode(output, "hash-signup")
					return nil
				case *tg.AuthSignInRequest:
					setAuthSignUpRequired(output, "Terms")
					return nil
				case *tg.HelpAcceptTermsOfServiceRequest:
					return nil
				}
				return nil
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.ErrorIs(t, err, tgboxauth.ErrSignUpInfoMissing)
	})

	t.Run("Accept TOS and register new user", func(t *testing.T) {
		eventsCh := make(chan tgboxauth.Event, 10)
		actionsCh := make(chan tgboxauth.Action, 10)

		sessID := "sess-signup-success"
		actionsCh <- tgboxauth.Action{SessionID: sessID, Value: "12345"}
		actionsCh <- tgboxauth.Action{SessionID: sessID, AcceptTOS: true}
		actionsCh <- tgboxauth.Action{SessionID: sessID, SignUp: &tgboxauth.SignUpInfo{FirstName: "John", LastName: "Doe"}}

		var acceptedTOSID int
		var registeredFirst string
		invoker := &mockInvoker{
			invokeFunc: func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
				switch req := input.(type) {
				case *tg.AuthSendCodeRequest:
					setAuthSentCode(output, "hash-reg")
					return nil
				case *tg.AuthSignInRequest:
					setAuthSignUpRequired(output, "Please agree to Terms")
					return nil
				case *tg.HelpAcceptTermsOfServiceRequest:
					acceptedTOSID = 1
					return nil
				case *tg.AuthSignUpRequest:
					registeredFirst = req.FirstName
					setAuthAuthorization(output, 888)
					return nil
				}
				return nil
			},
		}

		tgClient := tg.NewClient(invoker)
		authClient := auth.NewClient(tgClient, rand.Reader, 12345, "mock_hash")
		fsm := tgboxauth.NewHeadlessFSM(sessID, eventsCh, actionsCh,
			tgboxauth.WithTimeout(200*time.Millisecond),
		)

		err := fsm.Authenticate(context.Background(), authClient, "+1234567890")
		require.NoError(t, err)
		assert.Equal(t, 1, acceptedTOSID)
		assert.Equal(t, "John", registeredFirst)
	})
}

func TestSignUpInfo_ExtractUserInfo(t *testing.T) {
	t.Parallel()

	var nilInfo *tgboxauth.SignUpInfo
	assert.Empty(t, nilInfo.ExtractUserInfo().FirstName)

	info := &tgboxauth.SignUpInfo{
		FirstName: "Alice",
		LastName:  "Smith",
	}
	extracted := info.ExtractUserInfo()
	assert.Equal(t, "Alice", extracted.FirstName)
	assert.Equal(t, "Smith", extracted.LastName)
}
