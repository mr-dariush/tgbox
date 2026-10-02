package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const (
	defaultTimeout    = 3 * time.Minute
	defaultMaxRetries = 3
)

// FSMOption is a functional configuration option for HeadlessFSM.
type FSMOption func(*HeadlessFSM)

// WithTimeout overrides the default deadline for each interactive input request.
func WithTimeout(d time.Duration) FSMOption {
	return func(f *HeadlessFSM) { f.timeout = d }
}

// WithMaxRetries overrides the default maximum number of OTP/password retry attempts.
func WithMaxRetries(r int) FSMOption {
	return func(f *HeadlessFSM) { f.maxRetries = r }
}

// HeadlessFSM coordinates the MTProto authentication lifecycle without any CLI interactions.
// It communicates with external systems (like Web Panels) via strictly typed channels.
type HeadlessFSM struct {
	sessionID  string
	events     chan<- Event
	actions    <-chan Action
	timeout    time.Duration
	maxRetries int
}

// NewHeadlessFSM constructs a new headless state machine for a specific authentication session.
// Use WithTimeout and WithMaxRetries to override defaults.
func NewHeadlessFSM(sessionID string, events chan<- Event, actions <-chan Action, opts ...FSMOption) *HeadlessFSM {
	fsm := &HeadlessFSM{
		sessionID:  sessionID,
		events:     events,
		actions:    actions,
		timeout:    defaultTimeout,
		maxRetries: defaultMaxRetries,
	}
	for _, opt := range opts {
		opt(fsm)
	}
	return fsm
}

// emit sends an event to the external watcher. It blocks until the watcher
// receives the event or ctx is canceled: silently dropping events like
// NEED_CODE would leave the watcher waiting forever.
func (f *HeadlessFSM) emit(ctx context.Context, ev Event) {
	ev.SessionID = f.sessionID
	select {
	case f.events <- ev:
	case <-ctx.Done():
	}
}

// askWithTimeout dispatches an event and blocks until the external client submits a matching action,
// enforcing a strict deadline to prevent memory leaks and suspended goroutines.
func (f *HeadlessFSM) askWithTimeout(ctx context.Context, ev Event) (Action, error) {
	askCtx, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()

	f.emit(askCtx, ev)

	for {
		select {
		case <-askCtx.Done():
			return Action{}, ErrInputTimeout
		case act := <-f.actions:
			// Discard orphaned or mismatched responses
			if act.SessionID != f.sessionID {
				continue
			}
			return act, nil
		}
	}
}

// Authenticate drives the precise authentication flow, managing MTProto calls directly to allow local retry loops.
func (f *HeadlessFSM) Authenticate(ctx context.Context, client *auth.Client, phone string) (err error) {
	defer func() {
		// Report terminal errors to the external watcher. If the flow failed
		// because ctx itself was canceled, fall back to a short detached
		// context so the error event still gets delivered.
		if err != nil {
			emitCtx := ctx
			if ctx.Err() != nil {
				var cancel context.CancelFunc
				emitCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
			}
			f.emit(emitCtx, Event{
				Type:    EventTypeError,
				Message: err.Error(),
			})
		}
	}()

	// 1. Resolve phone number if not statically provided
	if phone == "" {
		act, askErr := f.askWithTimeout(ctx, Event{Type: EventTypeNeedPhone})
		if askErr != nil {
			return askErr
		}
		phone = act.Value
	}

	// 2. Transmit the SMS code request to Telegram servers
	sentCode, sendErr := client.SendCode(ctx, phone, auth.SendCodeOptions{})
	if sendErr != nil {
		return errWrap(sendErr, "send code failed")
	}

	var codeLen int
	var codeHash string

	switch sc := sentCode.(type) {
	case *tg.AuthSentCode:
		codeHash = sc.PhoneCodeHash
		if typed, ok := sc.Type.(interface{ GetLength() int }); ok {
			codeLen = typed.GetLength()
		}
	case *tg.AuthSentCodeSuccess:
		// Rare scenario: Session was already fully authorized (No OTP required)
		if _, ok := sc.Authorization.(*tg.AuthAuthorization); ok {
			f.emit(ctx, Event{Type: EventTypeSuccess})
			return nil
		}
	}

	// 3. Process the OTP validation phase with isolated retries
	var signInErr error
	for attempt := 1; attempt <= f.maxRetries; attempt++ {
		eventType := EventTypeNeedCode
		if attempt > 1 {
			eventType = EventTypeInvalidCode
		}

		act, askErr := f.askWithTimeout(ctx, Event{
			Type:       eventType,
			Attempt:    attempt,
			CodeLength: codeLen,
		})
		if askErr != nil {
			return askErr
		}

		_, signInErr = client.SignIn(ctx, phone, act.Value, codeHash)
		if signInErr == nil {
			break // Code was correct
		}

		// Progress to Next Lifecycle Stage (2FA or Registration)
		if errors.Is(signInErr, auth.ErrPasswordAuthNeeded) || tgerr.Is(signInErr, "SESSION_PASSWORD_NEEDED") {
			break
		}
		if _, ok := errors.AsType[*auth.SignUpRequired](signInErr); ok {
			break
		}

		// Safe Retry on Wrong Code
		if tgerr.Is(signInErr, "PHONE_CODE_INVALID") {
			continue
		}

		return errWrap(signInErr, "sign in failed")
	}

	if tgerr.Is(signInErr, "PHONE_CODE_INVALID") {
		return ErrMaxRetriesPhoneCode
	}

	// 4. Two-Factor Authentication (2FA) Lifecycle
	if errors.Is(signInErr, auth.ErrPasswordAuthNeeded) || tgerr.Is(signInErr, "SESSION_PASSWORD_NEEDED") {
		var pwErr error
		for attempt := 1; attempt <= f.maxRetries; attempt++ {
			eventType := EventTypeNeedPassword
			if attempt > 1 {
				eventType = EventTypeInvalidPassword
			}

			act, askErr := f.askWithTimeout(ctx, Event{Type: eventType, Attempt: attempt})
			if askErr != nil {
				return askErr
			}

			_, pwErr = client.Password(ctx, act.Value)
			if pwErr == nil {
				break // 2FA succeeded
			}

			if errors.Is(pwErr, auth.ErrPasswordInvalid) || tgerr.Is(pwErr, "PASSWORD_HASH_INVALID") {
				continue // Retry password
			}

			return errWrap(pwErr, "password verification failed")
		}

		if errors.Is(pwErr, auth.ErrPasswordInvalid) || tgerr.Is(pwErr, "PASSWORD_HASH_INVALID") {
			return ErrMaxRetriesPassword
		}
		signInErr = nil // Clear error as password validation successfully bypassed it
	}

	// 5. New Account Registration (SignUp) Lifecycle
	if signUpErr, ok := errors.AsType[*auth.SignUpRequired](signInErr); ok {
		// Evaluate if Telegram mandates explicit Terms of Service acceptance
		if signUpErr.TermsOfService.Text != "" {
			act, askErr := f.askWithTimeout(ctx, Event{
				Type: EventTypeNeedTOS,
				TOS:  &TOSInfo{Text: signUpErr.TermsOfService.Text},
			})
			if askErr != nil {
				return askErr
			}
			if !act.AcceptTOS {
				return ErrTOSDeclined
			}
			if err := client.AcceptTOS(ctx, signUpErr.TermsOfService.ID); err != nil {
				return errWrap(err, "accept terms of service failed")
			}
		}

		// Retrieve Personal Information required for Account Generation
		act, askErr := f.askWithTimeout(ctx, Event{Type: EventTypeNeedSignUp})
		if askErr != nil {
			return askErr
		}
		if act.SignUp == nil || act.SignUp.FirstName == "" {
			return ErrSignUpInfoMissing
		}

		_, err := client.SignUp(ctx, auth.SignUp{
			PhoneNumber:   phone,
			PhoneCodeHash: codeHash,
			FirstName:     act.SignUp.FirstName,
			LastName:      act.SignUp.LastName,
		})
		if err != nil {
			return errWrap(err, "sign up failed")
		}
	}

	f.emit(ctx, Event{Type: EventTypeSuccess})
	return nil
}

// errWrap prefixes and wraps an error, preserving it for [errors.Is]/[errors.As].
func errWrap(err error, msg string) error {
	return fmt.Errorf("tgbox/auth: %s: %w", msg, err)
}
