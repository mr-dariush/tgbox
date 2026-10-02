// Package auth provides a headless, event-driven MTProto authentication state machine.
package auth

import "github.com/gotd/td/telegram/auth"

// EventType categorizes the specific state of the authentication state machine.
type EventType string

const (
	// EventTypeNeedPhone indicates the FSM is waiting for a phone number.
	EventTypeNeedPhone EventType = "NEED_PHONE"

	// EventTypeNeedCode indicates the FSM is waiting for the OTP code.
	EventTypeNeedCode EventType = "NEED_CODE"

	// EventTypeInvalidCode indicates the previously provided code was invalid.
	// The FSM is waiting for the user to try again (up to max retries).
	EventTypeInvalidCode EventType = "INVALID_CODE"

	// EventTypeNeedPassword indicates 2FA is enabled and FSM requires a password.
	EventTypeNeedPassword EventType = "NEED_PASSWORD"

	// EventTypeInvalidPassword indicates the 2FA password was wrong.
	EventTypeInvalidPassword EventType = "INVALID_PASSWORD"

	// EventTypeNeedSignUp indicates the phone is not registered yet. FSM requires user's name.
	EventTypeNeedSignUp EventType = "NEED_SIGN_UP"

	// EventTypeNeedTOS indicates the user must accept Telegram's Terms of Service.
	EventTypeNeedTOS EventType = "NEED_TOS"

	// EventTypeSuccess signals that the MTProto session is now successfully authorized.
	EventTypeSuccess EventType = "AUTH_SUCCESS"

	// EventTypeError signals a fatal authentication error (e.g., banned number, too many retries).
	EventTypeError EventType = "AUTH_ERROR"
)

// Event represents a signal dispatched from the TGBox FSM to the external client (e.g., Web Panel).
type Event struct {
	// SessionID is the UUIDv4 unique identifier for this authentication flow.
	SessionID string `json:"session_id"`

	// Type represents the current FSM state demanding user action.
	Type EventType `json:"type"`

	// Message provides optional context (e.g., specific error description from MTProto).
	Message string `json:"message,omitempty"`

	// CodeLength specifies the expected length of the OTP code, if applicable.
	CodeLength int `json:"code_length,omitempty"`

	// Attempt holds the current retry attempt count (e.g., 1 out of 3).
	Attempt int `json:"attempt,omitempty"`

	// TOS holds the Terms of Service information if Type == EventTypeNeedTOS.
	TOS *TOSInfo `json:"tos,omitempty"`
}

// Action represents the user's input submitted from the external client (e.g., Web Panel) back to the FSM.
type Action struct {
	// SessionID must exactly match the UUIDv4 provided by the Event.
	SessionID string `json:"session_id"`

	// Value represents the raw input string (Phone, Code, or Password).
	Value string `json:"value,omitempty"`

	// SignUp holds user details if answering EventTypeNeedSignUp.
	SignUp *SignUpInfo `json:"sign_up,omitempty"`

	// AcceptTOS specifies whether the user agreed to the Terms of Service.
	AcceptTOS bool `json:"accept_tos,omitempty"`
}

// SignUpInfo contains mandatory fields required to register a brand new Telegram account.
type SignUpInfo struct {
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name,omitempty"`
}

// TOSInfo encapsulates Telegram's Terms of Service requirements.
type TOSInfo struct {
	Text string `json:"text"`
}

// ExtractUserInfo safely transforms our module's SignUpInfo into the structure expected by gotd.
func (s *SignUpInfo) ExtractUserInfo() auth.UserInfo {
	if s == nil {
		return auth.UserInfo{}
	}
	return auth.UserInfo{
		FirstName: s.FirstName,
		LastName:  s.LastName,
	}
}
