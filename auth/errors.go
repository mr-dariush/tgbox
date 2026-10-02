package auth

import "errors"

var (
	// ErrInputTimeout indicates that the user failed to provide the required input within the specified deadline.
	ErrInputTimeout = errors.New("auth: headless input timeout exceeded or context canceled")

	// ErrMaxRetriesPhoneCode indicates that the flow was aborted after too many invalid OTP attempts.
	ErrMaxRetriesPhoneCode = errors.New("auth: max retries exceeded for invalid phone code")

	// ErrMaxRetriesPassword indicates that the flow was aborted after too many invalid 2FA password attempts.
	ErrMaxRetriesPassword = errors.New("auth: max retries exceeded for invalid password")

	// ErrTOSDeclined indicates that the Telegram Terms of Service were rejected by the user.
	ErrTOSDeclined = errors.New("auth: user declined terms of service")

	// ErrSignUpInfoMissing indicates that the mandatory registration parameters (like first_name) were omitted.
	ErrSignUpInfoMissing = errors.New("auth: mandatory sign-up info (first_name) not provided")
)
