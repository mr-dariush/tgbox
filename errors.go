package tgbox

import "errors"

var (
	// ErrNilUpdate indicates that the context does not carry a valid update payload.
	ErrNilUpdate = errors.New("tgbox: context update payload is nil")

	// ErrNoPeer indicates that no recognizable target peer could be extracted from the update.
	ErrNoPeer = errors.New("tgbox: update structure does not present any recognizable peer")

	// ErrNoMessageID indicates that the update lacks a message ID required for editing operations.
	ErrNoMessageID = errors.New("tgbox: current update has no associated message ID to edit")

	// ErrNoCallbackQuery indicates that the context lacks an active callback query for the requested operation.
	ErrNoCallbackQuery = errors.New("tgbox: context does not hold an active callback query")

	// ErrNoInlineQuery indicates that the context lacks an active inline query for the requested operation.
	ErrNoInlineQuery = errors.New("tgbox: context does not hold an active inline query")

	// ErrFSMNotConfigured indicates that the conversational FSM engine or storage is missing.
	ErrFSMNotConfigured = errors.New("tgbox: conversational FSM engine/storage is not configured")

	// ErrInvalidFSMContext indicates that FSM operations were called on an update lacking ChatID or SenderID.
	ErrInvalidFSMContext = errors.New("tgbox: FSM operations require a valid chat and user context")

	// ErrNetworkNotConfigured indicates that network-level operations like Suspend/Resume require WithNetworkProfile.
	ErrNetworkNotConfigured = errors.New("tgbox: network profile is not configured")

	// ErrExpirationWatchNotSupported indicates that the underlying FSM storage backend does not support keyspace expiration notifications.
	ErrExpirationWatchNotSupported = errors.New("tgbox: FSM storage does not support reactive expiration watching")
)
