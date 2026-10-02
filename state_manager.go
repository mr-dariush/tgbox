package tgbox

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/mr-dariush/tgbox/internal/fsm"
)

// defaultStateManager manages user conversational states using an underlying fsm.Storage driver.
type defaultStateManager struct {
	storage fsm.Storage
}

var _ StateManager = (*defaultStateManager)(nil)

// NewStateManager constructs a new StateManager with the given persistence storage backend.
func NewStateManager(storage fsm.Storage) StateManager {
	return &defaultStateManager{
		storage: storage,
	}
}

// GetState retrieves the active conversational state for the given chat and user identifiers.
func (s *defaultStateManager) GetState(ctx context.Context, chatID, userID int64) (string, error) {
	if s == nil || s.storage == nil {
		return "", ErrFSMNotConfigured
	}
	if chatID == 0 || userID == 0 {
		return "", ErrInvalidFSMContext
	}

	return s.storage.Get(ctx, fsmKey(chatID, userID))
}

// SetState assigns a conversational state with a TTL for the specified chat and user identifiers.
func (s *defaultStateManager) SetState(ctx context.Context, chatID, userID int64, state string, ttl time.Duration) error {
	if s == nil || s.storage == nil {
		return ErrFSMNotConfigured
	}
	if chatID == 0 || userID == 0 {
		return ErrInvalidFSMContext
	}

	return s.storage.Set(ctx, fsmKey(chatID, userID), state, ttl)
}

// ClearState deletes any active conversational state for the specified chat and user identifiers.
func (s *defaultStateManager) ClearState(ctx context.Context, chatID, userID int64) error {
	if s == nil || s.storage == nil {
		return ErrFSMNotConfigured
	}
	if chatID == 0 || userID == 0 {
		return ErrInvalidFSMContext
	}

	return s.storage.Delete(ctx, fsmKey(chatID, userID))
}

// WatchExpired attaches a reactive expiration listener if the underlying storage implements fsm.ExpirationWatcher.
// It parses expired keys into chatID and userID before invoking the provided handler.
func (s *defaultStateManager) WatchExpired(ctx context.Context, handler func(chatID, userID int64)) error {
	if s == nil || s.storage == nil {
		return ErrFSMNotConfigured
	}

	watcher, ok := s.storage.(fsm.ExpirationWatcher)
	if !ok {
		return ErrExpirationWatchNotSupported
	}

	return watcher.WatchExpired(ctx, func(key string) {
		chatID, userID, err := parseFSMKey(key)
		if err != nil || handler == nil {
			return
		}
		handler(chatID, userID)
	})
}

// parseFSMKey parses a raw FSM key (e.g., "100:200" or "fsm:100:200") into chatID and userID.
func parseFSMKey(raw string) (int64, int64, error) {
	trimmed := strings.TrimPrefix(raw, "fsm:")
	parts := strings.Split(trimmed, ":")
	if len(parts) != 2 {
		return 0, 0, errors.New("tgbox: malformed FSM key format")
	}

	chatID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("tgbox: invalid chat_id in FSM key: %w", err)
	}

	userID, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("tgbox: invalid user_id in FSM key: %w", err)
	}

	return chatID, userID, nil
}
