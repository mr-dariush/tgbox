package tgbox

import (
	"context"
	"time"

	"github.com/gotd/td/tg"

	"github.com/mr-dariush/tgbox/keyboard"
)

// MessageService defines the segregated contract for sending, replying to, and modifying messages.
// It decouples message delivery mechanics from the request-scoped Context.
type MessageService interface {
	SendText(ctx context.Context, peer tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error)
	SendHTML(ctx context.Context, peer tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error)
	SendMarkdown(ctx context.Context, peer tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error)
	SendRichMarkdown(ctx context.Context, peer tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error)
	SendRich(ctx context.Context, peer tg.InputPeerClass, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error)
	ReplyText(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error)
	ReplyHTML(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error)
	ReplyMarkdown(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error)
	ReplyRichMarkdown(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error)
	ReplyRich(ctx context.Context, peer tg.InputPeerClass, msgID int, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error)
	EditActive(ctx context.Context, peer tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error)
	EditRich(ctx context.Context, peer tg.InputPeerClass, msgID int, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error)
	EditReplyMarkup(ctx context.Context, peer tg.InputPeerClass, msgID int, markup keyboard.ReplyMarkup) (tg.UpdatesClass, error)
	AnswerCallback(ctx context.Context, queryID int64, alert bool, text string) error
}

// AdminService defines administrative operations for supergroups and channels.
type AdminService interface {
	Promote(ctx context.Context, channel tg.InputChannelClass, user tg.InputUserClass) error
	Ban(ctx context.Context, channel tg.InputChannelClass, participant tg.InputPeerClass) error
}

// StateManager manages conversational finite-state machine transitions independently of the request context.
type StateManager interface {
	GetState(ctx context.Context, chatID, userID int64) (string, error)
	SetState(ctx context.Context, chatID, userID int64, state string, ttl time.Duration) error
	ClearState(ctx context.Context, chatID, userID int64) error
	WatchExpired(ctx context.Context, handler func(chatID, userID int64)) error
}
