package tgbox

import (
	"github.com/gotd/td/tg"
)

// UpdateKind defines the normalized category of the incoming Telegram update.
type UpdateKind int

const (
	// KindRaw represents an un-normalized raw update.
	KindRaw UpdateKind = iota
	// KindMessage represents a new incoming message update.
	KindMessage
	// KindEditedMessage represents an edited message update.
	KindEditedMessage
	// KindDeletedMessages represents a message deletion event.
	KindDeletedMessages
	// KindCallbackQuery represents an inline button click event.
	KindCallbackQuery
	// KindInlineQuery represents an inline search query.
	KindInlineQuery
	// KindChatMemberUpdated represents a chat member status change.
	KindChatMemberUpdated
	// KindChatJoinRequest represents a pending request to join a chat.
	KindChatJoinRequest
	// KindChatMigrateTo represents a basic group upgrade to a supergroup channel.
	KindChatMigrateTo
)

// ChatMigrateTo contains group migration details (old chat ID and target channel ID).
type ChatMigrateTo struct {
	OldChatID    int64
	NewChannelID int64
}

// Update wraps various Telegram updates into a single normalized structure.
type Update struct {
	// Kind defines the classification of the update.
	Kind UpdateKind

	// Message contains the normalized message object if applicable.
	Message *tg.Message

	// MessageService contains service message details if applicable.
	MessageService *tg.MessageService

	// CallbackQuery contains the callback query information if applicable.
	CallbackQuery *tg.UpdateBotCallbackQuery

	// InlineQuery contains the inline search query if applicable.
	InlineQuery *tg.UpdateBotInlineQuery

	// ChatMember contains the chat participant update if applicable.
	ChatMember *tg.UpdateChatParticipant

	// ChannelMember contains the channel participant update if applicable.
	ChannelMember *tg.UpdateChannelParticipant

	// JoinRequest contains the pending join request info if applicable.
	JoinRequest *tg.UpdatePendingJoinRequests

	// ChatMigrateTo contains group migration details if Kind == KindChatMigrateTo.
	ChatMigrateTo *ChatMigrateTo

	// Raw holds the original, unmodified raw update class from MTProto.
	Raw tg.UpdateClass

	// IsOutgoing indicates if this update was triggered synthetically by an outgoing RPC call.
	IsOutgoing bool
}

// GetMessage returns the associated message class, implementing AnswerableMessageUpdate.
func (u *Update) GetMessage() tg.MessageClass {
	if u.Message != nil {
		return u.Message
	}
	if u.MessageService != nil {
		return u.MessageService
	}
	return nil
}

// peerID flattens a tg.PeerClass into its numeric ID.
func peerID(p tg.PeerClass) int64 {
	switch p := p.(type) {
	case *tg.PeerUser:
		return p.UserID
	case *tg.PeerChat:
		return p.ChatID
	case *tg.PeerChannel:
		return p.ChannelID
	}
	return 0
}

// ChatID extracts the ID of the chat/peer where this update took place.
// Returns 0 if the update is not associated with a specific chat.
func (u *Update) ChatID() int64 {
	switch {
	case u.ChatMigrateTo != nil:
		return u.ChatMigrateTo.NewChannelID
	case u.Message != nil:
		return peerID(u.Message.PeerID)
	case u.MessageService != nil:
		return peerID(u.MessageService.PeerID)
	case u.CallbackQuery != nil:
		return peerID(u.CallbackQuery.Peer)
	case u.ChatMember != nil:
		return u.ChatMember.ChatID
	case u.ChannelMember != nil:
		return u.ChannelMember.ChannelID
	case u.JoinRequest != nil:
		return peerID(u.JoinRequest.Peer)
	}
	return 0
}

// SenderID extracts the user ID of the sender who triggered this update.
// Returns 0 if the sender cannot be identified.
func (u *Update) SenderID() int64 {
	switch {
	case u.Message != nil:
		if u.Message.FromID != nil {
			if p, ok := u.Message.FromID.(*tg.PeerUser); ok {
				return p.UserID
			}
		}
		if id, ok := u.Message.GetFromID(); ok {
			if p, ok := id.(*tg.PeerUser); ok {
				return p.UserID
			}
			return 0
		}
		// FromID is omitted for one-on-one chats: the sender is the peer
		// user for incoming messages. Outgoing messages have no sender we
		// can derive here.
		if p, ok := u.Message.PeerID.(*tg.PeerUser); ok && !u.Message.Out {
			return p.UserID
		}
	case u.MessageService != nil:
		if u.MessageService.FromID != nil {
			if p, ok := u.MessageService.FromID.(*tg.PeerUser); ok {
				return p.UserID
			}
		}
	case u.CallbackQuery != nil:
		return u.CallbackQuery.UserID
	case u.InlineQuery != nil:
		return u.InlineQuery.UserID
	case u.ChatMember != nil:
		return u.ChatMember.UserID
	case u.ChannelMember != nil:
		return u.ChannelMember.UserID
	}
	return 0
}
