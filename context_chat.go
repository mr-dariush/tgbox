package tgbox

import (
	"context"
	"fmt"

	contribstorage "github.com/gotd/contrib/storage"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/message/entity"
	"github.com/gotd/td/telegram/message/html"
	"github.com/gotd/td/telegram/message/markdown"
	"github.com/gotd/td/telegram/message/peer"
	"github.com/gotd/td/telegram/message/rich"
	"github.com/gotd/td/telegram/query/dialogs"
	"github.com/gotd/td/tg"

	"github.com/mr-dariush/tgbox/keyboard"
)

// SendOption configures a message dynamically before dispatch.
type SendOption func(b *message.Builder) error

// WithMarkup assigns an inline or reply keyboard to the message.
func WithMarkup(markup keyboard.ReplyMarkup) SendOption {
	return func(b *message.Builder) error {
		if markup == nil {
			return nil
		}
		tgMarkup, err := keyboard.TranslateReplyMarkup(markup)
		if err != nil {
			return err
		}
		b.Markup(tgMarkup)
		return nil
	}
}

func applySendOptions(b *message.Builder, opts []SendOption) error {
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return err
		}
	}
	return nil
}

// chatService implements MessageService, managing MTProto message transmission and edits.
type chatService struct {
	sender      *message.Sender
	rpc         *tg.Client
	peerStorage contribstorage.PeerStorage
}

var _ MessageService = (*chatService)(nil)

// NewChatService instantiates a decoupled messaging service bound to the client lifecycle.
func NewChatService(sender *message.Sender, rpc *tg.Client, peerStorage contribstorage.PeerStorage) MessageService {
	return &chatService{
		sender:      sender,
		rpc:         rpc,
		peerStorage: peerStorage,
	}
}

func (s *chatService) userResolver(ctx context.Context) entity.UserResolver {
	return func(id int64) (tg.InputUserClass, error) {
		if s.peerStorage != nil {
			p, err := s.peerStorage.Find(ctx, contribstorage.PeerKey{
				Kind: dialogs.User,
				ID:   id,
			})
			if err == nil && p.User != nil {
				return &tg.InputUser{
					UserID:     p.User.ID,
					AccessHash: p.User.AccessHash,
				}, nil
			}
		}
		return &tg.InputUser{UserID: id}, nil
	}
}

// SendText transmits a plain text message to the specified peer.
func (s *chatService) SendText(ctx context.Context, p tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.Text(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("tgbox: send text: %w", err)
	}
	return upd, nil
}

// SendHTML transmits an HTML-formatted message to the specified peer.
func (s *chatService) SendHTML(ctx context.Context, p tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.StyledText(ctx, html.String(s.userResolver(ctx), text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: send html: %w", err)
	}
	return upd, nil
}

// SendMarkdown transmits a Markdown-formatted message to the specified peer.
func (s *chatService) SendMarkdown(ctx context.Context, p tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.StyledText(ctx, markdown.String(s.userResolver(ctx), text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: send markdown: %w", err)
	}
	return upd, nil
}

// SendRichMarkdown transmits a message formatted with Telegram's native Rich Text engine,
// rendering GFM markdown tables, headings, checklists, and collapsible details natively.
func (s *chatService) SendRichMarkdown(ctx context.Context, p tg.InputPeerClass, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.RichMessage(ctx, rich.Markdown(text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: send rich markdown: %w", err)
	}
	return upd, nil
}

// SendRich transmits a structured tg.InputRichMessageClass directly to the specified peer.
func (s *chatService) SendRich(ctx context.Context, p tg.InputPeerClass, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.RichMessage(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("tgbox: send rich message: %w", err)
	}
	return upd, nil
}

// ReplyText replies with a plain text message to a specific message ID.
func (s *chatService) ReplyText(ctx context.Context, p tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p).Reply(msgID)
	if err := applySendOptions(b, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.Text(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("tgbox: reply text: %w", err)
	}
	return upd, nil
}

// ReplyHTML replies with an HTML-formatted message to a specific message ID.
func (s *chatService) ReplyHTML(ctx context.Context, p tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p).Reply(msgID)
	if err := applySendOptions(b, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.StyledText(ctx, html.String(s.userResolver(ctx), text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: reply html: %w", err)
	}
	return upd, nil
}

// ReplyMarkdown replies with a Markdown-formatted message to a specific message ID.
func (s *chatService) ReplyMarkdown(ctx context.Context, p tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p).Reply(msgID)
	if err := applySendOptions(b, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.StyledText(ctx, markdown.String(s.userResolver(ctx), text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: reply markdown: %w", err)
	}
	return upd, nil
}

// ReplyRichMarkdown replies to a specific message ID using Telegram's native Rich Text engine.
func (s *chatService) ReplyRichMarkdown(ctx context.Context, p tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p).Reply(msgID)
	if err := applySendOptions(b, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.RichMessage(ctx, rich.Markdown(text))
	if err != nil {
		return nil, fmt.Errorf("tgbox: reply rich markdown: %w", err)
	}
	return upd, nil
}

// ReplyRich replies with a structured tg.InputRichMessageClass to a specific message ID.
func (s *chatService) ReplyRich(ctx context.Context, p tg.InputPeerClass, msgID int, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p).Reply(msgID)
	if err := applySendOptions(b, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.RichMessage(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("tgbox: reply rich message: %w", err)
	}
	return upd, nil
}

// EditActive modifies the text of an existing message.
func (s *chatService) EditActive(ctx context.Context, p tg.InputPeerClass, msgID int, text string, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.Edit(msgID).Text(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("tgbox: edit message: %w", err)
	}
	return upd, nil
}

// EditRich modifies an existing message with a structured tg.InputRichMessageClass.
func (s *chatService) EditRich(ctx context.Context, p tg.InputPeerClass, msgID int, msg tg.InputRichMessageClass, opts ...SendOption) (tg.UpdatesClass, error) {
	b := s.sender.To(p)
	if err := applySendOptions(&b.Builder, opts); err != nil {
		return nil, fmt.Errorf("tgbox: apply send options: %w", err)
	}
	upd, err := b.Edit(msgID).RichMessage(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("tgbox: edit rich message: %w", err)
	}
	return upd, nil
}

// EditReplyMarkup modifies the keyboard markup of an existing message without changing the text.
func (s *chatService) EditReplyMarkup(ctx context.Context, p tg.InputPeerClass, msgID int, markup keyboard.ReplyMarkup) (tg.UpdatesClass, error) {
	tgMarkup, err := keyboard.TranslateReplyMarkup(markup)
	if err != nil {
		return nil, fmt.Errorf("tgbox: translate reply markup: %w", err)
	}
	req := &tg.MessagesEditMessageRequest{
		Peer: p,
		ID:   msgID,
	}
	if tgMarkup != nil {
		req.SetReplyMarkup(tgMarkup)
	}
	upd, err := s.rpc.MessagesEditMessage(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("tgbox: edit reply markup: %w", err)
	}
	return upd, nil
}

// AnswerCallback sends a callback notification or alert back to Telegram servers.
func (s *chatService) AnswerCallback(ctx context.Context, queryID int64, alert bool, text string) error {
	req := &tg.MessagesSetBotCallbackAnswerRequest{
		QueryID: queryID,
		Alert:   alert,
	}
	if text != "" {
		req.SetMessage(text)
	}
	if _, err := s.rpc.MessagesSetBotCallbackAnswer(ctx, req); err != nil {
		return fmt.Errorf("tgbox: answer callback: %w", err)
	}
	return nil
}

// CurrentPeer extracts the target MTProto input peer from the context update safely.
func (c *Context) CurrentPeer() (tg.InputPeerClass, error) {
	if c == nil || c.Update == nil {
		return nil, ErrNilUpdate
	}

	if c.Update.Message != nil {
		p, err := c.safeEntities().ExtractPeer(c.Update.Message.PeerID)
		if err != nil {
			return nil, fmt.Errorf("tgbox: extract peer from message: %w", err)
		}
		return p, nil
	}

	if c.Update.CallbackQuery != nil {
		p, err := c.safeEntities().ExtractPeer(c.Update.CallbackQuery.Peer)
		if err != nil {
			return nil, fmt.Errorf("tgbox: extract peer from callback query: %w", err)
		}
		return p, nil
	}

	switch raw := c.Update.Raw.(type) {
	case *tg.UpdateNewMessage:
		if msg, ok := raw.Message.AsNotEmpty(); ok {
			p, err := c.safeEntities().ExtractPeer(msg.GetPeerID())
			if err != nil {
				return nil, fmt.Errorf("tgbox: extract peer from raw new message: %w", err)
			}
			return p, nil
		}
	case *tg.UpdateNewChannelMessage:
		if msg, ok := raw.Message.AsNotEmpty(); ok {
			p, err := c.safeEntities().ExtractPeer(msg.GetPeerID())
			if err != nil {
				return nil, fmt.Errorf("tgbox: extract peer from raw new channel message: %w", err)
			}
			return p, nil
		}
	case *tg.UpdateBotCallbackQuery:
		p, err := c.safeEntities().ExtractPeer(raw.Peer)
		if err != nil {
			return nil, fmt.Errorf("tgbox: extract peer from raw callback query: %w", err)
		}
		return p, nil
	}

	return nil, ErrNoPeer
}

// PeerPromise wraps CurrentPeer inside a peer.Promise.
func (c *Context) PeerPromise() peer.Promise {
	return func(_ context.Context) (tg.InputPeerClass, error) {
		return c.CurrentPeer()
	}
}

// ResolveUser resolves a user ID to an InputUserClass using entities extracted in the current update.
func (c *Context) ResolveUser(id int64) (tg.InputUserClass, error) {
	if c != nil && c.Entities != nil {
		if u, ok := c.Entities.Users[id]; ok {
			return &tg.InputUser{
				UserID:     u.ID,
				AccessHash: u.AccessHash,
			}, nil
		}
	}
	return &tg.InputUser{UserID: id}, nil
}

// safeEntities extracts peer.Entities safely without risking nil-pointer dereferences.
func (c *Context) safeEntities() peer.Entities {
	var ents tg.Entities
	if c != nil && c.Entities != nil {
		ents = *c.Entities
	}
	return peer.EntitiesFromUpdate(ents)
}
