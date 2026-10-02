package tgbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
)

// adminService implements AdminService for channel and supergroup moderation.
type adminService struct {
	rpc *tg.Client
}

var _ AdminService = (*adminService)(nil)

// NewAdminService creates an instance of AdminService decoupled from Context.
func NewAdminService(rpc *tg.Client) AdminService {
	return &adminService{rpc: rpc}
}

// DefaultAdminRights returns standard administrative permissions for channel moderation.
func DefaultAdminRights() tg.ChatAdminRights {
	return tg.ChatAdminRights{
		AddAdmins:      true,
		PostMessages:   true,
		EditMessages:   true,
		DeleteMessages: true,
		BanUsers:       true,
		PinMessages:    true,
		InviteUsers:    true,
	}
}

// DefaultBannedRights returns standard restriction permissions to ban a member.
func DefaultBannedRights() tg.ChatBannedRights {
	return tg.ChatBannedRights{
		ViewMessages: true,
		EmbedLinks:   true,
		SendMessages: true,
		SendMedia:    true,
		SendStickers: true,
		SendGifs:     true,
		SendGames:    true,
		SendInline:   true,
	}
}

// BuildPromoteRequest constructs a ChannelsEditAdminRequest with default administrative permissions.
func BuildPromoteRequest(channel tg.InputChannelClass, user tg.InputUserClass) *tg.ChannelsEditAdminRequest {
	return &tg.ChannelsEditAdminRequest{
		Channel:     channel,
		UserID:      user,
		AdminRights: DefaultAdminRights(),
	}
}

// BuildBanRequest constructs a ChannelsEditBannedRequest with default banned restrictions.
func BuildBanRequest(channel tg.InputChannelClass, participant tg.InputPeerClass) *tg.ChannelsEditBannedRequest {
	return &tg.ChannelsEditBannedRequest{
		Channel:      channel,
		Participant:  participant,
		BannedRights: DefaultBannedRights(),
	}
}

// Promote promotes a member to administrator with comprehensive standard rights.
func (s *adminService) Promote(ctx context.Context, channel tg.InputChannelClass, user tg.InputUserClass) error {
	req := BuildPromoteRequest(channel, user)
	if _, err := s.rpc.ChannelsEditAdmin(ctx, req); err != nil {
		return fmt.Errorf("tgbox: promote admin: %w", err)
	}
	return nil
}

// Ban restricts and removes a participant from a supergroup or channel.
func (s *adminService) Ban(ctx context.Context, channel tg.InputChannelClass, participant tg.InputPeerClass) error {
	req := BuildBanRequest(channel, participant)
	if _, err := s.rpc.ChannelsEditBanned(ctx, req); err != nil {
		return fmt.Errorf("tgbox: ban participant: %w", err)
	}
	return nil
}

// ActiveChannelAndSender resolves the current update's channel and sender purely from update metadata.
func (c *Context) ActiveChannelAndSender() (*tg.InputChannel, tg.InputUserClass, error) {
	if c == nil || c.Update == nil {
		return nil, nil, ErrNilUpdate
	}

	peer, err := c.CurrentPeer()
	if err != nil {
		return nil, nil, fmt.Errorf("tgbox: resolve active peer: %w", err)
	}

	channel, ok := peer.(*tg.InputPeerChannel)
	if !ok {
		return nil, nil, errors.New("tgbox: active peer is not a channel/supergroup")
	}

	senderID := c.Update.SenderID()
	if senderID == 0 {
		return nil, nil, errors.New("tgbox: active update has no identifiable sender")
	}

	user, err := c.ResolveUser(senderID)
	if err != nil {
		return nil, nil, fmt.Errorf("tgbox: resolve sender: %w", err)
	}

	return &tg.InputChannel{
		ChannelID:  channel.ChannelID,
		AccessHash: channel.AccessHash,
	}, user, nil
}
