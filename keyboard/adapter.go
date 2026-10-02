package keyboard

import (
	"fmt"

	"github.com/gotd/td/tg"
)

// applyStyleToTg populates the MTProto button style based on keyboard.buttonStyle.
func applyStyleToTg(style buttonStyle) tg.KeyboardButtonStyle {
	var tgStyle tg.KeyboardButtonStyle
	if style.primary {
		tgStyle.BgPrimary = true
	}
	if style.danger {
		tgStyle.BgDanger = true
	}
	if style.success {
		tgStyle.BgSuccess = true
	}
	if style.iconID != 0 {
		tgStyle.SetIcon(style.iconID)
	}
	return tgStyle
}

// translateInlineButton converts a keyboard InlineButton to its MTProto equivalent.
func translateInlineButton(btn InlineButton) (tg.KeyboardInlineButton, error) {
	style := applyStyleToTg(btn.Style)
	res := tg.KeyboardInlineButton{
		Text:  btn.Text,
		Style: style,
	}

	switch {
	case btn.URL != "":
		res.Type = &tg.InlineButtonTypeURL{URL: btn.URL}
		return res, nil

	case btn.CallbackData != "":
		res.Type = &tg.InlineButtonTypeCallback{Data: []byte(btn.CallbackData)}
		return res, nil

	case btn.WebAppURL != "":
		res.Type = &tg.InlineButtonTypeWebView{URL: btn.WebAppURL}
		return res, nil

	case btn.SwitchInlineQuery != nil:
		res.Type = &tg.InlineButtonTypeSwitchInline{Query: *btn.SwitchInlineQuery}
		return res, nil

	case btn.SwitchInlineQueryCurrentChat != nil:
		res.Type = &tg.InlineButtonTypeSwitchInline{Query: *btn.SwitchInlineQueryCurrentChat, SamePeer: true}
		return res, nil

	case btn.Pay:
		res.Type = &tg.InlineButtonTypeBuy{}
		return res, nil

	default:
		return tg.KeyboardInlineButton{}, fmt.Errorf("tgbox: inline button %q must specify an action (URL, Callback, WebApp, etc.)", btn.Text)
	}
}

// translateReplyButton converts a keyboard ReplyButton to its MTProto equivalent.
func translateReplyButton(btn ReplyButton) tg.KeyboardButton {
	style := applyStyleToTg(btn.Style)
	res := tg.KeyboardButton{
		Text:  btn.Text,
		Style: style,
	}

	switch {
	case btn.RequestContact:
		res.Type = &tg.ButtonTypeRequestPhone{}
		return res

	case btn.RequestLocation:
		res.Type = &tg.ButtonTypeRequestGeoLocation{}
		return res

	case btn.WebAppURL != nil:
		res.Type = &tg.ButtonTypeSimpleWebView{URL: *btn.WebAppURL}
		return res

	case btn.RequestPoll != nil:
		res.Type = &tg.ButtonTypeRequestPoll{Quiz: *btn.RequestPoll}
		return res

	case btn.RequestUsers != nil:
		p := &tg.RequestPeerTypeUser{}
		if btn.RequestUsers.IsBot != nil {
			p.SetBot(*btn.RequestUsers.IsBot)
		}
		if btn.RequestUsers.IsPremium != nil {
			p.SetPremium(*btn.RequestUsers.IsPremium)
		}

		qty := max(btn.RequestUsers.MaxQuantity, 1)

		res.Type = &tg.ButtonTypeRequestPeer{
			ButtonID:    *btn.RequestPeerButtonID,
			PeerType:    p,
			MaxQuantity: qty,
		}
		return res

	case btn.RequestGroup != nil:
		p := &tg.RequestPeerTypeChat{
			Creator:        btn.RequestGroup.IsCreator,
			BotParticipant: btn.RequestGroup.BotIsMember,
		}
		if btn.RequestGroup.IsForum != nil {
			p.SetForum(*btn.RequestGroup.IsForum)
		}
		if btn.RequestGroup.HasUsername != nil {
			p.SetHasUsername(*btn.RequestGroup.HasUsername)
		}
		if btn.RequestGroup.UserAdminRights != nil {
			p.SetUserAdminRights(*btn.RequestGroup.UserAdminRights)
		}
		if btn.RequestGroup.BotAdminRights != nil {
			p.SetBotAdminRights(*btn.RequestGroup.BotAdminRights)
		}

		res.Type = &tg.ButtonTypeRequestPeer{
			ButtonID: *btn.RequestPeerButtonID,
			PeerType: p,
		}
		return res

	case btn.RequestChannel != nil:
		p := &tg.RequestPeerTypeBroadcast{
			Creator: btn.RequestChannel.IsCreator,
		}
		if btn.RequestChannel.HasUsername != nil {
			p.SetHasUsername(*btn.RequestChannel.HasUsername)
		}
		if btn.RequestChannel.UserAdminRights != nil {
			p.SetUserAdminRights(*btn.RequestChannel.UserAdminRights)
		}
		if btn.RequestChannel.BotAdminRights != nil {
			p.SetBotAdminRights(*btn.RequestChannel.BotAdminRights)
		}

		res.Type = &tg.ButtonTypeRequestPeer{
			ButtonID: *btn.RequestPeerButtonID,
			PeerType: p,
		}
		return res

	default:
		res.Type = &tg.ButtonTypeDefault{}
		return res
	}
}

// TranslateReplyMarkup converts a keyboard ReplyMarkup to its MTProto equivalent.
func TranslateReplyMarkup(markup ReplyMarkup) (tg.ReplyMarkupClass, error) {
	if markup == nil {
		return nil, nil
	}

	switch m := markup.(type) {
	case InlineKeyboard:
		var tgRows []tg.KeyboardInlineButtonRow
		for _, row := range m.Rows {
			var tgButtons []tg.KeyboardInlineButton
			for _, btn := range row {
				tgBtn, err := translateInlineButton(btn)
				if err != nil {
					return nil, err
				}
				tgButtons = append(tgButtons, tgBtn)
			}
			tgRows = append(tgRows, tg.KeyboardInlineButtonRow{Buttons: tgButtons})
		}
		return &tg.ReplyInlineMarkup{Rows: tgRows}, nil

	case ReplyKeyboard:
		var tgRows []tg.KeyboardButtonRow
		for _, row := range m.Rows {
			var tgButtons []tg.KeyboardButton
			for _, btn := range row {
				tgButtons = append(tgButtons, translateReplyButton(btn))
			}
			tgRows = append(tgRows, tg.KeyboardButtonRow{Buttons: tgButtons})
		}

		res := &tg.ReplyKeyboardMarkup{
			Resize:    m.Resize,
			SingleUse: m.OneTime,
			Selective: m.Selective,
			Rows:      tgRows,
		}
		if m.PlaceholderText != "" {
			res.SetPlaceholder(m.PlaceholderText)
		}
		return res, nil

	case RemoveKeyboard:
		return &tg.ReplyKeyboardHide{Selective: m.Selective}, nil

	default:
		return nil, fmt.Errorf("keyboard: unsupported reply markup type %T", markup)
	}
}
