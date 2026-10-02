// Package filter provides built-in filter implementations for TGBox.
package filter

import (
	"strings"

	"github.com/gotd/td/tg"

	"github.com/mr-dariush/tgbox"
)

// All always returns true.
func All(_ *tgbox.Context, _ *tgbox.Update) bool {
	return true
}

// Private passes if the update is a message from a private chat.
func Private(_ *tgbox.Context, update *tgbox.Update) bool {
	if update == nil || update.Message == nil {
		return false
	}
	_, ok := update.Message.PeerID.(*tg.PeerUser)
	return ok
}

// Group passes if the update is a message from a group or supergroup/channel.
func Group(_ *tgbox.Context, update *tgbox.Update) bool {
	if update == nil || update.Message == nil {
		return false
	}
	switch update.Message.PeerID.(type) {
	case *tg.PeerChat, *tg.PeerChannel:
		return true
	}
	return false
}

// Incoming passes for updates that were not produced by this client's own
// outgoing RPC calls.
func Incoming(_ *tgbox.Context, update *tgbox.Update) bool {
	return update != nil && !update.IsOutgoing
}

// Command passes if the message starts with one of the given commands,
// optionally suffixed with a bot mention ("/start@my_bot").
func Command(cmds ...string) tgbox.Filter {
	return func(_ *tgbox.Context, update *tgbox.Update) bool {
		if update == nil || update.Message == nil {
			return false
		}

		text := update.Message.Message
		if !strings.HasPrefix(text, "/") {
			return false
		}

		parts := strings.Fields(text)
		if len(parts) == 0 {
			return false
		}

		cmd := strings.ToLower(strings.TrimPrefix(parts[0], "/"))
		if idx := strings.Index(cmd, "@"); idx != -1 {
			cmd = cmd[:idx]
		}

		for _, c := range cmds {
			if cmd == strings.ToLower(c) {
				return true
			}
		}
		return false
	}
}

// And combines multiple filters with logical AND.
func And(filters ...tgbox.Filter) tgbox.Filter {
	return func(ctx *tgbox.Context, update *tgbox.Update) bool {
		for _, f := range filters {
			if !f(ctx, update) {
				return false
			}
		}
		return true
	}
}

// Or combines multiple filters with logical OR.
func Or(filters ...tgbox.Filter) tgbox.Filter {
	return func(ctx *tgbox.Context, update *tgbox.Update) bool {
		for _, f := range filters {
			if f(ctx, update) {
				return true
			}
		}
		return false
	}
}

// Not negates a filter.
func Not(f tgbox.Filter) tgbox.Filter {
	return func(ctx *tgbox.Context, update *tgbox.Update) bool {
		return !f(ctx, update)
	}
}

// State passes if the current user's conversational state in the provided StateManager matches expected.
// It is 100% non-blocking, safe against nil contexts, and relies strictly on the underlying storage driver.
// Retrieval errors or expired states are safely treated as no match.
func State(sm tgbox.StateManager, expected string) tgbox.Filter {
	return func(ctx *tgbox.Context, update *tgbox.Update) bool {
		upd := update
		if upd == nil && ctx != nil {
			upd = ctx.Update
		}
		if sm == nil || upd == nil {
			return false
		}

		chatID := upd.ChatID()
		userID := upd.SenderID()
		if chatID == 0 || userID == 0 {
			return false
		}

		state, err := sm.GetState(ctx.Context(), chatID, userID)
		if err != nil {
			return false
		}
		return state == expected
	}
}
