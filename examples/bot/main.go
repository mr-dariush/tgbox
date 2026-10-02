// Package main is a sample implementation of a Telegram bot using the tgbox framework.
// It demonstrates command handling, explicit dependency injection via controllers, and stateless conversational FSM.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mr-dariush/tgbox"
	"github.com/mr-dariush/tgbox/filter"
)

// version is injected at build time via -ldflags="-X main.version=...".
var version = "dev"

// RegistrationController manages the user registration conversational flow using explicit dependency injection.
type RegistrationController struct {
	chat  tgbox.MessageService
	state tgbox.StateManager
}

// NewRegistrationController constructs a RegistrationController with explicit dependencies.
func NewRegistrationController(chat tgbox.MessageService, state tgbox.StateManager) *RegistrationController {
	return &RegistrationController{
		chat:  chat,
		state: state,
	}
}

// RegisterRoutes registers the controller's endpoints onto the provided Registrar.
func (c *RegistrationController) RegisterRoutes(r tgbox.Registrar) error {
	r.OnMessage(filter.Command("start"), c.handleStart)
	r.OnMessage(filter.Command("register"), c.handleRegister)
	r.OnState("waiting_for_name", c.handleWaitingForName)
	return nil
}

func (c *RegistrationController) handleStart(ctx *tgbox.Context, upd *tgbox.Update) error {
	peer, err := ctx.CurrentPeer()
	if err != nil {
		return fmt.Errorf("resolve peer: %w", err)
	}

	msgID := 0
	if upd != nil && upd.Message != nil {
		msgID = upd.Message.ID
	}

	if _, err := c.chat.ReplyText(ctx.Context(), peer, msgID, "Hello! Send /register to start the stateless FSM."); err != nil {
		return fmt.Errorf("reply to /start: %w", err)
	}
	return nil
}

func (c *RegistrationController) handleRegister(ctx *tgbox.Context, upd *tgbox.Update) error {
	peer, err := ctx.CurrentPeer()
	if err != nil {
		return fmt.Errorf("resolve peer: %w", err)
	}

	chatID := upd.ChatID()
	userID := upd.SenderID()
	if err := c.state.SetState(ctx.Context(), chatID, userID, "waiting_for_name", 5*time.Minute); err != nil {
		return fmt.Errorf("set state: %w", err)
	}

	msgID := 0
	if upd != nil && upd.Message != nil {
		msgID = upd.Message.ID
	}

	_, err = c.chat.ReplyText(ctx.Context(), peer, msgID, "Great! Please send me your full name.")
	return err
}

func (c *RegistrationController) handleWaitingForName(ctx *tgbox.Context, upd *tgbox.Update) error {
	peer, err := ctx.CurrentPeer()
	if err != nil {
		return fmt.Errorf("resolve peer: %w", err)
	}

	name := ""
	msgID := 0
	if upd != nil && upd.Message != nil {
		name = upd.Message.Message
		msgID = upd.Message.ID
	}

	chatID := upd.ChatID()
	userID := upd.SenderID()
	if err := c.state.ClearState(ctx.Context(), chatID, userID); err != nil {
		return fmt.Errorf("clear state: %w", err)
	}

	replyText := fmt.Sprintf("Nice to meet you, %s! Your state has been cleared.", name)
	_, err = c.chat.ReplyText(ctx.Context(), peer, msgID, replyText)
	return err
}

func main() {
	log.Printf("Starting tgbox bot (version: %s)...", version)

	// Initialize the tgbox client with required credentials.
	client, err := tgbox.New("mybot",
		tgbox.WithAppID(123456),              // Your App ID
		tgbox.WithAppHash("your_app_hash"),   // Your App Hash
		tgbox.WithBotToken("your_bot_token"), // Your Bot Token
	)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close client: %v", err)
		}
	}()

	// Mount the controller with explicit dependencies via constructor injection.
	regCtrl := NewRegistrationController(client.Chat(), client.State())
	if err := client.RegisterController(regCtrl); err != nil {
		log.Printf("failed to register controller: %v", err)
		return
	}

	// Graceful shutdown handling for OS interrupts.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := client.Run(ctx); err != nil {
		log.Printf("Bot stopped: %v", err)
	}
}
