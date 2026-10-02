// Package engine provides the core MTProto connection management.
package engine

import (
	"context"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// Engine defines the core contract for MTProto network operations.
type Engine interface {
	// Connect establishes the connection to Telegram.
	Connect(ctx context.Context) error

	// Disconnect gracefully closes the connection.
	Disconnect() error

	// API returns the high-level MTProto API client.
	API() *tg.Client

	// Raw returns the underlying telegram client.
	Raw() *telegram.Client

	// SetUpdateHandler sets the callback for incoming updates.
	SetUpdateHandler(handler telegram.UpdateHandler)
}
