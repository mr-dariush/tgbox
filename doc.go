// Package tgbox is a lightweight, high-performance Telegram bot and userbot
// framework built on top of gotd/td (MTProto).
//
// The client is configured with functional options and exposes a simple,
// handler-based API for processing updates:
//
//	client, err := tgbox.New("mybot",
//		tgbox.WithAppID(123456),
//		tgbox.WithAppHash("hash"),
//		tgbox.WithBotToken("token"),
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	client.OnCommand("/start", func(ctx *tgbox.Context, upd *tgbox.Update) error {
//		_, err := ctx.ReplyText("Hello!")
//		return err
//	})
//
//	if err := client.Run(context.Background()); err != nil {
//		log.Fatal(err)
//	}
//
// Subpackages provide focused building blocks: filter (update filters),
// keyboard (inline/reply keyboard builders), and auth (headless MTProto
// authentication). Low-level MTProto plumbing lives under internal and is
// not part of the public API.
package tgbox
