package filter_test

import (
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/mr-dariush/tgbox"
	"github.com/mr-dariush/tgbox/filter"
	"github.com/mr-dariush/tgbox/internal/fsm"
)

func TestMessageFilters(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		update                   *tgbox.Update
		private, group, incoming bool
	}{
		{"nil", nil, false, false, false},
		{"empty", &tgbox.Update{}, false, false, true},
		{"private", &tgbox.Update{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 1}}}, true, false, true},
		{"chat", &tgbox.Update{Message: &tg.Message{PeerID: &tg.PeerChat{ChatID: 1}}}, false, true, true},
		{"channel", &tgbox.Update{Message: &tg.Message{PeerID: &tg.PeerChannel{ChannelID: 1}}, IsOutgoing: true}, false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, filter.All(nil, tc.update))
			assert.Equal(t, tc.private, filter.Private(nil, tc.update))
			assert.Equal(t, tc.group, filter.Group(nil, tc.update))
			assert.Equal(t, tc.incoming, filter.Incoming(nil, tc.update))
		})
	}
}

func TestCommand(t *testing.T) {
	f := filter.Command("START", "help")
	assert.False(t, f(nil, nil))
	assert.False(t, f(nil, &tgbox.Update{}))
	for _, tc := range []struct {
		text string
		want bool
	}{
		{"/start", true}, {"/HELP@my_bot args", true}, {"/start\narg", true}, {"/starter", false}, {"hello /start", false}, {"", false}, {" /start", false}, {"/", false},
	} {
		t.Run(tc.text, func(t *testing.T) {
			assert.Equal(t, tc.want, f(nil, &tgbox.Update{Message: &tg.Message{Message: tc.text}}))
		})
	}
}

func TestFilterComposition(t *testing.T) {
	assert.True(t, filter.And()(nil, nil))
	assert.False(t, filter.Or()(nil, nil))
	yes := filter.All
	no := filter.Not(yes)
	unreachable := func(_ *tgbox.Context, _ *tgbox.Update) bool { t.Fatal("filter did not short circuit"); return false }
	assert.False(t, filter.And(yes, no, unreachable)(nil, nil))
	assert.True(t, filter.And(yes)(nil, nil))
	assert.True(t, filter.Or(no, yes, unreachable)(nil, nil))
	assert.False(t, filter.Or(no)(nil, nil))
}

func TestStateFilter(t *testing.T) {
	storage := fsm.NewMemoryStorage(t.Context(), time.Hour)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	sm := tgbox.NewStateManager(storage)
	update := &tgbox.Update{Message: &tg.Message{PeerID: &tg.PeerUser{UserID: 7}}}
	ctx := &tgbox.Context{Update: update}
	f := filter.State(sm, "waiting")
	assert.False(t, f(ctx, nil))
	require.NoError(t, sm.SetState(t.Context(), 7, 7, "waiting", time.Hour))
	assert.True(t, f(ctx, nil))
	assert.True(t, f(nil, update))
	assert.False(t, filter.State(sm, "other")(ctx, update))
	assert.False(t, f(nil, nil))
	assert.False(t, f(nil, &tgbox.Update{}))
	assert.False(t, filter.State(nil, "waiting")(ctx, update))
	assert.False(t, filter.State(tgbox.NewStateManager(nil), "waiting")(ctx, update))
}
