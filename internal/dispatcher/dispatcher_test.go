package dispatcher

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDispatcherGroupControl(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result error
		want   []string
	}{
		{"advance", nil, []string{"first", "last"}},
		{"continue", fmt.Errorf("wrapped: %w", ErrContinueGroups), []string{"first", "second", "last"}},
		{"end", fmt.Errorf("wrapped: %w", ErrEndGroups), []string{"first"}},
		{"error", errors.New("handler failed"), []string{"first", "last"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			d := NewDispatcher(1, 1, func(_, u int) uint64 { return uint64(u) }, func(_, _ int) int { return 1 }, slog.New(slog.NewTextHandler(&logs, nil)))
			var calls []string
			add := func(kind, group int, name string, result error) {
				d.AddRoute(kind, func(_, _ int) bool { return true }, func(_, _ int) error { calls = append(calls, name); return result }, nil, group)
			}
			add(1, 10, "last", nil)
			add(2, -2, "wrong kind", nil)
			d.AddRoute(1, func(_, _ int) bool { return false }, func(_, _ int) error { t.Fatal("rejected filter ran"); return nil }, nil, -1)
			add(1, 0, "first", tc.result)
			add(1, 0, "second", nil)
			d.processEnvelope(0, 1)
			assert.Equal(t, tc.want, calls)
			if tc.name == "error" {
				assert.Contains(t, logs.String(), "handler failed")
			}
		})
	}
}

func TestDispatcherWildcardAndMiddleware(t *testing.T) {
	d := NewDispatcher(1, 1, func(_, _ int) uint64 { return 0 }, func(_, u int) int { return u }, nil)
	d.SetWildcardKind(0)
	var calls []string
	middleware := func(name string) Middleware[int, int] {
		return func(next Handler[int, int]) Handler[int, int] {
			return func(c, u int) error {
				calls = append(calls, name+" before")
				err := next(c, u)
				calls = append(calls, name+" after")
				return err
			}
		}
	}
	d.AddRoute(1, func(_, _ int) bool { return true }, func(_, _ int) error { calls = append(calls, "handler"); return ErrContinueGroups }, []Middleware[int, int]{middleware("outer"), middleware("inner")}, 0)
	d.AddRoute(0, func(_, _ int) bool { return true }, func(_, _ int) error { calls = append(calls, "wildcard"); return ErrContinueGroups }, nil, 0)
	d.processEnvelope(0, 1)
	assert.Equal(t, []string{"outer before", "inner before", "handler", "inner after", "outer after", "wildcard"}, calls)
	calls = nil
	d.processEnvelope(0, 0)
	assert.Equal(t, []string{"wildcard"}, calls, "wildcard kind must not run twice")
}

func TestDispatcherRoutingAndInterceptor(t *testing.T) {
	d := NewDispatcher(0, 0, func(c, u int) uint64 { return uint64(c + u) }, func(_, _ int) int { return 1 }, nil)
	require.Len(t, d.shards, 1)
	require.Equal(t, 1, cap(d.shards[0].queue))
	d.SetInterceptor(func(_, u int) bool { return u == 1 })
	d.Handle(2, 1)
	assert.Empty(t, d.shards[0].queue)
	d.Handle(2, 3)
	assert.Equal(t, Envelope[int, int]{Ctx: 2, Update: 3}, <-d.shards[0].queue)
	d.SetInterceptor(nil)
	d.Handle(2, 1)
	assert.Equal(t, Envelope[int, int]{Ctx: 2, Update: 1}, <-d.shards[0].queue)
}

func TestDispatcherRecoversPanics(t *testing.T) {
	var logs bytes.Buffer
	d := NewDispatcher(1, 1, func(_, _ int) uint64 { return 0 }, func(_, _ int) int { return 1 }, slog.New(slog.NewTextHandler(&logs, nil)))
	d.AddRoute(1, func(_, _ int) bool { return true }, func(_, _ int) error { panic("test panic") }, nil, 0)
	require.NotPanics(t, func() { d.processEnvelope(0, 0) })
	assert.Contains(t, logs.String(), "test panic")
}
