package auth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type emptyAffinityFallback struct{}

func (*emptyAffinityFallback) Pick(context.Context, string, string, core.Options, []*Auth) (*Auth, error) {
	return nil, nil
}

func TestSessionAffinityEmptyFallbackPreservesBinding(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		for _, cached := range []bool{false, true} {
			for _, cancelled := range []bool{false, true} {
				t.Run(fmt.Sprintf("prepared=%t/cached=%t/cancelled=%t", prepared, cached, cancelled), func(t *testing.T) {
					selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &emptyAffinityFallback{}})
					defer selector.Stop()
					options := core.Options{Headers: http.Header{"X-Session-Id": {"session"}}}
					if cached {
						selector.BindSession(t.Context(), "codex", "model", options, "old")
					}
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if cancelled {
						cancel()
					}
					var a *Auth
					var bind func()
					var err error
					available := []*Auth{{ID: "other", Provider: "codex", Status: StatusActive}}
					if prepared {
						a, bind, err = selector.pickWithPreparedFallbackDeferredBinding(ctx, "codex", "model", options, available, func() (*Auth, error) { return nil, nil })
					} else {
						a, bind, err = selector.pickWithFallbackDeferredBinding(ctx, "codex", "model", options, available, selector.fallback)
					}
					if a != nil || bind != nil || errors.Is(err, context.Canceled) != cancelled || (!cancelled && err != nil) {
						t.Fatalf("empty fallback result changed: auth=%v bind=%t err=%v", a, bind != nil, err)
					}
					want := ""
					if cached {
						want = "old"
					}
					if got := selector.cachedAuthID("codex", "model", options); got != want {
						t.Fatalf("empty fallback overwrote binding: got %q, want %q", got, want)
					}
				})
			}
		}
	}
}
