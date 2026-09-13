package auth

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type historyPolicyExecutor struct {
	*requestLimitOperationExecutor
	fingerprints []string
}

func (e *historyPolicyExecutor) PrepareProviderRequest(_ context.Context, _ core.Request, opts core.Options, _ core.RequestOperation) (any, error) {
	e.fingerprints = append(e.fingerprints, SessionAffinityFingerprint(opts))
	return nil, nil
}

func TestSessionAffinityUseHistoryControlsAllMessageFallbacks(t *testing.T) {
	for _, mode := range []string{"basic", "subagents", "lcp"} {
		for _, useHistory := range []bool{false, true} {
			for _, operation := range []string{"execute", "count", "stream"} {
				t.Run(fmt.Sprintf("%s/history=%t/%s", mode, useHistory, operation), func(t *testing.T) {
					selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
						Fallback: &RoundRobinSelector{}, UseHistory: &useHistory, Subagents: mode == "subagents", LCP: mode == "lcp",
					})
					t.Cleanup(selector.Stop)
					if !useHistory && selector.historyMatcher != nil {
						t.Fatal("disabled history allocated a prefix matcher")
					}
					manager := NewManager(nil, selector, nil)
					manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityUseHistory: &useHistory}})
					exec := &historyPolicyExecutor{requestLimitOperationExecutor: &requestLimitOperationExecutor{}}
					manager.RegisterExecutor(exec)
					for _, id := range []string{"a", "b"} {
						if _, err := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test"}); err != nil {
							t.Fatal(err)
						}
					}
					ctx := affinityCallerContext(t, "caller", "test")
					opts := core.Options{Headers: make(http.Header), SourceFormat: sdktranslator.FormatCodex}
					for range 2 {
						req := core.Request{Payload: []byte(`{"instructions":"system","input":"same user text"}`)}
						var err error
						switch operation {
						case "count":
							_, err = manager.ExecuteCount(ctx, []string{"test"}, req, opts)
						case "stream":
							var stream *core.StreamResult
							stream, err = manager.ExecuteStream(ctx, []string{"test"}, req, opts)
							if stream != nil {
								for chunk := range stream.Chunks {
									if chunk.Err != nil {
										err = chunk.Err
									}
								}
							}
						default:
							_, err = manager.Execute(ctx, []string{"test"}, req, opts)
						}
						if err != nil {
							t.Fatal(err)
						}
					}
					want := []string{"a", "b"}
					if useHistory {
						want[1] = "a"
					}
					if got := exec.callIDs(); !reflect.DeepEqual(got, want) {
						t.Fatalf("credential selection=%v, want %v", got, want)
					}
					for _, digest := range exec.fingerprints {
						if (digest != "") != useHistory {
							t.Fatal("pool affinity did not follow history policy")
						}
					}
				})
			}
		}
	}
}

func TestSessionAffinityUseHistoryKeepsExplicitIdentities(t *testing.T) {
	off := false
	ctx := affinityCallerContext(t, "caller", "codex")
	for _, mode := range []string{"basic", "subagents", "lcp"} {
		t.Run(mode, func(t *testing.T) {
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, UseHistory: &off, Subagents: mode == "subagents", LCP: mode == "lcp"})
			t.Cleanup(selector.Stop)
			options := []core.Options{
				{Headers: http.Header{"Session-Id": {"explicit"}}},
				{Headers: http.Header{"X-Session-Id": {"explicit"}}},
				{Headers: make(http.Header), OriginalRequest: []byte(`{"conversation_id":"explicit","input":"same"}`)},
				{Headers: make(http.Header), OriginalRequest: []byte(`{"prompt_cache_key":"explicit","input":"same"}`)},
			}
			if mode != "basic" {
				options = append(options, core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"thread_id":"native-explicit","input":"same"}`)})
			}
			for _, opts := range options {
				selector.BindSession(ctx, "codex", "model", opts, "b")
				got, err := selector.Pick(ctx, "codex", "model", opts, []*Auth{{ID: "a"}, {ID: "b"}})
				if err != nil || got == nil || got.ID != "b" {
					t.Fatal("disabling message history discarded an explicit identity", err)
				}
			}
			if mode != "basic" {
				captured := captureAffinityIdentityWithHistoryPolicy(ctx, core.Request{Payload: []byte(`{"input":"same"}`)}, core.Options{Headers: make(http.Header)}, true, false)
				if captured.history != nil || captured.legacyPrimary != "" || captured.legacyFallback != "" {
					t.Fatal("disabled policy still captured message history")
				}
			}
			if mode == "subagents" {
				selector.BindSession(ctx, "codex", "model", subagentOptions("parent", "", false), "b")
				got, err := selector.Pick(ctx, "codex", "model", subagentOptions("child", "parent", false), []*Auth{{ID: "a"}, {ID: "b"}})
				if err != nil || got == nil || got.ID != "b" {
					t.Fatal("history policy disabled explicit parent inheritance", err)
				}
			}
		})
	}
}
