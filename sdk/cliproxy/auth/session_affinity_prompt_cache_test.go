package auth

import (
	"bytes"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/session"
)

func TestSessionAffinityPromptCacheFallbackPreservesExplicitIdentity(t *testing.T) {
	for _, mode := range []string{"basic", "subagents", "history"} {
		t.Run(mode, func(t *testing.T) {
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &FillFirstSelector{}, Subagents: mode == "subagents", LCP: mode == "history"})
			t.Cleanup(selector.Stop)
			ctx := affinityCallerContext(t, "cache-caller", "codex")
			original := core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"prompt_cache_key":"client-cache","input":"first"}`)}
			selector.BindSession(ctx, "codex", "model", original, "b")
			continued := core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"prompt_cache_key":"client-cache","input":"changed history"}`)}
			picked, err := selector.Pick(ctx, "codex", "model", continued, []*Auth{{ID: "a"}, {ID: "b"}})
			if err != nil || picked == nil || picked.ID != "b" {
				t.Fatal("explicit client cache key did not retain the binding", err)
			}
			different := core.Options{Headers: make(http.Header), OriginalRequest: []byte(`{"prompt_cache_key":"different-cache","input":"first"}`)}
			picked, err = selector.Pick(ctx, "codex", "model", different, []*Auth{{ID: "a"}, {ID: "b"}})
			if err != nil || picked == nil || picked.ID != "a" {
				t.Fatal("different cache keys shared an implicit binding", err)
			}
			continued.Headers = http.Header{"Session-Id": {"explicit-session"}}
			picked, err = selector.Pick(ctx, "codex", "model", continued, []*Auth{{ID: "a"}, {ID: "b"}})
			if err != nil || picked == nil || picked.ID != "a" {
				t.Fatal("cache key overrode an explicit session", err)
			}
		})
	}
}

func TestPromptCacheIdentityValidationAndPriority(t *testing.T) {
	body := []byte(`{"prompt_cache_key":"client-cache"}`)
	id := ExtractSessionID(nil, body, nil)
	if id == "" || strings.Contains(id, "client-cache") {
		t.Fatal("cache identity is absent or retains the raw cache key")
	}
	for _, raw := range []string{`{}`, `{"prompt_cache_key":""}`, `{"prompt_cache_key":null}`, `{"prompt_cache_key":4}`, `{"prompt_cache_key":["client-cache"]}`, `{"input":[{"arguments":{"prompt_cache_key":"client-cache"}}]}`} {
		if got := ExtractSessionID(nil, []byte(raw), nil); got != "" {
			t.Fatal("non-string, empty or tool cache data became an identity")
		}
	}
	if got := ExtractSessionID(nil, []byte(`{"prompt_cache_key":" client-cache "}`), nil); got == id {
		t.Fatal("cache identity discarded significant whitespace")
	}
	for _, options := range []core.Options{
		{Headers: http.Header{"Session-Id": {"explicit"}}, OriginalRequest: body},
		{Headers: http.Header{"X-Session-Id": {"explicit"}}, OriginalRequest: body},
		{OriginalRequest: []byte(`{"conversation_id":"explicit","prompt_cache_key":"client-cache"}`)},
		{OriginalRequest: []byte(`{"metadata":{"user_id":"explicit"},"prompt_cache_key":"client-cache"}`)},
	} {
		if got := ExtractSessionID(options.Headers, options.OriginalRequest, nil); got == id || got == "" {
			t.Fatal("cache fallback overrode a pre-existing explicit identifier")
		}
	}
	identity, ok := session.ExtractExplicitIdentity(nil, body, "generated-transport-id")
	if !ok || identity.SessionID != id || identity.ParentSessionID != "" || identity.IsFork || identity.IsSubagent {
		t.Fatal("cache key was masked by transport identity or invented parentage")
	}
}

func TestPromptCacheAffinityWorksWithoutOriginalRequestAndStaysOptional(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, operation := range []string{"execute", "count", "stream"} {
			t.Run(operation+"/"+map[bool]string{false: "off", true: "on"}[enabled], func(t *testing.T) {
				var selector Selector = &RoundRobinSelector{}
				if enabled {
					sticky := NewSessionAffinitySelector(selector)
					t.Cleanup(sticky.Stop)
					selector = sticky
				}
				manager := NewManager(nil, selector, nil)
				manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: enabled}})
				executor := &requestLimitOperationExecutor{}
				manager.RegisterExecutor(executor)
				for _, id := range []string{"a", "b"} {
					if _, err := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: id, Provider: "test"}); err != nil {
						t.Fatal(err)
					}
				}
				for _, input := range []string{"first", "different"} {
					request := core.Request{Payload: []byte(`{"prompt_cache_key":"shared-cache","input":"` + input + `"}`)}
					var err error
					switch operation {
					case "execute":
						_, err = manager.Execute(t.Context(), []string{"test"}, request, core.Options{})
					case "count":
						_, err = manager.ExecuteCount(t.Context(), []string{"test"}, request, core.Options{})
					default:
						var stream *core.StreamResult
						stream, err = manager.ExecuteStream(t.Context(), []string{"test"}, request, core.Options{})
						if stream != nil {
							for chunk := range stream.Chunks {
								if chunk.Err != nil {
									err = chunk.Err
								}
							}
						}
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				want := []string{"a", "b"}
				if enabled {
					want[1] = "a"
				}
				if got := executor.callIDs(); !reflect.DeepEqual(got, want) {
					t.Fatalf("selected credentials = %v, want %v", got, want)
				}
			})
		}
	}
}

func TestBasicAffinityIdentitySnapshotSurvivesPreparationAndBodyRelease(t *testing.T) {
	selector := NewSessionAffinitySelector(nil)
	t.Cleanup(selector.Stop)
	body := []byte(`{"prompt_cache_key":"before"}`)
	original := core.Options{Metadata: map[string]any{"existing": true}}
	prepared := selector.withBasicAffinityIdentity(t.Context(), core.Request{Payload: body}, original)
	want := ExtractSessionID(nil, body, nil)
	copy(body, bytes.ReplaceAll(body, []byte("before"), []byte("after!")))
	prepared.OriginalRequest = nil
	if got, _ := selector.sessionIDs(t.Context(), prepared); got != want {
		t.Fatal("preparation or released body changed the captured client cache key")
	}
	if len(original.Metadata) != 1 {
		t.Fatal("capturing identity mutated caller options")
	}
	strict := false
	manager := NewManager(nil, selector, nil)
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityFailover: &strict}})
	if !manager.strictSessionAffinityForRequest(t.Context(), core.Request{}, prepared) {
		t.Fatal("body release removed the strict session constraint")
	}
	absent := selector.withBasicAffinityIdentity(t.Context(), core.Request{Payload: []byte(`{}`)}, core.Options{})
	absent.OriginalRequest = []byte(`{"prompt_cache_key":"automatic"}`)
	if got, _ := selector.sessionIDs(t.Context(), absent); got != "" {
		t.Fatal("an automatically generated key became client affinity")
	}
}

func TestBasicAffinityKeepsSharedExplicitIDsAcrossClientKeys(t *testing.T) {
	selector := NewSessionAffinitySelector(&FillFirstSelector{})
	t.Cleanup(selector.Stop)
	for _, options := range []core.Options{
		{Headers: http.Header{"Session-Id": {"shared-session"}}},
		{Headers: make(http.Header), OriginalRequest: []byte(`{"prompt_cache_key":"shared-cache"}`)},
	} {
		first := affinityCallerContext(t, "client-a", "codex")
		second := affinityCallerContext(t, "client-b", "codex")
		selector.BindSession(first, "codex", "model", options, "b")
		captured := selector.withBasicAffinityIdentity(second, core.Request{}, options)
		picked, err := selector.Pick(second, "codex", "model", captured, []*Auth{{ID: "a"}, {ID: "b"}})
		if err != nil || picked == nil || picked.ID != "b" {
			t.Fatal("basic affinity unexpectedly separated identical IDs by client key", err)
		}
	}
}
