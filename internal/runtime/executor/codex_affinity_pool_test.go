package executor

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type codexPoolCapture struct {
	authID      string
	prepared    codexPreparedSessionIdentity
	fingerprint codexConvergedFingerprint
}

// Run the real manager/preflight/pool path without sending upstream requests.
type codexPoolCaptureExecutor struct {
	*CodexExecutor
	calls        []codexPoolCapture
	preparations int
	failFirst    func()
}

func (e *codexPoolCaptureExecutor) PrepareProviderRequest(ctx context.Context, req core.Request, opts core.Options, operation core.RequestOperation) (any, error) {
	e.preparations++
	return e.CodexExecutor.PrepareProviderRequest(ctx, req, opts, operation)
}

func (e *codexPoolCaptureExecutor) Execute(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options) (core.Response, error) {
	prepared := e.codexPreparedSessionIdentity(ctx, req, opts)
	fingerprint, err := resolveCodexConvergedFingerprint(auth, prepared, prepared.ClientThreadID)
	if err != nil {
		return core.Response{}, err
	}
	// Simulate the upstream dispatch represented by this capture executor.
	core.MarkUpstreamAttempt(ctx)
	e.calls = append(e.calls, codexPoolCapture{auth.ID, prepared, fingerprint})
	if len(e.calls) == 1 && e.failFirst != nil {
		e.failFirst()
		return core.Response{}, statusErr{code: http.StatusInternalServerError, msg: "fixture upstream failure"}
	}
	return core.Response{Payload: []byte(`{"output":[]}`)}, nil
}

func (e *codexPoolCaptureExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	response, err := e.Execute(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: response.Payload}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func newCodexPoolCaptureManager(t *testing.T, sticky bool) (*coreauth.Manager, *codexPoolCaptureExecutor, string) {
	t.Helper()
	var selector coreauth.Selector = &coreauth.RoundRobinSelector{}
	if sticky {
		s := coreauth.NewSessionAffinitySelector(selector)
		t.Cleanup(s.Stop)
		selector = s
	}
	cfg := &config.Config{Routing: config.RoutingConfig{SessionAffinity: sticky}, CodexFingerprint: config.CodexFingerprintConfig{SessionIdentityPoolSize: 4}}
	manager := coreauth.NewManager(nil, selector, nil)
	manager.SetConfig(cfg)
	exec := &codexPoolCaptureExecutor{CodexExecutor: NewCodexExecutor(cfg)}
	manager.RegisterExecutor(exec)
	model := "pool-fixture-" + t.Name()
	for _, name := range []string{"a", "b"} {
		auth := prepareCodexFingerprintAuthForTest(t, exec.CodexExecutor, &coreauth.Auth{
			ID: model + "-" + name, Provider: "codex", Status: coreauth.StatusActive,
			Metadata: map[string]any{"access_token": "fixture", "account_id": name, "codex_fingerprint_mode": "session"},
		})
		if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(auth.ID, "codex", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	}
	return manager, exec, model
}

func runCodexPoolCapture(t *testing.T, manager *coreauth.Manager, exec *codexPoolCaptureExecutor, model, caller, body string, opts core.Options) codexPoolCapture {
	t.Helper()
	ctx := contextWithCodexTestAPIKey(caller)
	req := core.Request{Model: model, Payload: []byte(body)}
	opts.SourceFormat = sdktranslator.FormatOpenAIResponse
	if opts.Stream {
		stream, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range stream.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	} else if _, err := manager.Execute(ctx, []string{"codex"}, req, opts); err != nil {
		t.Fatal(err)
	}
	return exec.calls[len(exec.calls)-1]
}

func TestCodexAffinityPoolReusesRoutingSessionAcrossClientsAndConnections(t *testing.T) {
	for _, header := range []string{"Session-Id", "Session_id", "X-Session-Id"} {
		t.Run(header, func(t *testing.T) {
			manager, exec, model := newCodexPoolCaptureManager(t, true)
			var first codexPoolCapture
			for i := range 8 {
				headers := make(http.Header)
				headers.Set(header, "same-logical-session")
				thread := uuid.NewString()
				headers.Set("Thread-Id", thread)
				got := runCodexPoolCapture(t, manager, exec, model, fmt.Sprintf("caller-%d", i), fmt.Sprintf(`{"prompt_cache_key":"cache-%d","input":"turn %d"}`, i, i), core.Options{
					Headers: headers, Stream: i%2 == 1,
					Metadata: map[string]any{core.ExecutionSessionMetadataKey: fmt.Sprintf("connection-%d", i)},
				})
				if i == 0 {
					first = got
					continue
				}
				if got.authID != first.authID || got.fingerprint.sessionID != first.fingerprint.sessionID || got.prepared.AffinityDigest != first.prepared.AffinityDigest {
					t.Fatalf("stable routed session changed credential or pool slot on request %d", i)
				}
				if got.fingerprint.turnID == first.fingerprint.turnID || got.fingerprint.threadID != thread {
					t.Fatal("pool alignment changed independent turn/client thread identity")
				}
			}
		})
	}
}

func TestCodexAffinityPoolReusesCacheConversationAndInitialMessages(t *testing.T) {
	for _, fixture := range []struct {
		name, first, next string
	}{
		{"cache", `{"prompt_cache_key":"shared","input":"first"}`, `{"prompt_cache_key":"shared","input":"next"}`},
		{"conversation", `{"conversation_id":"shared","input":"first"}`, `{"conversation_id":"shared","input":"next"}`},
		{"messages", `{"messages":[{"role":"user","content":"first"}]}`, `{"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`},
		{"responses", `{"instructions":"system","input":"first"}`, `{"instructions":"system","input":[{"role":"user","content":"first"},{"role":"assistant","content":"answer"},{"role":"user","content":"next"}]}`},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			manager, exec, model := newCodexPoolCaptureManager(t, true)
			first := runCodexPoolCapture(t, manager, exec, model, "caller-a", fixture.first, core.Options{})
			next := runCodexPoolCapture(t, manager, exec, model, "caller-b", fixture.next, core.Options{Stream: true})
			if first.authID != next.authID || first.fingerprint.sessionID != next.fingerprint.sessionID || first.prepared.AffinityDigest != next.prepared.AffinityDigest {
				t.Fatal("continuation kept its credential but changed the pool affinity")
			}
			if first.fingerprint.turnID == next.fingerprint.turnID {
				t.Fatal("separate logical requests reused a turn ID")
			}
		})
	}
}

func TestCodexAffinityPoolKeepsFallbackWhenDisabledOrUnidentified(t *testing.T) {
	for _, sticky := range []bool{false, true} {
		t.Run(fmt.Sprint(sticky), func(t *testing.T) {
			manager, exec, model := newCodexPoolCaptureManager(t, sticky)
			first := runCodexPoolCapture(t, manager, exec, model, "caller", `{"instructions":"shared template"}`, core.Options{})
			next := runCodexPoolCapture(t, manager, exec, model, "caller", `{"instructions":"shared template"}`, core.Options{})
			if first.prepared.AffinityKind != "turn" || next.prepared.AffinityKind != "turn" || first.prepared.AffinityDigest == next.prepared.AffinityDigest || first.authID == next.authID {
				t.Fatal("unidentified requests acquired an implicit shared binding")
			}
			if !sticky {
				a := runCodexPoolCapture(t, manager, exec, model, "caller-a", `{"prompt_cache_key":"cache"}`, core.Options{})
				b := runCodexPoolCapture(t, manager, exec, model, "caller-b", `{"prompt_cache_key":"cache"}`, core.Options{})
				if a.prepared.AffinityKind != "prompt_cache_key" || a.prepared.TenantDigest == b.prepared.TenantDigest {
					t.Fatal("disabled routing affinity changed existing caller isolation")
				}
			}
		})
	}
}

func TestCodexAffinityPoolRespectsRateLimitAndRetainsAccountSlot(t *testing.T) {
	manager, exec, model := newCodexPoolCaptureManager(t, true)
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 1}})
	const body = `{"prompt_cache_key":"cache"}`
	first := runCodexPoolCapture(t, manager, exec, model, "caller", body, core.Options{})
	next := runCodexPoolCapture(t, manager, exec, model, "caller", body, core.Options{})
	if first.authID == next.authID || first.fingerprint.sessionID == next.fingerprint.sessionID {
		t.Fatal("shared pool affinity bypassed capacity or merged account pools")
	}
	_, err := manager.Execute(contextWithCodexTestAPIKey("caller"), []string{"codex"}, core.Request{Model: model, Payload: []byte(body)}, core.Options{})
	if err == nil || len(exec.calls) != 2 {
		t.Fatal("exhausted credential limits allowed another execution")
	}
	manager.SetConfig(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true}})
	returned := runCodexPoolCapture(t, manager, exec, model, "caller", body, core.Options{Metadata: map[string]any{core.PinnedAuthMetadataKey: first.authID}})
	if first.fingerprint.sessionID != returned.fingerprint.sessionID {
		t.Fatal("returning to an available prior account changed its slot")
	}
}

func TestCodexAffinityPoolSnapshotSurvivesRetryReleaseAndReload(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			manager, exec, model := newCodexPoolCaptureManager(t, true)
			manager.SetRetryConfig(0, 0, 2)
			body := []byte(`{"input":"first user input"}`)
			headers := make(http.Header)
			exec.failFirst = func() {
				clear(body)
				headers.Set("Session-Id", "late-mutated-session")
				manager.SetConfigAndSelector(&config.Config{}, &coreauth.RoundRobinSelector{})
			}
			runCodexPoolCapture(t, manager, exec, model, "caller", `{}`, core.Options{OriginalRequest: body, Headers: headers, Stream: stream})
			if exec.preparations != 1 || len(exec.calls) != 2 {
				t.Fatalf("preflight/attempts = %d/%d, want 1/2", exec.preparations, len(exec.calls))
			}
			a, b := exec.calls[0], exec.calls[1]
			if a.authID == b.authID || a.prepared.AffinityKind != "session_affinity" || a.prepared.AffinityDigest != b.prepared.AffinityDigest || a.prepared.TurnID != b.prepared.TurnID {
				t.Fatal("credential retry, body release or config reload changed the logical identity")
			}
			fresh := runCodexPoolCapture(t, manager, exec, model, "caller", `{"input":"first user input"}`, core.Options{})
			if fresh.prepared.AffinityKind != "turn" {
				t.Fatal("new request did not adopt disabled routing affinity")
			}
		})
	}
}

func TestCodexAffinityPoolDoesNotInferDisabledHistory(t *testing.T) {
	manager, exec, model := newCodexPoolCaptureManager(t, true)
	off := false
	selector := coreauth.NewSessionAffinitySelectorWithConfig(coreauth.SessionAffinityConfig{Fallback: &coreauth.RoundRobinSelector{}, UseHistory: &off})
	t.Cleanup(selector.Stop)
	manager.SetConfigAndSelector(&config.Config{Routing: config.RoutingConfig{SessionAffinity: true, SessionAffinityUseHistory: &off}}, selector)
	const body = `{"instructions":"shared","input":"same user input"}`
	first := runCodexPoolCapture(t, manager, exec, model, "caller", body, core.Options{})
	next := runCodexPoolCapture(t, manager, exec, model, "caller", body, core.Options{Stream: true})
	if first.authID == next.authID || first.prepared.AffinityKind != "turn" || next.prepared.AffinityKind != "turn" || first.prepared.AffinityDigest == next.prepared.AffinityDigest {
		t.Fatal("disabled history still supplied routing or fingerprint affinity")
	}
	cache := `{"prompt_cache_key":"explicit","input":"same user input"}`
	a := runCodexPoolCapture(t, manager, exec, model, "caller-a", cache, core.Options{})
	b := runCodexPoolCapture(t, manager, exec, model, "caller-b", cache, core.Options{Stream: true})
	if a.authID != b.authID || a.fingerprint.sessionID != b.fingerprint.sessionID || a.prepared.AffinityKind != "session_affinity" {
		t.Fatal("disabling history stopped explicit cache-key affinity")
	}
}
