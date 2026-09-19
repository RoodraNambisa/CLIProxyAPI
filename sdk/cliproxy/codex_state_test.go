package cliproxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexStateAcquisitionUsesNativeResultAndDoesNotForwardOldState(t *testing.T) {
	state := strings.Repeat("a", 292)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Codex-Turn-State") != "" {
			t.Error("probe reused old state")
		}
		w.Header().Set("X-Codex-Turn-State", state)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"model\":\"actual-model\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"OK\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}}\n\n")
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true}}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	exec := executor.NewCodexExecutor(cfg)
	m.RegisterExecutor(exec)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "state-acquire", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "header:X-Codex-Turn-State": "old"}, Metadata: map[string]any{"access_token": "fixture-token", "account_id": "fixture-account"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "requested-model"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	s := &Service{cfg: cfg, coreManager: m}
	c := helps.StateCredential(a, "requested-model")
	c.Route = c.Model
	result, err := s.acquireCodexState(t.Context(), c, cfg.Codex.StateOverride.Resolved())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Completed || result.State != state || result.Model != "actual-model" || result.Answer != "OK" || result.Tokens != 3 {
		t.Fatalf("incorrect raw result: %+v", result)
	}
	if stored, _ := m.GetByID(a.ID); stored.Attributes["header:X-Codex-Turn-State"] != "old" {
		t.Fatal("probe mutated credential headers")
	}
}

func TestCodexStateDiagnosticOutsideScopeEndToEnd(t *testing.T) {
	for _, tc := range []struct {
		name   string
		policy config.CodexStateOverrideConfig
	}{
		{"credential", config.CodexStateOverrideConfig{Enabled: true, Priorities: []int{4}}},
		{"model", config.CodexStateOverrideConfig{Enabled: true, Models: []string{"other"}}},
		{"empty rules", config.CodexStateOverrideConfig{Enabled: true, Rules: &[]config.CodexStateRule{}}},
		{"skip rule", config.CodexStateOverrideConfig{Enabled: true, Rules: &[]config.CodexStateRule{{Action: "skip"}}}},
		{"disabled", config.CodexStateOverrideConfig{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const model = "gpt-5.6-luna"
			state := strings.Repeat("d", 292)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var body struct {
					Model string `json:"model"`
				}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Model != model || r.Header.Get("X-Codex-Turn-State") != "" {
					t.Error("acquisition lost alias resolution or forwarded old State")
				}
				w.Header().Set("X-Codex-Turn-State", state)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"model\":%q,\"status\":\"completed\",\"output\":[]}}\n\n", model)
			}))
			defer server.Close()
			tc.policy.Acquisition, tc.policy.ProxyMode = "all", "direct"
			cfg := &config.Config{Codex: config.CodexConfig{StateOverride: tc.policy}}
			m := auth.NewManager(nil, nil, nil)
			m.SetConfig(cfg)
			m.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: model, Alias: "friendly-luna"}}})
			a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "diagnostic-outside", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "header:X-Codex-Turn-State": "old"}, Metadata: map[string]any{"access_token": "fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			r := registry.GetGlobalRegistry()
			r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "friendly-luna", UpstreamID: model}})
			defer r.UnregisterClient(a.ID)
			s := &Service{cfg: cfg, coreManager: m}
			s.syncCodexState(cfg)
			defer func() {
				codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
				codexstate.Diagnostic.Sync(config.CodexStateOverrideConfig{}, nil)
				codexstate.Default.Wait()
				codexstate.Diagnostic.Wait()
			}()
			c := helps.StateCredential(a, model)
			c.Route = "friendly-luna"
			if ok, _ := codexstate.Diagnostic.QueueManual(c); !ok {
				t.Fatal("outside-scope manual test rejected")
			}
			s.syncCodexState(cfg)
			codexstate.Diagnostic.Tick(t.Context(), time.Now(), s.acquireCodexState)
			codexstate.Diagnostic.Wait()
			s.syncCodexState(cfg)
			if snapshots := codexstate.Diagnostic.Snapshots(a.ID, time.Now()); len(snapshots) != 1 || snapshots[0].Status != "valid" {
				t.Fatalf("scope synchronization removed or rejected diagnostic: %+v", snapshots)
			}
			ctx := helps.WithCodexStateDiagnostic(core.WithCodexStateSnapshot(t.Context()), "acquired", "", nil)
			headers := http.Header{}
			if err := helps.ApplyManagedState(ctx, cfg, a, model, headers); err != nil || headers.Get("X-Codex-Turn-State") != state {
				t.Fatalf("outside-scope reuse failed: %v", err)
			}
			headers = http.Header{}
			if err := helps.ApplyManagedState(t.Context(), cfg, a, model, headers); err != nil || headers.Get("X-Codex-Turn-State") != "" || len(codexstate.Default.Snapshots(a.ID, time.Now())) != 0 {
				t.Fatal("diagnostic State leaked into normal request scope")
			}
			codexstate.Diagnostic.Tick(t.Context(), time.Now().Add(24*time.Hour), s.acquireCodexState)
			codexstate.Diagnostic.Wait()
			if calls.Load() != 1 {
				t.Fatal("one-shot acquisition automatically renewed")
			}
			a.Disabled = true
			if _, err := m.Update(auth.WithSkipPersist(t.Context()), a); err != nil {
				t.Fatal(err)
			}
			s.syncCodexState(cfg)
			if len(codexstate.Diagnostic.Snapshots(a.ID, time.Now())) != 0 {
				t.Fatal("disabled credential retained diagnostic")
			}
		})
	}
}

func TestCodexStateManualUnregisteredAcquisitionAndDiagnosticReuse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Codex-Turn-State", strings.Repeat("s", 292))
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"fixture\",\"model\":\"gpt-6-astra\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	defer server.Close()
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{Enabled: true, Models: []string{"gpt-6-astra"}, Priorities: []int{3}, Acquisition: "all", ProxyMode: "direct"}}}
	m := auth.NewManager(nil, nil, nil)
	m.SetConfig(cfg)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "manual-unregistered", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "priority": "3"}, Metadata: map[string]any{"access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{cfg: cfg, coreManager: m}
	s.syncCodexState(cfg)
	t.Cleanup(func() { codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil); codexstate.Default.Wait() })
	c := helps.StateCredential(a, "gpt-6-astra")
	c.Route = c.Model
	if ok, _ := codexstate.Default.QueueManual(c); !ok {
		t.Fatal("manual model rejected")
	}
	s.syncCodexState(cfg)
	codexstate.Default.Tick(t.Context(), time.Now(), s.acquireCodexState)
	codexstate.Default.Wait()
	s.syncCodexState(cfg)
	if models := registry.GetGlobalRegistry().GetModelsForClient(a.ID); len(models) != 0 {
		t.Fatal("probe registered a model")
	}
	if state := codexstate.Default.Snapshots(a.ID, time.Now()); len(state) != 1 || state[0].Status != "valid" || !state[0].ManualOnly {
		t.Fatalf("manual acquisition failed: %+v", state)
	}
	for _, managed := range []bool{false, true} {
		ctx := core.WithCodexStateSnapshot(t.Context())
		if managed {
			ctx = helps.WithCodexStateDiagnostic(ctx, "managed", "", nil)
		}
		headers := http.Header{}
		if err := helps.ApplyManagedState(ctx, cfg, a, c.Model, headers); err != nil {
			t.Fatal(err)
		}
		if (headers.Get("X-Codex-Turn-State") != "") != managed {
			t.Fatal("manual state leaked to normal business scope or could not be reused")
		}
	}
	cfg.Codex.StateOverride.Priorities = []int{0}
	s.syncCodexState(cfg)
	if len(codexstate.Default.Snapshots(a.ID, time.Now())) != 0 {
		t.Fatal("scope removal retained manual pair")
	}
}
