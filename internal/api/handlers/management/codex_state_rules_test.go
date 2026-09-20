package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestStateRulePreviewAndManualAcquisitionUseSamePolicy(t *testing.T) {
	manager := auth.NewManager(nil, nil, nil)
	credential, err := manager.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "rule-account", FileName: "rule-account.json", Provider: "codex", Attributes: map[string]string{"priority": "4"}, Metadata: map[string]any{"access_token": "must-not-leak"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: "alias", UpstreamID: "model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	var policy config.CodexStateOverrideConfig
	if err := json.Unmarshal([]byte(`{"enabled":true,"rules":[{"id":"specific","name":"Special","credentials":["rule-account"],"models":["alias"],"settings":{"lengths":[332],"retry-seconds":2,"proxy-mode":"custom","proxy-url":"http://user:proxy-secret@fixture:80","acquisition":"manual"}},{"id":"generic","priorities":[4],"settings":{"lengths":[292]}}]}`), &policy); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: policy}}
	handler := &Handler{cfg: cfg, authManager: manager}
	codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	t.Cleanup(func() { codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil); codexstate.Default.Wait() })
	body, _ := json.Marshal(map[string]any{"name": credential.Index, "model": "alias", "config": policy})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state/preview", strings.NewReader(string(body)))
	handler.PreviewCodexState(c)
	if w.Code != 200 || gjson.Get(w.Body.String(), "match.rule_id").String() != "specific" || gjson.Get(w.Body.String(), "policy.lengths.0").Int() != 332 || !gjson.Get(w.Body.String(), "managed").Bool() {
		t.Fatalf("preview=%d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "proxy-secret") || strings.Contains(w.Body.String(), "must-not-leak") || len(codexstate.Default.Snapshots(credential.ID, time.Now())) != 0 {
		t.Fatal("preview leaked credentials or modified runtime")
	}
	codexstate.Default.Sync(policy, helps.ManagedStateModels(cfg, credential))
	action, _ := json.Marshal(map[string]string{"name": credential.ID, "model": "alias", "action": "acquire"})
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state", strings.NewReader(string(action)))
	handler.CodexStateAction(c)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	codexstate.Default.Tick(t.Context(), time.Now(), func(_ context.Context, c codexstate.Credential, p config.CodexStateOverrideConfig) (codexstate.Result, error) {
		if len(p.Lengths) != 1 || p.Lengths[0] != 332 || p.RetrySeconds != 2 {
			t.Error("manual acquisition disagrees with preview")
		}
		return codexstate.Result{Completed: true, Model: c.Model, State: strings.Repeat("s", 332)}, nil
	})
	codexstate.Default.Wait()
	headers := http.Header{}
	if err := helps.ApplyManagedState(t.Context(), cfg, credential, "model", headers); err != nil || len(headers.Get("X-Codex-Turn-State")) != 332 {
		t.Fatal("request guard disagrees with acquisition", err)
	}
	snapshot := codexstate.Default.Snapshots(credential.ID, time.Now())[0]
	if snapshot.RuleID != "specific" || snapshot.AllowedLengths[0] != 332 {
		t.Fatal("card snapshot lost rule metadata")
	}
	// First-match skip also prevents explicit manual acquisition, including aliases.
	(*cfg.Codex.StateOverride.Rules)[0].Action = "skip"
	if err := handler.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	codexstate.Default.Sync(cfg.Codex.StateOverride, helps.ManagedStateModels(cfg, credential))
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state", strings.NewReader(string(action)))
	handler.CodexStateAction(c)
	if w.Code != 400 {
		t.Fatal("manual action bypassed skip rule", w.Code)
	}
}

func TestStateModelOverridePreviewMatchesAcquisition(t *testing.T) {
	m := auth.NewManager(nil, nil, nil)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "nested-state", Provider: "codex", Attributes: map[string]string{"priority": "3"}})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "alias", UpstreamID: "sol"}})
	defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	var cfg config.CodexStateOverrideConfig
	if err := json.Unmarshal([]byte(`{"enabled":true,"max-retry-rounds":2,"rules":[{"id":"main","priorities":[3],"models":["sol"],"settings":{"acquisition":"manual","retry-round-interval-minutes":60},"model-overrides":[{"id":"sol","models":["sol"],"settings":{"match-model":false,"invalidate-on-model-mismatch":false,"max-retry-rounds":0}}]}]}`), &cfg); err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{Codex: config.CodexConfig{StateOverride: cfg}}, authManager: m}
	body, _ := json.Marshal(map[string]any{"name": a.ID, "model": "alias", "config": cfg})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("POST", "/auth-files/codex/state/preview", strings.NewReader(string(body)))
	h.PreviewCodexState(c)
	if w.Code != 200 || gjson.Get(w.Body.String(), "match.model_override_id").String() != "sol" || gjson.Get(w.Body.String(), "policy.match-model").Bool() || gjson.Get(w.Body.String(), "match.sources.match-model").String() != "model-override" {
		t.Fatalf("preview lost nested policy: %s", w.Body.String())
	}
	if gjson.Get(w.Body.String(), "policy.max-retry-rounds").Int() != 0 || gjson.Get(w.Body.String(), "policy.retry-round-interval-minutes").Int() != 60 || gjson.Get(w.Body.String(), "match.sources.max-retry-rounds").String() != "model-override" {
		t.Fatal("preview lost retry round inheritance")
	}
	codexstate.Diagnostic.Sync(cfg, nil, helps.StateCredential(a, ""))
	defer codexstate.Diagnostic.Sync(config.CodexStateOverrideConfig{}, nil)
	credential := helps.StateCredential(a, "sol")
	credential.Route = "alias"
	codexstate.Diagnostic.QueueManual(credential)
	codexstate.Diagnostic.Tick(t.Context(), time.Now(), func(_ context.Context, _ codexstate.Credential, p config.CodexStateOverrideConfig) (codexstate.Result, error) {
		if *p.MatchModel || p.InvalidateOnModelMismatch || p.MaxRetryRounds != 0 || p.RetryRoundIntervalMinutes != 60 {
			t.Error("manual acquisition ignored model override")
		}
		return codexstate.Result{Completed: true, Model: "other", State: strings.Repeat("s", 292)}, nil
	})
	codexstate.Diagnostic.Wait()
	if codexstate.Diagnostic.Snapshots(a.ID, time.Now())[0].Status != "valid" {
		t.Fatal("acquisition rejected explicitly allowed model mismatch")
	}
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/auth-files/codex/state/options", nil)
	h.GetCodexStateOptions(c)
	if !gjson.Get(w.Body.String(), "features.rule_model_overrides").Bool() || !gjson.Get(w.Body.String(), "features.state_retry_rounds").Bool() {
		t.Fatal("capability missing")
	}
}
