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
