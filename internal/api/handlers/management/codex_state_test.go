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
)

func TestCodexStateManualAcquisitionUsesBackendPolicyAndAlias(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{
		Enabled: true, Acquisition: "manual", Models: []string{"model"}, ProxyMode: "custom", ProxyURL: "http://proxy-{12}:secret@example.test:8000", Prompt: "configured prompt", TTLMinutes: 90,
		PlanLengths: []config.CodexStatePlanLengths{{PlanTypes: []string{"business"}, Lengths: []int{332}}},
	}}}
	m := auth.NewManager(nil, nil, nil)
	a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: "manual-state.json", FileName: "manual-state.json", Provider: "codex", Metadata: map[string]any{"account_id": "account", "plan_type": "ChatGPTBusinessPlan"}})
	if err != nil {
		t.Fatal(err)
	}
	r := registry.GetGlobalRegistry()
	r.RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "friendly-one", UpstreamID: "model"}, {ID: "friendly-two", UpstreamID: "model"}, {ID: "outside"}})
	defer r.UnregisterClient(a.ID)
	codexstate.Default.Sync(cfg.Codex.StateOverride, helps.ManagedStateModels(cfg, a))
	defer codexstate.Default.Sync(config.CodexStateOverrideConfig{}, nil)
	h := &Handler{cfg: cfg, authManager: m}
	run := func(model string) (int, map[string]json.RawMessage) {
		body, _ := json.Marshal(map[string]string{"name": a.FileName, "model": model, "action": "acquire"})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state", strings.NewReader(string(body)))
		h.CodexStateAction(c)
		var result map[string]json.RawMessage
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return w.Code, result
	}
	status, response := run("friendly-two")
	if status != 200 || string(response["model"]) != `"model"` || string(response["previous_acquired"]) != "0" {
		t.Fatalf("alias action=%d %+v", status, response)
	}
	if snapshot := codexstate.Default.Snapshots(a.ID, time.Now())[0]; snapshot.Status != "queued" || snapshot.Attempts != 0 {
		t.Fatalf("not queued: %+v", snapshot)
	}
	probe := func(_ context.Context, c codexstate.Credential, policy config.CodexStateOverrideConfig) (codexstate.Result, error) {
		if c.Plan != "team" || c.Model != "model" || policy.Lengths[0] != 332 || policy.ProxyURL != cfg.Codex.StateOverride.ProxyURL || policy.Prompt != "configured prompt" || policy.TTLMinutes != 90 {
			t.Error("manual acquisition did not use the backend subscription policy")
		}
		return codexstate.Result{State: strings.Repeat("s", 332), Model: c.Model, Completed: true}, nil
	}
	codexstate.Default.Tick(t.Context(), time.Now(), probe)
	codexstate.Default.Wait()
	status, response = run("friendly-one")
	if status != 200 || string(response["previous_acquired"]) != "1" || codexstate.Default.Snapshots(a.ID, time.Now())[0].Status != "queued" {
		t.Fatal("old valid state was reported as a new acquisition")
	}
	codexstate.Default.Tick(t.Context(), time.Now(), probe)
	codexstate.Default.Wait()
	if status, _ := run("outside"); status != 400 {
		t.Fatal("out-of-scope model accepted")
	}
	cfg.Codex.StateOverride.Enabled = false
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if status, _ := run("model"); status != 400 {
		t.Fatal("disabled state acquisition accepted")
	}
}
