package cliproxy

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
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
