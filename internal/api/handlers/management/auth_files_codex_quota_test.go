package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type codexQuotaViewExecutor struct {
	codexPlanRefreshTestExecutor
	observe func(http.Header)
}

func (e *codexQuotaViewExecutor) Execute(ctx context.Context, auth *coreauth.Auth, _ core.Request, _ core.Options) (core.Response, error) {
	observer := core.CodexQuotaObserverFromContext(ctx)
	if observer != nil {
		e.observe = func(headers http.Header) { observer(auth.ID, auth.RuntimeInstanceID(), "http", headers) }
	}
	return core.Response{Payload: []byte(`{"output":[]}`)}, nil
}

func TestCodexQuotaManagementListsReadLatestRuntimeObservation(t *testing.T) {
	h, manager, authDir := newAuthFilesPaginationTestHandler(t, true)
	cfg := &config.Config{AuthDir: authDir, Codex: config.CodexConfig{ObserveQuota: true}, RemoteManagement: config.RemoteManagement{AuthFilesPagination: config.AuthFilesPaginationConfig{Enabled: true}}}
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	manager.SetConfig(cfg)
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "quota.json", "codex", false, "", 0, false, "plus")
	path := filepath.Join(authDir, "quota.json")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient("quota.json", "codex", []*registry.ModelInfo{{ID: "quota-fixture"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient("quota.json") })
	executor := &codexQuotaViewExecutor{}
	manager.RegisterExecutor(executor)
	if _, err := manager.Execute(t.Context(), []string{"codex"}, core.Request{Model: "quota-fixture"}, core.Options{}); err != nil {
		t.Fatal(err)
	}
	if executor.observe == nil {
		t.Fatal("request did not install an observation sink")
	}
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	check := func(enabled bool, percent string) {
		t.Helper()
		for _, endpoint := range []string{"/auth-files", "/auth-files?paged=true&page_size=10"} {
			recorder := performAuthFilesPaginationRequest(router, endpoint)
			if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("quota list lost status or no-store contract")
			}
			var body struct {
				Files []struct {
					Enabled     bool                            `json:"quota_observation_enabled"`
					Observation *coreauth.CodexQuotaObservation `json:"quota_observation"`
				} `json:"files"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil || len(body.Files) != 1 {
				t.Fatalf("invalid auth list: %v", err)
			}
			file := body.Files[0]
			if file.Enabled != enabled || (file.Observation == nil) != (percent == "") {
				t.Fatal("wrong enabled state or historical observation visibility")
			}
			if percent != "" && (len(file.Observation.Signals) != 1 || file.Observation.Signals["X-Codex-Primary-Used-Percent"] != percent || file.Observation.Source != "http" || file.Observation.ObservedAt.IsZero()) {
				t.Fatal("list reused cached quota or exposed unfiltered response headers")
			}
		}
	}
	check(true, "")
	revision := manager.ManagementAuthCatalogRevision()
	executor.observe(http.Header{"X-Codex-Primary-Used-Percent": {"0"}, "Set-Cookie": {"private=fixture"}})
	if manager.ManagementAuthCatalogRevision() != revision {
		t.Fatal("quota observation invalidated the credential catalog cache")
	}
	check(true, "0")
	executor.observe(http.Header{"X-Codex-Primary-Used-Percent": {"25"}})
	check(true, "25")
	next := *cfg
	next.Codex.ObserveQuota = false
	if err := h.SetConfig(&next); err != nil {
		t.Fatal(err)
	}
	manager.SetConfig(&next)
	check(false, "25")
	current, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(current, original) {
		t.Fatal("passive observation or management reads changed the credential file")
	}
	if err := manager.Delete(coreauth.WithSkipPersist(t.Context()), "quota.json"); err != nil {
		t.Fatal(err)
	}
	registerAuthFilesPaginationTestAuth(t, manager, authDir, "quota.json", "codex", false, "", 0, false, "plus")
	executor.observe(http.Header{"X-Codex-Primary-Used-Percent": {"99"}})
	check(false, "")
}
