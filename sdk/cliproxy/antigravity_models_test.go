package cliproxy

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type antigravityModelRefreshExecutor struct {
	refreshes atomic.Int64
}

func (*antigravityModelRefreshExecutor) Identifier() string { return "antigravity" }

func (*antigravityModelRefreshExecutor) Execute(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*antigravityModelRefreshExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (e *antigravityModelRefreshExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	e.refreshes.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh-token"
	updated.Metadata["expired"] = time.Now().Add(time.Hour).Format(time.RFC3339)
	return updated, nil
}

func (*antigravityModelRefreshExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*antigravityModelRefreshExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestParseAntigravityModelCapabilityHintsAcceptsAuthoritativeEmptyList(t *testing.T) {
	hints, ok := parseAntigravityModelCapabilityHints([]byte(`{"webSearchModelIds":[]}`))
	if !ok {
		t.Fatal("valid empty webSearchModelIds should be authoritative")
	}
	if len(hints.WebSearchModelIDs) != 0 {
		t.Fatalf("web search model count = %d, want 0", len(hints.WebSearchModelIDs))
	}
	if _, okMissing := parseAntigravityModelCapabilityHints([]byte(`{"models":{}}`)); okMissing {
		t.Fatal("missing webSearchModelIds should not replace cached capability")
	}
}

func TestApplyAntigravityFetchedModelCapabilitiesRecomputesMembership(t *testing.T) {
	models := []*ModelInfo{{ID: "model-a", SupportsWebSearch: true}, {ID: "model-b"}}
	result := applyAntigravityFetchedModelCapabilities(models, antigravityModelCapabilityHints{
		WebSearchModelIDs: map[string]struct{}{"model-b": {}},
	})

	if result[0].SupportsWebSearch {
		t.Fatal("stale model-a capability was not cleared")
	}
	if !result[1].SupportsWebSearch {
		t.Fatal("model-b capability was not enabled")
	}
	if result[0].UpstreamID != "model-a" || result[1].UpstreamID != "model-b" {
		t.Fatalf("upstream IDs = (%q, %q), want original IDs", result[0].UpstreamID, result[1].UpstreamID)
	}
	if models[0].UpstreamID != "" || !models[0].SupportsWebSearch || models[1].UpstreamID != "" {
		t.Fatalf("source models were mutated: %#v", models)
	}

	result = applyAntigravityFetchedModelCapabilities(result, antigravityModelCapabilityHints{WebSearchModelIDs: map[string]struct{}{}})
	if result[0].SupportsWebSearch || result[1].SupportsWebSearch {
		t.Fatal("authoritative empty capability set did not clear all models")
	}
}

func TestFetchAntigravityModelCapabilityHintsDoesNotBypassInvalidProxy(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		requests.Add(1)
	}))
	defer server.Close()

	service := &Service{}
	auth := &coreauth.Auth{
		ProxyURL:   "://invalid-proxy",
		Attributes: map[string]string{"base_url": server.URL},
		Metadata:   map[string]any{"access_token": "token"},
	}
	if _, ok := service.fetchAntigravityModelCapabilityHintsForAuth(context.Background(), auth); ok {
		t.Fatal("invalid proxy unexpectedly produced capability hints")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("upstream request count = %d, want 0", got)
	}
}

func TestFetchAntigravityModelCapabilityHintsRefreshesRejectedAccessToken(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Header.Get("Authorization") != "Bearer fresh-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"webSearchModelIds":["gemini-3.1-flash-lite"]}`))
	}))
	defer server.Close()

	manager := coreauth.NewManager(nil, nil, nil)
	executor := &antigravityModelRefreshExecutor{}
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{
		ID:         "antigravity-expired-capability-token",
		Provider:   "antigravity",
		Attributes: map[string]string{"base_url": server.URL},
		Metadata: map[string]any{
			"access_token":  "expired-token",
			"refresh_token": "refresh-token",
			"expired":       time.Now().Add(-time.Hour).Format(time.RFC3339),
		},
	}
	installed, errRegister := manager.Register(context.Background(), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	service := &Service{coreManager: manager}
	hints, ok := service.fetchAntigravityModelCapabilityHintsForAuth(context.Background(), installed)
	if !ok {
		t.Fatal("capability discovery failed after refreshing the rejected token")
	}
	if _, exists := hints.WebSearchModelIDs["gemini-3.1-flash-lite"]; !exists {
		t.Fatalf("capability hints = %#v", hints.WebSearchModelIDs)
	}
	if got := executor.refreshes.Load(); got != 1 {
		t.Fatalf("refresh count = %d, want 1", got)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream request count = %d, want 2", got)
	}
	current, okCurrent := manager.GetByID(auth.ID)
	if !okCurrent || current.Metadata["access_token"] != "fresh-token" {
		t.Fatalf("refreshed auth was not persisted: %#v", current)
	}
}

func TestAntigravityModelBaseURLsMatchExecutorFallbackOrder(t *testing.T) {
	got := antigravityModelBaseURLs(nil)
	want := []string{antigravityModelBaseURLProd, antigravityModelBaseURLDaily, antigravityModelBaseURLDailySandbox}
	if len(got) != len(want) {
		t.Fatalf("base URL count = %d, want %d: %#v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("base URL %d = %q, want %q", i, got[i], want[i])
		}
	}
}
