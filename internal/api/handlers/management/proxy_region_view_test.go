package management

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestProxyRegionApplicationSeparatesPolicyAndSavedProxyValues(t *testing.T) {
	cfg := &config.Config{}
	cfg.ChatGPTWeb.Timezone = "UTC"
	cfg.ChatGPTWeb.BrowserRegion.Language = "en-US"
	manager := newProxyRuntimeTestManager(t, cfg)
	t.Cleanup(manager.Stop)
	authManager := coreauth.NewManager(nil, nil, nil)
	manager.SetAuthSource(authManager)
	authManager.SetProxyResolver(manager)
	auth, err := authManager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: "web", Provider: "chatgpt-web", ProxyURL: "http://example.invalid:80", Metadata: map[string]any{
		config.ProxyRegionMetadataKey: map[string]any{"timezone": "America/Toronto", "language": "en-CA"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: authManager, proxyPoolManager: manager}
	var queries atomic.Int32
	manager.IPRegions().SetObservationDemand(func(proxyregion.Route) { queries.Add(1) })
	view := h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.Value != "UTC" || view.Timezone.State != "not_following" || view.Timezone.ProxyValue != "America/Toronto" || view.Timezone.ProxySource != "credential_proxy" {
		t.Fatalf("off view: %+v", view)
	}
	cfg.ChatGPTWeb.BrowserRegion.AutoTimezone = true
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	view = h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.Value != "America/Toronto" || view.Timezone.State != "ready" || view.Language.Value != "en-US" || view.Language.State != "not_following" {
		t.Fatalf("independent flags: %+v", view)
	}
	cfg.ChatGPTWeb.BrowserRegion.AutoLanguage = true
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	view = h.proxyRegionApplication(t.Context(), auth, "")
	if view.Language.Value != "en-CA" || view.Language.Source != "credential_proxy" || view.Reason != "" || queries.Load() != 0 {
		t.Fatalf("manual preview: %+v calls=%d", view, queries.Load())
	}
	auth.Metadata[config.WebRegionMetadataKey] = map[string]any{"auto-timezone": false, "timezone": "Europe/Paris"}
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	view = h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.Value != "Europe/Paris" || view.Timezone.Source != "credential_settings" || view.Timezone.State != "not_following" {
		t.Fatalf("override: %+v", view)
	}
	if len(manager.SortedBindings()) != 0 {
		t.Fatal("preview allocated a binding")
	}
}

func TestProxyRegionBindingPageUsesNodeSourcesAndGuardsChangedBindings(t *testing.T) {
	cfg := proxyRuntimeTestConfig(newProxyRuntimeTraceProxy(t))
	cfg.ProxyPools[0].Region = config.ProxyRegionConfig{Timezone: "America/Toronto", Language: "en-CA"}
	cfg.ProxyPools[0].Entries[0].Region = config.ProxyRegionConfig{Timezone: "Europe/Paris"}
	cfg.Codex.TimeContext.Enabled = true
	manager := newProxyRuntimeTestManager(t, cfg)
	t.Cleanup(manager.Stop)
	authManager := coreauth.NewManager(nil, nil, nil)
	manager.SetAuthSource(authManager)
	authManager.SetProxyResolver(manager)
	auth := registerProxyRuntimeAuth(t, authManager, "codex")
	binding, err := manager.Resolve(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: authManager, proxyPoolManager: manager}
	view := h.proxyRegionApplication(t.Context(), auth, binding.BindingID)
	if view.Timezone.Value != "Europe/Paris" || view.Timezone.Source != "proxy_node" || view.Language.State != "not_applicable" {
		t.Fatalf("node view: %+v", view)
	}
	if changed := h.proxyRegionApplication(t.Context(), auth, "old-binding"); changed.Reason != "route_changed" || changed.Timezone.Value != "" {
		t.Fatalf("mixed old and new binding: %+v", changed)
	}
	router := proxyRuntimeTestRouter(h)
	router.GET("/proxy-ip/credentials", h.GetProxyRegionCredentials)
	router.POST("/proxy-ip/preview", h.PreviewProxyIP)
	router.POST("/proxy-ip/refresh", h.RefreshProxyIP)
	response := performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-bindings?paged=true&page=1&page_size=1", "")
	var page struct {
		Bindings   []proxyBindingRegionRow `json:"bindings"`
		Pagination proxyRegionPagination   `json:"pagination"`
	}
	decodeProxyRuntimeResponse(t, response, &page)
	if response.Code != 200 || len(page.Bindings) != 1 || page.Bindings[0].Application.Timezone.Value != "Europe/Paris" || page.Pagination.Total != 1 {
		t.Fatal(response.Code, response.Body.String())
	}
	unbound := registerProxyRuntimeAuth(t, authManager, "unbound")
	body, _ := json.Marshal(map[string]string{"auth_index": unbound.Index})
	response = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-ip/preview", string(body))
	if response.Code != 200 || !strings.Contains(response.Body.String(), `"reason":"route_unresolved"`) || !strings.Contains(response.Body.String(), `"kind":"configuration_preview"`) {
		t.Fatal(response.Code, response.Body.String())
	}
	response = performProxyRuntimeRequest(t, router, http.MethodPost, "/proxy-ip/refresh", string(body))
	if response.Code != 409 || len(manager.SortedBindings()) != 1 {
		t.Fatal("preview or refresh allocated an unbound route", response.Code)
	}
	response = performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-ip/credentials", "")
	var options struct {
		Credentials []proxyRegionCredentialOption `json:"credentials"`
		Pagination  proxyRegionPagination         `json:"pagination"`
	}
	decodeProxyRuntimeResponse(t, response, &options)
	if len(options.Credentials) != 1 || options.Credentials[0].ID != "codex" || options.Pagination.Total != 1 {
		t.Fatal(response.Body.String())
	}
	response = performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-ip/credentials?scope=all&search=unbound", "")
	decodeProxyRuntimeResponse(t, response, &options)
	if len(options.Credentials) != 1 || options.Credentials[0].ID != "unbound" {
		t.Fatal(response.Body.String())
	}
	for _, query := range []string{"scope=unknown", "page_size=100000", "page=0", "page=1&page=2"} {
		response = performProxyRuntimeRequest(t, router, http.MethodGet, "/proxy-ip/credentials?"+query, "")
		if response.Code != 400 {
			t.Fatalf("query %s status %d", query, response.Code)
		}
	}
}

func TestProxyRegionCredentialFilter100000With5000Bindings(t *testing.T) {
	summaries := make([]coreauth.ProxyCredentialSummary, 100000)
	bound := make(map[string]proxypool.BindingStatus, 5000)
	for i := range summaries {
		id := fmt.Sprintf("auth-%06d", i)
		summaries[i] = coreauth.ProxyCredentialSummary{ID: id, Index: id, Name: id + ".json", Provider: "chatgpt-web", Disabled: i%2 == 0, BindingOwnerID: id}
		if i < 5000 {
			bound[id] = proxypool.BindingStatus{Pool: "canada", Entry: "node", AuthID: id}
		}
	}
	options := filterProxyRegionCredentials(summaries, bound, "bound", "chatgpt-web", "")
	if len(options) != 5000 {
		t.Fatal(len(options))
	}
	start, end, page := proxyRegionPageBounds(250, 20, len(options))
	if page.TotalPages != 250 || len(options[start:end]) != 20 {
		t.Fatal(page)
	}
	data, _ := json.Marshal(options[start:end])
	if len(data) > 12000 {
		t.Fatal("page payload unexpectedly large", len(data))
	}
	if len(filterProxyRegionCredentials(summaries, bound, "bound", "chatgpt-web", "canada")) != 5000 {
		t.Fatal("pool search lost matches")
	}
	if got := filterProxyRegionCredentials(summaries, bound, "bound", "chatgpt-web", "auth-004999"); len(got) != 1 || got[0].ID != "auth-004999" {
		t.Fatal(got)
	}
	linked := []coreauth.ProxyCredentialSummary{{ID: "linked-web", Provider: "chatgpt-web", BindingOwnerID: "auth-004999"}}
	if got := filterProxyRegionCredentials(linked, bound, "bound", "chatgpt-web", "canada"); len(got) != 1 || got[0].Pool != "canada" {
		t.Fatal("linked consumer missing from bound filter", got)
	}
}

func TestProxyRegionCodexDisabledFixedAndFallback(t *testing.T) {
	cfg := &config.Config{}
	manager := newProxyRuntimeTestManager(t, cfg)
	t.Cleanup(manager.Stop)
	authManager := coreauth.NewManager(nil, nil, nil)
	manager.SetAuthSource(authManager)
	authManager.SetProxyResolver(manager)
	auth := registerProxyRuntimeAuth(t, authManager, "codex")
	h := &Handler{cfg: cfg, authManager: authManager, proxyPoolManager: manager}
	view := h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.State != "disabled" || view.Timezone.Value != "" {
		t.Fatal(view)
	}
	cfg.Codex.TimeContext = config.CodexTimeContextConfig{Enabled: true, Mode: "manual", Timezone: "Asia/Tokyo"}
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	view = h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.State != "fixed" || view.Timezone.Source != "provider_settings" || view.Timezone.Value != "Asia/Tokyo" {
		t.Fatal(view)
	}
	cfg.Codex.TimeContext = config.CodexTimeContextConfig{Enabled: true, Mode: "proxy", LookupFailure: "fallback", FallbackTimezone: "Europe/Paris"}
	if err := h.SetConfig(cfg); err != nil {
		t.Fatal(err)
	}
	view = h.proxyRegionApplication(t.Context(), auth, "")
	if view.Timezone.State != "fallback" || view.Timezone.Value != "Europe/Paris" {
		t.Fatal(view)
	}
	data, _ := json.Marshal(view)
	if !strings.Contains(string(data), "configuration_preview") {
		t.Fatal("preview missing provenance")
	}
}
