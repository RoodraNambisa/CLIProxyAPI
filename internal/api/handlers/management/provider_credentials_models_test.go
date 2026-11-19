package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestProviderCredentialDiscoveryProxyRoutes(t *testing.T) {
	catalog := func(id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health" {
				w.WriteHeader(http.StatusOK)
				return
			}
			if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer draft-key" {
				t.Errorf("unexpected model request at %s", r.URL.Path)
			}
			_, _ = w.Write([]byte(`{"data":[{"id":"` + id + `"}]}`))
		}))
	}
	upstream, proxy := catalog("direct-catalog"), catalog("proxy-catalog")
	defer upstream.Close()
	defer proxy.Close()
	direct := config.ProxyRuleTargetConfig{Direct: true}
	pool := config.ProxyRuleTargetConfig{Pool: "runtime"}
	for _, tc := range []struct {
		name, global, explicit string
		zeroPriority           bool
		target                 config.ProxyRuleTargetConfig
		rulePriority           int
		dead                   bool
		want                   string
		code                   int
	}{
		{name: "blank defaults to direct", want: "direct-catalog", code: 200},
		{name: "negative priority skips zero rule and inherits global", target: direct, rulePriority: 0, global: proxy.URL, want: "proxy-catalog", code: 200},
		{name: "zero priority direct rule needs no credential ID", target: direct, zeroPriority: true, rulePriority: 0, want: "direct-catalog", code: 200},
		{name: "matching direct rule works on unsaved credential", target: direct, rulePriority: -5, global: proxy.URL, want: "direct-catalog", code: 200},
		{name: "matching pool routes unsaved credential", target: pool, rulePriority: -5, want: "proxy-catalog", code: 200},
		{name: "unavailable required pool returns safe pool name", target: pool, rulePriority: -5, dead: true, want: `"proxy_pool":"runtime"`, code: 503},
		{name: "explicit direct overrides required pool", target: pool, rulePriority: -5, dead: true, explicit: "direct", global: proxy.URL, want: "direct-catalog", code: 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := proxy.URL
			if tc.dead {
				node = "http://user:proxy-marker@127.0.0.1:1"
			}
			cfg := proxyRuntimeTestConfig(node)
			cfg.ProxyURL = tc.global
			cfg.ProxyHealthCheck.Endpoints = []config.ProxyHealthCheckEndpointConfig{{Name: "local", URL: upstream.URL + "/health", Mode: "http-status"}}
			cfg.ProxyRules = nil
			if tc.target.Direct || tc.target.Pool != "" {
				cfg.ProxyRules = []config.ProxyRuleConfig{{Name: "catalog-route", Providers: []string{"codex"}, Priorities: []int{tc.rulePriority}, Targets: []config.ProxyRuleTargetConfig{tc.target}}}
			}
			pm := newProxyRuntimeTestManager(t, cfg)
			am := coreauth.NewManager(nil, nil, nil)
			am.SetProxyResolver(pm)
			h := &Handler{cfg: cfg, authManager: am, proxyPoolManager: pm}
			r := gin.New()
			r.POST("/models", h.DiscoverProviderCredentialModels)
			priority := -5
			if tc.zeroPriority {
				priority = 0
			}
			body, _ := json.Marshal(gin.H{"kind": "codex", "values": gin.H{"base-url": upstream.URL, "api-key": "draft-key", "priority": priority, "proxy-url": tc.explicit}})
			w := performProxyRuntimeRequest(t, r, http.MethodPost, "/models", string(body))
			if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.want) || strings.Contains(w.Body.String(), "proxy-marker") {
				t.Fatalf("want %d with %s, got %d %s", tc.code, tc.want, w.Code, w.Body.String())
			}
			if len(pm.SortedBindings()) != 0 {
				t.Fatal("model discovery persisted a draft binding")
			}
		})
	}
}

func TestProviderCatalogDraftUsesSelectedOpenAIKeyAndPriority(t *testing.T) {
	zero := 0
	group := config.OpenAICompatibility{
		Name: "my-channel", Priority: 5, BaseURL: "https://catalog.invalid",
		Headers:       map[string]string{"obsolete-header": "removed"},
		APIManagement: config.APIManagement{ModelsEndpoint: "https://parent.invalid/models"},
		APIKeyEntries: []config.OpenAICompatibilityAPIKey{
			{Disabled: true, APIKey: "disabled"},
			{APIKey: "selected", Priority: &zero, ProxyURL: "http://proxy.invalid:8080", APIManagement: config.APIManagement{
				RuntimeID: "selected-runtime", ModelsEndpoint: "https://child.invalid/models"}},
		},
	}
	tests := []struct {
		name            string
		parent          bool
		values          string
		priority        int
		proxy, endpoint string
	}{
		{"parent uses selected key override", true, `{"priority":-5}`, 0, "http://proxy.invalid:8080", "https://child.invalid/models"},
		{"edited child priority", false, `{"priority":-5,"proxy-url":"","api-key":""}`, -5, "", "https://child.invalid/models"},
		{"child restores inheritance", false, `{"priority":null,"models-endpoint":"","headers":{}}`, 5, "http://proxy.invalid:8080", "https://parent.invalid/models"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Config{OpenAICompatibility: []config.OpenAICompatibility{group}}
			refs := cfg.APICredentialRefs()
			ref := refs[2]
			if tc.parent {
				ref = refs[0]
			}
			var values map[string]json.RawMessage
			if err := json.Unmarshal([]byte(tc.values), &values); err != nil {
				t.Fatal(err)
			}
			fields, auth, err := providerCatalogDraft(ref, values)
			if err != nil || auth.ID != "selected-runtime" || auth.Provider != "my-channel" || auth.Attributes["priority"] != strconv.Itoa(tc.priority) ||
				fields.APIKey != "selected" || fields.ProxyURL != tc.proxy || fields.ModelsEndpoint != tc.endpoint {
				t.Fatalf("wrong effective catalog settings: %v, %+v", err, fields)
			}
			if values["headers"] != nil && len(fields.Headers) != 0 {
				t.Fatal("cleared headers remained in discovery")
			}
		})
	}
	group.APIKeyEntries[1].Priority = nil
	cfg := config.Config{OpenAICompatibility: []config.OpenAICompatibility{group}}
	_, a, err := providerCatalogDraft(cfg.APICredentialRefs()[0], map[string]json.RawMessage{"priority": []byte("-5")})
	if err != nil || a.Attributes["priority"] != "-5" {
		t.Fatal("parent edit did not reach inherited key priority", err)
	}
}

func TestProviderCatalogEditKeepsRuntimeBindingAndClearsSavedProxy(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"upstream key rejected"}`))
	}))
	defer upstream.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("model request used the old proxy at %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	cfg := proxyRuntimeTestConfig(proxy.URL)
	cfg.ProxyRules[0].Priorities = []int{5}
	cfg.ProxyHealthCheck.Endpoints = []config.ProxyHealthCheckEndpointConfig{{Name: "local", URL: upstream.URL + "/health", Mode: "http-status"}}
	cfg.CodexKey = []config.CodexKey{{BaseURL: upstream.URL, APIKey: "draft-key", ProxyURL: proxy.URL, Priority: 5,
		APIManagement: config.APIManagement{CredentialID: "catalog-card", RuntimeID: "runtime-card"}}}
	pm := newProxyRuntimeTestManager(t, cfg)
	_, err := pm.Resolve(t.Context(), &coreauth.Auth{ID: "runtime-card", Provider: "codex", Attributes: map[string]string{"priority": "5"}})
	if err != nil {
		t.Fatal(err)
	}
	before := pm.SortedBindings()
	if len(before) != 1 {
		t.Fatal("missing saved binding")
	}
	am := coreauth.NewManager(nil, nil, nil)
	am.SetProxyResolver(pm)
	h := &Handler{cfg: cfg, authManager: am}
	r := gin.New()
	r.POST("/credentials/:id/models", h.DiscoverProviderCredentialModels)
	w := performProxyRuntimeRequest(t, r, http.MethodPost, "/credentials/catalog-card/models", `{"values":{"priority":-5,"proxy-url":""}}`)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"status_code":401`) || !strings.Contains(w.Body.String(), "upstream key rejected") {
		t.Fatalf("expected the upstream auth response from the edited route, got %d %s", w.Code, w.Body.String())
	}
	if !reflect.DeepEqual(before, pm.SortedBindings()) || cfg.CodexKey[0].Priority != 5 || cfg.CodexKey[0].ProxyURL != proxy.URL {
		t.Fatal("a draft edit changed the credential or its binding")
	}
}
