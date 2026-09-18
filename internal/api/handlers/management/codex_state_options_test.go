package management

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexStateOptionsUsesRegisteredCatalogWithoutRequiringEnabledState(t *testing.T) {
	m := auth.NewManager(nil, nil, nil)
	for _, fixture := range []struct {
		id, provider, priority string
		disabled, apiKey       bool
	}{
		{"codex-a", "codex", "", false, false}, {"codex-b", "codex", "3", false, false},
		{"codex-disabled", "codex", "5", true, false}, {"grok", "xai", "1", false, false}, {"api-key", "codex", "1", false, true},
	} {
		attrs := map[string]string{}
		if fixture.priority != "" {
			attrs["priority"] = fixture.priority
		}
		if fixture.apiKey {
			attrs["api_key"] = "secret-api-key"
		}
		a, err := m.Register(auth.WithSkipPersist(t.Context()), &auth.Auth{ID: fixture.id, FileName: fixture.id + ".json", Provider: fixture.provider, Attributes: attrs, Disabled: fixture.disabled, Metadata: map[string]any{"access_token": "secret-access", "plan_type": "ChatGPTBusinessPlan"}})
		if err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(a.ID, fixture.provider, []*registry.ModelInfo{{ID: "alias", UpstreamID: "upstream"}, {ID: "actual"}, {ID: "gpt-image-2"}, {ID: fixture.id + "-unique"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
	}
	h := &Handler{cfg: &config.Config{}, authManager: m}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/auth-files/codex/state/options", nil)
	h.GetCodexStateOptions(c)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var result struct {
		Credentials []codexStateCredentialOption `json:"credentials"`
		Models      []codexStateModelOption      `json:"models"`
		Priorities  []int                        `json:"priorities"`
		Plans       []string                     `json:"plans"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Credentials) != 3 || !reflect.DeepEqual(result.Priorities, []int{0, 3, 5}) || !reflect.DeepEqual(result.Plans, []string{"team"}) {
		t.Fatalf("wrong metadata: %+v", result)
	}
	if len(result.Models) != 4 {
		t.Fatalf("wrong catalog: %+v", result.Models)
	}
	if result.Models[1] != (codexStateModelOption{ID: "alias", UpstreamID: "upstream"}) {
		t.Fatal("alias relationship lost")
	}
	for _, bad := range []string{"secret-access", "secret-api-key", "gpt-image-2", "grok-unique", "codex-disabled-unique", "api-key-unique"} {
		if strings.Contains(w.Body.String(), bad) {
			t.Fatalf("unexpected field %q", bad)
		}
	}
}

func TestCodexStateProxyCheckExpandsTemplateWithoutSavingOrLeakingSecrets(t *testing.T) {
	var sessions []string
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(r.Header.Get("Proxy-Authorization"), "Basic "))
		if err != nil {
			t.Error(err)
		}
		parts := strings.Split(string(decoded), ":")
		if len(parts) != 2 || parts[1] != "fixture-secret" {
			t.Error("missing proxy authentication")
			return
		}
		fields := strings.Split(parts[0], "-")
		if len(fields) != 3 || !regexp.MustCompile(`^\d{12}$`).MatchString(fields[1]) || fields[1] != fields[2] {
			t.Error("placeholder was not expanded consistently")
			return
		}
		sessions = append(sessions, fields[1])
		if r.Header.Get("Authorization") != "" {
			t.Error("account credentials sent to trace endpoint")
		}
		_, _ = w.Write([]byte("ip=203.0.113.8\nloc=TH\n"))
	}))
	defer proxy.Close()
	previous := proxyTraceURL
	proxyTraceURL = "http://trace.invalid/cdn-cgi/trace"
	t.Cleanup(func() { proxyTraceURL = previous })
	proxyURL, _ := url.Parse(proxy.URL)
	template := "http://session-{12}-{12}:fixture-secret@" + proxyURL.Host
	cfg := &config.Config{Codex: config.CodexConfig{StateOverride: config.CodexStateOverrideConfig{ProxyURL: "saved-value"}}}
	h := &Handler{cfg: cfg}
	for range 2 {
		body, _ := json.Marshal(map[string]string{"proxy-url": template})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state/proxy/check", strings.NewReader(string(body)))
		h.CheckCodexStateProxy(c)
		var result proxyCheckResponse
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || !result.OK || result.IP != "203.0.113.8" || strings.Contains(w.Body.String(), "fixture-secret") {
			t.Fatalf("bad proxy result: %s", w.Body.String())
		}
	}
	if len(sessions) != 2 || sessions[0] == sessions[1] {
		t.Fatal("independent checks reused a session")
	}
	if cfg.Codex.StateOverride.ProxyURL != "saved-value" {
		t.Fatal("proxy test saved settings")
	}
	for _, invalid := range []string{"", "http://user-{0}:fixture-secret@localhost:80", "http://user-{65}:fixture-secret@localhost:80", "http://user-{random}:fixture-secret@localhost:80"} {
		body, _ := json.Marshal(map[string]string{"proxy-url": invalid})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/codex/state/proxy/check", strings.NewReader(string(body)))
		h.CheckCodexStateProxy(c)
		if w.Code != 400 || strings.Contains(w.Body.String(), "fixture-secret") {
			t.Fatalf("invalid proxy accepted or leaked: %s", w.Body.String())
		}
	}
}
