package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestXAIHeadersPreviewIsOfflineAndUsesUnsavedConfiguration(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(500) }))
	defer server.Close()
	cfg := &config.Config{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: "preview.json", FileName: "preview.json", Provider: "xai", Attributes: map[string]string{"api_key": "secret-token", "base_url": server.URL, "header:X-Override": "credential-secret"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	draft := config.XAIConfig{RequestCompression: "auto", DynamicHeaders: true, SpoofSessionIdentity: true, SessionIdentityPoolSize: 4, HeaderDefaults: config.XAIHeaderDefaults{ClientVersion: "1.0.52"}, Headers: map[string]string{"X-Override": "global-secret", "X-Dynamic": "$X-Example", "X-Missing": "$X-Absent"}}
	for _, purpose := range []string{"inference", "quota", "models", "device-login", "token-exchange"} {
		body, _ := json.Marshal(helps.XAIHeaderPreviewInput{Name: auth.FileName, Model: "grok-4.7", Purpose: purpose, Config: &draft, ClientHeaders: http.Header{"X-Example": {"client-secret"}}})
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("POST", "/auth-files/xai/headers/preview", strings.NewReader(string(body)))
		h.PreviewXAIHeaders(c)
		if w.Code != 200 {
			t.Fatalf("%s %d %s", purpose, w.Code, w.Body)
		}
		for _, secret := range []string{"secret-token", "global-secret", "credential-secret", "client-secret"} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatal("secret leaked in preview")
			}
		}
		var result helps.XAITransmission
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		if !result.Preview {
			t.Fatal("not marked preview")
		}
		if purpose == "inference" {
			found, missing, example := false, false, false
			for _, header := range result.Headers {
				switch header.Name {
				case "X-Override":
					found = header.Source == "credential-config"
				case "X-Missing":
					missing = header.Missing && header.Source == "dynamic-template"
				}
				example = example || header.Example
			}
			if !found || !missing || !example {
				t.Fatalf("sources: %+v", result.Headers)
			}
		}
	}
	current, _ := manager.GetByID(auth.ID)
	if calls.Load() != 0 || len(helps.XAIIdentitySeed(current)) != 0 || cfg.XAI.DynamicHeaders {
		t.Fatal("preview mutated runtime or contacted upstream")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/auth-files/xai/options", nil)
	h.GetXAIOptions(c)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"headers_preview":true`) || strings.Contains(w.Body.String(), "secret-token") {
		t.Fatal(w.Body.String())
	}
}

func TestXAIProbeIncludesSafeFinalHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(probeResponsesFixture))
	}))
	defer server.Close()
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{}
	manager.SetConfig(cfg)
	_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "grok.json", FileName: "grok.json", Provider: "xai", Attributes: map[string]string{"api_key": "do-not-disclose", "base_url": server.URL}})
	if err != nil {
		t.Fatal(err)
	}
	code, result := runModelProbe(t, &Handler{cfg: cfg, authManager: manager}, t.Context(), modelProbeRequest{Name: "grok.json", Model: "grok-4.6", Protocol: "responses", Stream: true})
	if code != 200 || !result.Success || len(result.XAIDiagnostics) != 1 || result.XAIDiagnostics[0].Status != 200 {
		t.Fatalf("probe %+v", result)
	}
	encoded, _ := json.Marshal(result.XAIDiagnostics)
	if strings.Contains(string(encoded), "do-not-disclose") {
		t.Fatal("token leaked")
	}
}
