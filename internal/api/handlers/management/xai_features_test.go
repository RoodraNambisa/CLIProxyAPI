package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	xaiauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestXAIPKCEManualCallbackBindingAndSingleConsumption(t *testing.T) {
	state := "xai-pkce-fixture"
	RegisterOAuthSession(state, "xai")
	defer CompleteOAuthSession(state)
	pending := &xaiPKCEPending{flow: &xaiauth.PKCEFlow{RedirectURI: "http://127.0.0.1:56121/callback"}, callback: make(chan oauthCallbackFilePayload, 1)}
	h := &Handler{xaiPKCE: map[string]*xaiPKCEPending{state: pending}}
	wrong := "http://127.0.0.1:9999/callback?state=" + state + "&code=fixture"
	if h.submitXAIPKCECallback(state, "fixture", "", wrong) == nil {
		t.Fatal("wrong redirect accepted")
	}
	if h.submitXAIPKCECallback(state, "fixture", "", "") != nil {
		t.Fatal("valid callback rejected")
	}
	if h.submitXAIPKCECallback(state, "fixture", "", "") == nil {
		t.Fatal("duplicate callback accepted")
	}
	if callback := <-pending.callback; callback.Code != "fixture" {
		t.Fatal("callback lost")
	}
}

func TestXAIModelRefreshKeepsCredentialDirectoryOnFailure(t *testing.T) {
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" || r.Header.Get("X-Global") != "inherited" || r.Header.Get("X-Exception") != "credential" {
			t.Error("model request did not inherit credential/global settings")
		}
		if fail.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"grok-catalog-fixture"}]}`))
	}))
	defer server.Close()
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{XAI: config.XAIConfig{Headers: map[string]string{"X-Global": "inherited", "X-Exception": "global"}}}
	manager.RegisterExecutor(runtimeexecutor.NewXAIExecutor(cfg))
	auth, err := manager.Register(t.Context(), &coreauth.Auth{ID: "xai-model-fixture.json", Provider: "xai", FileName: "xai-model-fixture.json", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture", "header:X-Exception": "credential"}, Metadata: map[string]any{"xai_identity_seed": "keep-this"}})
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, authManager: manager}
	for _, failure := range []bool{false, true} {
		fail.Store(failure)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/xai/models/refresh", strings.NewReader(`{"name":"xai-model-fixture.json"}`))
		h.RefreshXAIModels(c)
		var response struct {
			Models []struct {
				ID string `json:"id"`
			} `json:"models"`
			UsingCached bool `json:"using_cached"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Models) != 1 || response.Models[0].ID != "grok-catalog-fixture" || response.UsingCached != failure {
			t.Fatalf("refresh response: %d %s", w.Code, w.Body.String())
		}
	}
	current, _ := manager.GetByID(auth.ID)
	if helps.XAIModelsForAuth(current) == nil || current.Metadata["xai_identity_seed"] != "keep-this" {
		t.Fatal("catalog persistence discarded credential metadata")
	}
}

func TestXAIReloginKeepsSettingsAndSeparatesChangedAccounts(t *testing.T) {
	for _, subject := range []string{"same-account", "changed-account"} {
		t.Run(subject, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			file := xaiauth.CredentialFileName("fixture@x.ai", "same-account")
			previous, err := manager.Register(t.Context(), &coreauth.Auth{ID: file, FileName: file, Provider: "xai", ProxyURL: "http://proxy.invalid", Attributes: map[string]string{"header:X-Keep": "keep", "base_url": "https://eu-west-1.api.x.ai/v1"}, Metadata: map[string]any{"type": "xai", "base_url": "https://eu-west-1.api.x.ai/v1", "sub": "same-account", "headers": map[string]any{"X-Keep": "keep"}, "using_api": true, "websockets": true, helps.XAIIdentitySeedKey: "seed-fixture", helps.XAIModelCatalogKey: "catalog-fixture"}})
			if err != nil {
				t.Fatal(err)
			}
			state := "grok-relogin-" + subject
			RegisterOAuthSession(state, "xai")
			defer CompleteOAuthSession(state)
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			h.saveXAIAuthBundle(t.Context(), state, xaiauth.NewXAIAuth(nil), &xaiauth.AuthBundle{TokenData: xaiauth.TokenData{AccessToken: "new-fixture-token", RefreshToken: "new-refresh-fixture", Email: "fixture@x.ai", Subject: subject}})
			current, _ := manager.GetByID(file)
			if current.Metadata["access_token"] != "new-fixture-token" || current.Metadata["using_api"] != true || current.Metadata["websockets"] != true || current.Attributes["header:X-Keep"] != "keep" || current.ProxyURL != "http://proxy.invalid" || current.Metadata["base_url"] != "https://eu-west-1.api.x.ai/v1" || current.Attributes["base_url"] != "https://eu-west-1.api.x.ai/v1" {
				t.Fatal("relogin discarded credential settings or tokens")
			}
			if subject == "same-account" {
				if current.Metadata[helps.XAIIdentitySeedKey] != "seed-fixture" {
					t.Fatal("relogin changed the persistent identity")
				}
			} else if current.RuntimeInstanceID() == previous.RuntimeInstanceID() {
				t.Fatal("changed account retained the previous runtime instance")
			} else if current.Metadata[helps.XAIIdentitySeedKey] != nil || current.Metadata[helps.XAIModelCatalogKey] != nil {
				t.Fatal("changed account inherited identity or model privileges")
			}
		})
	}
}
