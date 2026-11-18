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
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestProviderCredentialListPaginationAndSecretRedaction(t *testing.T) {
	h, m, dir := newAuthFilesPaginationTestHandler(t, true)
	registerAuthFilesPaginationTestAuth(t, m, dir, "oauth.json", "codex", false, "", 5, true, "plus")
	h.cfg.CodexKey = []config.CodexKey{{APIKey: "super-secret-marker", BaseURL: "https://example.invalid", Priority: 3, APIManagement: config.APIManagement{ChannelName: "Primary channel"}}}
	if _, err := synthesizer.PrepareManagedCredentials(h.cfg); err != nil {
		t.Fatal(err)
	}
	auths, err := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{Config: h.cfg, Now: time.Now(), IDGenerator: synthesizer.NewStableIDGenerator()})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range auths {
		if _, err = m.Register(coreauth.WithSkipPersist(context.Background()), a); err != nil {
			t.Fatal(err)
		}
	}
	r := gin.New()
	r.GET("/auth-files", h.ListAuthFiles)
	for _, test := range []struct {
		query string
		want  int
	}{{"?paged=true", 1}, {"?paged=true&include_config=true", 2}, {"?paged=true&include_config=true&source=api", 1}, {"?include_config=true&source=api", 1}, {"?paged=true&include_config=true&search=Primary", 1}} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/auth-files"+test.query, nil))
		var body struct {
			Files []map[string]any `json:"files"`
		}
		if json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body.Files) != test.want {
			t.Fatalf("%s: %d %s", test.query, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "super-secret-marker") {
			t.Fatal("list exposed upstream key")
		}
	}
}

func TestProviderCredentialPatchRotationAndConflict(t *testing.T) {
	cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "original-key", BaseURL: "https://example.invalid"}}}
	if _, err := synthesizer.PrepareManagedCredentials(cfg); err != nil {
		t.Fatal(err)
	}
	path := writeTestConfigFile(t)
	if err := config.SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: cfg, configFilePath: path}
	h.SetRuntimeConfigApplier(func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
		return config.RuntimeApplyResult{Applied: true}, nil
	})
	ref := cfg.APICredentialRefs()[0]
	id, index, rev := ref.Management.CredentialID, ref.Management.CredentialIndex, ref.Revision()
	r := gin.New()
	r.Use(h.ConfigMutationMiddleware())
	r.PATCH("/provider-credentials/:id", h.PatchProviderCredential)
	body, _ := json.Marshal(map[string]any{"revision": rev, "values": map[string]any{"channel-name": "Production", "api-key": "new-key", "base-url": "https://new.invalid", "priority": 5}})
	call := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPatch, "/provider-credentials/"+id, strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		return w
	}
	if w := call(); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(); w.Code != 409 {
		t.Fatal("stale edit accepted", w.Code, w.Body.String())
	}
	loaded, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	key := loaded.CodexKey[0]
	if key.CredentialID != id || key.CredentialIndex != index || key.APIKey != "new-key" || key.ChannelName != "Production" || key.Priority != 5 {
		t.Fatal("rotation lost identity or changes")
	}
}

func TestProviderCredentialDiscoveryUsesSelectedKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("Authorization") != "Bearer draft-key" {
			t.Errorf("incorrect model discovery: %s %s", r.URL.Path, r.Header.Get("Authorization"))
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"model-a"}]}`))
	}))
	defer upstream.Close()
	h := &Handler{cfg: &config.Config{}}
	r := gin.New()
	r.POST("/provider-credentials/models", h.DiscoverProviderCredentialModels)
	b, _ := json.Marshal(map[string]any{"kind": "codex", "values": map[string]any{"base-url": upstream.URL, "api-key": "draft-key"}})
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/provider-credentials/models", strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "model-a") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestProviderCredentialInheritanceAndParentKeyIdentity(t *testing.T) {
	zero := 0
	childModels := []config.OpenAICompatibilityModel{{Name: "child"}}
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "parent", BaseURL: "https://example.invalid", Priority: 5, Models: []config.OpenAICompatibilityModel{{Name: "shared"}}, APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "secret", Priority: &zero, Models: &childModels}}}}}
	if _, err := synthesizer.PrepareManagedCredentials(cfg); err != nil {
		t.Fatal(err)
	}
	refs := cfg.APICredentialRefs()
	parent, child := refs[0], refs[1]
	if err := applyProviderCredentialValues(child, map[string]json.RawMessage{"priority": []byte("null"), "models": []byte("null"), "models-mode": []byte("null")}); err != nil {
		t.Fatal(err)
	}
	key := child.Value.(*config.OpenAICompatibilityAPIKey)
	effective := config.EffectiveOpenAIEntry(parent.Parent, key)
	if key.Priority != nil || key.Models != nil || effective.Priority != 5 || effective.Models[0].Name != "shared" {
		t.Fatal("inheritance was not restored")
	}
	id, index := key.CredentialID, key.CredentialIndex
	payload, _ := json.Marshal([]map[string]any{{"credential-id": id, "credential-index": "tampered", "api-key": "rotated", "priority": 0}})
	if err := applyProviderCredentialValues(parent, map[string]json.RawMessage{"api-key-entries": payload}); err != nil {
		t.Fatal(err)
	}
	key = &parent.Parent.APIKeyEntries[0]
	if key.CredentialID != id || key.CredentialIndex != index || key.APIKey != "rotated" || key.Priority == nil || *key.Priority != 0 {
		t.Fatal("parent edit lost key identity or explicit zero")
	}
	forged, _ := json.Marshal([]map[string]any{{"credential-id": "foreign-id", "api-key": "secret"}})
	if err := applyProviderCredentialValues(parent, map[string]json.RawMessage{"api-key-entries": forged}); err == nil {
		t.Fatal("foreign child identity accepted")
	}
}

func TestProviderCredentialYAMLUploadPreparesStableCards(t *testing.T) {
	path := writeTestConfigFile(t)
	h := &Handler{cfg: &config.Config{}, configFilePath: path}
	h.SetRuntimeConfigApplier(func(_ context.Context, cfg *config.Config) (config.RuntimeApplyResult, error) {
		if len(cfg.CodexKey) != 1 || cfg.CodexKey[0].CredentialID == "" {
			t.Error("unprepared API credentials published")
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	})
	r := gin.New()
	r.Use(h.ConfigMutationMiddleware())
	r.PUT("/config.yaml", h.PutConfigYAML)
	body := "codex-api-key:\n  - api-key: sample\n    base-url: https://example.invalid\n    channel-name: Uploaded\n    future-option: retained\n"
	ids := []string{}
	for range 2 {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodPut, "/config.yaml", strings.NewReader(body)))
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		cfg, err := config.LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, cfg.CodexKey[0].CredentialID)
	}
	if ids[0] == "" || ids[0] != ids[1] {
		t.Fatal("YAML roundtrip changed identity")
	}
}
