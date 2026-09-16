package management

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialRoutingAliasPatchAndCollision(t *testing.T) {
	store := &memoryAuthStore{}
	manager := coreauth.NewManager(store, nil, nil)
	for _, name := range []string{"a.json", "b.json"} {
		if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: name, FileName: name, Provider: "xai", Metadata: map[string]any{"type": "xai"}}); err != nil {
			t.Fatal(err)
		}
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	patch := func(name, alias string, status int) {
		t.Helper()
		response := performAPIKeyConfigRequest(t, h.PatchAuthFileFields, http.MethodPatch, "/auth-files/fields", fmt.Sprintf(`{"name":%q,"routing_alias":%q}`, name, alias))
		if response.Code != status {
			t.Fatalf("alias patch: %d %s", response.Code, response.Body.String())
		}
	}
	before, _ := manager.GetByID("a.json")
	patch("a.json", " Grok-Test ", 200)
	selected, err := manager.ResolveCredentialTarget("GROK-TEST")
	if err != nil || selected.ID != "a.json" || selected.Index != before.Index {
		t.Fatalf("alias resolution: %v %v", selected, err)
	}
	patch("b.json", "grok-test", 409)
	patch("b.json", before.Index, 409)
	patch("a.json", "bad alias", 400)
	patch("a.json", "", 200)
	if _, err := manager.ResolveCredentialTarget("grok-test"); err == nil {
		t.Fatal("cleared alias remained active")
	}
	patch("b.json", "grok-test", 200)
	// Reloading stored metadata preserves the selected alias and ID.
	reloaded := coreauth.NewManager(store, nil, nil)
	if err := reloaded.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := reloaded.ResolveCredentialTarget("grok-test")
	if err != nil || loaded.ID != "b.json" {
		t.Fatalf("alias did not survive reload: %v", err)
	}
}

func TestCredentialTargetingConfigPersistsAndRenames(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("api-keys: [key-a]\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"key-a"}}}
	h := NewHandler(cfg, path, nil)
	for _, body := range []string{`{"api-key":"key-a","allow-credential-targeting":true}`, `{"api-key":"key-a","providers":["xai"]}`} {
		r := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/api-key-groups", body)
		if r.Code != 200 {
			t.Fatalf("patch: %d %s", r.Code, r.Body.String())
		}
	}
	r := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/api-keys", `{"old":"key-a","new":"key-b"}`)
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	if len(cfg.APIKeyGroups) != 1 || !cfg.APIKeyGroups[0].AllowCredentialTargeting || cfg.APIKeyGroups[0].APIKey != "key-b" || cfg.APIKeyGroups[0].Providers[0] != "xai" {
		t.Fatal("key rename lost targeting/permissions")
	}
	loaded, err := config.LoadConfig(path)
	if err != nil || !loaded.APIKeyGroups[0].AllowCredentialTargeting {
		t.Fatalf("config roundtrip: %v", err)
	}
	r = performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/api-key-groups", `{"api-key":"key-b","allow-credential-targeting":null}`)
	if r.Code != 200 || cfg.APIKeyGroups[0].AllowCredentialTargeting {
		t.Fatal("clear did not disable targeting")
	}
}
