package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAPIKeyGroupsPatchAndAPIKeyRenameMigration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a]\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"key-a"}}}
	h := NewHandler(cfg, configPath, nil)

	patchGroup := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":" key-a ","providers":["Codex","xAI","codex"]}`)
	if patchGroup.Code != http.StatusOK {
		t.Fatalf("patch group status = %d, want 200; body=%s", patchGroup.Code, patchGroup.Body.String())
	}
	wantProviders := []string{"codex", "xai"}
	if len(cfg.APIKeyGroups) != 1 || cfg.APIKeyGroups[0].APIKey != "key-a" || strings.Join(cfg.APIKeyGroups[0].Providers, ",") != strings.Join(wantProviders, ",") {
		t.Fatalf("groups after patch = %#v", cfg.APIKeyGroups)
	}

	rename := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":" key-a ","new":" key-b "}`)
	if rename.Code != http.StatusOK {
		t.Fatalf("rename status = %d, want 200; body=%s", rename.Code, rename.Body.String())
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "key-b" {
		t.Fatalf("APIKeys after rename = %#v, want trimmed replacement", cfg.APIKeys)
	}
	if len(cfg.APIKeyGroups) != 1 || cfg.APIKeyGroups[0].APIKey != "key-b" {
		t.Fatalf("groups after rename = %#v", cfg.APIKeyGroups)
	}

	remove := performAPIKeyConfigRequest(t, h.DeleteAPIKeys, http.MethodDelete, "/v0/management/api-keys?value=key-b", "")
	if remove.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", remove.Code, remove.Body.String())
	}
	if len(cfg.APIKeyGroups) != 0 {
		t.Fatalf("groups after key delete = %#v, want empty", cfg.APIKeyGroups)
	}
	saved, errRead := os.ReadFile(configPath)
	if errRead != nil {
		t.Fatalf("read config: %v", errRead)
	}
	if strings.Contains(string(saved), "api-key: key-b") {
		t.Fatalf("orphaned api-key-groups persisted:\n%s", saved)
	}
}

func TestPutAPIKeysPrunesOrphanedGroups(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a, key-b]\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		APIKeys: []string{"key-a", "key-b"},
		APIKeyGroups: []config.APIKeyGroup{
			{APIKey: "key-a", Providers: []string{"codex"}},
			{APIKey: "key-b", Providers: []string{"xai"}},
		},
	}}
	h := NewHandler(cfg, configPath, nil)

	response := performAPIKeyConfigRequest(t, h.PutAPIKeys, http.MethodPut, "/v0/management/api-keys", `[" key-b "]`)
	if response.Code != http.StatusOK {
		t.Fatalf("put status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "key-b" {
		t.Fatalf("APIKeys after put = %#v, want trimmed keys", cfg.APIKeys)
	}
	if len(cfg.APIKeyGroups) != 1 || cfg.APIKeyGroups[0].APIKey != "key-b" {
		t.Fatalf("groups after put = %#v", cfg.APIKeyGroups)
	}
}

func TestPutAPIKeysRejectsEmptyAndDuplicateValuesAfterTrim(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantError string
	}{
		{name: "empty", body: `["key-a","   "]`, wantError: "api key cannot be empty"},
		{name: "duplicate", body: `["key-a"," key-a "]`, wantError: "api key already exists"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			originalConfig := "api-keys: [original]\n"
			if errWrite := os.WriteFile(configPath, []byte(originalConfig), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}
			cfg := &config.Config{SDKConfig: config.SDKConfig{
				APIKeys:      []string{"original"},
				APIKeyGroups: []config.APIKeyGroup{{APIKey: "original", Providers: []string{"codex"}}},
			}}
			h := NewHandler(cfg, configPath, nil)

			response := performAPIKeyConfigRequest(t, h.PutAPIKeys, http.MethodPut, "/v0/management/api-keys", test.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("put status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), test.wantError) {
				t.Fatalf("put body = %s, want error %q", response.Body.String(), test.wantError)
			}
			if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "original" {
				t.Fatalf("APIKeys changed after rejected put: %#v", cfg.APIKeys)
			}
			if len(cfg.APIKeyGroups) != 1 || cfg.APIKeyGroups[0].APIKey != "original" {
				t.Fatalf("APIKeyGroups changed after rejected put: %#v", cfg.APIKeyGroups)
			}
			saved, errRead := os.ReadFile(configPath)
			if errRead != nil {
				t.Fatalf("read config: %v", errRead)
			}
			if string(saved) != originalConfig {
				t.Fatalf("config changed after rejected put:\n%s", saved)
			}
		})
	}
}

func TestPatchAPIKeyGroupsEmptyProvidersClearsRestriction(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a]\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		APIKeys:      []string{"key-a"},
		APIKeyGroups: []config.APIKeyGroup{{APIKey: "key-a", Providers: []string{"codex"}}},
	}}
	h := NewHandler(cfg, configPath, nil)

	response := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":"key-a","providers":[]}`)
	if response.Code != http.StatusOK {
		t.Fatalf("patch status = %d, want 200; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.APIKeyGroups) != 0 {
		t.Fatalf("groups after unrestricted patch = %#v, want empty", cfg.APIKeyGroups)
	}
}

func TestPatchAPIKeysKeepsGroupOnRemainingDuplicate(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [same, same]\napi-key-groups: []\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		APIKeys:      []string{"same", "same"},
		APIKeyGroups: []config.APIKeyGroup{{APIKey: "same", Providers: []string{"codex"}}},
	}}
	h := NewHandler(cfg, configPath, nil)

	first := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"index":0,"value":" new "}`)
	if first.Code != http.StatusOK {
		t.Fatalf("first rename status = %d, want 200; body=%s", first.Code, first.Body.String())
	}
	if strings.Join(cfg.APIKeys, ",") != "new,same" {
		t.Fatalf("APIKeys after first rename = %#v, want [new same]", cfg.APIKeys)
	}
	if len(cfg.APIKeyGroups) != 2 || cfg.APIKeyGroups[0].APIKey != "same" || cfg.APIKeyGroups[1].APIKey != "new" {
		t.Fatalf("group was not copied while duplicate remained: %#v", cfg.APIKeyGroups)
	}

	second := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":"same","new":" newer "}`)
	if second.Code != http.StatusOK {
		t.Fatalf("second rename status = %d, want 200; body=%s", second.Code, second.Body.String())
	}
	if strings.Join(cfg.APIKeys, ",") != "new,newer" {
		t.Fatalf("APIKeys after second rename = %#v, want [new newer]", cfg.APIKeys)
	}
	if len(cfg.APIKeyGroups) != 2 || cfg.APIKeyGroups[0].APIKey != "newer" || cfg.APIKeyGroups[1].APIKey != "new" {
		t.Fatalf("last duplicate group was not migrated: %#v", cfg.APIKeyGroups)
	}
}

func TestPatchAPIKeysRejectsDuplicateReplacementAfterTrim(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "index value", body: `{"index":0,"value":" key-b "}`},
		{name: "old new", body: `{"old":"key-a","new":" key-b "}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a, key-b]\n"), 0o600); errWrite != nil {
				t.Fatalf("write config: %v", errWrite)
			}
			cfg := &config.Config{SDKConfig: config.SDKConfig{
				APIKeys:      []string{"key-a", "key-b"},
				APIKeyGroups: []config.APIKeyGroup{{APIKey: "key-a", Providers: []string{"codex"}}},
			}}
			h := NewHandler(cfg, configPath, nil)

			response := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", test.body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("patch status = %d, want 400; body=%s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), "api key already exists") {
				t.Fatalf("patch body = %s, want duplicate error", response.Body.String())
			}
			if strings.Join(cfg.APIKeys, ",") != "key-a,key-b" {
				t.Fatalf("APIKeys changed after rejected patch: %#v", cfg.APIKeys)
			}
			if len(cfg.APIKeyGroups) != 1 || cfg.APIKeyGroups[0].APIKey != "key-a" {
				t.Fatalf("APIKeyGroups changed after rejected patch: %#v", cfg.APIKeyGroups)
			}
		})
	}
}

func TestPatchAPIKeysOldNotFoundReturnsNotFound(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a]\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"key-a"}}}
	h := NewHandler(cfg, configPath, nil)

	response := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":"missing","new":" key-b "}`)
	if response.Code != http.StatusNotFound {
		t.Fatalf("patch status = %d, want 404; body=%s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "api key not found") {
		t.Fatalf("patch body = %s, want not-found error", response.Body.String())
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "key-a" {
		t.Fatalf("APIKeys changed after missing old key: %#v", cfg.APIKeys)
	}
}

func TestPatchAPIKeysRejectsEmptyReplacement(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("api-keys: [key-a]\n"), 0o600); errWrite != nil {
		t.Fatalf("write config: %v", errWrite)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"key-a"}}}
	h := NewHandler(cfg, configPath, nil)

	response := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"index":0,"value":"  "}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("rename status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "key-a" {
		t.Fatalf("APIKeys changed after rejected rename: %#v", cfg.APIKeys)
	}

	response = performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":"missing","new":"  "}`)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("append status = %d, want 400; body=%s", response.Code, response.Body.String())
	}
	if len(cfg.APIKeys) != 1 || cfg.APIKeys[0] != "key-a" {
		t.Fatalf("APIKeys changed after rejected append: %#v", cfg.APIKeys)
	}
}

func performAPIKeyConfigRequest(t *testing.T, handler gin.HandlerFunc, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request
	handler(ctx)
	return recorder
}
