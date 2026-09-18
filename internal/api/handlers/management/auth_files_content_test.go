package management

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func authContentFixture(t *testing.T) (*Handler, *coreauth.Manager, *gin.Engine, string, map[string]any) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	name := "editor.json"
	path := filepath.Join(dir, name)
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(dir)
	manager := coreauth.NewManager(store, nil, nil)
	cfg := &config.Config{AuthDir: dir}
	manager.SetConfig(cfg)
	_, err := manager.Register(t.Context(), &coreauth.Auth{
		ID: name, FileName: name, Provider: "codex", Attributes: map[string]string{"path": path},
		Metadata: map[string]any{"type": "codex", "access_token": "fixture-access", "refresh_token": "fixture-refresh", "openai_device_id": "device-original", "note": "remove me", "priority": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	router := gin.New()
	router.PUT("/auth-files/content", h.PutAuthFileContent)
	router.PATCH("/auth-files/fields", h.PatchAuthFileFields)
	return h, manager, router, path, readAuthContent(t, path)
}

func readAuthContent(t *testing.T, path string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err = json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cloneAuthContent(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	data, _ := json.Marshal(value)
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func putAuthContent(t *testing.T, router http.Handler, name string, original, content any) *httptest.ResponseRecorder {
	t.Helper()
	data, err := json.Marshal(map[string]any{"name": name, "original": original, "content": content})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/auth-files/content", bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestAuthFileContentEditsFullJSONAndProjectsRuntime(t *testing.T) {
	_, manager, router, path, original := authContentFixture(t)
	next := cloneAuthContent(t, original)
	next["access_token"] = "fixture-new-access"
	next["openai_device_id"] = "device-edited"
	next["custom"] = map[string]any{"nested": true}
	next["priority"] = 7
	next["prefix"] = "edited"
	next["proxy_url"] = "socks5://user:pass@192.0.2.1:1080"
	next["proxy_binding"] = map[string]any{"version": 1, "node_id": strings.Repeat("a", 64), "port": 1080}
	delete(next, "note")
	response := putAuthContent(t, router, "editor.json", original, next)
	if response.Code != http.StatusOK {
		t.Fatalf("save status %d: %s", response.Code, response.Body.String())
	}
	saved := readAuthContent(t, path)
	if saved["access_token"] != "fixture-new-access" || saved["refresh_token"] != "fixture-refresh" || saved["openai_device_id"] != "device-edited" || saved["custom"] == nil || saved["note"] != nil {
		t.Fatal("full JSON edits or preserved fields were lost")
	}
	current, _ := manager.GetByID("editor.json")
	if current.Prefix != "edited" || current.ProxyURL != next["proxy_url"] || current.Attributes["priority"] != "7" || coreauth.ReadProxyBindingMemory(current) == nil {
		t.Fatal("saved settings were not applied to runtime")
	}
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatal("credential file permissions changed")
	}
}

func TestAuthFileContentRejectsStaleEditorAndExternalChanges(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(map[bool]string{false: "refresh", true: "external"}[external], func(t *testing.T) {
			_, manager, router, path, original := authContentFixture(t)
			next := cloneAuthContent(t, original)
			next["note"] = "draft"
			if external {
				changed := cloneAuthContent(t, original)
				changed["refresh_token"] = "fixture-rotated"
				data, _ := json.Marshal(changed)
				if err := os.WriteFile(path, data, 0o600); err != nil {
					t.Fatal(err)
				}
			} else {
				current, _ := manager.GetByID("editor.json")
				if _, ok, err := manager.UpdateRuntimeMetadataIfCurrent(t.Context(), current, map[string]any{"refresh_token": "fixture-rotated"}); err != nil || !ok {
					t.Fatal("could not simulate token refresh")
				}
			}
			response := putAuthContent(t, router, "editor.json", original, next)
			if response.Code != http.StatusConflict {
				t.Fatalf("stale save returned %d", response.Code)
			}
			if readAuthContent(t, path)["refresh_token"] != "fixture-rotated" {
				t.Fatal("stale editor overwrote refreshed credential")
			}
		})
	}
}

func TestAuthFileContentRejectsInvalidObjectsAndBindings(t *testing.T) {
	_, _, router, path, original := authContentFixture(t)
	for _, change := range []map[string]any{
		{"type": "xai"}, {"type": nil}, {"proxy_binding": map[string]any{"version": 1, "node_id": "invalid", "port": 1080}},
		{"proxy_url": "invalid-proxy"}, {"priority": "invalid"}, {"credential_uid": "replacement"},
	} {
		next := cloneAuthContent(t, original)
		for key, value := range change {
			next[key] = value
		}
		response := putAuthContent(t, router, "editor.json", original, next)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("invalid edit returned %d: %s", response.Code, response.Body.String())
		}
	}
	for _, value := range []any{nil, []any{}, "text"} {
		if response := putAuthContent(t, router, "editor.json", original, value); response.Code != http.StatusBadRequest {
			t.Fatalf("nonobject content returned %d", response.Code)
		}
	}
	if response := putAuthContent(t, router, "../editor.json", original, original); response.Code == http.StatusOK {
		t.Fatal("path traversal was accepted")
	}
	if readAuthContent(t, path)["refresh_token"] != original["refresh_token"] {
		t.Fatal("rejected edit changed the file")
	}
}

func TestAuthFileProxyBindingFieldCanBeSetAndCleared(t *testing.T) {
	_, manager, router, path, _ := authContentFixture(t)
	for _, binding := range []any{map[string]any{"version": 1, "node_id": strings.Repeat("b", 64), "port": 3334, "placeholders": []string{"session1"}}, map[string]any{"version": 1, "direct": true}, nil} {
		data, _ := json.Marshal(map[string]any{"names": []string{"editor.json"}, "fields": map[string]any{"proxy_binding": binding}})
		response := performProxyConfigRequest(router, http.MethodPatch, "/auth-files/fields", string(data))
		if response.Code != http.StatusOK {
			t.Fatalf("proxy_binding patch returned %d: %s", response.Code, response.Body.String())
		}
		current, _ := manager.GetByID("editor.json")
		if (coreauth.ReadProxyBindingMemory(current) == nil) != (binding == nil) {
			t.Fatal("binding field update not applied")
		}
		saved := readAuthContent(t, path)
		if saved["refresh_token"] != "fixture-refresh" || (saved["proxy_binding"] == nil) != (binding == nil) {
			t.Fatal("binding patch replaced unrelated credential fields")
		}
	}
}
