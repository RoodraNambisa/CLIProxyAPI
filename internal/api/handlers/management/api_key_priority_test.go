package management

import (
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/tidwall/gjson"
)

func TestAPIKeyPriorityPatchPreservesOtherRestrictionsAndRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("api-keys: [fixture]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"fixture"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "fixture", Providers: []string{"codex"}}}}}
	h := NewHandler(cfg, path, nil)
	for _, body := range []string{
		`{"api-key":"fixture","allowed-priorities":[2,1,2]}`,
		`{"api-key":"fixture","excluded-priorities":[2]}`,
		`{"api-key":"fixture","providers":["claude"]}`,
	} {
		response := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", body)
		if response.Code != http.StatusOK {
			t.Fatal("valid restriction patch failed", response.Body.String())
		}
	}
	group := cfg.APIKeyGroups[0]
	if !reflect.DeepEqual(group.Providers, []string{"claude"}) || !reflect.DeepEqual(group.AllowedPriorities, config.APIKeyPriorityList{1, 2}) || !reflect.DeepEqual(group.ExcludedPriorities, config.APIKeyPriorityList{2}) {
		t.Fatal("patching one restriction erased another", group)
	}
	before, _ := os.ReadFile(path)
	bad := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":"fixture","allowed-priorities":[null]}`)
	after, _ := os.ReadFile(path)
	if bad.Code != http.StatusBadRequest || string(before) != string(after) || !reflect.DeepEqual(cfg.APIKeyGroups[0], group) {
		t.Fatal("invalid restriction was saved or replaced active policy")
	}
	rename := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":"fixture","new":"renamed"}`)
	if rename.Code != http.StatusOK {
		t.Fatal("rename failed")
	}
	loaded, err := config.LoadConfig(path)
	if err != nil || loaded.APIKeyGroups[0].APIKey != "renamed" || !reflect.DeepEqual(loaded.APIKeyGroups[0].AllowedPriorities, group.AllowedPriorities) || !reflect.DeepEqual(loaded.APIKeyGroups[0].ExcludedPriorities, group.ExcludedPriorities) {
		t.Fatal("saved/reloaded rename lost priority restrictions", err)
	}
	list := performAPIKeyConfigRequest(t, h.GetAPIKeyGroups, http.MethodGet, "/v0/management/api-key-groups", "")
	if gjson.GetBytes(list.Body.Bytes(), "available-priorities").Raw != "[0,1,2]" {
		t.Fatal("known and configured priority choices missing")
	}
	clear := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", `{"api-key":"renamed","allowed-priorities":[],"excluded-priorities":null}`)
	if clear.Code != http.StatusOK || len(cfg.APIKeyGroups[0].AllowedPriorities) != 0 || len(cfg.APIKeyGroups[0].ExcludedPriorities) != 0 || len(cfg.APIKeyGroups[0].Providers) != 1 {
		t.Fatal("clearing priority restrictions erased provider access")
	}
}
