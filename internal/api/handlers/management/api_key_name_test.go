package management

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestAPIKeyNamePatchPreservesRestrictionsAndFollowsKeyReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("api-keys: [fixture]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"fixture"}, APIKeyGroups: []config.APIKeyGroup{{APIKey: "fixture", Providers: []string{"codex"}, AllowedPriorities: []int{1}, ExcludedPriorities: []int{2}}}}}
	h := NewHandler(cfg, path, nil)
	patch := func(body string, wantStatus int) {
		t.Helper()
		response := performAPIKeyConfigRequest(t, h.PatchAPIKeyGroups, http.MethodPatch, "/v0/management/api-key-groups", body)
		if response.Code != wantStatus {
			t.Fatalf("name patch status = %d, want %d", response.Code, wantStatus)
		}
	}
	patch(`{"api-key":"fixture","name":" 工作 🔑 "}`, http.StatusOK)
	group := cfg.APIKeyGroups[0]
	if group.Name != "工作 🔑" || !reflect.DeepEqual(group.Providers, []string{"codex"}) || !reflect.DeepEqual(group.AllowedPriorities, config.APIKeyPriorityList{1}) || !reflect.DeepEqual(group.ExcludedPriorities, config.APIKeyPriorityList{2}) {
		t.Fatal("naming changed access restrictions")
	}
	for _, invalid := range []string{`12`, `[]`, `"a\nb"`, `"` + strings.Repeat("名", 101) + `"`} {
		before, _ := os.ReadFile(path)
		patch(`{"api-key":"fixture","name":`+invalid+`}`, http.StatusBadRequest)
		after, _ := os.ReadFile(path)
		if string(before) != string(after) || !reflect.DeepEqual(cfg.APIKeyGroups[0], group) {
			t.Fatal("invalid name changed the saved configuration")
		}
	}
	patch(`{"api-key":"fixture","providers":[]}`, http.StatusOK)
	if cfg.APIKeyGroups[0].Name != group.Name {
		t.Fatal("provider edit cleared an omitted name")
	}
	response := performAPIKeyConfigRequest(t, h.PatchAPIKeys, http.MethodPatch, "/v0/management/api-keys", `{"old":"fixture","new":"renamed"}`)
	if response.Code != http.StatusOK || cfg.APIKeyGroups[0].Name != group.Name {
		t.Fatal("key replacement lost its name")
	}
	loaded, err := config.LoadConfig(path)
	if err != nil || loaded.APIKeyGroups[0].Name != group.Name {
		t.Fatal("name was not persisted", err)
	}
	list := performAPIKeyConfigRequest(t, h.GetAPIKeyGroups, http.MethodGet, "/v0/management/api-key-groups", "")
	var listed struct {
		Groups         []config.APIKeyGroup `json:"api-key-groups"`
		NamesSupported bool                 `json:"names-supported"`
	}
	if err := json.Unmarshal(list.Body.Bytes(), &listed); err != nil || !listed.NamesSupported || len(listed.Groups) != 1 || listed.Groups[0].Name != group.Name {
		t.Fatal("management read omitted the name", err)
	}
	patch(`{"api-key":"renamed","name":null}`, http.StatusOK)
	if cfg.APIKeyGroups[0].Name != "" || len(cfg.APIKeyGroups[0].AllowedPriorities) != 1 {
		t.Fatal("clearing the name changed restrictions")
	}
}

func TestAPIKeyNameSurvivesDuplicateKeySplit(t *testing.T) {
	groups := []config.APIKeyGroup{{APIKey: "fixture", Name: "shared", Providers: []string{"codex"}}}
	split := copyAPIKeyGroup(groups, "fixture", "new")
	if len(split) != 2 || split[0].Name != "shared" || split[1].Name != "shared" {
		t.Fatal("splitting a legacy duplicate lost its name")
	}
}
