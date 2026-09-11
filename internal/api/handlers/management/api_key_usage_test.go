package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	configaccess "github.com/router-for-me/CLIProxyAPI/v6/internal/access/config_access"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	"github.com/tidwall/gjson"
)

func TestGetAPIKeysReportsUsageWithoutPersistingOrRecordingReads(t *testing.T) {
	const key = "management-usage-fixture"
	path := filepath.Join(t.TempDir(), "config.yaml")
	const original = "api-keys: [management-usage-fixture]\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{key}}}
	configaccess.Register(&cfg.SDKConfig)
	t.Cleanup(func() { configaccess.Register(nil) })
	h := NewHandler(cfg, path, nil)
	read := func() string {
		response := performAPIKeyConfigRequest(t, h.GetAPIKeys, http.MethodGet, "/v0/management/api-keys", "")
		if response.Code != http.StatusOK || gjson.GetBytes(response.Body.Bytes(), "api-keys.0").Str != key {
			t.Fatal("management list contract changed")
		}
		return gjson.GetBytes(response.Body.Bytes(), "last-used."+key).Str
	}
	if read() != "" {
		t.Fatal("management read counted as key use")
	}
	manager := sdkaccess.NewManager()
	manager.SetProviders(sdkaccess.RegisteredProviders())
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	request.Header.Set("Authorization", "Bearer "+key)
	if _, err := manager.Authenticate(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	stamp := read()
	if stamp == "" || read() != stamp {
		t.Fatal("management read lost or changed the observed timestamp")
	}
	saved, err := os.ReadFile(path)
	if err != nil || string(saved) != original {
		t.Fatal("usage observation wrote configuration")
	}
}
