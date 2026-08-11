package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestGetConfigExposesLiveLogsWithoutRemoteManagementSecrets(t *testing.T) {
	const secretKey = "management-secret-must-not-leak"
	const accessPath = "hidden-access-path-must-not-leak"
	cfg := &config.Config{RemoteManagement: config.RemoteManagement{
		SecretKey:  secretKey,
		AccessPath: accessPath,
		LiveLogs:   config.LiveLogsConfig{Enabled: true},
	}}
	handler := NewHandlerWithoutConfigFilePath(cfg, nil)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/config", nil)

	handler.GetConfig(context)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	for _, secret := range []string{secretKey, accessPath} {
		if strings.Contains(recorder.Body.String(), secret) {
			t.Fatalf("config response leaked %q: %s", secret, recorder.Body.String())
		}
	}
	var response struct {
		RemoteManagement map[string]json.RawMessage `json:"remote-management"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode config response: %v", errDecode)
	}
	if len(response.RemoteManagement) != 1 {
		t.Fatalf("remote-management = %#v, want only live-logs", response.RemoteManagement)
	}
	var liveLogs config.LiveLogsConfig
	if errDecode := json.Unmarshal(response.RemoteManagement["live-logs"], &liveLogs); errDecode != nil {
		t.Fatalf("decode live-logs: %v", errDecode)
	}
	if !liveLogs.Enabled {
		t.Fatal("live-logs.enabled = false, want true")
	}
}
