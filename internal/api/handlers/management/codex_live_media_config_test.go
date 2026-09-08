package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestGetConfigCodexMediaRetainsPublicFieldsAndHidesICESecrets(t *testing.T) {
	cfg := &config.Config{Codex: config.CodexConfig{LiveMediaRelay: config.CodexLiveMediaRelayConfig{
		Enabled: true, MaxSessions: 32,
		ICEServers: []config.CodexLiveICEServer{{URLs: []string{"turn:fixture.invalid"}, Username: "user-fixture", Credential: "secret-fixture"}},
	}}}
	h := NewHandlerWithoutConfigFilePath(cfg, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/config", nil)
	h.GetConfig(c)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "turn:fixture.invalid") {
		t.Fatalf("media config GET failed: %d", w.Code)
	}
	if strings.Contains(w.Body.String(), "secret-fixture") || strings.Contains(w.Body.String(), "user-fixture") {
		t.Fatal("media config JSON exposed ICE credentials")
	}
	if cfg.Codex.LiveMediaRelay.ICEServers[0].Credential != "secret-fixture" {
		t.Fatal("config GET erased stored ICE credentials")
	}
}
