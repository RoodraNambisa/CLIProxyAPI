package management

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestChatGPTWebTLSConfigUploadAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "chatgpt-web:\n  tls-trust-anchors-enabled: true\n"
	if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(cfg, path, nil)
	fail := false
	h.SetRuntimeConfigApplier(func(_ context.Context, c *config.Config) (config.RuntimeApplyResult, error) {
		if fail && c.ChatGPTWeb.TrustAnchorsEnabled() {
			return config.RuntimeApplyResult{}, errors.New("test apply failure")
		}
		return config.RuntimeApplyResult{Applied: true}, nil
	})
	put := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPut, "/config.yaml", strings.NewReader(body))
		h.PutConfigYAML(c)
		return w
	}
	disabled := "chatgpt-web:\n  tls-trust-anchors-enabled: false\n"
	if w := put(disabled); w.Code != 200 {
		t.Fatalf("save %d: %s", w.Code, w.Body.String())
	}
	if h.currentConfig().ChatGPTWeb.TrustAnchorsEnabled() {
		t.Fatal("false not published")
	}
	fail = true
	if w := put(initial); w.Code != 500 {
		t.Fatalf("rollback status %d", w.Code)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != disabled || h.currentConfig().ChatGPTWeb.TrustAnchorsEnabled() {
		t.Fatal("rollback lost previous setting")
	}
	if w := put("chatgpt-web:\n  tls-trust-anchors-enabled: invalid\n"); w.Code < 400 {
		t.Fatal("invalid boolean accepted")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	h.GetChatGPTWebCapabilities(c)
	if !strings.Contains(w.Body.String(), "\"tls_trust_anchors_v1\"") {
		t.Fatal("TLS capability absent")
	}
}
