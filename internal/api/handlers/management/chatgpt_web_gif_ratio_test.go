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

func TestChatGPTWebGIFAspectRatioSaveRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "images:\n  chatgpt-web:\n    gif-allow-aspect-ratio-mismatch: false\n    gif-aspect-ratio-max-error-percent: 5\n"
	if err := os.WriteFile(path, []byte(initial), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(cfg, path, nil)
	h.SetRuntimeConfigApplier(func(context.Context, *config.Config) (config.RuntimeApplyResult, error) {
		return config.RuntimeApplyResult{}, errors.New("apply failed")
	})
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPut, "/config.yaml", strings.NewReader(strings.ReplaceAll(strings.ReplaceAll(initial, "false", "true"), ": 5", ": 2")))
	h.PutConfigYAML(c)
	if w.Code != 500 {
		t.Fatalf("save status=%d", w.Code)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != initial || cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch || cfg.Images.ChatGPTWeb.Resolved().GIFAspectRatioMaxErrorPercent != 5 {
		t.Fatal("failed save modified settings")
	}
	w = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(w)
	h.GetChatGPTWebCapabilities(c)
	if !strings.Contains(w.Body.String(), "gif_aspect_ratio_exception_v1") {
		t.Fatal("capability missing")
	}
}
