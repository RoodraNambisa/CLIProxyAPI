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
	"github.com/tidwall/gjson"
)

func TestChatGPTWebImageFormatsSaveRollbackAndCapabilities(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	initial := "images:\n  chatgpt-web:\n    model-output-formats:\n      gpt-image-2.5: [png, gif]\n"
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
	put := func(body string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPut, "/config.yaml", strings.NewReader(body))
		h.PutConfigYAML(c)
		return w
	}
	if w := put("images:\n  chatgpt-web:\n    model-output-formats:\n      gpt-image-2.5: []\n"); w.Code < 400 {
		t.Fatal("empty override accepted")
	}
	if w := put(strings.Replace(initial, "[png, gif]", "[png]", 1)); w.Code != 500 {
		t.Fatalf("save status=%d", w.Code)
	}
	data, _ := os.ReadFile(path)
	if string(data) != initial {
		t.Fatal("rollback changed disk")
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	h.GetChatGPTWebCapabilities(c)
	if !strings.Contains(w.Body.String(), "image_model_output_formats_v1") || gjson.GetBytes(w.Body.Bytes(), "image_model_output_formats.aliases.#(model==\"gpt-image-2.5\").output_formats.1").String() != "gif" {
		t.Fatalf("capabilities=%s", w.Body.String())
	}
}
