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
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexUserAgentConfigValidationDoesNotPersistInvalidValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	original := []byte("port: 8317\n")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	h := &Handler{cfg: &config.Config{}, configFilePath: path}
	for _, body := range []string{`codex-header-defaults: {user-agent: "codex-tui/0.153.4 bad\u0000"}`, `codex-header-defaults: {user-agent: "codex-tui/0.153.4 bad\u007f"}`, `codex-header-defaults: {user-agent: "\ncodex-tui/0.153.4"}`} {
		rr := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rr)
		c.Request = httptest.NewRequest(http.MethodPut, "/config.yaml", strings.NewReader(body))
		h.PutConfigYAML(c)
		if rr.Code != http.StatusUnprocessableEntity {
			t.Fatalf("status=%d body=%s", rr.Code, rr.Body)
		}
		stored, err := os.ReadFile(path)
		if err != nil || string(stored) != string(original) {
			t.Fatal("invalid UA overwrote config")
		}
	}
}

func TestCredentialUserAgentValidationBeforeTrimming(t *testing.T) {
	for _, value := range []string{"bad\x00", "bad\x7f", "\ngood", "good\r"} {
		headers := map[string]string{"uSeR-aGeNt": value}
		if _, err := normalizeReplacementAuthHeaders(headers); err == nil {
			t.Fatal("batch editor accepted invalid UA")
		}
		if err := validateBatchAuthFileFields(&coreauth.Auth{Provider: "codex"}, authFileFieldValues{headersSet: true, headers: headers}); err == nil {
			t.Fatal("legacy editor accepted invalid UA")
		}
	}
}
