package management

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestRequestScopedErrorUploadRejectsBeforeReplacingFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	for _, form := range []bool{false, true} {
		dir := t.TempDir()
		path := filepath.Join(dir, "rules.json")
		before := []byte(`{"type":"codex","request_scoped_errors":[{"status":400,"match":["fixture"],"action":"stop"}]}`)
		if err := os.WriteFile(path, before, 0600); err != nil {
			t.Fatal(err)
		}
		manager := coreauth.NewManager(nil, nil, nil)
		h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, manager)
		for _, rules := range []string{
			`[{"status":400.5,"match":["fixture"],"action":"stop"}]`,
			`[{"status":400,"match-regexr":["private-pattern["],"action":"stop"}]`,
			`[{"status":400,"match":["fixture"],"action":"invalid"}]`,
		} {
			body := []byte(`{"type":"codex","request_scoped_errors":` + rules + `}`)
			request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=rules.json", bytes.NewReader(body))
			if form {
				var buffer bytes.Buffer
				writer := multipart.NewWriter(&buffer)
				part, err := writer.CreateFormFile("file", "rules.json")
				if err != nil {
					t.Fatal(err)
				}
				if _, err = part.Write(body); err != nil {
					t.Fatal(err)
				}
				if err = writer.Close(); err != nil {
					t.Fatal(err)
				}
				request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &buffer)
				request.Header.Set("Content-Type", writer.FormDataContentType())
			}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = request
			h.UploadAuthFile(c)
			after, err := os.ReadFile(path)
			if w.Code != http.StatusBadRequest || err != nil || !bytes.Equal(before, after) || bytes.Contains(w.Body.Bytes(), []byte("private-pattern")) {
				t.Fatalf("invalid upload changed file or response: status=%d", w.Code)
			}
			if _, ok := manager.GetByID("rules.json"); ok {
				t.Fatal("invalid upload installed credential")
			}
		}
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=rules.json", bytes.NewReader(before))
		h.UploadAuthFile(c)
		installed, ok := manager.GetByID("rules.json")
		if w.Code != http.StatusOK || !ok || installed.Metadata["request_scoped_errors"] == nil {
			t.Fatal("valid rules did not upload after rejection")
		}
	}
}
