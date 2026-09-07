package management

import (
	"bytes"
	"encoding/json"
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

func TestCredentialWeightUploadRejectsInvalidReplacementAndListsConfiguredValue(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "weight.json")
	before := []byte(`{"type":"codex","weight":7}`)
	if err := os.WriteFile(path, before, 0600); err != nil {
		t.Fatal(err)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	cfg := &config.Config{AuthDir: dir}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)
	upload := func(body []byte, form bool) int {
		request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=weight.json", bytes.NewReader(body))
		if form {
			var buffer bytes.Buffer
			writer := multipart.NewWriter(&buffer)
			part, err := writer.CreateFormFile("file", "weight.json")
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
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = request
		h.UploadAuthFile(c)
		return recorder.Code
	}
	for _, form := range []bool{false, true} {
		for _, weight := range []string{"1.5", "-9223372036854775809", "0,\"weight\":7", "null"} {
			if upload([]byte(`{"type":"codex","weight":`+weight+`}`), form) != http.StatusBadRequest {
				t.Fatal("invalid uploaded weight was accepted")
			}
			after, err := os.ReadFile(path)
			if err != nil || !bytes.Equal(after, before) {
				t.Fatal("invalid upload replaced existing credential data")
			}
		}
	}
	for _, body := range []string{`{"type":"codex","weight":0}`, `{"type":"codex"}`} {
		if upload([]byte(body), false) != http.StatusOK {
			t.Fatal("valid weight upload failed")
		}
		wantPresent := body != `{"type":"codex"}`
		current, ok := manager.GetByID("weight.json")
		if !ok {
			t.Fatal("uploaded credential was not installed")
		}
		if weight, present := configuredAuthFileWeight(current); present != wantPresent || (present && weight != 0) {
			t.Fatal("replacement retained old weight or lost explicit zero")
		}
		for _, handler := range []*Handler{h, NewHandlerWithoutConfigFilePath(cfg, nil)} {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
			handler.ListAuthFiles(c)
			var response struct {
				Files []struct {
					Weight *int `json:"weight"`
				} `json:"files"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != http.StatusOK || len(response.Files) != 1 || (response.Files[0].Weight != nil) != wantPresent || (wantPresent && *response.Files[0].Weight != 0) {
				t.Fatal("memory and disk listings disagree about configured weight")
			}
		}
	}
}
