package management

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestUploadAuthFile_BatchMultipart(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	files := []struct {
		name    string
		content string
	}{
		{name: "alpha.json", content: `{"type":"codex","email":"alpha@example.com"}`},
		{name: "beta.json", content: `{"type":"claude","email":"beta@example.com"}`},
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile("file", file.name)
		if err != nil {
			t.Fatalf("failed to create multipart file: %v", err)
		}
		if _, err = part.Write([]byte(file.content)); err != nil {
			t.Fatalf("failed to write multipart content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got, ok := payload["uploaded"].(float64); !ok || int(got) != len(files) {
		t.Fatalf("expected uploaded=%d, got %#v", len(files), payload["uploaded"])
	}

	for _, file := range files {
		fullPath := filepath.Join(authDir, file.name)
		data, err := os.ReadFile(fullPath)
		if err != nil {
			t.Fatalf("expected uploaded file %s to exist: %v", file.name, err)
		}
		if string(data) != file.content {
			t.Fatalf("expected file %s content %q, got %q", file.name, file.content, string(data))
		}
	}

	auths := manager.List()
	if len(auths) != len(files) {
		t.Fatalf("expected %d auth entries, got %d", len(files), len(auths))
	}
}

func TestUploadAuthFile_BatchMultipart_InvalidJSONDoesNotOverwriteExistingFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	existingName := "alpha.json"
	existingContent := `{"type":"codex","email":"alpha@example.com"}`
	if err := os.WriteFile(filepath.Join(authDir, existingName), []byte(existingContent), 0o600); err != nil {
		t.Fatalf("failed to seed existing auth file: %v", err)
	}

	files := []struct {
		name    string
		content string
	}{
		{name: existingName, content: `{"type":"codex"`},
		{name: "beta.json", content: `{"type":"claude","email":"beta@example.com"}`},
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, err := writer.CreateFormFile("file", file.name)
		if err != nil {
			t.Fatalf("failed to create multipart file: %v", err)
		}
		if _, err = part.Write([]byte(file.content)); err != nil {
			t.Fatalf("failed to write multipart content: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = req

	h.UploadAuthFile(ctx)

	if rec.Code != http.StatusMultiStatus {
		t.Fatalf("expected upload status %d, got %d with body %s", http.StatusMultiStatus, rec.Code, rec.Body.String())
	}
	var payload struct {
		Failed []struct {
			Name   string `json:"name"`
			Status int    `json:"status"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if len(payload.Failed) != 1 || payload.Failed[0].Name != existingName || payload.Failed[0].Status != http.StatusBadRequest {
		t.Fatalf("batch response = %#v", payload)
	}

	data, err := os.ReadFile(filepath.Join(authDir, existingName))
	if err != nil {
		t.Fatalf("expected existing auth file to remain readable: %v", err)
	}
	if string(data) != existingContent {
		t.Fatalf("expected existing auth file to remain %q, got %q", existingContent, string(data))
	}

	betaData, err := os.ReadFile(filepath.Join(authDir, "beta.json"))
	if err != nil {
		t.Fatalf("expected valid auth file to be created: %v", err)
	}
	if string(betaData) != files[1].content {
		t.Fatalf("expected beta auth file content %q, got %q", files[1].content, string(betaData))
	}
}

func TestUploadAuthFile_InvalidJSONReturnsBadRequestWithoutOverwrite(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	for _, test := range []struct {
		name    string
		request func(t *testing.T) *http.Request
	}{
		{
			name: "raw",
			request: func(t *testing.T) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=alpha.json", bytes.NewBufferString(`{"type":"codex"`))
			},
		},
		{
			name: "multipart",
			request: func(t *testing.T) *http.Request {
				t.Helper()
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, errCreate := writer.CreateFormFile("file", "alpha.json")
				if errCreate != nil {
					t.Fatalf("create multipart file: %v", errCreate)
				}
				if _, errWrite := part.Write([]byte(`{"type":"codex"`)); errWrite != nil {
					t.Fatalf("write multipart content: %v", errWrite)
				}
				if errClose := writer.Close(); errClose != nil {
					t.Fatalf("close multipart writer: %v", errClose)
				}
				request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
				request.Header.Set("Content-Type", writer.FormDataContentType())
				return request
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			authDir := t.TempDir()
			const existingContent = `{"type":"codex","email":"alpha@example.com"}`
			path := filepath.Join(authDir, "alpha.json")
			if errWrite := os.WriteFile(path, []byte(existingContent), 0o600); errWrite != nil {
				t.Fatalf("seed existing auth: %v", errWrite)
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = test.request(t)

			h.UploadAuthFile(ctx)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			data, errRead := os.ReadFile(path)
			if errRead != nil || string(data) != existingContent {
				t.Fatalf("existing auth = %s, error=%v", data, errRead)
			}
		})
	}
}

func TestUploadAuthFile_BatchMultipart_RetiredItemFailsWithoutBlockingValidItem(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	files := []struct {
		name    string
		content string
	}{
		{name: "retired.json", content: `{"type":"gemini","email":"legacy@example.com"}`},
		{name: "active.json", content: `{"type":"codex","email":"active@example.com"}`},
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, errCreate := writer.CreateFormFile("file", file.name)
		if errCreate != nil {
			t.Fatalf("create multipart file: %v", errCreate)
		}
		if _, errWrite := part.Write([]byte(file.content)); errWrite != nil {
			t.Fatalf("write multipart content: %v", errWrite)
		}
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = request
	h.UploadAuthFile(ctx)

	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	var payload struct {
		Uploaded int `json:"uploaded"`
		Failed   []struct {
			Name   string `json:"name"`
			Status int    `json:"status"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if payload.Uploaded != 1 || len(payload.Failed) != 1 || payload.Failed[0].Name != "retired.json" || payload.Failed[0].Status != http.StatusGone {
		t.Fatalf("batch response = %#v", payload)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "retired.json")); !os.IsNotExist(errStat) {
		t.Fatalf("retired auth file was written: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "active.json")); errStat != nil {
		t.Fatalf("active auth file was not written: %v", errStat)
	}
	if _, ok := manager.GetByID("active.json"); !ok {
		t.Fatal("active auth was not registered")
	}
}

func TestUploadAuthFile_BatchMultipart_InvalidNameIncludesBadRequestStatus(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	authDir := t.TempDir()
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, errCreate := writer.CreateFormFile("file", `C:\bad.json`)
	if errCreate != nil {
		t.Fatalf("create multipart file: %v", errCreate)
	}
	if _, errWrite := part.Write([]byte(`{"type":"codex"}`)); errWrite != nil {
		t.Fatalf("write multipart content: %v", errWrite)
	}
	validPart, errValid := writer.CreateFormFile("file", "valid.json")
	if errValid != nil {
		t.Fatalf("create valid multipart file: %v", errValid)
	}
	if _, errWrite := validPart.Write([]byte(`{"type":"codex"}`)); errWrite != nil {
		t.Fatalf("write valid multipart content: %v", errWrite)
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatalf("close multipart writer: %v", errClose)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	ctx.Request = request
	h.UploadAuthFile(ctx)

	if recorder.Code != http.StatusMultiStatus {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
	}
	var payload struct {
		Failed []struct {
			Status int `json:"status"`
		} `json:"failed"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode response: %v", errDecode)
	}
	if len(payload.Failed) != 1 || payload.Failed[0].Status != http.StatusBadRequest {
		t.Fatalf("batch response = %#v", payload)
	}
}

func TestUploadAuthFile_RejectsPortableWindowsVolumeNamesAsBadRequest(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tests := []struct {
		name    string
		request func(t *testing.T) *http.Request
	}{
		{
			name: "raw",
			request: func(t *testing.T) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name=C%3Aauth.json", bytes.NewBufferString(`{"type":"codex"}`))
			},
		},
		{
			name: "multipart",
			request: func(t *testing.T) *http.Request {
				t.Helper()
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, errCreate := writer.CreateFormFile("file", `C:auth.json`)
				if errCreate != nil {
					t.Fatalf("create multipart file: %v", errCreate)
				}
				if _, errWrite := part.Write([]byte(`{"type":"codex"}`)); errWrite != nil {
					t.Fatalf("write multipart file: %v", errWrite)
				}
				if errClose := writer.Close(); errClose != nil {
					t.Fatalf("close multipart writer: %v", errClose)
				}
				request := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files", &body)
				request.Header.Set("Content-Type", writer.FormDataContentType())
				return request
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authDir := t.TempDir()
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = test.request(t)
			h.UploadAuthFile(ctx)
			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			entries, errRead := os.ReadDir(authDir)
			if errRead != nil || len(entries) != 0 {
				t.Fatalf("auth dir entries = %#v, error=%v; want empty", entries, errRead)
			}
		})
	}
}

func TestDeleteAuthFile_BatchQuery(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	files := []string{"alpha.json", "beta.json"}
	for _, name := range files {
		if err := os.WriteFile(filepath.Join(authDir, name), []byte(`{"type":"codex"}`), 0o600); err != nil {
			t.Fatalf("failed to write auth file %s: %v", name, err)
		}
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(
		http.MethodDelete,
		"/v0/management/auth-files?name="+url.QueryEscape(files[0])+"&name="+url.QueryEscape(files[1]),
		nil,
	)
	ctx.Request = req

	h.DeleteAuthFile(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, rec.Code, rec.Body.String())
	}

	var payload map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if got, ok := payload["deleted"].(float64); !ok || int(got) != len(files) {
		t.Fatalf("expected deleted=%d, got %#v", len(files), payload["deleted"])
	}

	for _, name := range files {
		if _, err := os.Stat(filepath.Join(authDir, name)); !os.IsNotExist(err) {
			t.Fatalf("expected auth file %s to be removed, stat err: %v", name, err)
		}
	}
}
