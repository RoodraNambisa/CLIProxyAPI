package management

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestWriteAuthFile_RetiredPathCannotBeReusedUntilDeleted(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","email":"legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	if errWrite := h.writeAuthFile(t.Context(), fileName, []byte(`{"type":"codex"}`)); !errors.Is(errWrite, errGeminiCLIAuthGone) {
		t.Fatalf("first writeAuthFile() error = %v, want retired error", errWrite)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"external"}`), 0o600); errWrite != nil {
		t.Fatalf("externally rewrite retired path: %v", errWrite)
	}
	if errWrite := h.writeAuthFile(t.Context(), fileName, []byte(`{"type":"codex","access_token":"managed"}`)); !errors.Is(errWrite, errGeminiCLIAuthGone) {
		t.Fatalf("second writeAuthFile() error = %v, want retired error", errWrite)
	}
	if _, exists := manager.GetByID(fileName); exists {
		t.Fatal("retired path was registered after external rewrite")
	}
}

func TestUploadAuthFile_RejectsQuarantinedDeletionTarget(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	const fileName = "pending-delete.json"
	path := filepath.Join(authDir, fileName)
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name="+fileName, strings.NewReader(`{"type":"codex","access_token":"replacement"}`))
	h.UploadAuthFile(ctx)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("UploadAuthFile() status = %d, want %d; body=%s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("quarantined target was written: %v", errStat)
	}
	if _, exists := manager.GetByID(fileName); exists {
		t.Fatal("quarantined target was registered")
	}
}

func TestWriteAuthFile_RevalidatesRetiredTargetUnderProcessLock(t *testing.T) {
	authDir := t.TempDir()
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = &memoryAuthStore{}
	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })

	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer root.Close()
	unlock, errLock := authfileguard.LockRootTarget(root, fileName)
	if errLock != nil {
		t.Fatalf("lock auth target: %v", errLock)
	}
	locked := true
	defer func() {
		if locked {
			_ = unlock()
		}
	}()

	result := make(chan error, 1)
	go func() {
		result <- h.writeAuthFile(t.Context(), fileName, []byte(`{"type":"codex"}`))
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, errRead := os.ReadDir(authDir)
		waiting := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".auth-upload-") {
				waiting = true
				break
			}
		}
		if waiting {
			break
		}
		if errRead != nil || time.Now().After(deadline) {
			t.Fatalf("upload did not reach target lock: %v", errRead)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case errWrite := <-result:
		t.Fatalf("writeAuthFile() bypassed process lock: %v", errWrite)
	default:
	}
	retired := []byte(`{"type":"gemini","email":"legacy@example.com"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write concurrent retired auth: %v", errWrite)
	}
	if errUnlock := unlock(); errUnlock != nil {
		t.Fatalf("unlock auth target: %v", errUnlock)
	}
	locked = false
	select {
	case errWrite := <-result:
		if !errors.Is(errWrite, errGeminiCLIAuthGone) {
			t.Fatalf("writeAuthFile() error = %v, want retired error", errWrite)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writeAuthFile() did not finish after target unlock")
	}
	persisted, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(persisted, retired) {
		t.Fatalf("retired target after upload = %q, %v", persisted, errRead)
	}
}

func TestRetiredGeminiCLIAuthFile_RemainsManageableWithoutRegistration(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	const fileName = "legacy-gemini.json"
	const content = `{"type":"gemini","email":"legacy@example.com","access_token":"secret-token"}`
	if errWrite := os.WriteFile(filepath.Join(authDir, fileName), []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	if got := len(manager.List()); got != 0 {
		t.Fatalf("registered auth count = %d, want 0", got)
	}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("ListAuthFiles() status = %d, body = %s", listRecorder.Code, listRecorder.Body.String())
	}
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	if len(payload.Files) != 1 {
		t.Fatalf("listed file count = %d, want 1", len(payload.Files))
	}
	if unsupported, _ := payload.Files[0]["unsupported"].(bool); !unsupported {
		t.Fatalf("unsupported = %#v, want true", payload.Files[0]["unsupported"])
	}
	if got, _ := payload.Files[0]["status"].(string); got != "unsupported" {
		t.Fatalf("status = %q, want unsupported", got)
	}
	if _, exposed := payload.Files[0]["access_token"]; exposed {
		t.Fatal("list response exposed access_token")
	}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(fileName), nil)
	h.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusOK || downloadRecorder.Body.String() != content {
		t.Fatalf("DownloadAuthFile() status = %d, body = %q", downloadRecorder.Code, downloadRecorder.Body.String())
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("DeleteAuthFile() status = %d, body = %s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(authDir, fileName)); !os.IsNotExist(errStat) {
		t.Fatalf("retired auth file still exists after delete: %v", errStat)
	}
}

func TestGeminiAPIKeyFileIsNotMarkedRetired(t *testing.T) {
	retired, metadata, errParse := parseRetiredGeminiCLIAuthFile([]byte(`{"type":"gemini","api_key":"active-key"}`))
	if errParse != nil {
		t.Fatalf("parseRetiredGeminiCLIAuthFile() error = %v", errParse)
	}
	if retired {
		t.Fatal("Gemini API key file was marked as retired Gemini CLI auth")
	}
	if metadata["api_key"] != "active-key" {
		t.Fatalf("parsed metadata = %#v", metadata)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, coreauth.NewManager(nil, nil, nil))
	auth, errBuild := h.buildAuthFromFileData(filepath.Join(h.cfg.AuthDir, "gemini-key.json"), []byte(`{"type":"gemini","api_key":"active-key"}`))
	if errBuild != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", errBuild)
	}
	if auth.Attributes["api_key"] != "active-key" || coreauth.IsRetiredGeminiCLIAuth(auth) {
		t.Fatalf("file-backed Gemini API key auth = %#v", auth)
	}
}

func TestRetiredGeminiCLIAuthFile_OverridesStaleRuntimeEntry(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	const fileName = "same.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","email":"active@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	auth, errBuild := h.buildAuthFromFileData(filePath, nil)
	if errBuild != nil {
		t.Fatalf("build supported auth: %v", errBuild)
	}
	auth.Attributes = map[string]string{}
	auth.FileName = fileName
	auth.Status = coreauth.StatusError
	auth.Unavailable = true
	auth.NextRetryAfter = time.Now().Add(time.Hour)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register supported auth: %v", errRegister)
	}
	const retiredContent = `{"type":"gemini","email":"legacy@example.com"}`
	if errWrite := os.WriteFile(filePath, []byte(retiredContent), 0o600); errWrite != nil {
		t.Fatalf("replace with retired auth: %v", errWrite)
	}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	if len(payload.Files) != 1 || payload.Files[0]["retired"] != true || payload.Files[0]["provider"] != "gemini-cli" {
		t.Fatalf("listed files = %#v, want one retired Gemini CLI entry", payload.Files)
	}

	requests := []struct {
		name   string
		method string
		path   string
		body   string
		call   func(*gin.Context)
	}{
		{name: "models", method: http.MethodGet, path: "/v0/management/auth-files/models?name=" + url.QueryEscape(fileName), call: h.GetAuthFileModels},
		{name: "status", method: http.MethodPatch, path: "/v0/management/auth-files/status", body: `{"name":"same.json","disabled":true}`, call: h.PatchAuthFileStatus},
		{name: "fields", method: http.MethodPatch, path: "/v0/management/auth-files/fields", body: `{"name":"same.json","priority":1}`, call: h.PatchAuthFileFields},
		{name: "cooldown", method: http.MethodPost, path: "/v0/management/auth-files/cooldowns/clear-selected", body: `{"names":["same.json"]}`, call: h.ClearSelectedAuthCooldowns},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			request.call(ctx)
			if recorder.Code != http.StatusGone {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
			}
		})
	}
	clearAllRecorder := httptest.NewRecorder()
	clearAllContext, _ := gin.CreateTestContext(clearAllRecorder)
	clearAllContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear", nil)
	h.ClearAllAuthCooldowns(clearAllContext)
	if clearAllRecorder.Code != http.StatusOK {
		t.Fatalf("clear all status = %d, want %d; body=%s", clearAllRecorder.Code, http.StatusOK, clearAllRecorder.Body.String())
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil || !current.Unavailable || current.NextRetryAfter.IsZero() {
		t.Fatalf("stale runtime cooldown was modified: %#v", current)
	}

	persisted, errRead := os.ReadFile(filePath)
	if errRead != nil || string(persisted) != retiredContent {
		t.Fatalf("retired file after rejected mutations = %q, error=%v", persisted, errRead)
	}
}

func TestAPICallRejectsAuthBackedByRetiredGeminiCLIFile(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	const fileName = "stale-api-call.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","access_token":"active-token"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	auth, errBuild := h.buildAuthFromFileData(filePath, nil)
	if errBuild != nil {
		t.Fatalf("build supported auth: %v", errBuild)
	}
	registered, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("register supported auth: %v", errRegister)
	}
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"gemini","access_token":"retired-token"}`), 0o600); errWrite != nil {
		t.Fatalf("replace auth with retired file: %v", errWrite)
	}

	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	defer upstream.Close()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":%q}`, registered.EnsureIndex(), upstream.URL)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.APICall(ctx)

	if recorder.Code != http.StatusGone {
		t.Fatalf("APICall() status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0", got)
	}
}

func TestAPICallFailsClosedWhenBackingFileCannotBeVerified(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	const fileName = "missing-api-call.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","access_token":"active-token"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	auth, errBuild := h.buildAuthFromFileData(filePath, nil)
	if errBuild != nil {
		t.Fatalf("build supported auth: %v", errBuild)
	}
	registered, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("register supported auth: %v", errRegister)
	}
	if errRemove := os.Remove(filePath); errRemove != nil {
		t.Fatalf("remove backing file: %v", errRemove)
	}

	var upstreamCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		upstreamCalls.Add(1)
	}))
	defer upstream.Close()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	body := fmt.Sprintf(`{"auth_index":%q,"method":"GET","url":%q}`, registered.EnsureIndex(), upstream.URL)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/api-call", strings.NewReader(body))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.APICall(ctx)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("APICall() status = %d, want %d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
	if got := upstreamCalls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0", got)
	}
}

func TestUploadAuthFile_RejectsRetiredGeminiCLIWithoutOverwrite(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tests := []struct {
		name       string
		newRequest func(t *testing.T, fileName string, content []byte) *http.Request
	}{
		{
			name: "raw",
			newRequest: func(t *testing.T, fileName string, content []byte) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name="+url.QueryEscape(fileName), bytes.NewReader(content))
			},
		},
		{
			name: "multipart",
			newRequest: func(t *testing.T, fileName string, content []byte) *http.Request {
				t.Helper()
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, errCreate := writer.CreateFormFile("file", fileName)
				if errCreate != nil {
					t.Fatalf("create multipart file: %v", errCreate)
				}
				if _, errWrite := part.Write(content); errWrite != nil {
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
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			const fileName = "existing.json"
			supported := []byte(`{"type":"codex","email":"active@example.com"}`)
			if errWrite := h.writeAuthFile(t.Context(), fileName, supported); errWrite != nil {
				t.Fatalf("write supported auth: %v", errWrite)
			}

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = test.newRequest(t, fileName, []byte(`{"type":"gemini","email":"legacy@example.com"}`))
			h.UploadAuthFile(ctx)

			if recorder.Code != http.StatusGone {
				t.Fatalf("upload status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
			}
			persisted, errRead := os.ReadFile(filepath.Join(authDir, fileName))
			if errRead != nil || !bytes.Equal(persisted, supported) {
				t.Fatalf("existing file after rejected upload = %q, error=%v", persisted, errRead)
			}
			current, ok := manager.GetByID(fileName)
			if !ok || current == nil || !strings.EqualFold(current.Provider, "codex") {
				t.Fatalf("runtime auth after rejected upload = %#v, want codex", current)
			}
		})
	}
}

func TestUploadAuthFile_RefusesToOverwriteRetiredGeminiCLIFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tests := []struct {
		name       string
		newRequest func(t *testing.T, fileName string, content []byte) *http.Request
	}{
		{
			name: "raw",
			newRequest: func(t *testing.T, fileName string, content []byte) *http.Request {
				t.Helper()
				return httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name="+url.QueryEscape(fileName), bytes.NewReader(content))
			},
		},
		{
			name: "multipart",
			newRequest: func(t *testing.T, fileName string, content []byte) *http.Request {
				t.Helper()
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, errCreate := writer.CreateFormFile("file", fileName)
				if errCreate != nil {
					t.Fatalf("create multipart file: %v", errCreate)
				}
				if _, errWrite := part.Write(content); errWrite != nil {
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
			manager := coreauth.NewManager(nil, nil, nil)
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			const fileName = "legacy.json"
			retired := []byte(`{"type":"gemini-cli","email":"legacy@example.com"}`)
			if errWrite := os.WriteFile(filepath.Join(authDir, fileName), retired, 0o600); errWrite != nil {
				t.Fatalf("write retired auth: %v", errWrite)
			}

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = test.newRequest(t, fileName, []byte(`{"type":"codex","email":"active@example.com"}`))
			h.UploadAuthFile(ctx)

			if recorder.Code != http.StatusGone {
				t.Fatalf("upload status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
			}
			persisted, errRead := os.ReadFile(filepath.Join(authDir, fileName))
			if errRead != nil || !bytes.Equal(persisted, retired) {
				t.Fatalf("retired file after rejected overwrite = %q, error=%v", persisted, errRead)
			}
			if got := len(manager.List()); got != 0 {
				t.Fatalf("runtime auth count = %d, want 0", got)
			}
		})
	}
}

func TestRetiredGeminiCLIAuthFile_CustomAuthIDCannotBypassMutationGuard(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	const fileName = "legacy.json"
	const authID = "legacy-custom-id"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","email":"active@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	auth, errBuild := h.buildAuthFromFileData(filePath, nil)
	if errBuild != nil {
		t.Fatalf("build supported auth: %v", errBuild)
	}
	auth.ID = authID
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register supported auth: %v", errRegister)
	}
	const retiredContent = `{"type":"gemini","email":"legacy@example.com"}`
	if errWrite := os.WriteFile(filePath, []byte(retiredContent), 0o600); errWrite != nil {
		t.Fatalf("replace with retired auth: %v", errWrite)
	}

	requests := []struct {
		name   string
		method string
		path   string
		body   string
		call   func(*gin.Context)
	}{
		{name: "models", method: http.MethodGet, path: "/v0/management/auth-files/models?name=" + url.QueryEscape(authID), call: h.GetAuthFileModels},
		{name: "status", method: http.MethodPatch, path: "/v0/management/auth-files/status", body: `{"name":"legacy-custom-id","disabled":true}`, call: h.PatchAuthFileStatus},
		{name: "fields", method: http.MethodPatch, path: "/v0/management/auth-files/fields", body: `{"name":"legacy-custom-id","priority":1}`, call: h.PatchAuthFileFields},
		{name: "cooldown", method: http.MethodPost, path: "/v0/management/auth-files/cooldowns/clear-selected", body: `{"names":["legacy-custom-id"]}`, call: h.ClearSelectedAuthCooldowns},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			request.call(ctx)
			if recorder.Code != http.StatusGone {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusGone, recorder.Body.String())
			}
		})
	}
	if persisted, errRead := os.ReadFile(filePath); errRead != nil || string(persisted) != retiredContent {
		t.Fatalf("retired file after rejected custom-ID mutations = %q, error=%v", persisted, errRead)
	}
}

func TestRetiredGeminiCLIAuthFile_CooldownDoesNotMatchNestedBasename(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "team")
	if errMkdir := os.MkdirAll(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	nestedPath := filepath.Join(nestedDir, "same.json")
	if errWrite := os.WriteFile(nestedPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested auth: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	auth, errBuild := h.buildAuthFromFileData(nestedPath, nil)
	if errBuild != nil {
		t.Fatalf("build nested auth: %v", errBuild)
	}
	auth.Status = coreauth.StatusError
	auth.Unavailable = true
	auth.NextRetryAfter = time.Now().Add(time.Hour)
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register nested auth: %v", errRegister)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, "same.json"), []byte(`{"type":"gemini-cli"}`), 0o600); errWrite != nil {
		t.Fatalf("write root retired auth: %v", errWrite)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-selected", strings.NewReader(`{"names":["same.json"]}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.ClearSelectedAuthCooldowns(ctx)

	if recorder.Code != http.StatusGone {
		t.Fatalf("clear cooldown status = %d, want 410; body=%s", recorder.Code, recorder.Body.String())
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil || !current.Unavailable || current.NextRetryAfter.IsZero() {
		t.Fatalf("nested auth cooldown was cleared through basename collision: %#v", current)
	}
}

func TestRetiredGeminiCLIAuthFile_TakesPrecedenceOverSameNamedCustomAuthID(t *testing.T) {
	authDir := t.TempDir()
	backingPath := filepath.Join(authDir, "real.json")
	if errWrite := os.WriteFile(backingPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported backing file: %v", errWrite)
	}
	const retiredName = "alias.json"
	if errWrite := os.WriteFile(filepath.Join(authDir, retiredName), []byte(`{"type":"gemini-cli"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       retiredName,
		FileName: "real.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": backingPath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register custom-ID auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	requests := []struct {
		name   string
		method string
		path   string
		body   string
		call   func(*gin.Context)
	}{
		{name: "models", method: http.MethodGet, path: "/v0/management/auth-files/models?name=" + url.QueryEscape(retiredName), call: h.GetAuthFileModels},
		{name: "status", method: http.MethodPatch, path: "/v0/management/auth-files/status", body: `{"name":"alias.json","disabled":true}`, call: h.PatchAuthFileStatus},
		{name: "fields", method: http.MethodPatch, path: "/v0/management/auth-files/fields", body: `{"name":"alias.json","priority":1}`, call: h.PatchAuthFileFields},
		{name: "cooldown", method: http.MethodPost, path: "/v0/management/auth-files/cooldowns/clear-selected", body: `{"names":["alias.json"]}`, call: h.ClearSelectedAuthCooldowns},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			request.call(ctx)
			if recorder.Code != http.StatusGone {
				t.Fatalf("status = %d, want 410; body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if current, exists := manager.GetByID(retiredName); !exists || current == nil || current.Disabled {
		t.Fatalf("custom-ID auth changed through retired file name: %#v", current)
	}
}

func TestRetiredGeminiCLIAuthFile_InSubdirectoryRemainsExplicitlyManageable(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "team")
	if errMkdir := os.MkdirAll(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	const fileName = "team/legacy-gemini.json"
	const content = `{"type":"gemini-cli","email":"legacy@example.com","access_token":"secret-token"}`
	if errWrite := os.WriteFile(filepath.Join(nestedDir, "legacy-gemini.json"), []byte(content), 0o600); errWrite != nil {
		t.Fatalf("write nested retired auth: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode nested list response: %v", errDecode)
	}
	if len(payload.Files) != 0 {
		t.Fatalf("nested retired file leaked into top-level listing: %#v", payload.Files)
	}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(fileName), nil)
	h.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusOK || downloadRecorder.Body.String() != content {
		t.Fatalf("nested download status = %d, body = %q", downloadRecorder.Code, downloadRecorder.Body.String())
	}

	archiveRecorder := httptest.NewRecorder()
	archiveContext, _ := gin.CreateTestContext(archiveRecorder)
	archiveContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/download-all", strings.NewReader(`{"names":["team/legacy-gemini.json"]}`))
	archiveContext.Request.Header.Set("Content-Type", "application/json")
	h.DownloadAuthFilesArchive(archiveContext)
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("nested retired archive status = %d, want 200; body=%s", archiveRecorder.Code, archiveRecorder.Body.String())
	}
	archiveFiles := unzipAuthArchive(t, archiveRecorder.Body.Bytes())
	if got := archiveFiles[fileName]; got != content || len(archiveFiles) != 1 {
		t.Fatalf("nested retired archive files = %#v, want %q", archiveFiles, content)
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("nested delete status = %d, body = %s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(nestedDir, "legacy-gemini.json")); !os.IsNotExist(errStat) {
		t.Fatalf("nested retired auth still exists after delete: %v", errStat)
	}
}

func TestRetiredGeminiCLIAuthFile_InSubdirectoryIncludedInAllOperations(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "team")
	if errMkdir := os.MkdirAll(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	const rootName = "active.json"
	const nestedRetiredName = "team/legacy-gemini.json"
	const nestedSupportedName = "team/codex.json"
	if errWrite := os.WriteFile(filepath.Join(authDir, rootName), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write root auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, filepath.FromSlash(nestedRetiredName)), []byte(`{"type":"gemini-cli","email":"legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested retired auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, filepath.FromSlash(nestedSupportedName)), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested supported auth: %v", errWrite)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = &memoryAuthStore{}

	archiveRecorder := httptest.NewRecorder()
	archiveContext, _ := gin.CreateTestContext(archiveRecorder)
	archiveContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/download-all", strings.NewReader(`{"all":true}`))
	archiveContext.Request.Header.Set("Content-Type", "application/json")
	h.DownloadAuthFilesArchive(archiveContext)
	if archiveRecorder.Code != http.StatusOK {
		t.Fatalf("download all status = %d, body = %s", archiveRecorder.Code, archiveRecorder.Body.String())
	}
	archiveFiles := unzipAuthArchive(t, archiveRecorder.Body.Bytes())
	if _, exists := archiveFiles[rootName]; !exists {
		t.Fatalf("root auth missing from archive: %#v", archiveFiles)
	}
	if _, exists := archiveFiles[nestedRetiredName]; !exists {
		t.Fatalf("nested retired auth missing from archive: %#v", archiveFiles)
	}
	if _, exists := archiveFiles[nestedSupportedName]; exists {
		t.Fatalf("nested supported auth included in archive: %#v", archiveFiles)
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?all=true", nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("delete all status = %d, body = %s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(authDir, rootName)); !os.IsNotExist(errStat) {
		t.Fatalf("root auth still exists after delete all: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, filepath.FromSlash(nestedRetiredName))); !os.IsNotExist(errStat) {
		t.Fatalf("nested retired auth still exists after delete all: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, filepath.FromSlash(nestedSupportedName))); errStat != nil {
		t.Fatalf("nested supported auth changed by delete all: %v", errStat)
	}
}

func TestListAuthFiles_OmitsNestedManagedSupportedAuth(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "team")
	if errMkdir := os.MkdirAll(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	const fileName = "team/codex.json"
	if errWrite := os.WriteFile(filepath.Join(nestedDir, "codex.json"), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested supported auth: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	auth, errBuild := h.buildAuthFromFileData(filepath.Join(authDir, filepath.FromSlash(fileName)), nil)
	if errBuild != nil {
		t.Fatalf("build nested auth: %v", errBuild)
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register nested auth: %v", errRegister)
	}

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(context)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	if len(payload.Files) != 0 {
		t.Fatalf("nested managed auth leaked into top-level listing: %#v", payload.Files)
	}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(fileName), nil)
	h.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported download status = %d, want 400; body=%s", downloadRecorder.Code, downloadRecorder.Body.String())
	}

	archiveRecorder := httptest.NewRecorder()
	archiveContext, _ := gin.CreateTestContext(archiveRecorder)
	archiveContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/download-all", strings.NewReader(`{"names":["team/codex.json"]}`))
	archiveContext.Request.Header.Set("Content-Type", "application/json")
	h.DownloadAuthFilesArchive(archiveContext)
	if archiveRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported archive status = %d, want 400; body=%s", archiveRecorder.Code, archiveRecorder.Body.String())
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported delete status = %d, want 400; body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}

	modelsRecorder := httptest.NewRecorder()
	modelsContext, _ := gin.CreateTestContext(modelsRecorder)
	modelsContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/models?name="+url.QueryEscape(fileName), nil)
	h.GetAuthFileModels(modelsContext)
	if modelsRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported models status = %d, want 400; body=%s", modelsRecorder.Code, modelsRecorder.Body.String())
	}

	statusRecorder := httptest.NewRecorder()
	statusContext, _ := gin.CreateTestContext(statusRecorder)
	statusContext.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(`{"name":"team/codex.json","disabled":true}`))
	statusContext.Request.Header.Set("Content-Type", "application/json")
	h.PatchAuthFileStatus(statusContext)
	if statusRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported status patch = %d, want 400; body=%s", statusRecorder.Code, statusRecorder.Body.String())
	}

	fieldsRecorder := httptest.NewRecorder()
	fieldsContext, _ := gin.CreateTestContext(fieldsRecorder)
	fieldsContext.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/fields", strings.NewReader(`{"name":"team/codex.json","note":"hidden"}`))
	fieldsContext.Request.Header.Set("Content-Type", "application/json")
	h.PatchAuthFileFields(fieldsContext)
	if fieldsRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported fields patch = %d, want 400; body=%s", fieldsRecorder.Code, fieldsRecorder.Body.String())
	}

	cooldownRecorder := httptest.NewRecorder()
	cooldownContext, _ := gin.CreateTestContext(cooldownRecorder)
	cooldownContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-selected", strings.NewReader(`{"names":["team/codex.json"]}`))
	cooldownContext.Request.Header.Set("Content-Type", "application/json")
	h.ClearSelectedAuthCooldowns(cooldownContext)
	if cooldownRecorder.Code != http.StatusBadRequest {
		t.Fatalf("nested supported cooldown clear = %d, want 400; body=%s", cooldownRecorder.Code, cooldownRecorder.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(authDir, filepath.FromSlash(fileName))); errStat != nil {
		t.Fatalf("nested supported auth changed after rejected operations: %v", errStat)
	}
	if _, exists := manager.GetByID(auth.ID); !exists {
		t.Fatal("nested supported runtime auth was removed")
	}
}

func TestDeleteRetiredGeminiCLIAuthFile_DoesNotMatchNestedRuntimeByBaseName(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "team")
	if errMkdir := os.MkdirAll(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	const fileName = "same.json"
	rootPath := filepath.Join(authDir, fileName)
	nestedPath := filepath.Join(nestedDir, fileName)
	if errWrite := os.WriteFile(rootPath, []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write root retired auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(nestedPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested supported auth: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	nestedAuth, errBuild := h.buildAuthFromFileData(nestedPath, nil)
	if errBuild != nil {
		t.Fatalf("build nested auth: %v", errBuild)
	}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), nestedAuth); errRegister != nil {
		t.Fatalf("register nested auth: %v", errRegister)
	}

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(context)
	if recorder.Code != http.StatusOK {
		t.Fatalf("delete root retired auth status = %d, body = %s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(rootPath); !os.IsNotExist(errStat) {
		t.Fatalf("root retired auth still exists: %v", errStat)
	}
	if _, errStat := os.Stat(nestedPath); errStat != nil {
		t.Fatalf("nested supported auth was removed: %v", errStat)
	}
	if _, ok := manager.GetByID(nestedAuth.ID); !ok {
		t.Fatal("nested supported runtime auth was removed")
	}
}

func TestManagedAuthFiles_RejectSymlinkEntriesAndResolveSymlinkRoot(t *testing.T) {
	realDir := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDir, linkRoot); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	const targetName = "target.json"
	const aliasName = "alias.json"
	if errWrite := os.WriteFile(filepath.Join(realDir, targetName), []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write target auth: %v", errWrite)
	}
	if errSymlink := os.Symlink(filepath.Join(realDir, targetName), filepath.Join(realDir, aliasName)); errSymlink != nil {
		t.Fatalf("create auth file symlink: %v", errSymlink)
	}
	externalTarget := filepath.Join(t.TempDir(), "external.json")
	const externalContent = `{"type":"codex","email":"external@example.com"}`
	if errWrite := os.WriteFile(externalTarget, []byte(externalContent), 0o600); errWrite != nil {
		t.Fatalf("write external symlink target: %v", errWrite)
	}
	const uploadAliasName = "upload-alias.json"
	if errSymlink := os.Symlink(externalTarget, filepath.Join(realDir, uploadAliasName)); errSymlink != nil {
		t.Fatalf("create upload symlink: %v", errSymlink)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: linkRoot}, manager)
	h.tokenStore = &memoryAuthStore{}
	if errWrite := h.writeAuthFile(t.Context(), uploadAliasName, []byte(`{"type":"codex"}`)); errWrite == nil {
		t.Fatal("writeAuthFile followed an existing symlink")
	}
	uploadRecorder := httptest.NewRecorder()
	uploadContext, _ := gin.CreateTestContext(uploadRecorder)
	uploadContext.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files?name="+url.QueryEscape(uploadAliasName), strings.NewReader(`{"type":"codex"}`))
	h.UploadAuthFile(uploadContext)
	if uploadRecorder.Code != http.StatusBadRequest {
		t.Fatalf("raw symlink upload status = %d, want 400; body=%s", uploadRecorder.Code, uploadRecorder.Body.String())
	}
	if got, errRead := os.ReadFile(externalTarget); errRead != nil || string(got) != externalContent {
		t.Fatalf("external symlink target changed: content=%q error=%v", got, errRead)
	}
	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode symlink-root list: %v", errDecode)
	}
	if len(payload.Files) != 1 || payload.Files[0]["name"] != targetName {
		t.Fatalf("symlink-root files = %#v, want only %q", payload.Files, targetName)
	}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(aliasName), nil)
	h.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusBadRequest {
		t.Fatalf("symlink download status = %d, want 400", downloadRecorder.Code)
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(aliasName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusBadRequest {
		t.Fatalf("symlink delete status = %d, want 400", deleteRecorder.Code)
	}
	if _, errStat := os.Stat(filepath.Join(realDir, targetName)); errStat != nil {
		t.Fatalf("symlink target was removed: %v", errStat)
	}
}

func TestRetiredDiskFileDoesNotBindRuntimeOnlyAuthWithSameDisplayName(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "same.json"
	if errWrite := os.WriteFile(filepath.Join(authDir, fileName), []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	runtimeAuth := &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "aistudio",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
			"path":         filepath.Join(authDir, fileName),
		},
	}
	if _, errRegister := manager.Register(t.Context(), runtimeAuth); errRegister != nil {
		t.Fatalf("register runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	var listed struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &listed); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	if len(listed.Files) != 2 {
		t.Fatalf("listed files = %#v, want runtime and retired entries", listed.Files)
	}
	for _, disabled := range []bool{true, false} {
		statusRecorder := httptest.NewRecorder()
		statusContext, _ := gin.CreateTestContext(statusRecorder)
		statusBody := fmt.Sprintf(`{"name":%q,"disabled":%t}`, fileName, disabled)
		statusContext.Request = httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", strings.NewReader(statusBody))
		statusContext.Request.Header.Set("Content-Type", "application/json")
		h.PatchAuthFileStatus(statusContext)
		if statusRecorder.Code != http.StatusOK {
			t.Fatalf("runtime-only status update = %d, want 200; body=%s", statusRecorder.Code, statusRecorder.Body.String())
		}
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	current, ok := manager.GetByID(runtimeAuth.ID)
	if !ok || current == nil || current.Disabled {
		t.Fatalf("runtime-only auth after retired delete = %#v, want active", current)
	}
}

func TestManagedAuthFiles_UnixBackslashNameDoesNotAliasSubdirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("backslash is a path separator on Windows")
	}
	authDir := t.TempDir()
	const rootName = `team\legacy.json`
	const nestedName = "team/legacy.json"
	const rootContent = `{"type":"gemini","email":"root@example.com"}`
	const nestedContent = `{"type":"gemini","email":"nested@example.com"}`
	if errWrite := os.WriteFile(filepath.Join(authDir, rootName), []byte(rootContent), 0o600); errWrite != nil {
		t.Fatalf("write backslash auth: %v", errWrite)
	}
	if errMkdir := os.MkdirAll(filepath.Join(authDir, "team"), 0o700); errMkdir != nil {
		t.Fatalf("create nested dir: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, filepath.FromSlash(nestedName)), []byte(nestedContent), 0o600); errWrite != nil {
		t.Fatalf("write nested auth: %v", errWrite)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = &memoryAuthStore{}

	downloadRecorder := httptest.NewRecorder()
	downloadContext, _ := gin.CreateTestContext(downloadRecorder)
	downloadContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/download?name="+url.QueryEscape(rootName), nil)
	h.DownloadAuthFile(downloadContext)
	if downloadRecorder.Code != http.StatusOK || downloadRecorder.Body.String() != rootContent {
		t.Fatalf("backslash download status = %d, body = %q", downloadRecorder.Code, downloadRecorder.Body.String())
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(rootName), nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("backslash delete status = %d, body = %s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	if got, errRead := os.ReadFile(filepath.Join(authDir, filepath.FromSlash(nestedName))); errRead != nil || string(got) != nestedContent {
		t.Fatalf("nested auth changed: content=%q error=%v", got, errRead)
	}
}

func TestSameManagedAuthFileUsesFilesystemIdentity(t *testing.T) {
	authDir := t.TempDir()
	const firstName = "first.json"
	const secondName = "second.json"
	firstPath := filepath.Join(authDir, firstName)
	if errWrite := os.WriteFile(firstPath, []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	if errLink := os.Link(firstPath, filepath.Join(authDir, secondName)); errLink != nil {
		t.Skipf("hard links are unavailable: %v", errLink)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	if !h.sameManagedAuthFile(firstName, secondName) {
		t.Fatal("sameManagedAuthFile() = false for two names of the same file")
	}
}

func TestWriteAuthFile_UsesResolvedSymlinkRootForAuthID(t *testing.T) {
	realDir := t.TempDir()
	linkRoot := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDir, linkRoot); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: linkRoot}, manager)
	if errWrite := h.writeAuthFile(t.Context(), "supported.json", []byte(`{"type":"codex"}`)); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	if _, ok := manager.GetByID("supported.json"); !ok {
		t.Fatalf("manager auth IDs = %#v, want supported.json", manager.List())
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("manager auth count = %d, want 1", got)
	}
}

func TestListAuthFiles_DoesNotFoldCaseWhenMergingRetiredFiles(t *testing.T) {
	authDir := t.TempDir()
	if errWrite := os.WriteFile(filepath.Join(authDir, "foo.json"), []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       "runtime-upper",
		FileName: "Foo.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(t.TempDir(), "Foo.json"),
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(context)
	var payload struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &payload); errDecode != nil {
		t.Fatalf("decode list response: %v", errDecode)
	}
	names := make(map[string]struct{}, len(payload.Files))
	for _, file := range payload.Files {
		if name, _ := file["name"].(string); name != "" {
			names[name] = struct{}{}
		}
	}
	if _, ok := names["Foo.json"]; !ok {
		t.Fatalf("runtime Foo.json missing from %#v", names)
	}
	if _, ok := names["foo.json"]; !ok {
		t.Fatalf("retired foo.json missing from %#v", names)
	}
}
