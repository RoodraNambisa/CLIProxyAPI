package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	internalstore "github.com/router-for-me/CLIProxyAPI/v6/internal/store"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type strictDeleteFileStore struct {
	conditionalDeletePathStore
	deleteCalls int
}

type embeddedFileDeleteStore struct {
	*sdkAuth.FileTokenStore
	deleteCalls int
	baseDir     string
}

type embeddedGitDeleteStore struct {
	*internalstore.GitTokenStore
	deleteCalls int
	baseDir     string
}

type concurrentDeleteStore struct {
	conditionalDeletePathStore
	peerPath    string
	deleteCalls int
}

type committedDeleteStore struct {
	deleteCalls int
}

type committedCleanupFailureStore struct {
	path string
}

type committedReplacementStore struct {
	path        string
	replacement []byte
}

type selectiveFailureDeleteStore struct {
	conditionalDeletePathStore
	failName string
}

type rolledBackDeleteStore struct{}

type retiringRolledBackDeleteStore struct {
	conditionalDeletePathStore
}

type unconditionalDeleteStore struct {
	deleteCalls int
}

type blockingFallbackDeleteStore struct {
	started chan struct{}
	release chan struct{}
	mu      sync.Mutex
	source  []byte
}

type conditionalDeletePathStore struct {
	baseDir string
}

type logicalIDDeleteStore struct {
	expectedID string
	gotID      string
	source     []byte
}

func (s *conditionalDeletePathStore) SetBaseDir(baseDir string) {
	s.baseDir = baseDir
}

func (s *conditionalDeletePathStore) resolvePath(id string) string {
	if filepath.IsAbs(id) || strings.TrimSpace(s.baseDir) == "" {
		return id
	}
	return filepath.Join(s.baseDir, filepath.FromSlash(id))
}

func conditionalDeleteTestFile(ctx context.Context, id, expectedSourceHash string, deleteFile func(context.Context, string) error) error {
	data, errRead := os.ReadFile(id)
	if errRead != nil && !os.IsNotExist(errRead) {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errRead)
	}
	if errRead == nil && !coreauth.SourceHashMatchesBytes(expectedSourceHash, data) {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, authfileguard.ErrPersistGenerationStale)
	}
	return deleteFile(ctx, id)
}

func (*strictDeleteFileStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*strictDeleteFileStore) Save(context.Context, *coreauth.Auth) (string, error) { return "", nil }

func (*logicalIDDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*logicalIDDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) { return "", nil }

func (s *logicalIDDeleteStore) Delete(_ context.Context, id string) error {
	s.gotID = id
	s.source = nil
	return nil
}

func (s *logicalIDDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	s.gotID = id
	if id != s.expectedID || !coreauth.SourceHashMatchesBytes(expectedSourceHash, s.source) {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, authfileguard.ErrPersistGenerationStale)
	}
	return s.Delete(ctx, id)
}

func (*unconditionalDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*unconditionalDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *unconditionalDeleteStore) Delete(context.Context, string) error {
	s.deleteCalls++
	return nil
}

func (*blockingFallbackDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*blockingFallbackDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *blockingFallbackDeleteStore) Delete(context.Context, string) error {
	close(s.started)
	<-s.release
	return nil
}

func (s *blockingFallbackDeleteStore) DeleteIfSourceHashMatches(_ context.Context, _ string, expectedSourceHash string) error {
	close(s.started)
	<-s.release
	s.mu.Lock()
	defer s.mu.Unlock()
	if !coreauth.SourceHashMatchesBytes(expectedSourceHash, s.source) {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, authfileguard.ErrPersistGenerationStale)
	}
	s.source = nil
	return nil
}

func (s *blockingFallbackDeleteStore) replaceSource(data []byte) {
	s.mu.Lock()
	s.source = bytes.Clone(data)
	s.mu.Unlock()
}

func (s *strictDeleteFileStore) Delete(_ context.Context, id string) error {
	s.deleteCalls++
	return os.Remove(s.resolvePath(id))
}

func (s *strictDeleteFileStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, s.resolvePath(id), expectedSourceHash, s.Delete)
}

func TestDeleteAuthFileFallbackSerializesConcurrentUpload(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write initial auth: %v", errWrite)
	}
	initial := []byte(`{"type":"codex","access_token":"old"}`)
	store := &blockingFallbackDeleteStore{started: make(chan struct{}), release: make(chan struct{}), source: initial}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	type deleteResult struct {
		status int
		err    error
	}
	deleted := make(chan deleteResult, 1)
	go func() {
		_, status, errDelete := h.deleteAuthFileByName(t.Context(), fileName)
		deleted <- deleteResult{status: status, err: errDelete}
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback deletion did not start")
	}

	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	uploaded := make(chan error, 1)
	go func() { uploaded <- h.writeAuthFile(t.Context(), fileName, replacement) }()
	select {
	case errUpload := <-uploaded:
		t.Fatalf("upload bypassed fallback deletion lock: %v", errUpload)
	case <-time.After(150 * time.Millisecond):
	}
	close(store.release)
	select {
	case result := <-deleted:
		if result.err != nil || result.status != http.StatusOK {
			t.Fatalf("delete result = status %d, error %v", result.status, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fallback deletion did not finish")
	}
	select {
	case errUpload := <-uploaded:
		if errUpload != nil {
			t.Fatalf("writeAuthFile() error = %v", errUpload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("upload did not finish after deletion")
	}
	persisted, errRead := os.ReadFile(path)
	if errRead != nil || string(persisted) != string(replacement) {
		t.Fatalf("replacement auth = %s, error %v", persisted, errRead)
	}
}

func TestDeleteAuthFileFallbackPreservesExternalReplacement(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write initial auth: %v", errWrite)
	}
	initial := []byte(`{"type":"codex","access_token":"old"}`)
	store := &blockingFallbackDeleteStore{started: make(chan struct{}), release: make(chan struct{}), source: initial}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	type deleteResult struct {
		status int
		err    error
	}
	deleted := make(chan deleteResult, 1)
	go func() {
		_, status, errDelete := h.deleteAuthFileByName(t.Context(), fileName)
		deleted <- deleteResult{status: status, err: errDelete}
	}()
	select {
	case <-store.started:
	case <-time.After(5 * time.Second):
		t.Fatal("fallback deletion did not start")
	}

	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	tempPath := filepath.Join(authDir, "replacement.tmp")
	if errWrite := os.WriteFile(tempPath, replacement, 0o600); errWrite != nil {
		t.Fatalf("write replacement: %v", errWrite)
	}
	if errRename := os.Rename(tempPath, path); errRename != nil {
		t.Fatalf("replace auth file: %v", errRename)
	}
	store.replaceSource(replacement)
	close(store.release)

	select {
	case result := <-deleted:
		if !errors.Is(result.err, authfileguard.ErrPersistGenerationStale) || result.status != http.StatusInternalServerError {
			t.Fatalf("delete result = status %d, error %v", result.status, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fallback deletion did not finish")
	}
	persisted, errRead := os.ReadFile(path)
	if errRead != nil || string(persisted) != string(replacement) {
		t.Fatalf("replacement auth = %s, error %v", persisted, errRead)
	}
}

func (s *embeddedFileDeleteStore) Delete(_ context.Context, id string) error {
	s.deleteCalls++
	return os.Remove(resolveConditionalDeleteTestPath(s.baseDir, id))
}

func (s *embeddedFileDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, resolveConditionalDeleteTestPath(s.baseDir, id), expectedSourceHash, s.Delete)
}

func (s *embeddedFileDeleteStore) SetBaseDir(baseDir string) {
	s.baseDir = baseDir
	s.FileTokenStore.SetBaseDir(baseDir)
}

func (s *embeddedGitDeleteStore) Delete(_ context.Context, id string) error {
	s.deleteCalls++
	return os.Remove(resolveConditionalDeleteTestPath(s.baseDir, id))
}

func (s *embeddedGitDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, resolveConditionalDeleteTestPath(s.baseDir, id), expectedSourceHash, s.Delete)
}

func (s *embeddedGitDeleteStore) SetBaseDir(baseDir string) {
	s.baseDir = baseDir
	s.GitTokenStore.SetBaseDir(baseDir)
}

func resolveConditionalDeleteTestPath(baseDir, id string) string {
	if filepath.IsAbs(id) || strings.TrimSpace(baseDir) == "" {
		return id
	}
	return filepath.Join(baseDir, filepath.FromSlash(id))
}

func (*concurrentDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*concurrentDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) { return "", nil }

func (s *concurrentDeleteStore) Delete(_ context.Context, id string) error {
	s.deleteCalls++
	if filepath.Base(id) == "alpha.json" {
		_ = os.Remove(s.peerPath)
	}
	return os.Remove(s.resolvePath(id))
}

func (s *concurrentDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, s.resolvePath(id), expectedSourceHash, s.Delete)
}

func (*committedDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*committedDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) { return "", nil }

func (s *committedDeleteStore) Delete(_ context.Context, id string) error {
	s.deleteCalls++
	_ = id
	return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeCommitted, errors.New("delete acknowledgement failed"))
}

func (s *committedDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return s.Delete(ctx, id)
}

func (*committedCleanupFailureStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*committedCleanupFailureStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *committedCleanupFailureStore) Delete(context.Context, string) error {
	if errRemove := os.Remove(s.path); errRemove != nil {
		return errRemove
	}
	return os.Mkdir(s.path, 0o700)
}

func (s *committedCleanupFailureStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (*committedReplacementStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*committedReplacementStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *committedReplacementStore) Delete(context.Context, string) error {
	return os.WriteFile(s.path, s.replacement, 0o600)
}

func (s *committedReplacementStore) DeleteIfSourceHashMatches(ctx context.Context, id, _ string) error {
	return s.Delete(ctx, id)
}

func (*selectiveFailureDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*selectiveFailureDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *selectiveFailureDeleteStore) Delete(_ context.Context, id string) error {
	if filepath.Base(id) == s.failName {
		return errors.New("selective delete failure")
	}
	return os.Remove(s.resolvePath(id))
}

func (s *selectiveFailureDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, s.resolvePath(id), expectedSourceHash, s.Delete)
}

func (*rolledBackDeleteStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }

func (*rolledBackDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) { return "", nil }

func (*rolledBackDeleteStore) Delete(context.Context, string) error {
	return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("delete rolled back"))
}

func (s *rolledBackDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return s.Delete(ctx, id)
}

func (*retiringRolledBackDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*retiringRolledBackDeleteStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}

func (s *retiringRolledBackDeleteStore) Delete(_ context.Context, id string) error {
	if errWrite := os.WriteFile(s.resolvePath(id), []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		return errWrite
	}
	return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("delete rolled back after auth retired"))
}

func (s *retiringRolledBackDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	return conditionalDeleteTestFile(ctx, s.resolvePath(id), expectedSourceHash, s.Delete)
}

func TestDeleteAuthFileAllContinuesWhenListedFileDisappears(t *testing.T) {
	authDir := t.TempDir()
	for _, name := range []string{"alpha.json", "beta.json", "gamma.json"} {
		if errWrite := os.WriteFile(filepath.Join(authDir, name), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
			t.Fatalf("write %s: %v", name, errWrite)
		}
	}
	nestedDir := filepath.Join(authDir, "nested")
	if errMkdir := os.Mkdir(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested directory: %v", errMkdir)
	}
	nestedSidecar := filepath.Join(nestedDir, "sidecar.json")
	if errWrite := os.WriteFile(nestedSidecar, []byte(`{"not":"an auth file"}`), 0o600); errWrite != nil {
		t.Fatalf("write nested sidecar: %v", errWrite)
	}
	store := &concurrentDeleteStore{peerPath: filepath.Join(authDir, "beta.json")}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?all=true", nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete all status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.deleteCalls != 2 {
		t.Fatalf("Delete() calls = %d, want 2", store.deleteCalls)
	}
	entries, errRead := os.ReadDir(authDir)
	if errRead != nil {
		t.Fatalf("read auth dir: %v", errRead)
	}
	for _, entry := range entries {
		if strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			t.Fatalf("auth file still exists after delete all: %s", entry.Name())
		}
	}
	if data, errRead := os.ReadFile(nestedSidecar); errRead != nil || string(data) != `{"not":"an auth file"}` {
		t.Fatalf("nested sidecar changed: data=%q error=%v", data, errRead)
	}
}

func TestDeleteAuthFileBatchFailureIncludesItemStatus(t *testing.T) {
	for _, tt := range []struct {
		name string
		url  func([]string) string
	}{
		{
			name: "query names",
			url: func(names []string) string {
				return "/v0/management/auth-files?name=" + url.QueryEscape(names[0]) + "&name=" + url.QueryEscape(names[1])
			},
		},
		{name: "all", url: func([]string) string { return "/v0/management/auth-files?all=true" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			authDir := t.TempDir()
			names := []string{"alpha.json", "beta.json"}
			for _, name := range names {
				if errWrite := os.WriteFile(filepath.Join(authDir, name), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
					t.Fatalf("write %s: %v", name, errWrite)
				}
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
			h.tokenStore = &selectiveFailureDeleteStore{failName: names[1]}

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodDelete, tt.url(names), nil)
			h.DeleteAuthFile(ctx)

			if recorder.Code != http.StatusMultiStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusMultiStatus, recorder.Body.String())
			}
			var payload struct {
				Failed []struct {
					Name   string `json:"name"`
					Status int    `json:"status"`
				} `json:"failed"`
			}
			if errDecode := json.Unmarshal(recorder.Body.Bytes(), &payload); errDecode != nil {
				t.Fatalf("decode response: %v", errDecode)
			}
			if len(payload.Failed) != 1 || payload.Failed[0].Name != names[1] || payload.Failed[0].Status != http.StatusInternalServerError {
				t.Fatalf("failed items = %#v", payload.Failed)
			}
		})
	}
}

func TestDeleteAuthFile_MissingAuthDirectoryReturnsNotFound(t *testing.T) {
	authDir := filepath.Join(t.TempDir(), "missing")
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name=auth.json", nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestDeleteAuthFile_MissingFileReturnsNotFound(t *testing.T) {
	for _, fileName := range []string{"missing.json", "missing/nested.json"} {
		t.Run(fileName, func(t *testing.T) {
			authDir := t.TempDir()
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
			h.DeleteAuthFile(ctx)
			if recorder.Code != http.StatusNotFound {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
			}
		})
	}
}

func TestDeleteAuthFile_DoesNotUseManagerPathOutsideAuthDir(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	tempDir := t.TempDir()
	authDir := filepath.Join(tempDir, "auth")
	externalDir := filepath.Join(tempDir, "external")
	if errMkdirAuth := os.MkdirAll(authDir, 0o700); errMkdirAuth != nil {
		t.Fatalf("failed to create auth dir: %v", errMkdirAuth)
	}
	if errMkdirExternal := os.MkdirAll(externalDir, 0o700); errMkdirExternal != nil {
		t.Fatalf("failed to create external dir: %v", errMkdirExternal)
	}

	fileName := "codex-user@example.com-plus.json"
	shadowPath := filepath.Join(authDir, fileName)
	realPath := filepath.Join(externalDir, fileName)
	if errWriteShadow := os.WriteFile(shadowPath, []byte(`{"type":"codex","email":"shadow@example.com"}`), 0o600); errWriteShadow != nil {
		t.Fatalf("failed to write shadow file: %v", errWriteShadow)
	}
	if errWriteReal := os.WriteFile(realPath, []byte(`{"type":"codex","email":"real@example.com"}`), 0o600); errWriteReal != nil {
		t.Fatalf("failed to write real file: %v", errWriteReal)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:          "legacy/" + fileName,
		FileName:    fileName,
		Provider:    "codex",
		Status:      coreauth.StatusError,
		Unavailable: true,
		Attributes: map[string]string{
			"path": realPath,
		},
		Metadata: map[string]any{
			"type":  "codex",
			"email": "real@example.com",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("failed to register auth record: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteCtx.Request = deleteReq
	h.DeleteAuthFile(deleteCtx)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, deleteRec.Code, deleteRec.Body.String())
	}
	if _, errStatReal := os.Stat(realPath); errStatReal != nil {
		t.Fatalf("expected external auth file to remain, stat err: %v", errStatReal)
	}
	if _, errStatShadow := os.Stat(shadowPath); !os.IsNotExist(errStatShadow) {
		t.Fatalf("expected requested auth-dir file to be removed, stat err: %v", errStatShadow)
	}

	listRec := httptest.NewRecorder()
	listCtx, _ := gin.CreateTestContext(listRec)
	listReq := httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	listCtx.Request = listReq
	h.ListAuthFiles(listCtx)

	if listRec.Code != http.StatusOK {
		t.Fatalf("expected list status %d, got %d with body %s", http.StatusOK, listRec.Code, listRec.Body.String())
	}
	var listPayload map[string]any
	if errUnmarshal := json.Unmarshal(listRec.Body.Bytes(), &listPayload); errUnmarshal != nil {
		t.Fatalf("failed to decode list payload: %v", errUnmarshal)
	}
	filesRaw, ok := listPayload["files"].([]any)
	if !ok {
		t.Fatalf("expected files array, payload: %#v", listPayload)
	}
	if len(filesRaw) != 1 {
		t.Fatalf("expected external runtime auth to remain listed, got %d entries", len(filesRaw))
	}
}

func TestDeleteAuthFile_FallbackToAuthDirPath(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	fileName := "fallback-user.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("failed to write auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	deleteRec := httptest.NewRecorder()
	deleteCtx, _ := gin.CreateTestContext(deleteRec)
	deleteReq := httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	deleteCtx.Request = deleteReq
	h.DeleteAuthFile(deleteCtx)

	if deleteRec.Code != http.StatusOK {
		t.Fatalf("expected delete status %d, got %d with body %s", http.StatusOK, deleteRec.Code, deleteRec.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("expected auth file to be removed from auth dir, stat err: %v", errStat)
	}
}

func TestDeleteAuthFile_WithNilManagerDeletesLocalFile(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "nil-manager.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	h.tokenStore = sdkAuth.NewFileTokenStore()

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
}

func TestDeleteAuthFile_DirectStoreCommittedOutcomeSucceeds(t *testing.T) {
	tests := []struct {
		name    string
		content string
		manager *coreauth.Manager
	}{
		{name: "manager unavailable", content: `{"type":"codex"}`},
		{name: "retired file", content: `{"type":"gemini"}`, manager: coreauth.NewManager(nil, nil, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authDir := t.TempDir()
			const fileName = "direct-delete.json"
			filePath := filepath.Join(authDir, fileName)
			if errWrite := os.WriteFile(filePath, []byte(test.content), 0o600); errWrite != nil {
				t.Fatalf("write auth file: %v", errWrite)
			}
			store := &committedDeleteStore{}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, test.manager)
			h.tokenStore = store

			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
			h.DeleteAuthFile(ctx)

			if recorder.Code != http.StatusOK {
				t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
			}
			if store.deleteCalls != 1 {
				t.Fatalf("Delete() calls = %d, want 1", store.deleteCalls)
			}
			if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
				t.Fatalf("auth file still exists: %v", errStat)
			}
		})
	}
}

func TestDeleteAuthFile_ManagerStoreCommittedOutcomeSucceeds(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "managed-committed-delete.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: "codex",
		Attributes: map[string]string{"path": filePath},
		Metadata:   map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	store := &committedDeleteStore{}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(filePath); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
	if _, exists := manager.GetByID(fileName); exists {
		t.Fatal("committed deletion left auth in manager")
	}
}

func TestDeleteAuthFile_CustomStoreKeepsQuarantineWhenLocalCleanupFails(t *testing.T) {
	configDir := t.TempDir()
	authDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	const fileName = "custom-cleanup-failure.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	h := NewHandler(&config.Config{AuthDir: authDir}, configPath, nil)
	h.tokenStore = &committedCleanupFailureStore{path: filePath}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if !authfileguard.IsQuarantined(filePath) {
		t.Fatal("local cleanup failure did not retain auth quarantine")
	}
	authfileguard.ClearQuarantined(filePath)
	if errLoad := watcher.LoadAuthDeleteQuarantine(configPath, authDir); errLoad != nil {
		t.Fatalf("reload auth delete quarantine: %v", errLoad)
	}
	if !authfileguard.IsQuarantined(filePath) {
		t.Fatal("restart did not restore custom store delete quarantine")
	}
	t.Cleanup(func() { authfileguard.ClearQuarantined(filePath) })
}

func TestDeleteAuthFile_CustomStoreKeepsQuarantineForConcurrentReplacement(t *testing.T) {
	configDir := t.TempDir()
	authDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	const fileName = "custom-replacement.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex","generation":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	replacement := []byte(`{"type":"codex","generation":"new"}`)
	h := NewHandler(&config.Config{AuthDir: authDir}, configPath, nil)
	h.tokenStore = &committedReplacementStore{path: filePath, replacement: replacement}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if got, errRead := os.ReadFile(filePath); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement changed: data=%q error=%v", got, errRead)
	}
	if !authfileguard.IsQuarantined(filePath) {
		t.Fatal("concurrent replacement did not retain auth quarantine")
	}
	t.Cleanup(func() { authfileguard.ClearQuarantined(filePath) })
}

func TestDeleteLocalAuthFileDurablyClearsPriorQuarantineWhenFileIsMissing(t *testing.T) {
	configDir := t.TempDir()
	authDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.yaml")
	const fileName = "missing/already-deleted.json"
	path := filepath.Join(authDir, fileName)
	oldGeneration := authfileguard.NewDeleteGeneration(coreauth.SourceHashFromBytes([]byte(`{"type":"codex"}`)))
	if errPersist := watcher.PersistAuthDeleteQuarantine(configPath, authDir, path, oldGeneration); errPersist != nil {
		t.Fatalf("persist prior quarantine: %v", errPersist)
	}
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer root.Close()
	h := NewHandler(&config.Config{AuthDir: authDir}, configPath, nil)
	if errDelete := h.deleteLocalAuthFileDurably(sdkAuth.NewFileTokenStore(), root, authDir, fileName); errDelete != nil {
		t.Fatalf("deleteLocalAuthFileDurably() error = %v", errDelete)
	}
	if authfileguard.IsQuarantined(path) {
		t.Fatal("idempotent missing-file deletion left quarantine active")
	}
	if errLoad := watcher.LoadAuthDeleteQuarantine(configPath, authDir); errLoad != nil {
		t.Fatalf("reload auth delete quarantine: %v", errLoad)
	}
	if authfileguard.IsQuarantined(path) {
		t.Fatal("idempotent missing-file deletion left durable tombstone")
	}
}

func TestDeleteAuthFile_RetiredRollbackDoesNotRestoreRuntimeAuth(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "retired-rollback.json"
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": path,
		},
	}); errRegister != nil {
		t.Fatalf("register stale runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &rolledBackDeleteStore{}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if auth, ok := manager.GetByID(fileName); ok || auth != nil {
		t.Fatalf("retired runtime auth was restored after rollback: %#v", auth)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("rolled-back retired file was removed: %v", errStat)
	}
}

func TestDeleteAuthFile_RetiredDuringRollbackDoesNotRestoreRuntimeAuth(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "retired-during-delete.json"
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"active"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       fileName,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": path,
		},
	}); errRegister != nil {
		t.Fatalf("register runtime auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &retiringRolledBackDeleteStore{}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if auth, ok := manager.GetByID(fileName); ok || auth != nil {
		t.Fatalf("runtime auth was restored after file became retired: %#v", auth)
	}
	data, errRead := os.ReadFile(path)
	if errRead != nil || !coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		t.Fatalf("retired file = %s, %v; want retained retired content", data, errRead)
	}
}

func TestDeleteAuthFile_CustomStoreOwnsDeletionOnce(t *testing.T) {
	authDir := t.TempDir()
	fileName := "custom-store.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	store := &strictDeleteFileStore{}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.deleteCalls != 1 {
		t.Fatalf("custom store Delete() calls = %d, want 1", store.deleteCalls)
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
}

func TestDeleteAuthFile_CustomStoreUsesLogicalAuthID(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "custom-file-name.json"
	const authID = "logical-store-record"
	filePath := filepath.Join(authDir, fileName)
	source := []byte(`{"type":"codex"}`)
	if errWrite := os.WriteFile(filePath, source, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       authID,
		FileName: fileName,
		Provider: "codex",
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	store := &logicalIDDeleteStore{expectedID: authID, source: source}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.gotID != authID {
		t.Fatalf("conditional delete ID = %q, want %q", store.gotID, authID)
	}
	if _, errStat := os.Stat(filePath); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
	if _, exists := manager.GetByID(authID); exists {
		t.Fatal("deleted auth remains in manager")
	}
}

func TestDeleteAuthFile_CustomStoreWithoutConditionalDeleteFailsClosed(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "unsafe-custom-store.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	store := &unconditionalDeleteStore{}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.deleteCalls != 0 {
		t.Fatalf("unsafe Delete() calls = %d, want 0", store.deleteCalls)
	}
	if _, errStat := os.Stat(filePath); errStat != nil {
		t.Fatalf("auth file was removed: %v", errStat)
	}
}

func TestDeleteAuthFile_EmbeddedFileStoreUsesCustomDelete(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "embedded-store.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	store := &embeddedFileDeleteStore{FileTokenStore: sdkAuth.NewFileTokenStore()}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.deleteCalls != 1 {
		t.Fatalf("embedded custom Delete() calls = %d, want 1", store.deleteCalls)
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
}

func TestDeleteAuthFile_EmbeddedGitStoreUsesCustomDelete(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "embedded-git-store.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	store := &embeddedGitDeleteStore{GitTokenStore: internalstore.NewGitTokenStore("", "", "", "")}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if store.deleteCalls != 1 {
		t.Fatalf("embedded Git custom Delete() calls = %d, want 1", store.deleteCalls)
	}
}

func TestDeleteAuthFile_LocalStoreUsesStableSymlinkRoot(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDir, linkDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	const fileName = "nested/auth.json"
	filePath := filepath.Join(realDir, filepath.FromSlash(fileName))
	if errMkdir := os.MkdirAll(filepath.Dir(filePath), 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"gemini-cli","email":"legacy@example.com"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	store := sdkAuth.NewFileTokenStore()
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: linkDir}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
}

func TestDeleteAuthFile_LocalStoreUsesOpenedRootSnapshot(t *testing.T) {
	authDirA := t.TempDir()
	authDirB := t.TempDir()
	const fileName = "auth.json"
	if errWrite := os.WriteFile(filepath.Join(authDirA, fileName), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(authDirB, fileName), []byte(`{"type":"codex","dir":"b"}`), 0o600); errWrite != nil {
		t.Fatalf("write hot-reloaded auth file: %v", errWrite)
	}
	root, errOpen := os.OpenRoot(authDirA)
	if errOpen != nil {
		t.Fatalf("open auth root: %v", errOpen)
	}
	defer root.Close()

	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(authDirB)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDirB}, coreauth.NewManager(nil, nil, nil))
	h.tokenStore = store
	deletedName, status, errDelete := h.deleteAuthFileByNameAtRoot(t.Context(), root, authDirA, authDirA, fileName)
	if errDelete != nil || status != http.StatusOK {
		t.Fatalf("delete = (%q, %d, %v), want success", deletedName, status, errDelete)
	}
	if _, errStat := os.Stat(filepath.Join(authDirA, fileName)); !os.IsNotExist(errStat) {
		t.Fatalf("opened-root auth file still exists: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(authDirB, fileName)); errStat != nil {
		t.Fatalf("hot-reloaded auth file was removed: %v", errStat)
	}
}

func TestDeleteAuthFile_ResolvesCustomAuthIDToBackingFile(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	filePath := filepath.Join(authDir, "real.json")
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       "tenant-a",
		FileName: "real.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filePath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape("tenant-a"), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(filePath); !os.IsNotExist(errStat) {
		t.Fatalf("custom-ID backing file still exists: %v", errStat)
	}
	if current, exists := manager.GetByID("tenant-a"); exists || current != nil {
		t.Fatalf("custom-ID auth remained after delete: %#v", current)
	}
}

func TestDeleteAuthFile_RemovesFileNameOnlyRuntimeAuth(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "filename-only.json"
	filePath := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(filePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       "custom-filename-only",
		FileName: fileName,
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	if found := h.findManagedFileAuth(fileName); found == nil || found.ID != "custom-filename-only" {
		t.Fatalf("findManagedFileAuth() = %#v", found)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape(fileName), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	current, exists := manager.GetByID("custom-filename-only")
	if exists || current != nil {
		t.Fatalf("FileName-only auth remained after delete: %#v", current)
	}
}

func TestDeleteAuthFile_ExistingFileNameTakesPrecedenceOverCustomAuthID(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	authDir := t.TempDir()
	backingPath := filepath.Join(authDir, "real.json")
	shadowPath := filepath.Join(authDir, "alias.json")
	if errWrite := os.WriteFile(backingPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write backing file: %v", errWrite)
	}
	const shadowContent = `{"type":"gemini-cli"}`
	if errWrite := os.WriteFile(shadowPath, []byte(shadowContent), 0o600); errWrite != nil {
		t.Fatalf("write shadow file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       "alias.json",
		FileName: "real.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": backingPath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+url.QueryEscape("alias.json"), nil)
	h.DeleteAuthFile(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if _, errStat := os.Stat(backingPath); errStat != nil {
		t.Fatalf("custom-ID backing file was removed: %v", errStat)
	}
	if _, errStat := os.Stat(shadowPath); !os.IsNotExist(errStat) {
		t.Fatalf("same-name disk file still exists: %v", errStat)
	}
	if current, exists := manager.GetByID("alias.json"); !exists || current == nil || current.Disabled {
		t.Fatalf("custom-ID auth changed while deleting same-name disk file: %#v", current)
	}
}
