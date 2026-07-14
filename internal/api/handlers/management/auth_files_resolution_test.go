package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCaptureManagedAuthFileSnapshotRejectsLeafSymlink(t *testing.T) {
	authDir := t.TempDir()
	const targetName = "target.json"
	const aliasName = "alias.json"
	if errWrite := os.WriteFile(filepath.Join(authDir, targetName), []byte(`{"secret":"target"}`), 0o600); errWrite != nil {
		t.Fatalf("write target: %v", errWrite)
	}
	if errLink := os.Symlink(targetName, filepath.Join(authDir, aliasName)); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer func() {
		if errClose := root.Close(); errClose != nil {
			t.Errorf("close auth root: %v", errClose)
		}
	}()

	_, errSnapshot := captureManagedAuthFileSnapshotAtRoot(root, aliasName)
	if errSnapshot == nil || !strings.Contains(errSnapshot.Error(), "not a regular file") {
		t.Fatalf("capture symlink error = %v, want regular-file rejection", errSnapshot)
	}
	data, errRead := os.ReadFile(filepath.Join(authDir, targetName))
	if errRead != nil || string(data) != `{"secret":"target"}` {
		t.Fatalf("target changed: data=%q error=%v", data, errRead)
	}
}

func TestRemoveManagedAuthFileSnapshotUsesStableParent(t *testing.T) {
	authDir := t.TempDir()
	nestedDir := filepath.Join(authDir, "nested")
	if errMkdir := os.Mkdir(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	const relativePath = "nested/auth.json"
	const originalData = `{"secret":"original"}`
	if errWrite := os.WriteFile(filepath.Join(authDir, relativePath), []byte(originalData), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	externalDir := t.TempDir()
	const externalData = `{"secret":"external"}`
	if errWrite := os.WriteFile(filepath.Join(externalDir, "auth.json"), []byte(externalData), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer closeManagedAuthRoot(root)
	original, errSnapshot := captureManagedAuthFileSnapshotAtRoot(root, relativePath)
	if errSnapshot != nil {
		t.Fatalf("capture original snapshot: %v", errSnapshot)
	}
	parentRoot, leaf, closeParent, errParent := openManagedAuthSnapshotParent(root, relativePath)
	if errParent != nil {
		t.Fatalf("open stable parent: %v", errParent)
	}
	defer func() {
		if errClose := closeParent(); errClose != nil {
			t.Errorf("close stable parent: %v", errClose)
		}
	}()
	movedDir := filepath.Join(authDir, "moved")
	if errRename := os.Rename(nestedDir, movedDir); errRename != nil {
		t.Skipf("renaming an open directory is unavailable: %v", errRename)
	}
	if errLink := os.Symlink(externalDir, nestedDir); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}

	if errRemove := removeManagedAuthFileSnapshotAtParent(parentRoot, leaf, original); errRemove != nil {
		t.Fatalf("remove through stable parent: %v", errRemove)
	}
	if _, errStat := os.Stat(filepath.Join(movedDir, "auth.json")); !os.IsNotExist(errStat) {
		t.Fatalf("original auth still exists after removal: %v", errStat)
	}
	external, errRead := os.ReadFile(filepath.Join(externalDir, "auth.json"))
	if errRead != nil || string(external) != externalData {
		t.Fatalf("external auth changed: data=%q error=%v", external, errRead)
	}
}

func TestAuthFileMutationsDoNotResolveExistingFileNameAsCustomAuthID(t *testing.T) {
	authDir := t.TempDir()
	backingPath := filepath.Join(authDir, "real.json")
	if errWrite := os.WriteFile(backingPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write custom-ID backing file: %v", errWrite)
	}
	const fileName = "alias.json"
	if errWrite := os.WriteFile(filepath.Join(authDir, fileName), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write same-name disk file: %v", errWrite)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       fileName,
		FileName: "real.json",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": backingPath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register custom-ID auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(fileName, "codex", []*registry.ModelInfo{{ID: "custom-only-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(fileName) })
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	requests := []struct {
		name   string
		method string
		path   string
		body   string
		call   func(*gin.Context)
		want   int
	}{
		{name: "models", method: http.MethodGet, path: "/v0/management/auth-files/models?name=" + url.QueryEscape(fileName), call: h.GetAuthFileModels, want: http.StatusOK},
		{name: "status", method: http.MethodPatch, path: "/v0/management/auth-files/status", body: `{"name":"alias.json","disabled":true}`, call: h.PatchAuthFileStatus, want: http.StatusNotFound},
		{name: "fields", method: http.MethodPatch, path: "/v0/management/auth-files/fields", body: `{"name":"alias.json","priority":1}`, call: h.PatchAuthFileFields, want: http.StatusNotFound},
		{name: "cooldown", method: http.MethodPost, path: "/v0/management/auth-files/cooldowns/clear-selected", body: `{"names":["alias.json"]}`, call: h.ClearSelectedAuthCooldowns, want: http.StatusNotFound},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(request.method, request.path, strings.NewReader(request.body))
			ctx.Request.Header.Set("Content-Type", "application/json")
			request.call(ctx)
			if recorder.Code != request.want {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, request.want, recorder.Body.String())
			}
			if request.name == "models" && strings.Contains(recorder.Body.String(), "custom-only-model") {
				t.Fatalf("models response leaked same-named custom auth: %s", recorder.Body.String())
			}
		})
	}
	current, exists := manager.GetByID(fileName)
	if !exists || current == nil || current.Disabled || current.Status != coreauth.StatusActive {
		t.Fatalf("same-named custom auth changed: %#v", current)
	}
}

func TestExternalAuthPathDoesNotFallBackToManagedFileName(t *testing.T) {
	authDir := t.TempDir()
	externalDir := t.TempDir()
	const managedName = "victim.json"
	managedPath := filepath.Join(authDir, managedName)
	externalPath := filepath.Join(externalDir, managedName)
	if errWrite := os.WriteFile(managedPath, []byte(`{"type":"gemini"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired managed file: %v", errWrite)
	}
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	const authID = "tenant-a"
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       authID,
		FileName: managedName,
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": externalPath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register external auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "codex", []*registry.ModelInfo{{ID: "external-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}
	if name, managed := h.managedAuthBackingFileName(mustGetAuthByID(t, manager, authID)); managed || name != "" {
		t.Fatalf("managedAuthBackingFileName() = (%q, %t), want external", name, managed)
	}

	modelsRecorder := httptest.NewRecorder()
	modelsContext, _ := gin.CreateTestContext(modelsRecorder)
	modelsContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/models?name="+authID, nil)
	h.GetAuthFileModels(modelsContext)
	if modelsRecorder.Code != http.StatusOK || !strings.Contains(modelsRecorder.Body.String(), "external-model") {
		t.Fatalf("models status = %d, body=%s", modelsRecorder.Code, modelsRecorder.Body.String())
	}

	deleteRecorder := httptest.NewRecorder()
	deleteContext, _ := gin.CreateTestContext(deleteRecorder)
	deleteContext.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+authID, nil)
	h.DeleteAuthFile(deleteContext)
	if deleteRecorder.Code != http.StatusBadRequest {
		t.Fatalf("delete status = %d, want %d; body=%s", deleteRecorder.Code, http.StatusBadRequest, deleteRecorder.Body.String())
	}
	for _, path := range []string{managedPath, externalPath} {
		if _, errStat := os.Stat(path); errStat != nil {
			t.Fatalf("auth file %q changed: %v", path, errStat)
		}
	}
	if _, exists := manager.GetByID(authID); !exists {
		t.Fatal("external auth was removed from runtime")
	}
}

func TestListAuthFiles_KeepsNestedExternalAuth(t *testing.T) {
	authDir := t.TempDir()
	externalDir := t.TempDir()
	externalPath := filepath.Join(externalDir, "external.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth file: %v", errWrite)
	}

	manager := coreauth.NewManager(nil, nil, nil)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       "external-auth",
		FileName: "team/external.json",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": externalPath,
		},
	}); errRegister != nil {
		t.Fatalf("register external auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
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
	if len(payload.Files) != 1 || payload.Files[0]["name"] != "team/external.json" {
		t.Fatalf("external auth missing from list: %#v", payload.Files)
	}
}

func TestCustomAuthBackingSymlinkCannotRedirectDelete(t *testing.T) {
	authDir := t.TempDir()
	const victimName = "victim.json"
	victimPath := filepath.Join(authDir, victimName)
	if errWrite := os.WriteFile(victimPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write victim auth: %v", errWrite)
	}
	aliasPath := filepath.Join(authDir, "alias.json")
	if errSymlink := os.Symlink(victimName, aliasPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	const authID = "tenant-symlink"
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID:       authID,
		FileName: "alias.json",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": aliasPath,
		},
		Metadata: map[string]any{"type": "codex"},
	}); errRegister != nil {
		t.Fatalf("register symlink-backed auth: %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	h.tokenStore = &memoryAuthStore{}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodDelete, "/v0/management/auth-files?name="+authID, nil)
	h.DeleteAuthFile(ctx)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("delete status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
	if got, errRead := os.ReadFile(victimPath); errRead != nil || string(got) != `{"type":"codex"}` {
		t.Fatalf("victim auth changed: content=%q error=%v", got, errRead)
	}
	if info, errLstat := os.Lstat(aliasPath); errLstat != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("backing symlink changed: info=%v error=%v", info, errLstat)
	}
}

func TestBatchDownloadHelpersKeepSingleAuthRootSnapshot(t *testing.T) {
	firstDir := t.TempDir()
	secondDir := t.TempDir()
	const name = "auth.json"
	if errWrite := os.WriteFile(filepath.Join(firstDir, name), []byte(`{"type":"codex","source":"first"}`), 0o600); errWrite != nil {
		t.Fatalf("write first auth: %v", errWrite)
	}
	if errWrite := os.WriteFile(filepath.Join(secondDir, name), []byte(`{"type":"codex","source":"second"}`), 0o600); errWrite != nil {
		t.Fatalf("write second auth: %v", errWrite)
	}
	cfg := &config.Config{AuthDir: firstDir}
	h := NewHandlerWithoutConfigFilePath(cfg, coreauth.NewManager(nil, nil, nil))
	root, lexicalAuthDir, authDir, errRoot := h.openManagedAuthRootSnapshot()
	if errRoot != nil {
		t.Fatalf("openManagedAuthRootSnapshot() error = %v", errRoot)
	}
	defer closeManagedAuthRoot(root)
	cfg.AuthDir = secondDir

	names, errCanonical := h.canonicalAuthFileNamesAtRoot(root, lexicalAuthDir, authDir, []string{name})
	if errCanonical != nil || len(names) != 1 || names[0] != name {
		t.Fatalf("canonical names = %#v, error=%v", names, errCanonical)
	}
	file, status, errRead := readDownloadAuthFileAtRoot(root, authDir, names[0])
	if errRead != nil || status != http.StatusOK || file == nil || !strings.Contains(string(file.Data), `"source":"first"`) {
		t.Fatalf("snapshot read = %#v, status=%d, error=%v", file, status, errRead)
	}
}

func TestActualManagedAuthFileNameAtRootPreservesWindowsDiskCasing(t *testing.T) {
	authDir := t.TempDir()
	actualName := "Nested/Auth.JSON"
	path := filepath.Join(authDir, filepath.FromSlash(actualName))
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatalf("open auth root: %v", errRoot)
	}
	defer closeManagedAuthRoot(root)
	got, errActual := actualManagedAuthFileNameAtRootForOS(root, "nested/auth.json", "windows")
	if errActual != nil {
		t.Fatalf("actualManagedAuthFileNameAtRootForOS() error = %v", errActual)
	}
	if got != actualName {
		t.Fatalf("actual name = %q, want %q", got, actualName)
	}
	data, displayName, resolvedPath, errRead := readManagedAuthFileAtRootForOS(root, authDir, "nested/auth.json", "windows")
	if errRead != nil {
		t.Fatalf("readManagedAuthFileAtRootForOS() error = %v", errRead)
	}
	if displayName != actualName {
		t.Fatalf("display name = %q, want %q", displayName, actualName)
	}
	if resolvedPath != path {
		t.Fatalf("resolved path = %q, want %q", resolvedPath, path)
	}
	if string(data) != `{"type":"codex"}` {
		t.Fatalf("read data = %s", data)
	}
}

func mustGetAuthByID(t *testing.T, manager *coreauth.Manager, id string) *coreauth.Auth {
	t.Helper()
	auth, ok := manager.GetByID(id)
	if !ok || auth == nil {
		t.Fatalf("auth %q not found", id)
	}
	return auth
}
