package auth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	internalauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/kimi"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/vertex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/xai"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestExtractAccessToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata map[string]any
		expected string
	}{
		{
			"antigravity top-level access_token",
			map[string]any{"access_token": "tok-abc"},
			"tok-abc",
		},
		{
			"gemini nested token.access_token",
			map[string]any{
				"token": map[string]any{"access_token": "tok-nested"},
			},
			"tok-nested",
		},
		{
			"top-level takes precedence over nested",
			map[string]any{
				"access_token": "tok-top",
				"token":        map[string]any{"access_token": "tok-nested"},
			},
			"tok-top",
		},
		{
			"empty metadata",
			map[string]any{},
			"",
		},
		{
			"whitespace-only access_token",
			map[string]any{"access_token": "   "},
			"",
		},
		{
			"wrong type access_token",
			map[string]any{"access_token": 12345},
			"",
		},
		{
			"token is not a map",
			map[string]any{"token": "not-a-map"},
			"",
		},
		{
			"nested whitespace-only",
			map[string]any{
				"token": map[string]any{"access_token": "  "},
			},
			"",
		},
		{
			"fallback to nested when top-level empty",
			map[string]any{
				"access_token": "",
				"token":        map[string]any{"access_token": "tok-fallback"},
			},
			"tok-fallback",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractAccessToken(tt.metadata)
			if got != tt.expected {
				t.Errorf("extractAccessToken() = %q, want %q", got, tt.expected)
			}
		})
	}
}

func TestCaptureFileAuthSnapshotRejectsLeafSymlink(t *testing.T) {
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

	_, errSnapshot := captureFileAuthSnapshot(root, aliasName)
	if errSnapshot == nil || !strings.Contains(errSnapshot.Error(), "not a regular file") {
		t.Fatalf("capture symlink error = %v, want regular-file rejection", errSnapshot)
	}
}

func TestReadAuthFileSnapshotRejectsLeafSymlink(t *testing.T) {
	authDir := t.TempDir()
	targetPath := filepath.Join(authDir, "target.json")
	aliasPath := filepath.Join(authDir, "alias.json")
	if errWrite := os.WriteFile(targetPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write target: %v", errWrite)
	}
	if errLink := os.Symlink(filepath.Base(targetPath), aliasPath); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	if _, errRead := ReadAuthFileSnapshot(aliasPath); errRead == nil {
		t.Fatal("ReadAuthFileSnapshot() accepted a leaf symlink")
	}
}

func TestReadAuthFileSnapshotPreservesTrailingSpace(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not preserve trailing spaces in file names")
	}
	path := filepath.Join(t.TempDir(), "auth.json ")
	want := []byte(`{"type":"codex"}`)
	if errWrite := os.WriteFile(path, want, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	got, errRead := ReadAuthFileSnapshot(path)
	if errRead != nil {
		t.Fatalf("ReadAuthFileSnapshot() error = %v", errRead)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("ReadAuthFileSnapshot() = %q, want %q", got, want)
	}
}

func TestFileTokenStoreReadAuthFileSetsCanonicalSourceHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	data := []byte(`{"type":"claude","email":"reader@example.com"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := NewFileTokenStore()
	auth, err := store.readAuthFile(t.Context(), path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	if got, want := auth.Attributes[cliproxyauth.SourceHashAttributeKey], cliproxyauth.SourceHashFromBytes(wantRaw); got != want {
		t.Fatalf("source hash = %q, want %q", got, want)
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(data); rawHash == auth.Attributes[cliproxyauth.SourceHashAttributeKey] {
		t.Fatal("expected canonical source hash to differ from raw file hash")
	}
}

func TestFileTokenStoreReadsFileBackedGeminiAPIKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gemini-key.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","api_key":"active-key"}`), 0o600); errWrite != nil {
		t.Fatalf("write Gemini API key file: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(dir)
	auths, errList := store.List(t.Context())
	if errList != nil {
		t.Fatalf("List() error = %v", errList)
	}
	if len(auths) != 1 || auths[0].Attributes["api_key"] != "active-key" || cliproxyauth.IsRetiredGeminiCLIAuth(auths[0]) {
		t.Fatalf("listed Gemini API key auths = %#v", auths)
	}
}

func TestWriteRootFileAtomicallyForSnapshotRejectsConcurrentReplacement(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	original := []byte(`{"type":"codex","access_token":"original"}`)
	if errWrite := os.WriteFile(filepath.Join(dir, fileName), original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()

	expected, errSnapshot := captureFileTokenSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatalf("captureFileTokenSnapshot() error = %v", errSnapshot)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, data := range [][]byte{
		[]byte(`{"type":"codex","access_token":"first"}`),
		[]byte(`{"type":"codex","access_token":"second"}`),
	} {
		data := bytes.Clone(data)
		go func() {
			<-start
			results <- writeRootFileAtomicallyForSnapshot(root, fileName, data, &expected, filepath.Join(dir, fileName))
		}()
	}
	close(start)

	successes := 0
	stale := 0
	for range 2 {
		errWrite := <-results
		switch {
		case errWrite == nil:
			successes++
		case errors.Is(errWrite, authfileguard.ErrPersistGenerationStale):
			stale++
		default:
			t.Fatalf("concurrent write error = %v", errWrite)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("concurrent writes = %d success, %d stale; want 1/1", successes, stale)
	}
}

func TestFileTokenSnapshotRejectsSameContentReplacement(t *testing.T) {
	dir := t.TempDir()
	const fileName = "auth.json"
	data := []byte(`{"type":"codex","access_token":"same"}`)
	path := filepath.Join(dir, fileName)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	snapshot, errSnapshot := captureFileTokenSnapshot(root, fileName)
	if errSnapshot != nil {
		t.Fatalf("captureFileTokenSnapshot() error = %v", errSnapshot)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove original auth: %v", errRemove)
	}
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errValidate := snapshot.validate(root, fileName, path); !errors.Is(errValidate, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("validate() error = %v, want %v", errValidate, authfileguard.ErrPersistGenerationStale)
	}
}

func TestFileTokenStoreReadAuthFilePrefersMetadataPlanTypeForCodex(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	data := []byte(`{"type":"codex","email":"reader@example.com","plan_type":"team","id_token":"` + testCodexIDToken("acct-1", "pro") + `"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := NewFileTokenStore()
	auth, err := store.readAuthFile(t.Context(), path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	if got := auth.Attributes["plan_type"]; got != "team" {
		t.Fatalf("plan_type = %q, want %q", got, "team")
	}
}

func TestFileTokenStoreList_FollowsRootSymlinkAndSkipsFileSymlink(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDir, linkDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if errWrite := os.WriteFile(filepath.Join(realDir, "supported.json"), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write supported auth: %v", errWrite)
	}
	externalPath := filepath.Join(t.TempDir(), "external.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	if errSymlink := os.Symlink(externalPath, filepath.Join(realDir, "alias.json")); errSymlink != nil {
		t.Fatalf("create auth file symlink: %v", errSymlink)
	}

	store := NewFileTokenStore()
	store.SetBaseDir(linkDir)
	auths, errList := store.List(t.Context())
	if errList != nil {
		t.Fatalf("List() error = %v", errList)
	}
	if len(auths) != 1 || auths[0].FileName != "supported.json" {
		t.Fatalf("List() auths = %#v, want only supported.json", auths)
	}
}

func TestFileTokenStoreSave_RejectsSymlinkTarget(t *testing.T) {
	authDir := t.TempDir()
	externalPath := filepath.Join(t.TempDir(), "external.json")
	const externalContent = `{"type":"codex","email":"external@example.com"}`
	if errWrite := os.WriteFile(externalPath, []byte(externalContent), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	aliasPath := filepath.Join(authDir, "alias.json")
	if errSymlink := os.Symlink(externalPath, aliasPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:         "alias.json",
		FileName:   "alias.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": aliasPath},
		Metadata:   map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() followed a symlink target")
	}
	if got, errRead := os.ReadFile(externalPath); errRead != nil || string(got) != externalContent {
		t.Fatalf("external target changed: content=%q error=%v", got, errRead)
	}
}

func TestFileTokenStoreSaveAndDeleteRejectIntermediateSymlink(t *testing.T) {
	authDir := t.TempDir()
	externalDir := t.TempDir()
	linkPath := filepath.Join(authDir, "nested")
	if errSymlink := os.Symlink(externalDir, linkPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	targetPath := filepath.Join(linkPath, "auth.json")
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:         "nested/auth.json",
		FileName:   "nested/auth.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": targetPath},
		Metadata:   map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() followed an intermediate symlink")
	}
	if errDelete := store.Delete(t.Context(), "nested/auth.json"); errDelete == nil {
		t.Fatal("Delete() followed an intermediate symlink")
	}
	if _, errStat := os.Stat(filepath.Join(externalDir, "auth.json")); !os.IsNotExist(errStat) {
		t.Fatalf("external auth file exists after rejected operations: %v", errStat)
	}
}

func TestFileTokenStoreSaveCreatesMissingBaseDir(t *testing.T) {
	baseDir := filepath.Join(t.TempDir(), "missing", "auths")
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	savedPath, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	if savedPath != filepath.Join(baseDir, "auth.json") {
		t.Fatalf("Save() path = %q", savedPath)
	}
	if _, errStat := os.Stat(savedPath); errStat != nil {
		t.Fatalf("saved auth does not exist: %v", errStat)
	}
}

func TestFileTokenStoreSaveRefusesRetiredGeminiCLIOverwrite(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy.json")
	const retiredContent = `{"type":"gemini","access_token":"retired-token"}`
	if errWrite := os.WriteFile(path, []byte(retiredContent), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only error", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || string(got) != retiredContent {
		t.Fatalf("retired auth changed: content=%q error=%v", got, errRead)
	}
}

func TestFileTokenStoreSaveRejectsQuarantinedAuthPath(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "pending.json")
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "pending.json",
		FileName: "pending.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement-token"},
	})
	if !errors.Is(errSave, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("Save() error = %v, want pending deletion error", errSave)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("quarantined auth file was created: %v", errStat)
	}
}

func TestFileTokenStoreSaveRejectsRetiredFileCreatedDuringMarshal(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "auth.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	storage := &blockingMarshaledTokenStorage{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		data:    []byte(`{"type":"codex","access_token":"new"}`),
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
			ID: "auth.json", FileName: "auth.json", Provider: "codex", Storage: storage,
		})
		saveDone <- errSave
	}()
	select {
	case <-storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("storage marshal did not start")
	}
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		close(storage.release)
		t.Fatalf("write concurrent retired auth: %v", errWrite)
	}
	close(storage.release)
	select {
	case errSave := <-saveDone:
		if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
			t.Fatalf("Save() error = %v, want retired read-only", errSave)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Save() did not finish")
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, retired) {
		t.Fatalf("concurrent retired auth changed: data=%s error=%v", got, errRead)
	}
	authfileguard.ClearRetired(path)
}

func TestFileTokenStoreRetiredPathRequiresConfirmedDeleteBeforeReuse(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"retired-token"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	replacement := &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement-token"},
	}
	if _, errSave := store.Save(t.Context(), replacement); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("first Save() error = %v, want retired read-only error", errSave)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"external"}`), 0o600); errWrite != nil {
		t.Fatalf("externally rewrite retired path: %v", errWrite)
	}
	if _, errSave := store.Save(t.Context(), replacement); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("second Save() error = %v, want retired read-only error", errSave)
	}
	if errDelete := store.Delete(t.Context(), "legacy.json"); errDelete != nil {
		t.Fatalf("Delete() error = %v", errDelete)
	}
	if _, errSave := store.Save(t.Context(), replacement); errSave != nil {
		t.Fatalf("Save() after confirmed delete error = %v", errSave)
	}
}

func TestFileTokenStoreRetiredPathDoesNotBlockRedirectedBase(t *testing.T) {
	realDirA := t.TempDir()
	realDirB := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDirA, linkDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	aliasPath := filepath.Join(linkDir, "legacy.json")
	if errWrite := os.WriteFile(filepath.Join(realDirA, "legacy.json"), []byte(`{"type":"gemini","access_token":"retired"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	t.Cleanup(func() {
		authfileguard.ClearRetired(aliasPath)
		authfileguard.ClearRetired(filepath.Join(realDirA, "legacy.json"))
	})

	store := NewFileTokenStore()
	store.SetBaseDir(linkDir)
	replacement := &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	}
	if _, errSave := store.Save(t.Context(), replacement); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("first Save() error = %v, want retired read-only error", errSave)
	}
	if errRemove := os.Remove(linkDir); errRemove != nil {
		t.Fatalf("remove base symlink: %v", errRemove)
	}
	if errSymlink := os.Symlink(realDirB, linkDir); errSymlink != nil {
		t.Fatalf("redirect base symlink: %v", errSymlink)
	}
	if _, errSave := store.Save(t.Context(), replacement); errSave != nil {
		t.Fatalf("redirected Save() error = %v", errSave)
	}
	if data, errRead := os.ReadFile(filepath.Join(realDirB, "legacy.json")); errRead != nil || !bytes.Contains(data, []byte(`"type":"codex"`)) {
		t.Fatalf("redirect target replacement = %s, %v", data, errRead)
	}
	if !authfileguard.IsRetired(filepath.Join(realDirA, "legacy.json")) {
		t.Fatal("original retired path lost its quarantine")
	}
}

func TestFileTokenStoreSaveRefusesCreatingRetiredGeminiCLIAuth(t *testing.T) {
	authDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "gemini", "access_token": "retired-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only error", errSave)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "legacy.json")); !os.IsNotExist(errStat) {
		t.Fatalf("retired auth file was created: %v", errStat)
	}
}

func TestFileTokenStoreListDoesNotWriteFetchedAntigravityProjectID(t *testing.T) {
	authDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	path := filepath.Join(authDir, "antigravity.json")
	wantData := []byte(`{"type":"antigravity","access_token":"token"}`)
	if errWrite := os.WriteFile(path, wantData, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	originalFetch := fetchAntigravityProjectID
	fetchAntigravityProjectID = func(context.Context, string, *http.Client) (string, error) {
		return "project-from-upstream", nil
	}
	t.Cleanup(func() { fetchAntigravityProjectID = originalFetch })
	auths, errList := store.List(t.Context())
	if errList != nil {
		t.Fatalf("List() error = %v", errList)
	}
	if len(auths) != 1 || auths[0] == nil || auths[0].Metadata["project_id"] != "project-from-upstream" {
		t.Fatalf("listed auths = %#v", auths)
	}
	gotData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read auth file: %v", errRead)
	}
	if !bytes.Equal(gotData, wantData) {
		t.Fatalf("List() changed auth file: got %s want %s", gotData, wantData)
	}
	diskAuth := &cliproxyauth.Auth{}
	if errSync := cliproxyauth.SyncPersistedMetadataAndSourceHash(diskAuth, wantData); errSync != nil {
		t.Fatalf("SyncPersistedMetadataAndSourceHash() error = %v", errSync)
	}
	if got, want := auths[0].Attributes[cliproxyauth.SourceHashAttributeKey], diskAuth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != want {
		t.Fatalf("source hash = %q, want disk hash %q", got, want)
	}
}

func TestFileTokenStoreAntigravityDiscoveryUsesCallerContext(t *testing.T) {
	authDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	path := filepath.Join(authDir, "antigravity.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"antigravity","access_token":"token"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}

	type contextKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(t.Context(), contextKey{}, "caller-value"))
	defer cancel()
	fetchStarted := make(chan context.Context, 1)
	originalFetch := fetchAntigravityProjectID
	fetchAntigravityProjectID = func(fetchCtx context.Context, _ string, _ *http.Client) (string, error) {
		fetchStarted <- fetchCtx
		<-fetchCtx.Done()
		return "", fetchCtx.Err()
	}
	t.Cleanup(func() { fetchAntigravityProjectID = originalFetch })

	listDone := make(chan error, 1)
	go func() {
		_, errList := store.List(ctx)
		listDone <- errList
	}()
	select {
	case fetchCtx := <-fetchStarted:
		if got := fetchCtx.Value(contextKey{}); got != "caller-value" {
			t.Fatalf("fetch context value = %#v, want caller value", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("project discovery did not start")
	}
	cancel()
	select {
	case errList := <-listDone:
		if !errors.Is(errList, context.Canceled) {
			t.Fatalf("List() error = %v, want context canceled", errList)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("List() did not stop after caller cancellation")
	}
}

func TestFileTokenStoreAntigravityDiscoveryDoesNotBlockMutationAndDiscardsStaleResult(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(context.Context, *FileTokenStore) error
		verify func(*testing.T, string)
	}{
		{
			name: "Save",
			mutate: func(ctx context.Context, store *FileTokenStore) error {
				_, errSave := store.Save(ctx, &cliproxyauth.Auth{
					ID:       "antigravity.json",
					FileName: "antigravity.json",
					Provider: "antigravity",
					Metadata: map[string]any{"type": "antigravity", "access_token": "new-token"},
				})
				return errSave
			},
			verify: func(t *testing.T, path string) {
				t.Helper()
				data, errRead := os.ReadFile(path)
				if errRead != nil {
					t.Fatalf("read saved auth: %v", errRead)
				}
				if !bytes.Contains(data, []byte(`"access_token":"new-token"`)) || bytes.Contains(data, []byte("stale-project")) {
					t.Fatalf("saved auth = %s, want new token without stale project", data)
				}
			},
		},
		{
			name: "Delete",
			mutate: func(ctx context.Context, store *FileTokenStore) error {
				return store.Delete(ctx, "antigravity.json")
			},
			verify: func(t *testing.T, path string) {
				t.Helper()
				if _, errStat := os.Stat(path); !errors.Is(errStat, fs.ErrNotExist) {
					t.Fatalf("deleted auth still exists: %v", errStat)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authDir := t.TempDir()
			store := NewFileTokenStore()
			store.SetBaseDir(authDir)
			path := filepath.Join(authDir, "antigravity.json")
			if errWrite := os.WriteFile(path, []byte(`{"type":"antigravity","access_token":"old-token"}`), 0o600); errWrite != nil {
				t.Fatalf("write auth file: %v", errWrite)
			}

			fetchStarted := make(chan struct{}, 1)
			fetchContinue := make(chan struct{})
			originalFetch := fetchAntigravityProjectID
			fetchAntigravityProjectID = func(context.Context, string, *http.Client) (string, error) {
				fetchStarted <- struct{}{}
				<-fetchContinue
				return "stale-project", nil
			}
			t.Cleanup(func() { fetchAntigravityProjectID = originalFetch })

			type listResult struct {
				auths []*cliproxyauth.Auth
				err   error
			}
			listDone := make(chan listResult, 1)
			go func() {
				auths, errList := store.List(t.Context())
				listDone <- listResult{auths: auths, err: errList}
			}()
			select {
			case <-fetchStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("project discovery did not start")
			}

			mutationDone := make(chan error, 1)
			go func() { mutationDone <- tt.mutate(t.Context(), store) }()
			select {
			case errMutation := <-mutationDone:
				if errMutation != nil {
					close(fetchContinue)
					t.Fatalf("mutation error = %v", errMutation)
				}
			case <-time.After(5 * time.Second):
				close(fetchContinue)
				t.Fatal("mutation blocked on project discovery")
			}
			close(fetchContinue)
			tt.verify(t, path)

			select {
			case result := <-listDone:
				if result.err != nil {
					t.Fatalf("List() error = %v", result.err)
				}
				if len(result.auths) != 0 {
					t.Fatalf("List() published stale enriched auths = %#v", result.auths)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("List() did not finish")
			}
		})
	}
}

func TestFileTokenStoreSaveAllowsExplicitRelativePathWithoutBaseDir(t *testing.T) {
	t.Chdir(t.TempDir())
	store := NewFileTokenStore()
	auth := &cliproxyauth.Auth{
		ID:       "relative.json",
		FileName: "relative.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	savedPath, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	if savedPath != "relative.json" {
		t.Fatalf("Save() path = %q, want relative.json", savedPath)
	}
	if _, errStat := os.Stat("relative.json"); errStat != nil {
		t.Fatalf("relative auth was not written: %v", errStat)
	}
}

func TestFileTokenStoreLegacyStorageCannotRedirectPinnedBase(t *testing.T) {
	realDirA := t.TempDir()
	realDirB := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDirA, linkDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	storage := &blockingLegacyTokenStorage{started: make(chan string, 1), release: make(chan struct{})}
	store := NewFileTokenStore()
	store.SetBaseDir(linkDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  storage,
		Metadata: map[string]any{"type": "codex"},
	}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), auth)
		saveDone <- errSave
	}()
	var storagePath string
	select {
	case storagePath = <-storage.started:
		if _, inside := relativePathWithin(realDirA, storagePath); inside {
			t.Fatalf("legacy storage received managed path %q", storagePath)
		}
		if _, inside := relativePathWithin(realDirB, storagePath); inside {
			t.Fatalf("legacy storage received redirect target path %q", storagePath)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy storage save did not start")
	}
	if errRemove := os.Remove(linkDir); errRemove != nil {
		t.Fatalf("remove base symlink: %v", errRemove)
	}
	if errSymlink := os.Symlink(realDirB, linkDir); errSymlink != nil {
		t.Fatalf("replace base symlink: %v", errSymlink)
	}
	close(storage.release)
	select {
	case errSave := <-saveDone:
		if errSave != nil {
			t.Fatalf("Save() error = %v", errSave)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Save() did not finish")
	}
	if _, errStat := os.Stat(filepath.Join(realDirA, "auth.json")); errStat != nil {
		t.Fatalf("pinned auth was not written: %v", errStat)
	}
	if _, errStat := os.Stat(filepath.Join(realDirB, "auth.json")); !os.IsNotExist(errStat) {
		t.Fatalf("replacement base received auth file: %v", errStat)
	}
	if _, errStat := os.Stat(storagePath); !os.IsNotExist(errStat) {
		t.Fatalf("storage sandbox output was not cleaned up: %v", errStat)
	}
}

func TestFileTokenStoreSaveReturnsLexicalPathForSymlinkBase(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realDir, linkDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if errWrite := os.WriteFile(filepath.Join(realDir, "auth.json"), []byte(`{"type":"codex","label":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write existing auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(linkDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "label": "new"},
	}
	savedPath, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	if wantPath := filepath.Join(linkDir, "auth.json"); savedPath != wantPath {
		t.Fatalf("Save() path = %q, want lexical path %q", savedPath, wantPath)
	}
}

func TestFileTokenStoreDeleteMissingBaseDirIsIdempotent(t *testing.T) {
	store := NewFileTokenStore()
	store.SetBaseDir(filepath.Join(t.TempDir(), "missing", "auths"))
	if errDelete := store.Delete(t.Context(), "auth.json"); errDelete != nil {
		t.Fatalf("Delete() missing base dir error = %v", errDelete)
	}
}

func TestFileTokenStoreDeleteReportsUncertainAfterDirectorySyncFailure(t *testing.T) {
	baseDir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(baseDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	wantErr := errors.New("sync failed")
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	store.syncDirectory = func(*os.Root, string) error { return wantErr }
	errDelete := store.Delete(t.Context(), fileName)
	if !errors.Is(errDelete, wantErr) {
		t.Fatalf("Delete() error = %v, want %v", errDelete, wantErr)
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists after remove: %v", errStat)
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootIsIdempotent(t *testing.T) {
	baseDir := t.TempDir()
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	path := filepath.Join(baseDir, "missing.json")
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	store := NewFileTokenStore()
	if errDelete := store.DeleteAuthFileAtRoot(baseDir, root, "missing.json"); errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRoot() error = %v", errDelete)
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("idempotent deletion left retired marker")
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootPreparedRunsBeforeRemoval(t *testing.T) {
	baseDir := t.TempDir()
	const fileName = "auth.json"
	wantData := []byte(`{"type":"codex"}`)
	if errWrite := os.WriteFile(filepath.Join(baseDir, fileName), wantData, 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	prepared := false
	store := NewFileTokenStore()
	errDelete := store.DeleteAuthFileAtRootPrepared(baseDir, root, fileName, func(path string, data []byte) error {
		prepared = true
		resolvedBaseDir, errResolve := filepath.EvalSymlinks(baseDir)
		if errResolve != nil {
			t.Fatalf("resolve base dir: %v", errResolve)
		}
		if path != filepath.Join(resolvedBaseDir, fileName) {
			t.Fatalf("prepare path = %q, want %q", path, filepath.Join(resolvedBaseDir, fileName))
		}
		if !bytes.Equal(data, wantData) {
			t.Fatalf("prepare data = %s, want %s", data, wantData)
		}
		if _, errStat := os.Stat(path); errStat != nil {
			t.Fatalf("auth was removed before prepare: %v", errStat)
		}
		return nil
	})
	if errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRootPrepared() error = %v", errDelete)
	}
	if !prepared {
		t.Fatal("prepare was not called")
	}
	if _, errStat := os.Stat(filepath.Join(baseDir, fileName)); !os.IsNotExist(errStat) {
		t.Fatalf("auth file still exists: %v", errStat)
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootPreparedRunsForMissingFile(t *testing.T) {
	baseDir := t.TempDir()
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	prepared := false
	store := NewFileTokenStore()
	errDelete := store.DeleteAuthFileAtRootPrepared(baseDir, root, "missing.json", func(_ string, data []byte) error {
		prepared = true
		if data != nil {
			t.Fatalf("missing file prepare data = %q, want nil", data)
		}
		return nil
	})
	if errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRootPrepared() error = %v", errDelete)
	}
	if !prepared {
		t.Fatal("missing file deletion did not run prepare")
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootPreparedRunsForMissingParent(t *testing.T) {
	baseDir := t.TempDir()
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	prepared := false
	store := NewFileTokenStore()
	errDelete := store.DeleteAuthFileAtRootPrepared(baseDir, root, "missing/auth.json", func(_ string, data []byte) error {
		prepared = true
		if data != nil {
			t.Fatalf("missing parent prepare data = %q, want nil", data)
		}
		return nil
	})
	if errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRootPrepared() error = %v", errDelete)
	}
	if !prepared {
		t.Fatal("missing parent deletion did not run prepare")
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootPreparedPreservesReplacement(t *testing.T) {
	baseDir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(baseDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"original"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer closeFileTokenRoot(root)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	store := NewFileTokenStore()
	errDelete := store.DeleteAuthFileAtRootPrepared(baseDir, root, fileName, func(string, []byte) error {
		temporary := filepath.Join(baseDir, "replacement.tmp")
		if errWrite := os.WriteFile(temporary, replacement, 0o600); errWrite != nil {
			return errWrite
		}
		return os.Rename(temporary, path)
	})
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("replacement auth = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootPreparedFailureIsUncertain(t *testing.T) {
	baseDir := t.TempDir()
	const fileName = "auth.json"
	path := filepath.Join(baseDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	store := NewFileTokenStore()
	errDelete := store.DeleteAuthFileAtRootPrepared(baseDir, root, fileName, func(string, []byte) error {
		return root.Close()
	})
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errStat := os.Stat(path); errStat != nil {
		t.Fatalf("auth file changed after failed removal: %v", errStat)
	}
}

func TestFileTokenStoreDeleteAuthFileAtRootWaitsForCrossProcessTargetLock(t *testing.T) {
	baseDir := t.TempDir()
	if errWrite := os.WriteFile(filepath.Join(baseDir, "auth.json"), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	lockRoot, errLockRoot := os.OpenRoot(baseDir)
	if errLockRoot != nil {
		t.Fatalf("open lock root: %v", errLockRoot)
	}
	defer lockRoot.Close()
	unlockTarget, errLock := lockRootAuthTarget(lockRoot, "auth.json")
	if errLock != nil {
		t.Fatalf("lock auth target: %v", errLock)
	}

	deleteRoot, errDeleteRoot := os.OpenRoot(baseDir)
	if errDeleteRoot != nil {
		t.Fatalf("open delete root: %v", errDeleteRoot)
	}
	defer deleteRoot.Close()
	deleted := make(chan error, 1)
	go func() {
		deleted <- NewFileTokenStore().DeleteAuthFileAtRoot(baseDir, deleteRoot, "auth.json")
	}()
	select {
	case errDelete := <-deleted:
		t.Fatalf("delete completed while target lock was held: %v", errDelete)
	case <-time.After(50 * time.Millisecond):
	}
	if errUnlock := unlockTarget(); errUnlock != nil {
		t.Fatalf("unlock auth target: %v", errUnlock)
	}
	select {
	case errDelete := <-deleted:
		if errDelete != nil {
			t.Fatalf("DeleteAuthFileAtRoot() error = %v", errDelete)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("delete remained blocked after target lock was released")
	}
}

func TestFileTokenStoreDeleteMissingSymlinkBaseClearsRetiredGeneration(t *testing.T) {
	rootDir := t.TempDir()
	realDir := filepath.Join(rootDir, "real-auths")
	if errMkdir := os.Mkdir(realDir, 0o700); errMkdir != nil {
		t.Fatalf("create real auth dir: %v", errMkdir)
	}
	aliasDir := filepath.Join(rootDir, "auths")
	if errSymlink := os.Symlink(realDir, aliasDir); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	aliasPath := filepath.Join(aliasDir, "legacy.json")
	realPath := filepath.Join(realDir, "legacy.json")
	if errWrite := os.WriteFile(realPath, []byte(`{"type":"gemini","access_token":"retired"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	t.Cleanup(func() {
		authfileguard.ClearRetired(aliasPath)
		authfileguard.ClearRetired(realPath)
	})

	store := NewFileTokenStore()
	store.SetBaseDir(aliasDir)
	replacement := &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	}
	if _, errSave := store.Save(t.Context(), replacement); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("first Save() error = %v, want retired read-only error", errSave)
	}
	if !authfileguard.IsRetired(aliasPath) {
		t.Fatal("retired symlink alias was not marked")
	}
	if errRemove := os.RemoveAll(realDir); errRemove != nil {
		t.Fatalf("externally remove real auth dir: %v", errRemove)
	}
	if errDelete := store.Delete(t.Context(), "legacy.json"); errDelete != nil {
		t.Fatalf("Delete() missing symlink base error = %v", errDelete)
	}
	if errMkdir := os.Mkdir(realDir, 0o700); errMkdir != nil {
		t.Fatalf("recreate real auth dir: %v", errMkdir)
	}
	if _, errSave := store.Save(t.Context(), replacement); errSave != nil {
		t.Fatalf("Save() after confirmed missing-base delete error = %v", errSave)
	}
}

func TestFileTokenStoreDeleteExternalPathWhenBaseDirIsMissing(t *testing.T) {
	store := NewFileTokenStore()
	store.SetBaseDir(filepath.Join(t.TempDir(), "missing", "auths"))
	externalPath := filepath.Join(t.TempDir(), "external.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	if errDelete := store.Delete(t.Context(), externalPath); errDelete != nil {
		t.Fatalf("Delete() external path error = %v", errDelete)
	}
	if _, errStat := os.Stat(externalPath); !os.IsNotExist(errStat) {
		t.Fatalf("external auth still exists after Delete(): %v", errStat)
	}
}

func TestFileTokenStoreDeleteRelativePathWithoutBaseDir(t *testing.T) {
	workingDir := t.TempDir()
	previousWorkingDir, errWorkingDir := os.Getwd()
	if errWorkingDir != nil {
		t.Fatalf("get working directory: %v", errWorkingDir)
	}
	if errChdir := os.Chdir(workingDir); errChdir != nil {
		t.Fatalf("change working directory: %v", errChdir)
	}
	t.Cleanup(func() { _ = os.Chdir(previousWorkingDir) })

	const relativePath = "relative-auth.json"
	if errWrite := os.WriteFile(relativePath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write relative auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	if errDelete := store.Delete(t.Context(), relativePath); errDelete != nil {
		t.Fatalf("Delete() relative path error = %v", errDelete)
	}
	if _, errStat := os.Stat(relativePath); !os.IsNotExist(errStat) {
		t.Fatalf("relative auth still exists after Delete(): %v", errStat)
	}
}

func TestFileTokenStoreLegacyStorageUsesIsolatedOutputPath(t *testing.T) {
	baseDir := t.TempDir()
	storage := &recordingLegacyTokenStorage{called: make(chan string, 1)}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "nested/auth.json",
		FileName: "nested/auth.json",
		Provider: "codex",
		Storage:  storage,
		Metadata: map[string]any{"type": "codex"},
	}
	savedPath, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	var calledPath string
	select {
	case calledPath = <-storage.called:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy SaveTokenToFile was not invoked")
	}
	if calledPath == savedPath {
		t.Fatalf("legacy storage received final managed path %q", calledPath)
	}
	if _, inside := relativePathWithin(baseDir, calledPath); inside {
		t.Fatalf("legacy storage output path %q is inside managed root", calledPath)
	}
	if _, errStat := os.Stat(savedPath); errStat != nil {
		t.Fatalf("managed auth was not written: %v", errStat)
	}
	if _, errStat := os.Stat(calledPath); !os.IsNotExist(errStat) {
		t.Fatalf("storage sandbox output was not cleaned up: %v", errStat)
	}
}

func TestFileTokenStoreMarshaledStorageValidatesBeforeReplacing(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "auth.json")
	const original = `{"type":"codex","access_token":"old"}`
	if errWrite := os.WriteFile(path, []byte(original), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &invalidMarshaledTokenStorage{},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() accepted invalid marshaled storage data")
	}
	if got, errRead := os.ReadFile(path); errRead != nil || string(got) != original {
		t.Fatalf("original auth changed: content=%q error=%v", got, errRead)
	}
}

func TestFileTokenStoreNullStorageValidatesBeforeReplacing(t *testing.T) {
	baseDir := t.TempDir()
	path := filepath.Join(baseDir, "auth.json")
	const original = `{"type":"codex","access_token":"old"}`
	if errWrite := os.WriteFile(path, []byte(original), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &nullMarshaledTokenStorage{},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() accepted null storage data")
	}
	if got, errRead := os.ReadFile(path); errRead != nil || string(got) != original {
		t.Fatalf("original auth changed: content=%q error=%v", got, errRead)
	}
}

func TestFileTokenStoreSaveAndDelete_AllowsExplicitPathOutsideBaseDir(t *testing.T) {
	authDir := t.TempDir()
	externalDir := t.TempDir()
	externalPath := filepath.Join(externalDir, "external.json")
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:         "external.json",
		FileName:   "external.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": externalPath},
		Metadata:   map[string]any{"type": "codex", "email": "external@example.com"},
	}
	if savedPath, errSave := store.Save(t.Context(), auth); errSave != nil || savedPath != externalPath {
		t.Fatalf("Save() = (%q, %v), want explicit path %q", savedPath, errSave, externalPath)
	}
	if _, errStat := os.Stat(externalPath); errStat != nil {
		t.Fatalf("explicit path was not written: %v", errStat)
	}
	if errDelete := store.Delete(t.Context(), externalPath); errDelete != nil {
		t.Fatalf("Delete() explicit path error = %v", errDelete)
	}
	if _, errStat := os.Stat(externalPath); !os.IsNotExist(errStat) {
		t.Fatalf("explicit path still exists after Delete(): %v", errStat)
	}
}

func TestFileTokenStoreSaveExplicitPathCreatesMissingParentsThroughRoot(t *testing.T) {
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	externalRoot := t.TempDir()
	externalPath := filepath.Join(externalRoot, "missing", "nested", "external.json")
	auth := &cliproxyauth.Auth{
		ID:         "external.json",
		FileName:   "external.json",
		Provider:   "codex",
		Attributes: map[string]string{"path": externalPath},
		Metadata:   map[string]any{"type": "codex", "email": "external@example.com"},
	}
	if savedPath, errSave := store.Save(t.Context(), auth); errSave != nil || savedPath != externalPath {
		t.Fatalf("Save() = (%q, %v), want explicit path %q", savedPath, errSave, externalPath)
	}
	if _, errStat := os.Stat(externalPath); errStat != nil {
		t.Fatalf("explicit auth was not written: %v", errStat)
	}
}

func TestFileTokenStoreSaveAndDelete_RejectRelativePathOutsideBaseDir(t *testing.T) {
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	externalPath := filepath.Join(root, "external.json")
	const externalContent = `{"type":"codex"}`
	if errWrite := os.WriteFile(externalPath, []byte(externalContent), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:       "../external.json",
		FileName: "../external.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() accepted a relative path outside base dir")
	}
	if errDelete := store.Delete(t.Context(), "../external.json"); errDelete == nil {
		t.Fatal("Delete() accepted a relative path outside base dir")
	}
	if got, errRead := os.ReadFile(externalPath); errRead != nil || string(got) != externalContent {
		t.Fatalf("outside file changed: content=%q error=%v", got, errRead)
	}
}

func TestFileTokenStoreSaveStorageBackedAuthSetsCanonicalSourceHash(t *testing.T) {
	dir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(dir)

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &testTokenStorage{},
		Metadata: map[string]any{
			"type":                 "codex",
			"email":                "writer@example.com",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(nil, auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["access_token"].(string); !ok || got != "tok-storage" {
		t.Fatalf("metadata access_token = %#v, want %q", auth.Metadata["access_token"], "tok-storage")
	}
	if got, ok := auth.Metadata["refresh_token"].(string); !ok || got != "refresh-storage" {
		t.Fatalf("metadata refresh_token = %#v, want %q", auth.Metadata["refresh_token"], "refresh-storage")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if got, ok := auth.Metadata["disabled"].(bool); !ok || got {
		t.Fatalf("metadata disabled = %#v, want false", auth.Metadata["disabled"])
	}
	if !json.Valid(rawFile) {
		t.Fatalf("persisted file is not valid JSON: %s", rawFile)
	}
	if rawHash := cliproxyauth.SourceHashFromBytes(rawFile); rawHash != wantHash {
		t.Fatalf("raw storage file hash = %q, want %q", rawHash, wantHash)
	}
}

func TestFileTokenStoreSaveRejectsSymlinkCreatedByStorage(t *testing.T) {
	authDir := t.TempDir()
	managedPath := filepath.Join(authDir, "auth.json")
	if errWrite := os.WriteFile(managedPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write managed auth: %v", errWrite)
	}
	externalPath := filepath.Join(t.TempDir(), "external.json")
	const externalContent = `{"type":"codex","token":"external"}`
	if errWrite := os.WriteFile(externalPath, []byte(externalContent), 0o600); errWrite != nil {
		t.Fatalf("write external file: %v", errWrite)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  &symlinkTokenStorage{target: externalPath, called: make(chan string, 1)},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() accepted a storage-created symlink")
	}
	var calledPath string
	select {
	case calledPath = <-auth.Storage.(*symlinkTokenStorage).called:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy SaveTokenToFile was not invoked")
	}
	if calledPath == managedPath {
		t.Fatalf("legacy storage received final managed path %q", calledPath)
	}
	if _, inside := relativePathWithin(authDir, calledPath); inside {
		t.Fatalf("legacy storage output path %q is inside managed root", calledPath)
	}
	if info, errLstat := os.Lstat(managedPath); errLstat != nil || !info.Mode().IsRegular() {
		t.Fatalf("managed auth was replaced by isolated storage output: info=%v error=%v", info, errLstat)
	}
	if got, errRead := os.ReadFile(managedPath); errRead != nil || string(got) != `{"type":"codex"}` {
		t.Fatalf("managed auth changed: content=%q error=%v", got, errRead)
	}
	if got, errRead := os.ReadFile(externalPath); errRead != nil || string(got) != externalContent {
		t.Fatalf("external file changed: content=%q error=%v", got, errRead)
	}
	if _, errStat := os.Stat(calledPath); !os.IsNotExist(errStat) {
		t.Fatalf("storage sandbox output was not cleaned up: %v", errStat)
	}
}

func TestFileTokenStoreSaveFailsClosedWhenParentRedirectedDuringMarshal(t *testing.T) {
	baseDir := t.TempDir()
	nestedDir := filepath.Join(baseDir, "nested")
	if errMkdir := os.Mkdir(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested auth dir: %v", errMkdir)
	}
	externalDir := t.TempDir()
	probePath := filepath.Join(baseDir, "symlink-probe")
	if errSymlink := os.Symlink(externalDir, probePath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if errRemove := os.Remove(probePath); errRemove != nil {
		t.Fatalf("remove symlink probe: %v", errRemove)
	}

	storage := &blockingMarshaledTokenStorage{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		data:    []byte(`{"type":"codex","access_token":"new"}`),
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "nested/auth.json",
		FileName: "nested/auth.json",
		Provider: "codex",
		Storage:  storage,
		Metadata: map[string]any{"type": "codex"},
	}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), auth)
		saveDone <- errSave
	}()
	select {
	case <-storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("storage marshal did not start")
	}
	if errRemove := os.Remove(nestedDir); errRemove != nil {
		close(storage.release)
		t.Fatalf("remove nested auth dir: %v", errRemove)
	}
	if errSymlink := os.Symlink(externalDir, nestedDir); errSymlink != nil {
		close(storage.release)
		t.Fatalf("redirect nested auth dir: %v", errSymlink)
	}
	close(storage.release)
	select {
	case errSave := <-saveDone:
		if errSave == nil {
			t.Fatal("Save() accepted a parent redirected outside the stable root")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Save() did not finish")
	}
	if _, errStat := os.Stat(filepath.Join(externalDir, "auth.json")); !os.IsNotExist(errStat) {
		t.Fatalf("redirect target received auth data: %v", errStat)
	}
}

func TestFileTokenStoreSaveDoesNotFollowFileSymlinkCreatedDuringMarshal(t *testing.T) {
	baseDir := t.TempDir()
	managedPath := filepath.Join(baseDir, "auth.json")
	if errWrite := os.WriteFile(managedPath, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write managed auth: %v", errWrite)
	}
	externalPath := filepath.Join(t.TempDir(), "external.json")
	const externalContent = `{"type":"codex","access_token":"external"}`
	if errWrite := os.WriteFile(externalPath, []byte(externalContent), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	probePath := filepath.Join(baseDir, "symlink-probe")
	if errSymlink := os.Symlink(externalPath, probePath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if errRemove := os.Remove(probePath); errRemove != nil {
		t.Fatalf("remove symlink probe: %v", errRemove)
	}

	storage := &blockingMarshaledTokenStorage{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		data:    []byte(`{"type":"codex","access_token":"new"}`),
	}
	store := NewFileTokenStore()
	store.SetBaseDir(baseDir)
	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		FileName: "auth.json",
		Provider: "codex",
		Storage:  storage,
		Metadata: map[string]any{"type": "codex"},
	}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), auth)
		saveDone <- errSave
	}()
	select {
	case <-storage.started:
	case <-time.After(5 * time.Second):
		t.Fatal("storage marshal did not start")
	}
	if errRemove := os.Remove(managedPath); errRemove != nil {
		close(storage.release)
		t.Fatalf("remove managed auth: %v", errRemove)
	}
	if errSymlink := os.Symlink(externalPath, managedPath); errSymlink != nil {
		close(storage.release)
		t.Fatalf("replace managed auth with symlink: %v", errSymlink)
	}
	close(storage.release)
	var errSave error
	select {
	case errSave = <-saveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Save() did not finish")
	}
	if got, errRead := os.ReadFile(externalPath); errRead != nil || string(got) != externalContent {
		t.Fatalf("external auth changed: content=%q error=%v", got, errRead)
	}
	if errSave == nil {
		info, errLstat := os.Lstat(managedPath)
		if errLstat != nil || !info.Mode().IsRegular() {
			t.Fatalf("successful Save() did not atomically replace the symlink: info=%v error=%v", info, errLstat)
		}
	}
}

func TestFileTokenStoreSaveVertexStorageBackedAuthPreservesMetadataOnlyFields(t *testing.T) {
	dir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(dir)

	auth := &cliproxyauth.Auth{
		ID:       "vertex.json",
		FileName: "vertex.json",
		Provider: "vertex",
		Storage: &vertex.VertexCredentialStorage{
			ServiceAccount: map[string]any{
				"type":         "service_account",
				"project_id":   "vertex-project",
				"client_email": "vertex@example.com",
			},
			ProjectID: "vertex-project",
			Email:     "vertex@example.com",
			Location:  "us-central1",
		},
		Metadata: map[string]any{
			"type":                 "vertex",
			"email":                "vertex@example.com",
			"label":                "vertex-label",
			"tool_prefix_disabled": true,
		},
	}

	path, err := store.Save(nil, auth)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	if got, ok := auth.Metadata["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("metadata label = %#v, want %q", auth.Metadata["label"], "vertex-label")
	}
	if got, ok := auth.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", auth.Metadata["tool_prefix_disabled"])
	}
	if _, ok := auth.Metadata["service_account"].(map[string]any); !ok {
		t.Fatalf("metadata service_account = %#v, want object", auth.Metadata["service_account"])
	}

	rawFile, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(rawFile, &payload); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if got, ok := payload["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("persisted label = %#v, want %q", payload["label"], "vertex-label")
	}
	wantRaw, err := cliproxyauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := cliproxyauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[cliproxyauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
}

func TestFileTokenStoreSaveExistingProviderStorages(t *testing.T) {
	tests := []struct {
		name     string
		provider string
		storage  internalauth.TokenStorage
		want     map[string]any
	}{
		{
			name:     "codex",
			provider: "codex",
			storage:  &codex.CodexTokenStorage{AccessToken: "codex-token"},
			want:     map[string]any{"type": "codex", "access_token": "codex-token"},
		},
		{
			name:     "claude",
			provider: "claude",
			storage:  &claude.ClaudeTokenStorage{AccessToken: "claude-token"},
			want:     map[string]any{"type": "claude", "access_token": "claude-token"},
		},
		{
			name:     "kimi",
			provider: "kimi",
			storage:  &kimi.KimiTokenStorage{AccessToken: "kimi-token"},
			want:     map[string]any{"type": "kimi", "access_token": "kimi-token"},
		},
		{
			name:     "xai",
			provider: "xai",
			storage:  &xai.TokenStorage{AccessToken: "xai-token"},
			want:     map[string]any{"type": "xai", "access_token": "xai-token"},
		},
		{
			name:     "vertex",
			provider: "vertex",
			storage: &vertex.VertexCredentialStorage{
				ServiceAccount: map[string]any{"type": "service_account", "project_id": "vertex-project"},
				ProjectID:      "vertex-project",
			},
			want: map[string]any{"type": "vertex", "project_id": "vertex-project"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := NewFileTokenStore()
			store.SetBaseDir(t.TempDir())
			auth := &cliproxyauth.Auth{
				ID:       test.name + ".json",
				FileName: test.name + ".json",
				Provider: test.provider,
				Storage:  test.storage,
				Metadata: map[string]any{"type": test.provider, "label": "provider-regression"},
			}
			path, errSave := store.Save(t.Context(), auth)
			if errSave != nil {
				t.Fatalf("Save() error = %v", errSave)
			}
			data, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatalf("read persisted auth: %v", errRead)
			}
			var payload map[string]any
			if errUnmarshal := json.Unmarshal(data, &payload); errUnmarshal != nil {
				t.Fatalf("unmarshal persisted auth: %v", errUnmarshal)
			}
			for key, want := range test.want {
				if got := payload[key]; got != want {
					t.Fatalf("persisted %s = %#v, want %#v", key, got, want)
				}
			}
		})
	}
}

type testTokenStorage struct {
	metadata map[string]any
}

type symlinkTokenStorage struct {
	target string
	called chan string
}

type recordingLegacyTokenStorage struct {
	called chan string
}

type blockingLegacyTokenStorage struct {
	started chan string
	release chan struct{}
}

type blockingMarshaledTokenStorage struct {
	started chan struct{}
	release chan struct{}
	data    []byte
}

type invalidMarshaledTokenStorage struct{}

type nullMarshaledTokenStorage struct{}

func testCodexIDToken(accountID string, planType string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": accountID,
			"chatgpt_plan_type":  planType,
		},
	})
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func (s *testTokenStorage) SetMetadata(meta map[string]any) {
	if meta == nil {
		s.metadata = nil
		return
	}
	cloned := make(map[string]any, len(meta))
	for key, value := range meta {
		cloned[key] = value
	}
	s.metadata = cloned
}

func (s *testTokenStorage) SaveTokenToFile(authFilePath string) error {
	raw, errMarshal := s.MarshalTokenData()
	if errMarshal != nil {
		return errMarshal
	}
	return os.WriteFile(authFilePath, raw, 0o600)
}

func (s *testTokenStorage) MarshalTokenData() ([]byte, error) {
	payload := map[string]any{
		"access_token":  "tok-storage",
		"refresh_token": "refresh-storage",
	}
	for key, value := range s.metadata {
		payload[key] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (s *symlinkTokenStorage) SaveTokenToFile(authFilePath string) error {
	if s.called != nil {
		s.called <- authFilePath
	}
	if errRemove := os.Remove(authFilePath); errRemove != nil && !os.IsNotExist(errRemove) {
		return errRemove
	}
	return os.Symlink(s.target, authFilePath)
}

func (s *recordingLegacyTokenStorage) SaveTokenToFile(authFilePath string) error {
	s.called <- authFilePath
	return os.WriteFile(authFilePath, []byte(`{"type":"codex"}`), 0o600)
}

func (s *blockingLegacyTokenStorage) SaveTokenToFile(authFilePath string) error {
	s.started <- authFilePath
	<-s.release
	return os.WriteFile(authFilePath, []byte(`{"type":"codex"}`), 0o600)
}

func (*blockingMarshaledTokenStorage) SaveTokenToFile(string) error {
	return errors.New("legacy path should not be used")
}

func (s *blockingMarshaledTokenStorage) MarshalTokenData() ([]byte, error) {
	s.started <- struct{}{}
	<-s.release
	return append([]byte(nil), s.data...), nil
}

func (*invalidMarshaledTokenStorage) SaveTokenToFile(string) error {
	return errors.New("legacy path should not be used")
}

func (*invalidMarshaledTokenStorage) MarshalTokenData() ([]byte, error) {
	return []byte(`not-json`), nil
}

func (*nullMarshaledTokenStorage) SaveTokenToFile(string) error {
	return errors.New("legacy path should not be used")
}

func (*nullMarshaledTokenStorage) MarshalTokenData() ([]byte, error) {
	return []byte(`null`), nil
}
