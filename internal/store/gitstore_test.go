package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-git/v6"
	gitconfig "github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestGitTokenStoreReadAuthFileSetsCanonicalSourceHash(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	data := []byte(`{"type":"claude","email":"reader@example.com"}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := NewGitTokenStore("", "", "", "")
	auth, err := store.readAuthFile(path, dir)
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

func TestLockGitRepositorySerializesIndependentStoreProcesses(t *testing.T) {
	repoDir := filepath.Join(t.TempDir(), "workspace")
	unlockFirst, errFirst := lockGitRepository(repoDir)
	if errFirst != nil {
		t.Fatalf("first lockGitRepository() error = %v", errFirst)
	}
	secondLocked := make(chan func() error, 1)
	secondErr := make(chan error, 1)
	go func() {
		unlockSecond, errSecond := lockGitRepository(repoDir)
		if errSecond != nil {
			secondErr <- errSecond
			return
		}
		secondLocked <- unlockSecond
	}()
	select {
	case errSecond := <-secondErr:
		t.Fatalf("second lockGitRepository() error = %v", errSecond)
	case unlockSecond := <-secondLocked:
		_ = unlockSecond()
		t.Fatal("second repository lock completed while first lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	if errUnlock := unlockFirst(); errUnlock != nil {
		t.Fatalf("unlock first repository lock: %v", errUnlock)
	}
	select {
	case errSecond := <-secondErr:
		t.Fatalf("second lockGitRepository() error = %v", errSecond)
	case unlockSecond := <-secondLocked:
		if errUnlock := unlockSecond(); errUnlock != nil {
			t.Fatalf("unlock second repository lock: %v", errUnlock)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second repository lock remained blocked after first lock was released")
	}
}

func TestGitTokenStoreReadAuthFilePreservesDisabledState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "auth.json")
	if err := os.WriteFile(path, []byte(`{"type":"claude","email":"reader@example.com","disabled":true}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := NewGitTokenStore("", "", "", "")
	auth, err := store.readAuthFile(path, dir)
	if err != nil {
		t.Fatalf("readAuthFile returned error: %v", err)
	}
	if auth == nil {
		t.Fatal("expected auth to be loaded")
	}
	if !auth.Disabled {
		t.Fatal("expected auth to remain disabled")
	}
	if auth.Status != cliproxyauth.StatusDisabled {
		t.Fatalf("status = %q, want %q", auth.Status, cliproxyauth.StatusDisabled)
	}
}

func TestGitTokenStoreReadAuthFileRejectsUnsafeFiles(t *testing.T) {
	t.Run("symlink", func(t *testing.T) {
		parent := t.TempDir()
		authDir := filepath.Join(parent, "auths")
		if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
			t.Fatalf("create auth dir: %v", errMkdir)
		}
		outside := filepath.Join(parent, "outside.json")
		if errWrite := os.WriteFile(outside, []byte(`{"type":"codex","access_token":"outside"}`), 0o600); errWrite != nil {
			t.Fatalf("write outside auth: %v", errWrite)
		}
		path := filepath.Join(authDir, "linked.json")
		if errLink := os.Symlink(outside, path); errLink != nil {
			t.Fatalf("create auth symlink: %v", errLink)
		}
		store := NewGitTokenStore("", "", "", "")
		if _, errRead := store.readAuthFile(path, authDir); !errors.Is(errRead, errUnsafeGitAuthPath) {
			t.Fatalf("readAuthFile() error = %v, want unsafe path", errRead)
		}
	})

	t.Run("non_regular", func(t *testing.T) {
		parent := t.TempDir()
		authDir := filepath.Join(parent, "auths")
		path := filepath.Join(authDir, "directory.json")
		if errMkdir := os.MkdirAll(path, 0o700); errMkdir != nil {
			t.Fatalf("create non-regular auth path: %v", errMkdir)
		}
		store := NewGitTokenStore("", "", "", "")
		if _, errRead := store.readAuthFile(path, authDir); !errors.Is(errRead, errUnsafeGitAuthPath) {
			t.Fatalf("readAuthFile() error = %v, want unsafe path", errRead)
		}
	})
}

func TestGitTokenStoreListRejectsAuthSymlink(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	outside := filepath.Join(root, "outside.json")
	if errWrite := os.WriteFile(outside, []byte(`{"type":"codex","access_token":"outside"}`), 0o600); errWrite != nil {
		t.Fatalf("write outside auth: %v", errWrite)
	}
	if errLink := os.Symlink(outside, filepath.Join(authDir, "linked.json")); errLink != nil {
		t.Fatalf("create auth symlink: %v", errLink)
	}
	if _, errList := store.List(t.Context()); !errors.Is(errList, errUnsafeGitAuthPath) {
		t.Fatalf("List() error = %v, want unsafe path", errList)
	}
}

func TestGitTokenStoreSaveRejectsSymlinkPathComponent(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	outsideDir := filepath.Join(root, "outside")
	if errMkdir := os.MkdirAll(outsideDir, 0o700); errMkdir != nil {
		t.Fatalf("create outside dir: %v", errMkdir)
	}
	if errLink := os.Symlink(outsideDir, filepath.Join(authDir, "nested")); errLink != nil {
		t.Fatalf("create auth directory symlink: %v", errLink)
	}
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "nested/auth.json",
		FileName: "nested/auth.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, errUnsafeGitAuthPath) {
		t.Fatalf("Save() error = %v, want unsafe path", errSave)
	}
	if _, errStat := os.Stat(filepath.Join(outsideDir, "auth.json")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outside auth was written: %v", errStat)
	}
}

func TestGitTokenStoreSaveStorageBackedAuthSetsCanonicalSourceHash(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main",
		testBranchSpec{name: "main", contents: "remote default branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

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

	path, err := store.Save(context.Background(), auth)
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
	if rawHash := cliproxyauth.SourceHashFromBytes(rawFile); rawHash != wantHash {
		t.Fatalf("raw storage file hash = %q, want %q", rawHash, wantHash)
	}
}

func TestGitTokenStoreSaveSemanticallyEqualMetadataPreservesPersistedBytes(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	auth := &cliproxyauth.Auth{
		ID: "formatted.json", FileName: "formatted.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "same"},
	}
	canonical, errCanonical := cliproxyauth.CanonicalMetadataBytes(auth)
	if errCanonical != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", errCanonical)
	}
	var formatted bytes.Buffer
	if errIndent := json.Indent(&formatted, canonical, "", "  "); errIndent != nil {
		t.Fatalf("indent auth metadata: %v", errIndent)
	}
	advanceRemoteBranchFile(
		t,
		filepath.Join(root, "seed"),
		remoteDir,
		"main",
		filepath.Join("auths", auth.ID),
		formatted.Bytes(),
		"seed formatted auth",
	)

	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	gotData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved auth: %v", errRead)
	}
	if !bytes.Equal(gotData, formatted.Bytes()) {
		t.Fatalf("saved auth = %s, want original formatted bytes %s", gotData, formatted.Bytes())
	}
	if got, want := auth.Attributes[cliproxyauth.SourceHashAttributeKey], cliproxyauth.SourceHashFromBytes(canonical); got != want {
		t.Fatalf("source hash = %q, want canonical hash %q", got, want)
	}
}

func TestGitTokenStoreSemanticallyEqualSaveRollbackUsesPersistedBytes(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	auth := &cliproxyauth.Auth{
		ID: "formatted.json", FileName: "formatted.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "same"},
	}
	canonical, errCanonical := cliproxyauth.CanonicalMetadataBytes(auth)
	if errCanonical != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", errCanonical)
	}
	var formatted bytes.Buffer
	if errIndent := json.Indent(&formatted, canonical, "", "  "); errIndent != nil {
		t.Fatalf("indent auth metadata: %v", errIndent)
	}
	path := filepath.Join(authDir, auth.ID)
	if errWrite := os.WriteFile(path, formatted.Bytes(), 0o600); errWrite != nil {
		t.Fatalf("write formatted auth: %v", errWrite)
	}
	wantPushErr := errors.New("push failed")
	store.pushRepo = func(_ *git.Repository, _ *git.PushOptions) error { return wantPushErr }

	_, errSave := store.Save(t.Context(), auth)
	if !errors.Is(errSave, wantPushErr) {
		t.Fatalf("Save() error = %v, want push failure", errSave)
	}
	if errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("Save() error = %v, persisted bytes should allow clean rollback", errSave)
	}
	gotData, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(gotData, formatted.Bytes()) {
		t.Fatalf("local auth after rollback = %s, %v; want %s", gotData, errRead, formatted.Bytes())
	}
}

func TestGitTokenStoreSemanticallyEqualConcurrentRewriteIsNotRolledBack(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	plainMetadata := map[string]any{
		"type": "codex", "access_token": "same", "disabled": false, "race": "value",
	}
	initial, errInitial := json.MarshalIndent(plainMetadata, "", "  ")
	if errInitial != nil {
		t.Fatalf("marshal initial metadata: %v", errInitial)
	}
	replacement, errReplacement := json.Marshal(plainMetadata)
	if errReplacement != nil {
		t.Fatalf("marshal replacement metadata: %v", errReplacement)
	}
	path := filepath.Join(authDir, "concurrent.json")
	if errWrite := os.WriteFile(path, initial, 0o600); errWrite != nil {
		t.Fatalf("write initial auth: %v", errWrite)
	}
	raceValue := &writeFileOnMarshal{
		value: "value",
		write: func() error {
			return os.WriteFile(path, replacement, 0o600)
		},
	}
	store.pushRepo = func(_ *git.Repository, _ *git.PushOptions) error {
		t.Fatal("push should not run after a concurrent local rewrite")
		return nil
	}
	auth := &cliproxyauth.Auth{
		ID: "concurrent.json", FileName: "concurrent.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "same", "race": raceValue},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("Save() error = %v, want stale generation", errSave)
	}
	gotData, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(gotData, replacement) {
		t.Fatalf("concurrent auth after Save = %s, %v; want %s", gotData, errRead, replacement)
	}
}

func TestGitTokenStoreSaveRejectsStorageOutputBeforeReplacingLocalAuth(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "auth.json")
	original := []byte(`{"type":"codex","access_token":"old"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	auth := &cliproxyauth.Auth{
		ID: "auth.json", FileName: "auth.json", Provider: "codex",
		Storage:  staticTokenStorage{data: []byte(`{"type":"gemini","access_token":"legacy"}`)},
		Metadata: map[string]any{"type": "codex"},
	}
	if _, errSave := store.Save(t.Context(), auth); !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, original) {
		t.Fatalf("local auth changed: data=%s error=%v", got, errRead)
	}
}

func TestGitTokenStoreSaveRejectsQuarantinedAuthPath(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	path := filepath.Join(authDir, "pending.json")
	authfileguard.MarkQuarantined(path)
	t.Cleanup(func() { authfileguard.ClearQuarantined(path) })

	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "pending.json", FileName: "pending.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("Save() error = %v, want pending deletion error", errSave)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("quarantined auth file was created: %v", errStat)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", "pending.json"))
}

func TestGitTokenStoreSaveRejectsRetiredFileCreatedDuringMarshal(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "auth.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"old"}`), 0o600); errWrite != nil {
		t.Fatalf("write original auth: %v", errWrite)
	}
	storage := &blockingTokenStorage{started: make(chan struct{}), release: make(chan struct{}), data: []byte(`{"type":"codex","access_token":"new"}`)}
	saveDone := make(chan error, 1)
	go func() {
		_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{ID: "auth.json", FileName: "auth.json", Provider: "codex", Storage: storage})
		saveDone <- errSave
	}()
	<-storage.started
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		close(storage.release)
		t.Fatalf("write concurrent retired auth: %v", errWrite)
	}
	close(storage.release)
	if errSave := <-saveDone; !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, retired) {
		t.Fatalf("concurrent retired auth changed: data=%s error=%v", got, errRead)
	}
	authfileguard.ClearRetired(path)
}

func TestGitTokenStoreSaveRetriesRemoteWhenLocalContentMatches(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)

	storage := &testTokenStorage{afterSave: func() {
		advanceRemoteBranchFile(t, seedDir, remoteDir, "main", "concurrent.txt", []byte("advance\n"), "advance during auth save")
	}}
	auth := &cliproxyauth.Auth{
		ID:       "retry.json",
		FileName: "retry.json",
		Provider: "codex",
		Storage:  storage,
		Metadata: map[string]any{"type": "codex", "email": "retry@example.com"},
	}
	if _, errSave := store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("first Save() error = nil, want lease conflict")
	}
	path := filepath.Join(authDir, "retry.json")
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("local auth remained after failed push: %v", errStat)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", "retry.json"))

	storage.afterSave = nil
	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("retry Save() error = %v", errSave)
	}
	localData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read locally persisted auth after retry: %v", errRead)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "retry.json"), string(localData))
}

func TestGitTokenStoreSaveRefusesRetiredGeminiCLIOverwrite(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "legacy.json")
	const retiredContent = `{"type":"gemini-cli","access_token":"retired-token"}`
	if errWrite := os.WriteFile(path, []byte(retiredContent), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
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

func TestGitTokenStoreSaveRefusesCreatingRetiredGeminiCLIAuth(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(authDir)
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "gemini-cli",
		Metadata: map[string]any{"type": "gemini-cli", "access_token": "retired-token"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only error", errSave)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "legacy.json")); !os.IsNotExist(errStat) {
		t.Fatalf("retired auth file was created: %v", errStat)
	}
}

func TestGitTokenStorePersistAuthFilesRejectsRetiredGeminiCLIContent(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "legacy.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", "legacy.json"))
}

func TestGitTokenStoreDelete_CommitsWhenLocalFileAlreadyMissing(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main",
		testBranchSpec{name: "main", contents: "remote default branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	auth := &cliproxyauth.Auth{
		ID:       "auth.json",
		Provider: "claude",
		FileName: "auth.json",
		Metadata: map[string]any{"type": "claude", "email": "persist@example.com"},
	}
	path, err := store.Save(context.Background(), auth)
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove local auth file: %v", err)
	}

	if err := store.Delete(context.Background(), auth.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", "auth.json"))
}

func TestGitTokenStoreDeleteFailsClosedWhenPushResultIsUnknown(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID:       "rollback.json",
		FileName: "rollback.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "token"},
	}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	wantData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved auth: %v", errRead)
	}

	repo, errOpen := git.PlainOpen(filepath.Join(root, "workspace"))
	if errOpen != nil {
		t.Fatalf("open workspace repo: %v", errOpen)
	}
	cfg, errConfig := repo.Config()
	if errConfig != nil {
		t.Fatalf("read workspace config: %v", errConfig)
	}
	cfg.Remotes["origin"].URLs = []string{remoteDir, filepath.Join(root, "missing-push-remote")}
	if errSet := repo.SetConfig(cfg); errSet != nil {
		t.Fatalf("set failing push URL: %v", errSet)
	}

	errDelete := store.Delete(t.Context(), auth.ID)
	if errDelete == nil {
		t.Fatal("Delete() error = nil, want push failure")
	}
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain", outcome, ok)
	}
	if _, errRead := os.ReadFile(path); !errors.Is(errRead, os.ErrNotExist) {
		t.Fatalf("local auth restored after unknown push result: %v", errRead)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", auth.ID), string(wantData))
}

func TestGitTokenStoreDeleteRejectsStaleLocalGeneration(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	remoteData := []byte(`{"type":"codex","access_token":"remote"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", filepath.Join("auths", "local-generation.json"), remoteData, "seed remote auth")

	workspaceDir := filepath.Join(root, "workspace")
	authDir := filepath.Join(workspaceDir, "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "local-generation.json")
	localData := []byte(`{"type":"codex","access_token":"local"}`)
	if errWrite := os.WriteFile(path, localData, 0o600); errWrite != nil {
		t.Fatalf("write local generation: %v", errWrite)
	}
	errDelete := store.Delete(t.Context(), "local-generation.json")
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeRolledBack {
		t.Fatalf("delete outcome = %v, %t; want rolled back (error %v)", outcome, ok, errDelete)
	}
	if !errors.Is(errDelete, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("delete error = %v, want stale generation", errDelete)
	}
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, localData) {
		t.Fatalf("local auth after rejected delete = %s, %v; want %s", got, errRead, localData)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "local-generation.json"), string(remoteData))
	tombstoneDir := filepath.Join(workspaceDir, "config", ".cliproxy-delete-quarantine")
	if entries, errReadDir := os.ReadDir(tombstoneDir); errReadDir == nil && len(entries) != 0 {
		t.Fatalf("stale delete created tombstones: %#v", entries)
	} else if errReadDir != nil && !errors.Is(errReadDir, os.ErrNotExist) {
		t.Fatalf("read delete tombstones: %v", errReadDir)
	}
}

func TestGitTokenStoreSaveRestoresWorkspaceWhenPushFails(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	workspaceDir := filepath.Join(root, "workspace")
	authDir := filepath.Join(workspaceDir, "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID: "rollback-save.json", FileName: "rollback-save.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "old"},
	}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("initial Save() error = %v", errSave)
	}
	oldData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read initial auth: %v", errRead)
	}

	repo, errOpen := git.PlainOpen(workspaceDir)
	if errOpen != nil {
		t.Fatalf("open workspace repo: %v", errOpen)
	}
	cfg, errConfig := repo.Config()
	if errConfig != nil {
		t.Fatalf("read workspace config: %v", errConfig)
	}
	cfg.Remotes["origin"].URLs = []string{remoteDir, filepath.Join(root, "missing-push-remote")}
	if errSet := repo.SetConfig(cfg); errSet != nil {
		t.Fatalf("set failing push URL: %v", errSet)
	}
	auth.Metadata["access_token"] = "new"
	if _, errSave = store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want push failure")
	}
	gotData, errRead := os.ReadFile(path)
	if errRead != nil || !bytes.Equal(gotData, oldData) {
		t.Fatalf("local auth after failed push = %s, %v; want %s", gotData, errRead, oldData)
	}
	status, errStatus := repo.Worktree()
	if errStatus != nil {
		t.Fatalf("open workspace worktree: %v", errStatus)
	}
	workspaceStatus, errStatus := status.Status()
	if errStatus != nil {
		t.Fatalf("read workspace status: %v", errStatus)
	}
	if !workspaceStatus.IsClean() {
		t.Fatalf("workspace remained dirty after failed push: %s", workspaceStatus)
	}
}

func TestGitTokenStoreSaveAcceptsRemoteCommitAfterLostPushAcknowledgement(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID: "lost-ack.json", FileName: "lost-ack.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "old"},
	}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("initial Save() error = %v", errSave)
	}

	wantErr := errors.New("push acknowledgement lost")
	store.pushRepo = func(repo *git.Repository, options *git.PushOptions) error {
		if errPush := repo.Push(options); errPush != nil {
			return errPush
		}
		return wantErr
	}
	auth.Metadata["access_token"] = "new"
	if _, errSave = store.Save(t.Context(), auth); errSave != nil {
		t.Fatalf("Save() after committed push error = %v", errSave)
	}
	wantData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read saved auth: %v", errRead)
	}
	if !bytes.Contains(wantData, []byte(`"access_token":"new"`)) {
		t.Fatalf("saved auth = %s, want updated token", wantData)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", auth.ID), string(wantData))
}

func TestGitTokenStoreSaveRejectsMatchingBlobFromDifferentRemoteTip(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID: "same-blob.json", FileName: "same-blob.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "old"},
	}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("initial Save() error = %v", errSave)
	}
	oldData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read initial auth: %v", errRead)
	}

	store.pushRepo = func(_ *git.Repository, _ *git.PushOptions) error {
		newData, errReadNew := os.ReadFile(path)
		if errReadNew != nil {
			t.Fatalf("read pending auth: %v", errReadNew)
		}
		seedDir := filepath.Join(root, "seed")
		advanceRemoteBranchFile(t, seedDir, remoteDir, "main", filepath.Join("auths", auth.ID), newData, "write matching auth from concurrent actor")
		advanceRemoteBranchFile(t, seedDir, remoteDir, "main", "concurrent.txt", []byte("different tip\n"), "advance remote tip")
		return errors.New("push lease rejected")
	}
	auth.Metadata["access_token"] = "new"
	if _, errSave = store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want remote tip conflict")
	}
	if gotData, errReadLocal := os.ReadFile(path); errReadLocal != nil || !bytes.Equal(gotData, oldData) {
		t.Fatalf("local auth after conflict = %s, %v; want %s", gotData, errReadLocal, oldData)
	}
}

func TestGitTokenStoreSaveRollsBackWhenPushVerificationFails(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	workspaceDir := filepath.Join(root, "workspace")
	authDir := filepath.Join(workspaceDir, "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	auth := &cliproxyauth.Auth{
		ID: "probe-failure.json", FileName: "probe-failure.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "old"},
	}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("initial Save() error = %v", errSave)
	}
	oldData, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatalf("read initial auth: %v", errRead)
	}

	store.pushRepo = func(repo *git.Repository, _ *git.PushOptions) error {
		cfg, errConfig := repo.Config()
		if errConfig != nil {
			t.Fatalf("read repository config: %v", errConfig)
		}
		cfg.Remotes["origin"].URLs = []string{filepath.Join(root, "unreachable")}
		if errSet := repo.SetConfig(cfg); errSet != nil {
			t.Fatalf("set unreachable remote: %v", errSet)
		}
		return errors.New("push failed before acknowledgement")
	}
	auth.Metadata["access_token"] = "new"
	if _, errSave = store.Save(t.Context(), auth); errSave == nil {
		t.Fatal("Save() error = nil, want push verification failure")
	}
	if gotData, errReadLocal := os.ReadFile(path); errReadLocal != nil || !bytes.Equal(gotData, oldData) {
		t.Fatalf("local auth after failed verification = %s, %v; want %s", gotData, errReadLocal, oldData)
	}
}

func TestGitTokenStoreNoOpDeletionDoesNotRecreateMissingRemoteBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	remoteRepo, errRemoteRepo := git.PlainOpen(remoteDir)
	if errRemoteRepo != nil {
		t.Fatalf("open remote repository: %v", errRemoteRepo)
	}
	branch := plumbing.NewBranchReferenceName("main")
	if errRemove := remoteRepo.Storer.RemoveReference(branch); errRemove != nil {
		t.Fatalf("remove remote branch: %v", errRemove)
	}

	store.mu.Lock()
	remoteState, errRemote := store.remoteBranchPreconditionLocked()
	if errRemote == nil {
		errRemote = store.commitAndPushAgainstRemoteWithSnapshotsLocked(
			"delete missing auth",
			remoteState,
			map[string]authFileSnapshot{"auths/missing.json": {}},
			"auths/missing.json",
		)
	}
	store.mu.Unlock()
	if errRemote != nil {
		t.Fatalf("no-op deletion error = %v", errRemote)
	}
	assertRemoteBranchDoesNotExist(t, remoteDir, "main")
}

func TestGitTokenStoreFinalizeDeletionUsesAndRestoresBaseDirSnapshot(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main",
		testBranchSpec{name: "main", contents: "remote default branch\n"},
	)
	baseDirA := filepath.Join(root, "workspace-a", "auths")
	baseDirB := filepath.Join(root, "workspace-b", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDirA)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	const fileName = "auth.json"
	path := filepath.Join(baseDirA, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"retired"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove local auth file: %v", errRemove)
	}
	store.SetBaseDir(baseDirB)
	if errFinalize := store.FinalizeAuthFileDeletionAtBaseDir(t.Context(), baseDirA, fileName); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletionAtBaseDir() error = %v", errFinalize)
	}
	if gotBaseDir := store.AuthDir(); gotBaseDir != baseDirB {
		t.Fatalf("restored base dir = %q, want %q", gotBaseDir, baseDirB)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", fileName))
}

func TestGitTokenStoreFinalizeRetiredDeletionAcceptsLostPushAcknowledgement(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	const fileName = "legacy-lost-ack.json"
	path := filepath.Join(authDir, fileName)
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"retired"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove local retired auth: %v", errRemove)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })

	store.pushRepo = func(repo *git.Repository, options *git.PushOptions) error {
		if errPush := repo.Push(options); errPush != nil {
			return errPush
		}
		return errors.New("push acknowledgement lost")
	}
	if errFinalize := store.FinalizeAuthFileDeletionAtBaseDir(t.Context(), authDir, fileName); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletionAtBaseDir() error = %v", errFinalize)
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("retired marker remained after confirmed remote deletion")
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", fileName))
}

func TestGitTokenStoreFinalizeDeletionIgnoresRecreatedLocalFile(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove retired auth: %v", errRemove)
	}

	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatalf("recreate auth path: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })

	if errFinalize := store.FinalizeAuthFileDeletionAtBaseDir(t.Context(), authDir, fileName); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletionAtBaseDir() error = %v", errFinalize)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", fileName))
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("recreated local auth = %s, %v; want %s", got, errRead, replacement)
	}
	if authfileguard.IsRetired(path) {
		t.Fatal("recreated auth path remains retired after confirmed deletion")
	}

	if errPersist := store.PersistAuthFiles(t.Context(), "persist recreated auth", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", fileName), string(replacement))
}

func TestGitTokenStoreFinalizeRetiredDeletionPreservesRemoteReplacement(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove local retired auth: %v", errRemove)
	}

	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	advanceRemoteBranchFile(t, filepath.Join(root, "seed"), remoteDir, "main", filepath.Join("auths", fileName), replacement, "replace retired auth")
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("restore stale local retired auth: %v", errWrite)
	}
	authfileguard.MarkRetired(path)
	t.Cleanup(func() { authfileguard.ClearRetired(path) })
	if errPersist := store.PersistAuthFiles(t.Context(), "ignore stale retired mirror", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	if _, errStat := os.Stat(path); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("stale local retired auth still exists: %v", errStat)
	}
	if !authfileguard.IsRetired(path) {
		t.Fatal("stale mirror removal cleared quarantine before remote finalization")
	}
	if errFinalize := store.FinalizeAuthFileDeletionAtBaseDir(t.Context(), authDir, fileName); errFinalize != nil {
		t.Fatalf("FinalizeAuthFileDeletionAtBaseDir() error = %v", errFinalize)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", fileName), string(replacement))
	if authfileguard.IsRetired(path) {
		t.Fatal("replacement path remains retired")
	}
}

func TestGitTokenStoreFinalizeRetiredDeletionPreservesDifferentRetiredGeneration(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	original := []byte(`{"type":"gemini","access_token":"original"}`)
	if errWrite := os.WriteFile(path, original, 0o600); errWrite != nil {
		t.Fatalf("write original retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	if errSeed := store.commitAndPushLocked("seed original retired auth", rel); errSeed != nil {
		store.mu.Unlock()
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	originalState, errState := store.remoteBranchPreconditionLocked()
	store.mu.Unlock()
	if errState != nil {
		t.Fatalf("remoteBranchPreconditionLocked() error = %v", errState)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove local retired auth: %v", errRemove)
	}
	replacement := []byte(`{"type":"gemini","access_token":"replacement"}`)
	advanceRemoteBranchFile(t, filepath.Join(root, "seed"), remoteDir, "main", filepath.Join("auths", fileName), replacement, "replace retired auth")
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(original))
	identity := originalState.branch.String() + "@" + originalState.hash.String()
	if !generation.BindBackendIdentity("git:"+filepath.ToSlash(rel), identity) {
		t.Fatal("bind original git generation")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errFinalize := store.FinalizeAuthFileDeletionAtBaseDir(ctx, authDir, fileName); !errors.Is(errFinalize, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("FinalizeAuthFileDeletionAtBaseDir() error = %v, want uncertain", errFinalize)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", fileName), string(replacement))
}

func TestEnsureGitLocalAuthLockExclusionRejectsSymlink(t *testing.T) {
	repoDir := t.TempDir()
	infoDir := filepath.Join(repoDir, ".git", "info")
	if errMkdir := os.MkdirAll(infoDir, 0o700); errMkdir != nil {
		t.Fatalf("create git info directory: %v", errMkdir)
	}
	outside := filepath.Join(t.TempDir(), "outside")
	if errWrite := os.WriteFile(outside, []byte("unchanged\n"), 0o600); errWrite != nil {
		t.Fatalf("write outside file: %v", errWrite)
	}
	if errLink := os.Symlink(outside, filepath.Join(infoDir, "exclude")); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	if errExclude := ensureGitLocalAuthLockExclusion(repoDir); errExclude == nil {
		t.Fatal("ensureGitLocalAuthLockExclusion() error = nil, want symlink rejection")
	}
	if data, errRead := os.ReadFile(outside); errRead != nil || string(data) != "unchanged\n" {
		t.Fatalf("outside file = %q, %v", data, errRead)
	}
}

func TestGitTokenStoreEnsureRepositoryRejectsSymlinkRepositoryBeforeClone(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote\n"})
	outside := t.TempDir()
	repoLink := filepath.Join(root, "workspace")
	if errLink := os.Symlink(outside, repoLink); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(filepath.Join(repoLink, "auths"))
	if errEnsure := store.EnsureRepository(); !errors.Is(errEnsure, errUnsafeGitAuthPath) {
		t.Fatalf("EnsureRepository() error = %v, want unsafe path", errEnsure)
	}
	if _, errStat := os.Lstat(filepath.Join(outside, ".git")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("symlink target was modified before rejection: %v", errStat)
	}
}

func TestGitRetiredFinalizerLeasePreservesConcurrentRemoteReplacement(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	const fileName = "legacy.json"
	path := filepath.Join(authDir, fileName)
	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	remoteState, errRemote := store.remoteBranchPreconditionLocked()
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errRemote != nil {
		t.Fatalf("remoteBranchPreconditionLocked() error = %v", errRemote)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove local retired auth: %v", errRemove)
	}

	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	advanceRemoteBranchFile(t, filepath.Join(root, "seed"), remoteDir, "main", filepath.Join("auths", fileName), replacement, "replace retired auth concurrently")
	store.mu.Lock()
	errFinalize := store.finalizeRetiredAuthDeletionAgainstRemoteLocked(t.Context(), path, rel, remoteState, authfileguard.CaptureRetired(path))
	store.mu.Unlock()
	if errFinalize == nil {
		t.Fatal("finalizeRetiredAuthDeletionAgainstRemoteLocked() error = nil, want lease conflict")
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", fileName), string(replacement))
}

func TestGitTokenStoreDeleteAtBaseDirRemovesAndCommitsSnapshot(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	baseDirA := filepath.Join(root, "workspace-a", "auths")
	baseDirB := filepath.Join(root, "workspace-b", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDirA)
	auth := &cliproxyauth.Auth{ID: "auth.json", Provider: "claude", FileName: "auth.json", Metadata: map[string]any{"type": "claude"}}
	path, errSave := store.Save(t.Context(), auth)
	if errSave != nil {
		t.Fatalf("Save() error = %v", errSave)
	}
	store.SetBaseDir(baseDirB)
	rootSnapshot, errRoot := os.OpenRoot(baseDirA)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer rootSnapshot.Close()
	if errDelete := store.DeleteAuthFileAtRoot(t.Context(), baseDirA, rootSnapshot, auth.ID); errDelete != nil {
		t.Fatalf("DeleteAuthFileAtRoot() error = %v", errDelete)
	}
	if _, errStat := os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("snapshot auth still exists: %v", errStat)
	}
	if gotBaseDir := store.AuthDir(); gotBaseDir != baseDirB {
		t.Fatalf("restored base dir = %q, want %q", gotBaseDir, baseDirB)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", auth.ID))
}

func TestGitPersistAuthFilesRejectsRewritingRetiredHead(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "legacy.json")
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"rewritten"}`), 0o600); errWrite != nil {
		t.Fatalf("rewrite auth file: %v", errWrite)
	}
	errPersist := store.PersistAuthFiles(t.Context(), "sync", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "legacy.json"), string(retired))
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove retired auth file: %v", errRemove)
	}
	if errDelete := store.PersistAuthFiles(t.Context(), "delete retired auth", path); errDelete != nil {
		t.Fatalf("PersistAuthFiles() delete error = %v", errDelete)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", filepath.Join("auths", "legacy.json"))
}

func TestGitTokenStoreSaveRejectsRewritingRetiredHead(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "legacy.json")
	retired := []byte(`{"type":"gemini","access_token":"legacy"}`)
	if errWrite := os.WriteFile(path, retired, 0o600); errWrite != nil {
		t.Fatalf("write retired auth file: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	errSeed := store.commitAndPushLocked("seed retired auth", rel)
	store.mu.Unlock()
	if errSeed != nil {
		t.Fatalf("seed retired auth: %v", errSeed)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"external"}`), 0o600); errWrite != nil {
		t.Fatalf("rewrite local auth: %v", errWrite)
	}
	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID: "legacy.json", FileName: "legacy.json", Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "legacy.json"), string(retired))
}

func TestGitPersistAuthFilesRejectsRetiredAuthOnStaleRemote(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	remotePath := filepath.Join("auths", "legacy.json")
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", remotePath, []byte(`{"type":"codex","access_token":"initial"}`), "seed remote auth")
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	path := filepath.Join(authDir, "legacy.json")
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatalf("write local replacement: %v", errWrite)
	}
	retired := []byte(`{"type":"gemini","access_token":"remote"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", remotePath, retired, "retire remote auth")

	errPersist := store.PersistAuthFiles(t.Context(), "sync stale workspace", path)
	if !errors.Is(errPersist, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("PersistAuthFiles() error = %v, want retired read-only", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", remotePath, string(retired))
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("local replacement changed: data=%s error=%v", got, errRead)
	}
}

func TestGitTokenStoreSaveRejectsRetiredAuthOnStaleRemote(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	remotePath := filepath.Join("auths", "legacy.json")
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", remotePath, []byte(`{"type":"codex","access_token":"initial"}`), "seed remote auth")
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	path := filepath.Join(authDir, "legacy.json")
	local := []byte(`{"type":"codex","access_token":"local"}`)
	if errWrite := os.WriteFile(path, local, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	retired := []byte(`{"type":"gemini","access_token":"remote"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", remotePath, retired, "retire remote auth")

	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "legacy.json",
		FileName: "legacy.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, cliproxyauth.ErrRetiredGeminiCLIAuthReadOnly) {
		t.Fatalf("Save() error = %v, want retired read-only", errSave)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", remotePath, string(retired))
	if got, errRead := os.ReadFile(path); errRead != nil || !bytes.Equal(got, local) {
		t.Fatalf("local auth changed: data=%s error=%v", got, errRead)
	}
}

func TestGitTokenStoreSaveRejectsRemoteSymlink(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	path := filepath.Join(authDir, "linked.json")
	local := []byte(`{"type":"codex","access_token":"local"}`)
	if errWrite := os.WriteFile(path, local, 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	outside := filepath.Join(root, "outside.json")
	wantOutside := []byte(`{"type":"codex","access_token":"outside"}`)
	if errWrite := os.WriteFile(outside, wantOutside, 0o600); errWrite != nil {
		t.Fatalf("write outside auth: %v", errWrite)
	}
	advanceRemoteBranchSymlink(t, filepath.Join(root, "seed"), remoteDir, "main", filepath.Join("auths", "linked.json"), outside, "add remote auth symlink")

	_, errSave := store.Save(t.Context(), &cliproxyauth.Auth{
		ID:       "linked.json",
		FileName: "linked.json",
		Provider: "codex",
		Metadata: map[string]any{"type": "codex", "access_token": "replacement"},
	})
	if !errors.Is(errSave, errUnsafeGitAuthPath) {
		t.Fatalf("Save() error = %v, want unsafe path", errSave)
	}
	if gotOutside, errRead := os.ReadFile(outside); errRead != nil || !bytes.Equal(gotOutside, wantOutside) {
		t.Fatalf("outside auth = %s, %v; want %s", gotOutside, errRead, wantOutside)
	}
}

func TestGitEnsureRepositoryRejectsSymlinkAuthRoot(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	outside := filepath.Join(root, "outside")
	if errMkdir := os.MkdirAll(outside, 0o700); errMkdir != nil {
		t.Fatalf("create outside dir: %v", errMkdir)
	}
	advanceRemoteBranchSymlink(t, filepath.Join(root, "seed"), remoteDir, "main", "auths", outside, "add remote auth root symlink")

	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))
	if errEnsure := store.EnsureRepository(); !errors.Is(errEnsure, errUnsafeGitAuthPath) {
		t.Fatalf("EnsureRepository() error = %v, want unsafe path", errEnsure)
	}
	if _, errStat := os.Stat(filepath.Join(outside, ".gitkeep")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("outside auth root was modified: %v", errStat)
	}
}

func TestGitCommitAndPushRejectsRemoteAdvanceAfterPrecondition(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}

	path := filepath.Join(authDir, "concurrent.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write local replacement: %v", errWrite)
	}
	rel, errRel := store.relativeToRepo(path)
	if errRel != nil {
		t.Fatalf("relativeToRepo() error = %v", errRel)
	}
	store.mu.Lock()
	remoteState, errRemote := store.remoteBranchPreconditionLocked()
	store.mu.Unlock()
	if errRemote != nil {
		t.Fatalf("remoteBranchPreconditionLocked() error = %v", errRemote)
	}

	retired := []byte(`{"type":"gemini","access_token":"concurrent"}`)
	advanceRemoteBranchFile(t, filepath.Join(root, "seed"), remoteDir, "main", filepath.Join("auths", "concurrent.json"), retired, "concurrent remote retirement")
	store.mu.Lock()
	errPush := store.commitAndPushAgainstRemoteLocked("stale conditional push", remoteState, rel)
	store.mu.Unlock()
	if errPush == nil {
		t.Fatal("commitAndPushAgainstRemoteLocked() error = nil, want lease conflict")
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "concurrent.json"), string(retired))
}

func TestGitPersistAuthFilesDeleteUsesLatestRemoteTree(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	targetRel := filepath.Join("auths", "legacy.json")
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, []byte(`{"type":"codex","access_token":"initial"}`), "seed deletable auth")

	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	targetPath := filepath.Join(authDir, "legacy.json")
	if errRemove := os.Remove(targetPath); errRemove != nil {
		t.Fatalf("remove local auth: %v", errRemove)
	}

	retired := []byte(`{"type":"gemini","access_token":"retired"}`)
	concurrent := []byte(`{"type":"codex","access_token":"concurrent"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, retired, "retire auth before deletion")
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", filepath.Join("auths", "concurrent.json"), concurrent, "add concurrent auth")

	if errPersist := store.PersistAuthFiles(t.Context(), "delete retired auth", targetPath); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	assertRemoteBranchFileMissing(t, remoteDir, "main", targetRel)
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "concurrent.json"), string(concurrent))
}

func TestGitPersistAuthFilesPreservesReplacementFromOlderDeleteGeneration(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	targetRel := filepath.Join("auths", "auth.json")
	original := []byte(`{"type":"codex","access_token":"original"}`)
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, original, "seed original auth")

	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	targetPath := filepath.Join(authDir, "auth.json")
	if errRemove := os.Remove(targetPath); errRemove != nil {
		t.Fatalf("remove local auth: %v", errRemove)
	}
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, replacement, "replace auth before delete retry")

	ctx := authfileguard.WithExpectedDeleteHash(t.Context(), cliproxyauth.SourceHashFromBytes(original))
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", targetPath); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", targetRel, string(replacement))
	localData, errRead := os.ReadFile(targetPath)
	if errRead != nil {
		t.Fatalf("read restored local replacement: %v", errRead)
	}
	if !bytes.Equal(localData, replacement) {
		t.Fatalf("restored local replacement = %q, want %q", localData, replacement)
	}
}

func TestGitPersistAuthFilesRejectsChangedExpectedSnapshot(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	targetRel := filepath.Join("auths", "auth.json")
	remoteData := []byte(`{"type":"codex","access_token":"remote"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, remoteData, "seed remote auth")

	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	targetPath := filepath.Join(authDir, "auth.json")
	if errWrite := os.WriteFile(targetPath, []byte(`{"type":"codex","access_token":"original"}`), 0o600); errWrite != nil {
		t.Fatalf("write local auth: %v", errWrite)
	}
	expected := cliproxyauth.SourceHashFromBytes([]byte(`{"type":"codex","access_token":"replacement"}`))
	ctx := authfileguard.WithExpectedPersistHash(t.Context(), expected)
	if errPersist := store.PersistAuthFiles(ctx, "persist replacement", targetPath); !errors.Is(errPersist, authfileguard.ErrPersistGenerationStale) {
		t.Fatalf("PersistAuthFiles() error = %v, want ErrPersistGenerationStale", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", targetRel, string(remoteData))
}

func TestGitPersistAuthFilesPreservesSameContentNewRemoteTip(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	seedDir := filepath.Join(root, "seed")
	targetRel := filepath.Join("auths", "auth.json")
	data := []byte(`{"type":"codex","access_token":"same"}`)
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", targetRel, data, "seed original auth")

	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	store.mu.Lock()
	originalState, errState := store.remoteBranchPreconditionLocked()
	store.mu.Unlock()
	if errState != nil {
		t.Fatalf("remoteBranchPreconditionLocked() error = %v", errState)
	}
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	key := "git:" + filepath.ToSlash(targetRel)
	if !generation.BindBackendIdentity(key, originalState.branch.String()+"@"+originalState.hash.String()) {
		t.Fatal("failed to bind original git generation")
	}

	targetPath := filepath.Join(authDir, "auth.json")
	if errRemove := os.Remove(targetPath); errRemove != nil {
		t.Fatalf("remove local auth: %v", errRemove)
	}
	advanceRemoteBranchFile(t, seedDir, remoteDir, "main", "same-content-generation.txt", []byte("generation-b\n"), "advance remote with same auth content")
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(ctx, "retry old auth deletion", targetPath); !errors.Is(errPersist, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("PersistAuthFiles() error = %v, want uncertain generation", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", targetRel, string(data))
}

func TestGitPersistAuthFilesCompletesResumedDeleteWhenBlobIsAbsent(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	data := []byte(`{"type":"codex","access_token":"deleted"}`)
	generation := authfileguard.NewDeleteGeneration(cliproxyauth.SourceHashFromBytes(data))
	if !generation.BindBackendIdentity("git:auths/auth.json", "refs/heads/main@old-tip") {
		t.Fatal("bind old git identity")
	}
	ctx := authfileguard.WithDeleteGeneration(t.Context(), generation)
	if errPersist := store.PersistAuthFiles(ctx, "resume completed deletion", filepath.Join(authDir, "auth.json")); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
}

func TestGitPersistAuthFilesPreservesExistingEmptyFile(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote default branch\n"})
	authDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "main")
	store.SetBaseDir(authDir)
	if errEnsure := store.EnsureRepository(); errEnsure != nil {
		t.Fatalf("EnsureRepository() error = %v", errEnsure)
	}
	path := filepath.Join(authDir, "empty.json")
	if errWrite := os.WriteFile(path, nil, 0o600); errWrite != nil {
		t.Fatalf("write empty auth: %v", errWrite)
	}
	if errPersist := store.PersistAuthFiles(t.Context(), "persist empty auth", path); errPersist != nil {
		t.Fatalf("PersistAuthFiles() error = %v", errPersist)
	}
	assertRemoteBranchFileContents(t, remoteDir, "main", filepath.Join("auths", "empty.json"), "")
}

type testBranchSpec struct {
	name     string
	contents string
}

func TestEnsureRepositoryUsesRemoteDefaultBranchWhenBranchNotConfigured(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
		testBranchSpec{name: "release/2026", contents: "release branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "trunk", "remote default branch\n")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "trunk", "remote default branch updated\n", "advance trunk")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch updated\n", "advance release")

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository second call: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "trunk", "remote default branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryUsesConfiguredBranchWhenExplicitlySet(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
		testBranchSpec{name: "release/2026", contents: "release branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "release/2026")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch\n")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "trunk", "remote default branch updated\n", "advance trunk")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch updated\n", "advance release")

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository second call: %v", err)
	}

	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryReturnsErrorForMissingConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)

	store := NewGitTokenStore(remoteDir, "", "", "missing-branch")
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	err := store.EnsureRepository()
	if err == nil {
		t.Fatal("EnsureRepository succeeded, want error for nonexistent configured branch")
	}
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryReturnsErrorForMissingConfiguredBranchOnExistingRepositoryPull(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "trunk",
		testBranchSpec{name: "trunk", contents: "remote default branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}

	reopened := NewGitTokenStore(remoteDir, "", "", "missing-branch")
	reopened.SetBaseDir(baseDir)

	err := reopened.EnsureRepository()
	if err == nil {
		t.Fatal("EnsureRepository succeeded on reopen, want error for nonexistent configured branch")
	}
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "trunk")
	assertRemoteHeadBranch(t, remoteDir, "trunk")
}

func TestEnsureRepositoryInitializesEmptyRemoteUsingConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := filepath.Join(root, "remote.git")
	if _, err := git.PlainInit(remoteDir, true); err != nil {
		t.Fatalf("init bare remote: %v", err)
	}

	branch := "feature/gemini-fix"
	store := NewGitTokenStore(remoteDir, "", "", branch)
	store.SetBaseDir(filepath.Join(root, "workspace", "auths"))

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository: %v", err)
	}

	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), branch)
	assertRemoteBranchExistsWithCommit(t, remoteDir, branch)
	assertRemoteBranchDoesNotExist(t, remoteDir, "master")
}

func TestEnsureRepositoryExistingRepoSwitchesToConfiguredBranch(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	reopened := NewGitTokenStore(remoteDir, "", "", "develop")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository reopen: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	workspaceDir := filepath.Join(root, "workspace")
	if err := os.WriteFile(filepath.Join(workspaceDir, "branch.txt"), []byte("local develop update\n"), 0o600); err != nil {
		t.Fatalf("write local branch marker: %v", err)
	}

	reopened.mu.Lock()
	err := reopened.commitAndPushLocked("Update develop branch marker", "branch.txt")
	reopened.mu.Unlock()
	if err != nil {
		t.Fatalf("commitAndPushLocked: %v", err)
	}

	assertRepositoryHeadBranch(t, workspaceDir, "develop")
	assertRemoteBranchContents(t, remoteDir, "develop", "local develop update\n")
	assertRemoteBranchContents(t, remoteDir, "master", "remote master branch\n")
}

func TestEnsureRepositoryExistingRepoSwitchesToConfiguredBranchCreatedAfterClone(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	advanceRemoteBranchFromNewBranch(t, filepath.Join(root, "seed"), remoteDir, "release/2026", "release branch\n", "create release")

	reopened := NewGitTokenStore(remoteDir, "", "", "release/2026")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository reopen: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "release/2026", "release branch\n")
}

func TestEnsureRepositoryResetsToRemoteDefaultWhenBranchUnset(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	// First store pins to develop and prepares local workspace
	storePinned := NewGitTokenStore(remoteDir, "", "", "develop")
	storePinned.SetBaseDir(baseDir)
	if err := storePinned.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository pinned: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	// Second store has branch unset and should reset local workspace to remote default (master)
	storeDefault := NewGitTokenStore(remoteDir, "", "", "")
	storeDefault.SetBaseDir(baseDir)
	if err := storeDefault.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository default: %v", err)
	}
	// Local HEAD should now follow remote default (master)
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "master")

	// Make a local change and push using the store with branch unset; push should update remote master
	workspaceDir := filepath.Join(root, "workspace")
	if err := os.WriteFile(filepath.Join(workspaceDir, "branch.txt"), []byte("local master update\n"), 0o600); err != nil {
		t.Fatalf("write local master marker: %v", err)
	}
	storeDefault.mu.Lock()
	if err := storeDefault.commitAndPushLocked("Update master marker", "branch.txt"); err != nil {
		storeDefault.mu.Unlock()
		t.Fatalf("commitAndPushLocked: %v", err)
	}
	storeDefault.mu.Unlock()

	assertRemoteBranchContents(t, remoteDir, "master", "local master update\n")
}

func TestEnsureRepositoryFollowsRenamedRemoteDefaultBranchWhenAvailable(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "main", contents: "remote main branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	store := NewGitTokenStore(remoteDir, "", "", "")
	store.SetBaseDir(baseDir)

	if err := store.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository initial clone: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "master", "remote master branch\n")

	setRemoteHeadBranch(t, remoteDir, "main")
	advanceRemoteBranch(t, filepath.Join(root, "seed"), remoteDir, "main", "remote main branch updated\n", "advance main")

	reopened := NewGitTokenStore(remoteDir, "", "", "")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository after remote default rename: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "main", "remote main branch updated\n")
	assertRemoteHeadBranch(t, remoteDir, "main")
}

func TestEnsureRepositoryKeepsCurrentBranchWhenRemoteDefaultCannotBeResolved(t *testing.T) {
	root := t.TempDir()
	remoteDir := setupGitRemoteRepository(t, root, "master",
		testBranchSpec{name: "master", contents: "remote master branch\n"},
		testBranchSpec{name: "develop", contents: "remote develop branch\n"},
	)

	baseDir := filepath.Join(root, "workspace", "auths")
	pinned := NewGitTokenStore(remoteDir, "", "", "develop")
	pinned.SetBaseDir(baseDir)
	if err := pinned.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository pinned: %v", err)
	}
	assertRepositoryBranchAndContents(t, filepath.Join(root, "workspace"), "develop", "remote develop branch\n")

	authServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("WWW-Authenticate", `Basic realm="git"`)
		http.Error(w, "auth required", http.StatusUnauthorized)
	}))
	defer authServer.Close()

	repo, err := git.PlainOpen(filepath.Join(root, "workspace"))
	if err != nil {
		t.Fatalf("open workspace repo: %v", err)
	}
	cfg, err := repo.Config()
	if err != nil {
		t.Fatalf("read repo config: %v", err)
	}
	cfg.Remotes["origin"].URLs = []string{authServer.URL}
	if err := repo.SetConfig(cfg); err != nil {
		t.Fatalf("set repo config: %v", err)
	}

	reopened := NewGitTokenStore(remoteDir, "", "", "")
	reopened.SetBaseDir(baseDir)

	if err := reopened.EnsureRepository(); err != nil {
		t.Fatalf("EnsureRepository default branch fallback: %v", err)
	}
	assertRepositoryHeadBranch(t, filepath.Join(root, "workspace"), "develop")
}

func setupGitRemoteRepository(t *testing.T, root, defaultBranch string, branches ...testBranchSpec) string {
	t.Helper()

	remoteDir := filepath.Join(root, "remote.git")
	if _, err := git.PlainInit(remoteDir, true); err != nil {
		t.Fatalf("init bare remote: %v", err)
	}

	seedDir := filepath.Join(root, "seed")
	seedRepo, err := git.PlainInit(seedDir, false)
	if err != nil {
		t.Fatalf("init seed repo: %v", err)
	}
	if err := seedRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(defaultBranch))); err != nil {
		t.Fatalf("set seed HEAD: %v", err)
	}

	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}

	defaultSpec, ok := findBranchSpec(branches, defaultBranch)
	if !ok {
		t.Fatalf("missing default branch spec for %q", defaultBranch)
	}
	commitBranchMarker(t, seedDir, worktree, defaultSpec, "seed default branch")

	for _, branch := range branches {
		if branch.name == defaultBranch {
			continue
		}
		if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(defaultBranch)}); err != nil {
			t.Fatalf("checkout default branch %s: %v", defaultBranch, err)
		}
		if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch.name), Create: true}); err != nil {
			t.Fatalf("create branch %s: %v", branch.name, err)
		}
		commitBranchMarker(t, seedDir, worktree, branch, "seed branch "+branch.name)
	}

	if _, err := seedRepo.CreateRemote(&gitconfig.RemoteConfig{Name: "origin", URLs: []string{remoteDir}}); err != nil {
		t.Fatalf("create origin remote: %v", err)
	}
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitconfig.RefSpec{gitconfig.RefSpec("refs/heads/*:refs/heads/*")},
	}); err != nil {
		t.Fatalf("push seed branches: %v", err)
	}

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if err := remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(defaultBranch))); err != nil {
		t.Fatalf("set remote HEAD: %v", err)
	}

	return remoteDir
}

type testTokenStorage struct {
	metadata  map[string]any
	afterSave func()
}

type writeFileOnMarshal struct {
	value string
	write func() error
}

func (v *writeFileOnMarshal) MarshalJSON() ([]byte, error) {
	if v.write != nil {
		if errWrite := v.write(); errWrite != nil {
			return nil, errWrite
		}
		v.write = nil
	}
	return json.Marshal(v.value)
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

func (s *testTokenStorage) MetadataSnapshot() map[string]any {
	if s == nil || s.metadata == nil {
		return nil
	}
	cloned := make(map[string]any, len(s.metadata))
	for key, value := range s.metadata {
		cloned[key] = value
	}
	return cloned
}

func (s *testTokenStorage) SaveTokenToFile(authFilePath string) error {
	payload := map[string]any{
		"access_token":  "tok-storage",
		"refresh_token": "refresh-storage",
	}
	for key, value := range s.metadata {
		payload[key] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if errWrite := os.WriteFile(authFilePath, raw, 0o600); errWrite != nil {
		return errWrite
	}
	if s.afterSave != nil {
		s.afterSave()
	}
	return nil
}

func commitBranchMarker(t *testing.T, seedDir string, worktree *git.Worktree, branch testBranchSpec, message string) {
	t.Helper()

	if err := os.WriteFile(filepath.Join(seedDir, "branch.txt"), []byte(branch.contents), 0o600); err != nil {
		t.Fatalf("write branch marker for %s: %v", branch.name, err)
	}
	if _, err := worktree.Add("branch.txt"); err != nil {
		t.Fatalf("add branch marker for %s: %v", branch.name, err)
	}
	if _, err := worktree.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "CLIProxyAPI",
			Email: "cliproxy@local",
			When:  time.Unix(1711929600, 0),
		},
	}); err != nil {
		t.Fatalf("commit branch marker for %s: %v", branch.name, err)
	}
}

func advanceRemoteBranch(t *testing.T, seedDir, remoteDir, branch, contents, message string) {
	t.Helper()

	seedRepo, err := git.PlainOpen(seedDir)
	if err != nil {
		t.Fatalf("open seed repo: %v", err)
	}
	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch)}); err != nil {
		t.Fatalf("checkout branch %s: %v", branch, err)
	}
	commitBranchMarker(t, seedDir, worktree, testBranchSpec{name: branch, contents: contents}, message)
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []gitconfig.RefSpec{
			gitconfig.RefSpec(plumbing.NewBranchReferenceName(branch).String() + ":" + plumbing.NewBranchReferenceName(branch).String()),
		},
	}); err != nil {
		t.Fatalf("push branch %s update to %s: %v", branch, remoteDir, err)
	}
}

func advanceRemoteBranchFile(t *testing.T, seedDir, remoteDir, branch, relativePath string, data []byte, message string) {
	t.Helper()

	seedRepo, err := git.PlainOpen(seedDir)
	if err != nil {
		t.Fatalf("open seed repo: %v", err)
	}
	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch)}); err != nil {
		t.Fatalf("checkout branch %s: %v", branch, err)
	}
	path := filepath.Join(seedDir, filepath.FromSlash(relativePath))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create parent for %s: %v", relativePath, err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", relativePath, err)
	}
	if _, err := worktree.Add(filepath.ToSlash(relativePath)); err != nil {
		t.Fatalf("add %s: %v", relativePath, err)
	}
	if _, err := worktree.Commit(message, &git.CommitOptions{
		Author: &object.Signature{
			Name:  "CLIProxyAPI",
			Email: "cliproxy@local",
			When:  time.Now(),
		},
	}); err != nil {
		t.Fatalf("commit %s: %v", relativePath, err)
	}
	branchRef := plumbing.NewBranchReferenceName(branch)
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitconfig.RefSpec{gitconfig.RefSpec(branchRef.String() + ":" + branchRef.String())},
		Force:      true,
	}); err != nil {
		t.Fatalf("push branch %s update to %s: %v", branch, remoteDir, err)
	}
}

func advanceRemoteBranchSymlink(t *testing.T, seedDir, remoteDir, branch, relativePath, target, message string) {
	t.Helper()

	seedRepo, errOpen := git.PlainOpen(seedDir)
	if errOpen != nil {
		t.Fatalf("open seed repo: %v", errOpen)
	}
	worktree, errWorktree := seedRepo.Worktree()
	if errWorktree != nil {
		t.Fatalf("open seed worktree: %v", errWorktree)
	}
	if errCheckout := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch)}); errCheckout != nil {
		t.Fatalf("checkout branch %s: %v", branch, errCheckout)
	}
	path := filepath.Join(seedDir, filepath.FromSlash(relativePath))
	if errRemove := os.RemoveAll(path); errRemove != nil {
		t.Fatalf("remove existing %s: %v", relativePath, errRemove)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		t.Fatalf("create parent for %s: %v", relativePath, errMkdir)
	}
	if errLink := os.Symlink(target, path); errLink != nil {
		t.Fatalf("create symlink %s: %v", relativePath, errLink)
	}
	if _, errAdd := worktree.Add(filepath.ToSlash(relativePath)); errAdd != nil {
		t.Fatalf("add symlink %s: %v", relativePath, errAdd)
	}
	if _, errCommit := worktree.Commit(message, &git.CommitOptions{Author: &object.Signature{
		Name: "CLIProxyAPI", Email: "cliproxy@local", When: time.Now(),
	}}); errCommit != nil {
		t.Fatalf("commit symlink %s: %v", relativePath, errCommit)
	}
	branchRef := plumbing.NewBranchReferenceName(branch)
	if errPush := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs:   []gitconfig.RefSpec{gitconfig.RefSpec(branchRef.String() + ":" + branchRef.String())},
		Force:      true,
	}); errPush != nil {
		t.Fatalf("push branch %s symlink update to %s: %v", branch, remoteDir, errPush)
	}
}

func advanceRemoteBranchFromNewBranch(t *testing.T, seedDir, remoteDir, branch, contents, message string) {
	t.Helper()

	seedRepo, err := git.PlainOpen(seedDir)
	if err != nil {
		t.Fatalf("open seed repo: %v", err)
	}
	worktree, err := seedRepo.Worktree()
	if err != nil {
		t.Fatalf("open seed worktree: %v", err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName("master")}); err != nil {
		t.Fatalf("checkout master before creating %s: %v", branch, err)
	}
	if err := worktree.Checkout(&git.CheckoutOptions{Branch: plumbing.NewBranchReferenceName(branch), Create: true}); err != nil {
		t.Fatalf("create branch %s: %v", branch, err)
	}
	commitBranchMarker(t, seedDir, worktree, testBranchSpec{name: branch, contents: contents}, message)
	if err := seedRepo.Push(&git.PushOptions{
		RemoteName: "origin",
		RefSpecs: []gitconfig.RefSpec{
			gitconfig.RefSpec(plumbing.NewBranchReferenceName(branch).String() + ":" + plumbing.NewBranchReferenceName(branch).String()),
		},
	}); err != nil {
		t.Fatalf("push new branch %s update to %s: %v", branch, remoteDir, err)
	}
}

func findBranchSpec(branches []testBranchSpec, name string) (testBranchSpec, bool) {
	for _, branch := range branches {
		if branch.name == name {
			return branch, true
		}
	}
	return testBranchSpec{}, false
}

func assertRepositoryBranchAndContents(t *testing.T, repoDir, branch, wantContents string) {
	t.Helper()

	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("local repo head: %v", err)
	}
	if got, want := head.Name(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("local head branch = %s, want %s", got, want)
	}
	contents, err := os.ReadFile(filepath.Join(repoDir, "branch.txt"))
	if err != nil {
		t.Fatalf("read branch marker: %v", err)
	}
	if got := string(contents); got != wantContents {
		t.Fatalf("branch marker contents = %q, want %q", got, wantContents)
	}
}

func assertRepositoryHeadBranch(t *testing.T, repoDir, branch string) {
	t.Helper()

	repo, err := git.PlainOpen(repoDir)
	if err != nil {
		t.Fatalf("open local repo: %v", err)
	}
	head, err := repo.Head()
	if err != nil {
		t.Fatalf("local repo head: %v", err)
	}
	if got, want := head.Name(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("local head branch = %s, want %s", got, want)
	}
}

func assertRemoteHeadBranch(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	head, err := remoteRepo.Reference(plumbing.HEAD, false)
	if err != nil {
		t.Fatalf("read remote HEAD: %v", err)
	}
	if got, want := head.Target(), plumbing.NewBranchReferenceName(branch); got != want {
		t.Fatalf("remote HEAD target = %s, want %s", got, want)
	}
}

func setRemoteHeadBranch(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if err := remoteRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.NewBranchReferenceName(branch))); err != nil {
		t.Fatalf("set remote HEAD to %s: %v", branch, err)
	}
}

func assertRemoteBranchExistsWithCommit(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	if got := ref.Hash(); got == plumbing.ZeroHash {
		t.Fatalf("remote branch %s hash = %s, want non-zero hash", branch, got)
	}
}

func assertRemoteBranchDoesNotExist(t *testing.T, remoteDir, branch string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	if _, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false); err == nil {
		t.Fatalf("remote branch %s exists, want missing", branch)
	} else if err != plumbing.ErrReferenceNotFound {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
}

func assertRemoteBranchContents(t *testing.T, remoteDir, branch, wantContents string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	commit, err := remoteRepo.CommitObject(ref.Hash())
	if err != nil {
		t.Fatalf("read remote branch %s commit: %v", branch, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("read remote branch %s tree: %v", branch, err)
	}
	file, err := tree.File("branch.txt")
	if err != nil {
		t.Fatalf("read remote branch %s file: %v", branch, err)
	}
	contents, err := file.Contents()
	if err != nil {
		t.Fatalf("read remote branch %s contents: %v", branch, err)
	}
	if contents != wantContents {
		t.Fatalf("remote branch %s contents = %q, want %q", branch, contents, wantContents)
	}
}

func assertRemoteBranchFileMissing(t *testing.T, remoteDir, branch, filePath string) {
	t.Helper()

	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	commit, err := remoteRepo.CommitObject(ref.Hash())
	if err != nil {
		t.Fatalf("read remote branch %s commit: %v", branch, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("read remote branch %s tree: %v", branch, err)
	}
	if _, err := tree.File(filePath); err == nil {
		t.Fatalf("remote branch %s still contains %s", branch, filePath)
	}
}

func assertRemoteBranchFileContents(t *testing.T, remoteDir, branch, filePath, want string) {
	t.Helper()
	remoteRepo, err := git.PlainOpen(remoteDir)
	if err != nil {
		t.Fatalf("open remote repo: %v", err)
	}
	ref, err := remoteRepo.Reference(plumbing.NewBranchReferenceName(branch), false)
	if err != nil {
		t.Fatalf("read remote branch %s: %v", branch, err)
	}
	commit, err := remoteRepo.CommitObject(ref.Hash())
	if err != nil {
		t.Fatalf("read remote branch %s commit: %v", branch, err)
	}
	tree, err := commit.Tree()
	if err != nil {
		t.Fatalf("read remote branch %s tree: %v", branch, err)
	}
	file, err := tree.File(filepath.ToSlash(filePath))
	if err != nil {
		t.Fatalf("read remote branch %s file %s: %v", branch, filePath, err)
	}
	contents, err := file.Contents()
	if err != nil {
		t.Fatalf("read remote branch %s file contents: %v", branch, err)
	}
	if contents != want {
		t.Fatalf("remote branch %s file %s = %q, want %q", branch, filePath, contents, want)
	}
}
