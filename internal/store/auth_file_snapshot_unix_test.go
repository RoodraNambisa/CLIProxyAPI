//go:build darwin || linux

package store

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestWriteAuthFileAtomicallyRejectsReplacedParentWhileWaitingForTargetLock(t *testing.T) {
	dir := t.TempDir()
	const relativePath = "nested/auth.json"
	originalPath := filepath.Join(dir, filepath.FromSlash(relativePath))
	writeAuthSnapshotTestFile(t, originalPath)
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	expected, errExpected := captureAuthFileSnapshot(root, filepath.FromSlash(relativePath))
	if errExpected != nil {
		t.Fatal(errExpected)
	}
	unlockTarget, errLock := authfileguard.LockRootTarget(root, filepath.FromSlash(relativePath))
	if errLock != nil {
		t.Fatal(errLock)
	}
	result := make(chan error, 1)
	go func() {
		result <- writeAuthFileAtomicallyAtRoot(root, filepath.FromSlash(relativePath), []byte(`{"type":"codex","access_token":"writer"}`), &expected)
	}()

	oldParent := filepath.Join(dir, "nested")
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, errRead := os.ReadDir(oldParent)
		if errRead != nil {
			t.Fatal(errRead)
		}
		staged := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".auth-write-") {
				staged = true
				break
			}
		}
		if staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not stage its replacement")
		}
		time.Sleep(10 * time.Millisecond)
	}
	displacedParent := filepath.Join(dir, "nested-displaced")
	if errRename := os.Rename(oldParent, displacedParent); errRename != nil {
		t.Fatal(errRename)
	}
	if errMkdir := os.Mkdir(oldParent, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
	if errWrite := os.WriteFile(originalPath, replacement, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errUnlock := unlockTarget(); errUnlock != nil {
		t.Fatal(errUnlock)
	}

	select {
	case errWrite := <-result:
		if errWrite == nil {
			t.Fatal("write succeeded after its opened parent was replaced")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not finish after releasing the target lock")
	}
	got, errRead := os.ReadFile(originalPath)
	if errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("visible auth after parent replacement = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestWriteAuthFileAtomicallyRejectsReplacedRootWhileWaitingForTargetLock(t *testing.T) {
	parentDir := t.TempDir()
	authDir := filepath.Join(parentDir, "auths")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	const fileName = "auth.json"
	originalPath := filepath.Join(authDir, fileName)
	writeAuthSnapshotTestFile(t, originalPath)
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	expected, errExpected := captureAuthFileSnapshot(root, fileName)
	if errExpected != nil {
		t.Fatal(errExpected)
	}
	unlockTarget, errLock := authfileguard.LockRootTarget(root, fileName)
	if errLock != nil {
		t.Fatal(errLock)
	}
	result := make(chan error, 1)
	go func() {
		result <- writeAuthFileAtomicallyAtRoot(root, fileName, []byte("{\"type\":\"codex\",\"access_token\":\"writer\"}"), &expected)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries, errRead := os.ReadDir(authDir)
		if errRead != nil {
			t.Fatal(errRead)
		}
		staged := false
		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), ".auth-write-") {
				staged = true
				break
			}
		}
		if staged {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer did not stage its replacement")
		}
		time.Sleep(10 * time.Millisecond)
	}
	displacedRoot := filepath.Join(parentDir, "auths-displaced")
	if errRename := os.Rename(authDir, displacedRoot); errRename != nil {
		t.Fatal(errRename)
	}
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	replacement := []byte("{\"type\":\"codex\",\"access_token\":\"replacement\"}")
	if errWrite := os.WriteFile(originalPath, replacement, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errUnlock := unlockTarget(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	select {
	case errWrite := <-result:
		if errWrite == nil {
			t.Fatal("write succeeded after its root was replaced")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write did not finish after releasing the target lock")
	}
	got, errRead := os.ReadFile(originalPath)
	if errRead != nil || !bytes.Equal(got, replacement) {
		t.Fatalf("visible auth after root replacement = %s, %v; want %s", got, errRead, replacement)
	}
}

func TestOpenAuthSnapshotFileDoesNotBlockOnNamedPipe(t *testing.T) {
	dir := t.TempDir()
	const fileName = "raced.json"
	if errFIFO := syscall.Mkfifo(filepath.Join(dir, fileName), 0o600); errFIFO != nil {
		t.Skipf("named pipes unavailable: %v", errFIFO)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	result := make(chan error, 1)
	go func() {
		file, errOpen := openAuthSnapshotFile(root, fileName)
		if file != nil {
			errOpen = errors.Join(errOpen, file.Close())
		}
		result <- errOpen
	}()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("auth snapshot open blocked on a named pipe")
	}
}

type unsafeAuthSnapshotCase struct {
	name    string
	wantErr string
	setup   func(t *testing.T, authDir string) string
}

func unsafeAuthSnapshotCases() []unsafeAuthSnapshotCase {
	return []unsafeAuthSnapshotCase{
		{
			name:    "final_symlink_swap",
			wantErr: "symbolic link",
			setup: func(t *testing.T, authDir string) string {
				t.Helper()
				target := filepath.Join(authDir, "target.json")
				path := filepath.Join(authDir, "watched.json")
				writeAuthSnapshotTestFile(t, target)
				writeAuthSnapshotTestFile(t, path)
				if errRemove := os.Remove(path); errRemove != nil {
					t.Fatalf("remove watched auth before symlink swap: %v", errRemove)
				}
				if errLink := os.Symlink(filepath.Base(target), path); errLink != nil {
					t.Fatalf("replace watched auth with symlink: %v", errLink)
				}
				return path
			},
		},
		{
			name:    "intermediate_symlink",
			wantErr: "symbolic link",
			setup: func(t *testing.T, authDir string) string {
				t.Helper()
				targetDir := filepath.Join(authDir, "target")
				path := filepath.Join(targetDir, "watched.json")
				writeAuthSnapshotTestFile(t, path)
				linkDir := filepath.Join(authDir, "linked")
				if errLink := os.Symlink(filepath.Base(targetDir), linkDir); errLink != nil {
					t.Fatalf("create intermediate auth symlink: %v", errLink)
				}
				return filepath.Join(linkDir, filepath.Base(path))
			},
		},
		{
			name:    "non_regular_file",
			wantErr: "not a regular file",
			setup: func(t *testing.T, authDir string) string {
				t.Helper()
				path := filepath.Join(authDir, "watched.json")
				if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
					t.Fatalf("create auth FIFO parent: %v", errMkdir)
				}
				if errFIFO := syscall.Mkfifo(path, 0o600); errFIFO != nil {
					t.Fatalf("create auth FIFO: %v", errFIFO)
				}
				return path
			},
		},
	}
}

func writeAuthSnapshotTestFile(t *testing.T, path string) {
	t.Helper()
	if errMkdir := os.MkdirAll(filepath.Dir(path), 0o700); errMkdir != nil {
		t.Fatalf("create auth snapshot parent: %v", errMkdir)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"local"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth snapshot file: %v", errWrite)
	}
}

func requireUnsafeAuthSnapshotError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("PersistAuthFiles() error = %v, want containing %q", err, want)
	}
}

func TestGitPersistAuthFilesRejectsUnsafeLocalSnapshots(t *testing.T) {
	for _, tt := range unsafeAuthSnapshotCases() {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			remoteDir := setupGitRemoteRepository(t, root, "main", testBranchSpec{name: "main", contents: "remote branch\n"})
			repoDir := filepath.Join(root, "workspace")
			authDir := filepath.Join(repoDir, "auths")
			store := NewGitTokenStore(remoteDir, "", "", "main")
			store.SetBaseDir(authDir)
			if errEnsure := store.EnsureRepository(); errEnsure != nil {
				t.Fatalf("prepare git workspace: %v", errEnsure)
			}

			path := tt.setup(t, authDir)
			errPersist := store.PersistAuthFiles(t.Context(), "persist watched auth", path)
			requireUnsafeAuthSnapshotError(t, errPersist, tt.wantErr)
			assertRemoteBranchFileContents(t, remoteDir, "main", "branch.txt", "remote branch\n")
		})
	}
}

func TestObjectPersistAuthFilesRejectsUnsafeLocalSnapshots(t *testing.T) {
	for _, tt := range unsafeAuthSnapshotCases() {
		t.Run(tt.name, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet && r.URL.RawQuery == "location=" {
					w.Header().Set("Content-Type", "application/xml")
					_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>`))
					return
				}
				if r.Method == http.MethodHead {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				requests.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()

			store := newObjectTokenStoreForServer(t, server.URL)
			path := tt.setup(t, store.AuthDir())
			errPersist := store.PersistAuthFiles(t.Context(), "persist watched auth", path)
			requireUnsafeAuthSnapshotError(t, errPersist, tt.wantErr)
			if gotRequests := requests.Load(); gotRequests != 0 {
				t.Fatalf("object requests = %d, want 0", gotRequests)
			}
		})
	}
}

func TestPostgresPersistAuthFilesRejectsUnsafeLocalSnapshots(t *testing.T) {
	for _, tt := range unsafeAuthSnapshotCases() {
		t.Run(tt.name, func(t *testing.T) {
			backend := &postgresStoreTestBackend{authRecords: make(map[string][]byte)}
			db := newPostgresStoreTestSQLDB(t, backend)
			authDir := t.TempDir()
			store := &PostgresStore{
				db:      db,
				cfg:     PostgresStoreConfig{AuthTable: defaultAuthTable},
				authDir: authDir,
			}

			path := tt.setup(t, authDir)
			errPersist := store.PersistAuthFiles(t.Context(), "persist watched auth", path)
			requireUnsafeAuthSnapshotError(t, errPersist, tt.wantErr)
			if gotCalls := len(backend.execCallsSnapshot()); gotCalls != 1 {
				t.Fatalf("postgres exec calls = %d, want advisory lock only", gotCalls)
			}
		})
	}
}

func TestDeleteAuthFileTransactionDoesNotRestoreThroughRedirectedParent(t *testing.T) {
	baseDir := t.TempDir()
	nestedDir := filepath.Join(baseDir, "nested")
	if errMkdir := os.Mkdir(nestedDir, 0o700); errMkdir != nil {
		t.Fatalf("create nested dir: %v", errMkdir)
	}
	if errWrite := os.WriteFile(filepath.Join(nestedDir, "auth.json"), []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	externalDir := t.TempDir()
	probe := filepath.Join(baseDir, "probe")
	if errSymlink := os.Symlink(externalDir, probe); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if errRemove := os.Remove(probe); errRemove != nil {
		t.Fatalf("remove symlink probe: %v", errRemove)
	}
	root, errRoot := os.OpenRoot(baseDir)
	if errRoot != nil {
		t.Fatalf("OpenRoot() error = %v", errRoot)
	}
	defer root.Close()
	remoteErr := errors.New("remote deletion failed")
	errDelete := deleteAuthFileTransaction(root, filepath.Join("nested", "auth.json"), func(authFileSnapshot) error { return nil }, func() error {
		if errRename := os.Rename(nestedDir, nestedDir+"-old"); errRename != nil {
			t.Fatalf("rename nested dir: %v", errRename)
		}
		if errSymlink := os.Symlink(externalDir, nestedDir); errSymlink != nil {
			t.Fatalf("redirect nested dir: %v", errSymlink)
		}
		return remoteErr
	}, func() (authDeleteProbeState, error) { return authDeleteProbeOriginal, nil })
	if outcome, ok := cliproxyauth.DeleteOutcomeFromError(errDelete); !ok || outcome != cliproxyauth.DeleteOutcomeUncertain {
		t.Fatalf("delete outcome = %v, %t; want uncertain (error %v)", outcome, ok, errDelete)
	}
	if _, errStat := os.Stat(filepath.Join(externalDir, "auth.json")); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("redirect target received restored auth: %v", errStat)
	}
}
