//go:build darwin || linux

package store

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

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
