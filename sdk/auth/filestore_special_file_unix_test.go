//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package auth

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"golang.org/x/sys/unix"
)

func TestOpenFileTokenSnapshotFileDoesNotBlockOnNamedPipe(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "raced.json"
	if errFIFO := unix.Mkfifo(filepath.Join(authDir, fileName), 0o600); errFIFO != nil {
		t.Skipf("named pipes unavailable: %v", errFIFO)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	result := make(chan error, 1)
	go func() {
		file, errOpen := openFileTokenSnapshotFile(root, fileName)
		if file != nil {
			errOpen = errors.Join(errOpen, file.Close())
		}
		result <- errOpen
	}()
	select {
	case <-result:
	case <-time.After(time.Second):
		t.Fatal("snapshot open blocked on a named pipe")
	}
}

func TestCaptureFileAuthSnapshotDoesNotBlockOnNamedPipe(t *testing.T) {
	authDir := t.TempDir()
	const fileName = "delete-raced.json"
	if errFIFO := unix.Mkfifo(filepath.Join(authDir, fileName), 0o600); errFIFO != nil {
		t.Skipf("named pipes unavailable: %v", errFIFO)
	}
	root, errRoot := os.OpenRoot(authDir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	result := make(chan error, 1)
	go func() {
		_, errSnapshot := captureFileAuthSnapshot(root, fileName)
		result <- errSnapshot
	}()
	select {
	case errSnapshot := <-result:
		if errSnapshot == nil {
			t.Fatal("captureFileAuthSnapshot() accepted a named pipe")
		}
	case <-time.After(time.Second):
		t.Fatal("captureFileAuthSnapshot() blocked on a named pipe")
	}
}

func TestFileTokenStoreEmailScanSkipsNamedPipes(t *testing.T) {
	authDir := t.TempDir()
	if errFIFO := unix.Mkfifo(filepath.Join(authDir, "blocked.json"), 0o600); errFIFO != nil {
		t.Skipf("named pipes unavailable: %v", errFIFO)
	}
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	if auths, errList := store.List(t.Context()); errList != nil || len(auths) != 0 {
		t.Fatalf("List() = %d auths, %v", len(auths), errList)
	}
	if _, errSave := store.SaveIfAbsent(t.Context(), &cliproxyauth.Auth{
		ID:       "account.json",
		FileName: "account.json",
		Provider: "chatgpt-web",
		Metadata: map[string]any{"type": "chatgpt-web", "email": "account@example.com"},
	}); errSave != nil {
		t.Fatalf("SaveIfAbsent() error = %v", errSave)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, "account.json")); errStat != nil {
		t.Fatalf("saved auth missing: %v", errStat)
	}
}
