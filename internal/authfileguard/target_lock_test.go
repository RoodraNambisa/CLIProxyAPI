//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows

package authfileguard

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLockRootTargetSerializesIndependentRoots(t *testing.T) {
	dir := t.TempDir()
	firstRoot, errFirstRoot := os.OpenRoot(dir)
	if errFirstRoot != nil {
		t.Fatalf("open first root: %v", errFirstRoot)
	}
	defer firstRoot.Close()
	secondRoot, errSecondRoot := os.OpenRoot(dir)
	if errSecondRoot != nil {
		t.Fatalf("open second root: %v", errSecondRoot)
	}
	defer secondRoot.Close()
	unlockFirst, errFirst := LockRootTarget(firstRoot, "auth.json")
	if errFirst != nil {
		t.Fatalf("lock first target: %v", errFirst)
	}
	locked := true
	defer func() {
		if locked {
			_ = unlockFirst()
		}
	}()

	acquired := make(chan func() error, 1)
	errorsCh := make(chan error, 1)
	go func() {
		unlockSecond, errSecond := LockRootTarget(secondRoot, "auth.json")
		if errSecond != nil {
			errorsCh <- errSecond
			return
		}
		acquired <- unlockSecond
	}()
	select {
	case unlockSecond := <-acquired:
		_ = unlockSecond()
		t.Fatal("second root acquired target lock before first unlock")
	case errSecond := <-errorsCh:
		t.Fatalf("lock second target: %v", errSecond)
	case <-time.After(100 * time.Millisecond):
	}
	if errUnlock := unlockFirst(); errUnlock != nil {
		t.Fatalf("unlock first target: %v", errUnlock)
	}
	locked = false
	select {
	case unlockSecond := <-acquired:
		if errUnlock := unlockSecond(); errUnlock != nil {
			t.Fatalf("unlock second target: %v", errUnlock)
		}
	case errSecond := <-errorsCh:
		t.Fatalf("lock second target: %v", errSecond)
	case <-time.After(5 * time.Second):
		t.Fatal("second root did not acquire target lock")
	}
}

func TestLockRootTargetRejectsSymlinkLockFile(t *testing.T) {
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatalf("open root: %v", errRoot)
	}
	defer root.Close()
	outside := filepath.Join(t.TempDir(), "outside")
	if errWrite := os.WriteFile(outside, []byte("unchanged"), 0o600); errWrite != nil {
		t.Fatalf("write outside lock target: %v", errWrite)
	}
	digest := sha256.Sum256([]byte("auth.json"))
	lockName := fmt.Sprintf(".auth-lock-%x", digest[:16])
	if errLink := os.Symlink(outside, filepath.Join(dir, lockName)); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	if unlock, errLock := LockRootTarget(root, "auth.json"); errLock == nil {
		_ = unlock()
		t.Fatal("LockRootTarget() accepted symlink lock file")
	}
}
