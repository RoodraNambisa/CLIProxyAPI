//go:build windows

package authfileguard

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPinWindowsRootPreventsDirectoryRename(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		target string
	}{
		{name: "auth directory", target: "outer/auths"},
		{name: "ancestor directory", target: "outer"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			parent := t.TempDir()
			ancestor := filepath.Join(parent, "outer")
			dir := filepath.Join(ancestor, "auths")
			if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
				t.Fatal(errMkdir)
			}
			root, errRoot := os.OpenRoot(dir)
			if errRoot != nil {
				t.Fatal(errRoot)
			}
			defer root.Close()

			_, pinned, errPin := pinWindowsRootPath(root)
			if errPin != nil {
				t.Fatal(errPin)
			}
			target := filepath.Join(parent, filepath.FromSlash(testCase.target))
			moved := filepath.Join(parent, "moved")
			if errRename := os.Rename(target, moved); errRename == nil {
				_ = os.Rename(moved, target)
				_ = pinned.Close()
				t.Fatalf("pinned Windows %s was renamed", testCase.name)
			}
			if errClose := pinned.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			if errRename := os.Rename(target, moved); errRename != nil {
				t.Fatalf("rename released Windows %s: %v", testCase.name, errRename)
			}
			if errRename := os.Rename(moved, target); errRename != nil {
				t.Fatalf("restore Windows %s: %v", testCase.name, errRename)
			}
		})
	}
}

func TestLockRootTargetNormalizesWindowsPathCase(t *testing.T) {
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

	unlockFirst, errFirst := LockRootTarget(firstRoot, "Auth.JSON")
	if errFirst != nil {
		t.Fatalf("lock first target: %v", errFirst)
	}
	acquired := make(chan func() error, 1)
	errs := make(chan error, 1)
	go func() {
		unlockSecond, errSecond := LockRootTarget(secondRoot, "auth.json")
		if errSecond != nil {
			errs <- errSecond
			return
		}
		acquired <- unlockSecond
	}()
	select {
	case unlockSecond := <-acquired:
		_ = unlockSecond()
		_ = unlockFirst()
		t.Fatal("case-variant target lock acquired before first lock was released")
	case errSecond := <-errs:
		_ = unlockFirst()
		t.Fatalf("lock case-variant target: %v", errSecond)
	case <-time.After(100 * time.Millisecond):
	}
	if errUnlock := unlockFirst(); errUnlock != nil {
		t.Fatalf("unlock first target: %v", errUnlock)
	}
	select {
	case unlockSecond := <-acquired:
		if errUnlock := unlockSecond(); errUnlock != nil {
			t.Fatalf("unlock second target: %v", errUnlock)
		}
	case errSecond := <-errs:
		t.Fatalf("lock case-variant target: %v", errSecond)
	case <-time.After(5 * time.Second):
		t.Fatal("case-variant target lock did not proceed after release")
	}
}
