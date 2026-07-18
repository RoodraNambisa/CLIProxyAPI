//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package authfileguard

import (
	"os"
	"testing"
)

func TestValidatePersistentLockFileIdentityRejectsReplacedPath(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	const lockPath = ".auth-root-lock"
	if errWrite := root.WriteFile(lockPath, nil, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	file, errOpen := root.OpenFile(lockPath, os.O_RDWR, 0)
	if errOpen != nil {
		t.Fatal(errOpen)
	}
	defer file.Close()
	if errRemove := root.Remove(lockPath); errRemove != nil {
		t.Fatal(errRemove)
	}
	if errWrite := root.WriteFile(lockPath, nil, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	if _, errIdentity := validatePersistentLockFileIdentity(root, lockPath, file); errIdentity == nil {
		t.Fatal("replaced persistent lock path passed identity validation")
	}
}
