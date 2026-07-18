//go:build aix

package authfileguard

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAIXSerializesRootMutationLocks(t *testing.T) {
	if !serializeRootMutationLocks {
		t.Fatal("AIX root mutations must use exclusive record locks")
	}
	processExclusive, fileExclusive := rootWriterTurnstileModes(false)
	if !processExclusive || !fileExclusive {
		t.Fatalf("AIX mutation turnstile modes = process %v, file %v; want both exclusive", processExclusive, fileExclusive)
	}
}

func TestAIXPersistentLockIdentitySurvivesWorkingDirectoryChange(t *testing.T) {
	originalWorkingDirectory, errWorkingDirectory := os.Getwd()
	if errWorkingDirectory != nil {
		t.Fatal(errWorkingDirectory)
	}
	t.Cleanup(func() {
		if errChdir := os.Chdir(originalWorkingDirectory); errChdir != nil {
			t.Errorf("restore working directory: %v", errChdir)
		}
	})

	base := t.TempDir()
	authDir := filepath.Join(base, "auths")
	otherDir := filepath.Join(base, "other")
	if errMkdir := os.Mkdir(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errMkdir := os.Mkdir(otherDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errChdir := os.Chdir(base); errChdir != nil {
		t.Fatal(errChdir)
	}
	root, errRoot := os.OpenRoot("auths")
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	first, errFirst := RootTargetLockIdentity(root, "auth.json")
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	if errChdir := os.Chdir(otherDir); errChdir != nil {
		t.Fatal(errChdir)
	}
	second, errSecond := RootTargetLockIdentity(root, "auth.json")
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if first != second {
		t.Fatalf("persistent lock identity changed: %q != %q", first, second)
	}
}
