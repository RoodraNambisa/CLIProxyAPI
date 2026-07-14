//go:build windows

package authfileguard

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestNormalizePathKeyFoldsWindowsAliases(t *testing.T) {
	tests := []struct {
		left  string
		right string
	}{
		{left: `C:\AUTH\File.JSON`, right: `c:\auth\file.json`},
		{left: `\\?\C:\auth\file.json`, right: `C:\AUTH\FILE.JSON`},
		{left: `\\?\UNC\Server\Share\auth.json`, right: `\\server\share\AUTH.JSON`},
	}
	for _, test := range tests {
		if left, right := normalizePathKey(test.left), normalizePathKey(test.right); left != right {
			t.Fatalf("normalizePathKey(%q) = %q, normalizePathKey(%q) = %q", test.left, left, test.right, right)
		}
	}
}

func TestWindowsRetiredMarkerCoversFilesystemAliases(t *testing.T) {
	aliasTypes := []struct {
		name   string
		create func(string, string) error
	}{
		{name: "symlink", create: func(alias, target string) error {
			return os.Symlink(target, alias)
		}},
		{name: "junction", create: func(alias, target string) error {
			return exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", alias, target).Run()
		}},
	}
	for _, aliasType := range aliasTypes {
		t.Run(aliasType.name, func(t *testing.T) {
			testWindowsRetiredAliasLifecycle(t, aliasType.create)
		})
	}
}

func testWindowsRetiredAliasLifecycle(t *testing.T, createAlias func(string, string) error) {
	t.Helper()
	realRootA := t.TempDir()
	realRootB := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "auths")
	if errCreate := createAlias(aliasRoot, realRootA); errCreate != nil {
		t.Skipf("filesystem alias is unavailable: %v", errCreate)
	}
	aliasPath := filepath.Join(aliasRoot, "auth.json")
	realPathA := filepath.Join(realRootA, "auth.json")
	realPathB := filepath.Join(realRootB, "auth.json")
	t.Cleanup(func() {
		ClearRetired(aliasPath)
		ClearRetired(realPathA)
		ClearRetired(realPathB)
	})

	if aliasKey, realKey := normalizePathKey(aliasPath), normalizePathKey(realPathA); aliasKey != realKey {
		t.Fatalf("filesystem alias keys differ: alias=%q real=%q", aliasKey, realKey)
	}
	MarkRetired(aliasPath)
	if errRemove := os.Remove(aliasRoot); errRemove != nil {
		t.Fatalf("remove filesystem alias: %v", errRemove)
	}
	if errCreate := createAlias(aliasRoot, realRootB); errCreate != nil {
		t.Fatalf("redirect filesystem alias: %v", errCreate)
	}
	if !IsRetired(aliasPath) {
		t.Fatal("redirected filesystem alias bypassed the retired marker")
	}
	MarkRetired(aliasPath)
	ClearRetired(aliasPath)
	for _, path := range []string{aliasPath, realPathA, realPathB} {
		if IsRetired(path) {
			t.Fatalf("retired marker remained for %q", path)
		}
	}
}
