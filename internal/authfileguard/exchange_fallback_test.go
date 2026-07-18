package authfileguard

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"slices"
	"testing"
)

func TestExchangeFileByRenameLinksBeforeAtomicInstall(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	originalInfo, errOriginal := root.Lstat("auth.json")
	if errOriginal != nil {
		t.Fatal(errOriginal)
	}
	operations := newExchangeFileOperations(root)
	var events []string
	link := operations.link
	operations.link = func(oldPath, newPath string) error {
		events = append(events, "link")
		assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
		return link(oldPath, newPath)
	}
	syncDirectory := operations.syncDirectory
	syncCalls := 0
	operations.syncDirectory = func() error {
		syncCalls++
		events = append(events, "sync")
		if syncCalls == 1 {
			assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
		} else {
			assertExchangeFallbackFile(t, root, "auth.json", []byte("new"))
		}
		return syncDirectory()
	}
	rename := operations.rename
	operations.rename = func(oldPath, newPath string) error {
		events = append(events, "rename")
		if oldPath != "staged.json" || newPath != "auth.json" {
			t.Fatalf("install rename = %q -> %q", oldPath, newPath)
		}
		assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
		return rename(oldPath, newPath)
	}

	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if errExchange != nil {
		t.Fatal(errExchange)
	}
	if !slices.Equal(events, []string{"link", "sync", "rename", "sync"}) {
		t.Fatalf("exchange events = %v, want link, sync, rename, sync", events)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("new"))
	assertExchangeFallbackFile(t, root, displaced, []byte("old"))
	displacedInfo, errDisplaced := root.Lstat(displaced)
	if errDisplaced != nil || !os.SameFile(originalInfo, displacedInfo) {
		t.Fatalf("displaced generation did not preserve target identity: %v", errDisplaced)
	}
	if _, errStat := root.Lstat("staged.json"); !errors.Is(errStat, fs.ErrNotExist) {
		t.Fatalf("staged generation remains after fallback exchange: %v", errStat)
	}
}

func TestExchangeFileByRenameRestoresAfterInstalledGenerationSyncFailure(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errSync := errors.New("installed generation sync failed")
	operations := newExchangeFileOperations(root)
	syncDirectory := operations.syncDirectory
	syncCalls := 0
	operations.syncDirectory = func() error {
		syncCalls++
		if syncCalls == 2 {
			return errSync
		}
		return syncDirectory()
	}

	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after confirmed restore, want empty", displaced)
	}
	if !errors.Is(errExchange, errSync) {
		t.Fatalf("exchange error = %v, want installed sync failure", errExchange)
	}
	if errors.Is(errExchange, ErrExchangeOutcomeUncertain) || errors.Is(errExchange, ErrExchangeRestoreFailed) {
		t.Fatalf("confirmed restore was classified as uncertain: %v", errExchange)
	}
	if syncCalls != 4 {
		t.Fatalf("sync calls = %d, want 4", syncCalls)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameLeavesCanonicalTargetOnInstallFailure(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errInstall := errors.New("install failed")
	operations := newExchangeFileOperations(root)
	operations.rename = func(string, string) error { return errInstall }
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after cleanup, want empty", displaced)
	}
	if !errors.Is(errExchange, errInstall) {
		t.Fatalf("exchange error = %v, want install failure", errExchange)
	}
	if errors.Is(errExchange, ErrExchangeOutcomeUncertain) || errors.Is(errExchange, ErrExchangeRestoreFailed) {
		t.Fatalf("clean install failure was classified as uncertain: %v", errExchange)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameHandlesUnsupportedHardLinks(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errLink := errors.New("hard links unsupported")
	operations := newExchangeFileOperations(root)
	operations.link = func(string, string) error { return errLink }
	operations.rename = func(string, string) error {
		t.Fatal("rename called after hard-link failure")
		return nil
	}
	operations.syncDirectory = func() error {
		t.Fatal("sync called after hard-link failure")
		return nil
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after unsupported link, want empty", displaced)
	}
	if !errors.Is(errExchange, errLink) {
		t.Fatalf("exchange error = %v, want hard-link error", errExchange)
	}
	if errors.Is(errExchange, ErrExchangeOutcomeUncertain) {
		t.Fatalf("unsupported hard link changed a generation: %v", errExchange)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameContinuesAfterLinkLosesAcknowledgement(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errAcknowledgement := errors.New("link acknowledgement lost")
	operations := newExchangeFileOperations(root)
	link := operations.link
	operations.link = func(oldPath, newPath string) error {
		if errLink := link(oldPath, newPath); errLink != nil {
			return errLink
		}
		return errAcknowledgement
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if errExchange != nil {
		t.Fatalf("confirmed link acknowledgement loss failed exchange: %v", errExchange)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("new"))
	assertExchangeFallbackFile(t, root, displaced, []byte("old"))
}

func TestExchangeFileByRenameRestoresAfterInstallLosesAcknowledgement(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errAcknowledgement := errors.New("install acknowledgement lost")
	operations := newExchangeFileOperations(root)
	rename := operations.rename
	renameCalls := 0
	operations.rename = func(oldPath, newPath string) error {
		renameCalls++
		if errRename := rename(oldPath, newPath); errRename != nil {
			return errRename
		}
		if renameCalls == 1 {
			return errAcknowledgement
		}
		return nil
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after confirmed restore, want empty", displaced)
	}
	if !errors.Is(errExchange, errAcknowledgement) {
		t.Fatalf("exchange error = %v, want lost acknowledgement", errExchange)
	}
	if errors.Is(errExchange, ErrExchangeOutcomeUncertain) || errors.Is(errExchange, ErrExchangeRestoreFailed) {
		t.Fatalf("confirmed restore was classified as uncertain: %v", errExchange)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameMarksRestoreSyncFailureUncertain(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errAcknowledgement := errors.New("install acknowledgement lost")
	errSync := errors.New("restore sync failed")
	operations := newExchangeFileOperations(root)
	rename := operations.rename
	renameCalls := 0
	operations.rename = func(oldPath, newPath string) error {
		renameCalls++
		if errRename := rename(oldPath, newPath); errRename != nil {
			return errRename
		}
		if renameCalls == 1 {
			return errAcknowledgement
		}
		return nil
	}
	syncDirectory := operations.syncDirectory
	syncCalls := 0
	operations.syncDirectory = func() error {
		syncCalls++
		if syncCalls == 3 {
			return errSync
		}
		return syncDirectory()
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after in-memory restore, want empty", displaced)
	}
	for _, wantErr := range []error{errAcknowledgement, errSync, ErrExchangeOutcomeUncertain} {
		if !errors.Is(errExchange, wantErr) {
			t.Errorf("exchange error = %v, want %v", errExchange, wantErr)
		}
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameReportsUncertainWhenRestoreFails(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errInstall := errors.New("install failed")
	errRestore := errors.New("restore failed")
	operations := newExchangeFileOperations(root)
	rename := operations.rename
	operations.rename = func(oldPath, newPath string) error {
		if errRename := rename(oldPath, newPath); errRename != nil {
			return errRename
		}
		return errInstall
	}
	link := operations.link
	linkCalls := 0
	operations.link = func(oldPath, newPath string) error {
		linkCalls++
		if linkCalls == 2 {
			return errRestore
		}
		return link(oldPath, newPath)
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced == "" {
		t.Fatal("uncertain exchange did not return the displaced original path")
	}
	for _, wantErr := range []error{errInstall, errRestore, ErrExchangeOutcomeUncertain, ErrExchangeRestoreFailed} {
		if !errors.Is(errExchange, wantErr) {
			t.Errorf("exchange error = %v, want %v", errExchange, wantErr)
		}
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("new"))
	assertExchangeFallbackFile(t, root, displaced, []byte("old"))
}

func TestExchangeFileByRenameRollsBackBackupSyncFailure(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errSync := errors.New("directory sync failed")
	operations := newExchangeFileOperations(root)
	syncDirectory := operations.syncDirectory
	syncCalls := 0
	operations.syncDirectory = func() error {
		syncCalls++
		if syncCalls == 1 {
			return errSync
		}
		return syncDirectory()
	}
	operations.rename = func(string, string) error {
		t.Fatal("rename called after backup sync failure")
		return nil
	}
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced != "" {
		t.Fatalf("displaced path = %q after synced cleanup, want empty", displaced)
	}
	if !errors.Is(errExchange, errSync) {
		t.Fatalf("exchange error = %v, want sync failure", errExchange)
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
}

func TestExchangeFileByRenameClassifiesBackupCleanupFailure(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	writeExchangeFallbackGenerations(t, root)
	errInstall := errors.New("install failed")
	errCleanup := errors.New("cleanup failed")
	operations := newExchangeFileOperations(root)
	operations.rename = func(string, string) error { return errInstall }
	operations.remove = func(string) error { return errCleanup }
	displaced, errExchange := exchangeFileByRenameWith(root, "staged.json", "auth.json", operations)
	if displaced == "" {
		t.Fatal("cleanup failure did not return displaced path")
	}
	for _, wantErr := range []error{errInstall, errCleanup, ErrExchangeCleanupRequired} {
		if !errors.Is(errExchange, wantErr) {
			t.Errorf("exchange error = %v, want %v", errExchange, wantErr)
		}
	}
	assertExchangeFallbackFile(t, root, "auth.json", []byte("old"))
	assertExchangeFallbackFile(t, root, "staged.json", []byte("new"))
	assertExchangeFallbackFile(t, root, displaced, []byte("old"))
}

func writeExchangeFallbackGenerations(t *testing.T, root *os.Root) {
	t.Helper()
	if errWrite := root.WriteFile("auth.json", []byte("old"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := root.WriteFile("staged.json", []byte("new"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func assertExchangeFallbackFile(t *testing.T, root *os.Root, name string, want []byte) {
	t.Helper()
	got, errRead := root.ReadFile(name)
	if errRead != nil || !bytes.Equal(got, want) {
		t.Fatalf("%s = %q, %v; want %q", name, got, errRead, want)
	}
}
