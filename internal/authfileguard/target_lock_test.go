//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd || windows

package authfileguard

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestInstallStagedFileNoReplaceFallsBackWhenHardLinksAreUnavailable(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	if errWrite := root.WriteFile("staged.json", []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	cleanupWarning, errInstall := installStagedFileNoReplace(
		root,
		"staged.json",
		"auth.json",
		func(*os.Root, string, string) error { return fs.ErrInvalid },
	)
	if errInstall != nil || cleanupWarning != nil {
		t.Fatalf("install = cleanup %v, error %v", cleanupWarning, errInstall)
	}
	data, errRead := root.ReadFile("auth.json")
	if errRead != nil || string(data) != `{"type":"codex"}` {
		t.Fatalf("installed auth = %q, %v", data, errRead)
	}
	if _, errStat := root.Lstat("staged.json"); !errors.Is(errStat, fs.ErrNotExist) {
		t.Fatalf("staged file remains: %v", errStat)
	}
}

func TestIsPersistentLockPath(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: ".auth-root-lock", want: true},
		{path: ".auth-lock-root-turnstile", want: true},
		{path: "nested/.auth-root-lock", want: true},
		{path: "nested/.auth-lock-0123456789abcdef0123456789abcdef", want: true},
		{path: "nested/.auth-lock-short", want: false},
		{path: "nested/auth.json", want: false},
	}
	for _, test := range tests {
		if got := IsPersistentLockPath(test.path); got != test.want {
			t.Errorf("IsPersistentLockPath(%q) = %t, want %t", test.path, got, test.want)
		}
	}
}

func TestNormalizeTargetLockKeyFoldsUnicodeAliases(t *testing.T) {
	composed := normalizeTargetLockKey("CAF\u00c9.JSON", true)
	decomposed := normalizeTargetLockKey("cafe\u0301.json", true)
	if composed != decomposed {
		t.Fatalf("case-insensitive lock keys differ: %q != %q", composed, decomposed)
	}
	if normalizeTargetLockKey("CAF\u00c9.JSON", false) == normalizeTargetLockKey("cafe\u0301.json", false) {
		t.Fatal("case-sensitive lock keys were unexpectedly folded")
	}
}

func TestInstallStagedFileNoReplaceFallbackDoesNotReplaceExistingTarget(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	if errWrite := root.WriteFile("staged.json", []byte(`{"value":"new"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := root.WriteFile("auth.json", []byte(`{"value":"old"}`), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	_, errInstall := installStagedFileNoReplace(
		root,
		"staged.json",
		"auth.json",
		func(*os.Root, string, string) error { return fs.ErrInvalid },
	)
	if !errors.Is(errInstall, ErrPersistGenerationStale) {
		t.Fatalf("install error = %v, want stale generation", errInstall)
	}
	data, errRead := root.ReadFile("auth.json")
	if errRead != nil || string(data) != `{"value":"old"}` {
		t.Fatalf("existing auth = %q, %v", data, errRead)
	}
}

func TestInstallStagedFileNoReplacePreservesLinkFailureWhenFallbackFails(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	errLink := errors.New("hard link failed")
	_, errInstall := installStagedFileNoReplace(
		root,
		"missing-staged.json",
		"auth.json",
		func(*os.Root, string, string) error { return errLink },
	)
	if !errors.Is(errInstall, errLink) {
		t.Fatalf("install error = %v, want original hard-link failure", errInstall)
	}
}

func TestExchangeStagedFilePreservesBothGenerations(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()
	const stagedName = ".auth-staged"
	const targetName = "auth.json"
	oldData := []byte("old")
	newData := []byte("new")
	if errWrite := root.WriteFile(targetName, oldData, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := root.WriteFile(stagedName, newData, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	displaced, errExchange := ExchangeStagedFile(root, stagedName, targetName)
	if errors.Is(errExchange, ErrAtomicExchangeUnsupported) {
		t.Skipf("atomic exchange is unavailable on this filesystem: %v", errExchange)
	}
	if errExchange != nil {
		t.Fatal(errExchange)
	}
	if got, errRead := root.ReadFile(targetName); errRead != nil || !bytes.Equal(got, newData) {
		t.Fatalf("target after exchange = %q, %v; want %q", got, errRead, newData)
	}
	if got, errRead := root.ReadFile(displaced); errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("displaced after exchange = %q, %v; want %q", got, errRead, oldData)
	}
	displacedNew, errRestore := ExchangeStagedFile(root, displaced, targetName)
	if errRestore != nil {
		t.Fatal(errRestore)
	}
	if got, errRead := root.ReadFile(targetName); errRead != nil || !bytes.Equal(got, oldData) {
		t.Fatalf("target after restore = %q, %v; want %q", got, errRead, oldData)
	}
	if got, errRead := root.ReadFile(displacedNew); errRead != nil || !bytes.Equal(got, newData) {
		t.Fatalf("displaced new generation = %q, %v; want %q", got, errRead, newData)
	}
}

func TestLockRootTargetConcurrentFirstCreate(t *testing.T) {
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

	type lockResult struct {
		worker int
		unlock func() error
		err    error
	}
	for iteration := 0; iteration < 200; iteration++ {
		target := fmt.Sprintf("first-create-%d.json", iteration)
		start := make(chan struct{})
		release := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
		results := make(chan lockResult, 2)
		unlockErrors := make(chan lockResult, 2)
		var workers sync.WaitGroup
		workers.Add(2)
		for worker, root := range []*os.Root{firstRoot, secondRoot} {
			go func(worker int, root *os.Root) {
				defer workers.Done()
				<-start
				unlock, errLock := LockRootTarget(root, target)
				results <- lockResult{worker: worker, unlock: unlock, err: errLock}
				if errLock == nil {
					<-release[worker]
					if errUnlock := unlock(); errUnlock != nil {
						unlockErrors <- lockResult{worker: worker, err: errUnlock}
					}
				}
			}(worker, root)
		}
		close(start)

		first := <-results
		if first.err != nil {
			close(release[0])
			close(release[1])
			workers.Wait()
			t.Fatalf("iteration %d first lock error: %v", iteration, first.err)
		}
		select {
		case second := <-results:
			close(release[0])
			close(release[1])
			workers.Wait()
			t.Fatalf("iteration %d second worker %d acquired before worker %d released: %v", iteration, second.worker, first.worker, second.err)
		case <-time.After(time.Millisecond):
		}
		close(release[first.worker])
		second := <-results
		if second.err != nil {
			close(release[1-first.worker])
			workers.Wait()
			t.Fatalf("iteration %d second lock error: %v", iteration, second.err)
		}
		close(release[second.worker])
		workers.Wait()
		select {
		case unexpected := <-unlockErrors:
			t.Fatalf("iteration %d unlock error from worker %d: %v", iteration, unexpected.worker, unexpected.err)
		default:
		}
	}
}

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

func TestRootMutationLocksShareAndBlockRebuild(t *testing.T) {
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
	rebuildRoot, errRebuildRoot := os.OpenRoot(dir)
	if errRebuildRoot != nil {
		t.Fatalf("open rebuild root: %v", errRebuildRoot)
	}
	defer rebuildRoot.Close()

	unlockFirst, errFirst := LockRootMutation(firstRoot)
	if errFirst != nil {
		t.Fatalf("lock first mutation: %v", errFirst)
	}
	firstLocked := true
	defer func() {
		if firstLocked {
			_ = unlockFirst()
		}
	}()
	unlockSecond, errSecond := LockRootMutation(secondRoot)
	if errSecond != nil {
		t.Fatalf("lock second mutation: %v", errSecond)
	}
	secondLocked := true
	defer func() {
		if secondLocked {
			_ = unlockSecond()
		}
	}()

	acquired := make(chan func() error, 1)
	errorsCh := make(chan error, 1)
	go func() {
		unlockRebuild, errRebuild := LockRootRebuild(rebuildRoot)
		if errRebuild != nil {
			errorsCh <- errRebuild
			return
		}
		acquired <- unlockRebuild
	}()
	select {
	case unlockRebuild := <-acquired:
		_ = unlockRebuild()
		t.Fatal("rebuild acquired while mutation locks were held")
	case errRebuild := <-errorsCh:
		t.Fatalf("lock rebuild: %v", errRebuild)
	case <-time.After(100 * time.Millisecond):
	}
	if errUnlock := unlockFirst(); errUnlock != nil {
		t.Fatalf("unlock first mutation: %v", errUnlock)
	}
	firstLocked = false
	select {
	case unlockRebuild := <-acquired:
		_ = unlockRebuild()
		t.Fatal("rebuild acquired while second mutation lock was held")
	case errRebuild := <-errorsCh:
		t.Fatalf("lock rebuild: %v", errRebuild)
	case <-time.After(100 * time.Millisecond):
	}
	if errUnlock := unlockSecond(); errUnlock != nil {
		t.Fatalf("unlock second mutation: %v", errUnlock)
	}
	secondLocked = false
	select {
	case unlockRebuild := <-acquired:
		if errUnlock := unlockRebuild(); errUnlock != nil {
			t.Fatalf("unlock rebuild: %v", errUnlock)
		}
	case errRebuild := <-errorsCh:
		t.Fatalf("lock rebuild: %v", errRebuild)
	case <-time.After(5 * time.Second):
		t.Fatal("rebuild did not acquire after mutation locks were released")
	}
}

func TestRootRebuildKeepsSameProcessWriterPriority(t *testing.T) {
	dir := t.TempDir()
	initialRoot, errInitialRoot := os.OpenRoot(dir)
	if errInitialRoot != nil {
		t.Fatal(errInitialRoot)
	}
	defer initialRoot.Close()
	rebuildRoot, errRebuildRoot := os.OpenRoot(dir)
	if errRebuildRoot != nil {
		t.Fatal(errRebuildRoot)
	}
	defer rebuildRoot.Close()
	lateRoot, errLateRoot := os.OpenRoot(dir)
	if errLateRoot != nil {
		t.Fatal(errLateRoot)
	}
	defer lateRoot.Close()

	unlockInitial, errInitial := LockRootMutation(initialRoot)
	if errInitial != nil {
		t.Fatal(errInitial)
	}
	initialLocked := true
	defer func() {
		if initialLocked {
			_ = unlockInitial()
		}
	}()

	type lockResult struct {
		unlock func() error
		err    error
	}
	rebuildResult := make(chan lockResult, 1)
	go func() {
		unlock, errLock := LockRootRebuild(rebuildRoot)
		rebuildResult <- lockResult{unlock: unlock, err: errLock}
	}()
	waitForPersistentProcessWriter(t, rebuildRoot, rootWriterTurnstileFileName)

	lateResult := make(chan lockResult, 1)
	go func() {
		unlock, errLock := LockRootMutation(lateRoot)
		lateResult <- lockResult{unlock: unlock, err: errLock}
	}()
	waitForPersistentProcessReferences(t, lateRoot, rootWriterTurnstileFileName, 2)
	if errUnlock := unlockInitial(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	initialLocked = false

	var unlockRebuild func() error
	select {
	case result := <-rebuildResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		unlockRebuild = result.unlock
	case result := <-lateResult:
		if result.unlock != nil {
			_ = result.unlock()
		}
		t.Fatalf("late mutation acquired before waiting rebuild: %v", result.err)
	case <-time.After(5 * time.Second):
		t.Fatal("waiting rebuild did not acquire")
	}
	select {
	case result := <-lateResult:
		if result.unlock != nil {
			_ = result.unlock()
		}
		t.Fatalf("late mutation acquired while rebuild was held: %v", result.err)
	case <-time.After(100 * time.Millisecond):
	}
	if errUnlock := unlockRebuild(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	select {
	case result := <-lateResult:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if errUnlock := result.unlock(); errUnlock != nil {
			t.Fatal(errUnlock)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late mutation did not acquire after rebuild released")
	}
}

func TestIsPersistentLockFileName(t *testing.T) {
	tests := map[string]bool{
		".auth-root-lock":                             true,
		".auth-lock-root-turnstile":                   true,
		".auth-lock-0123456789abcdef0123456789abcdef": true,
		".auth-lock-0123456789ABCDEF0123456789ABCDEF": runtime.GOOS == "darwin" || runtime.GOOS == "windows",
		".auth-lock-0123456789abcdef":                 false,
		".auth-lock-old.json":                         false,
		".auth-root-lock.json":                        false,
		"auth.json":                                   false,
	}
	for name, want := range tests {
		if got := IsPersistentLockFileName(name); got != want {
			t.Errorf("IsPersistentLockFileName(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestTargetLockDoesNotCreateRootWriterTurnstile(t *testing.T) {
	root, errRoot := os.OpenRoot(t.TempDir())
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	unlock, errLock := LockRootTarget(root, "auth.json")
	if errLock != nil {
		t.Fatal(errLock)
	}
	if errUnlock := unlock(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	if _, errStat := root.Lstat(rootWriterTurnstileFileName); !errors.Is(errStat, fs.ErrNotExist) {
		t.Fatalf("target lock created root writer turnstile: %v", errStat)
	}
}

func TestIsPersistentLockFileNameNormalizesCaseInsensitiveAliases(t *testing.T) {
	for _, name := range []string{
		".AUTH-ROOT-LOCK",
		".AUTH-LOCK-ROOT-TURNSTILE",
		".AUTH-LOCK-0123456789ABCDEF0123456789ABCDEF",
	} {
		if !isPersistentLockFileName(name, true) {
			t.Fatalf("case-insensitive lock alias %q was not reserved", name)
		}
		if isPersistentLockFileName(name, false) {
			t.Fatalf("case-sensitive platform unexpectedly reserved alias %q", name)
		}
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

func TestLockRootRebuildRejectsSymlinkLockFile(t *testing.T) {
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
	if errLink := os.Symlink(outside, filepath.Join(dir, rootLockFileName)); errLink != nil {
		t.Skipf("symlink not supported: %v", errLink)
	}
	if unlock, errLock := LockRootRebuild(root); errLock == nil {
		_ = unlock()
		t.Fatal("LockRootRebuild() accepted symlink lock file")
	}
}
