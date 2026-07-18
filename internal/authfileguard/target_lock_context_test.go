//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris || windows

package authfileguard

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestLockRootTargetContextStopsWaitingAfterCancellation(t *testing.T) {
	dir := t.TempDir()
	firstRoot, errFirstRoot := os.OpenRoot(dir)
	if errFirstRoot != nil {
		t.Fatal(errFirstRoot)
	}
	defer firstRoot.Close()
	secondRoot, errSecondRoot := os.OpenRoot(dir)
	if errSecondRoot != nil {
		t.Fatal(errSecondRoot)
	}
	defer secondRoot.Close()

	unlockFirst, errFirst := LockRootTarget(firstRoot, "auth.json")
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	defer unlockFirst()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	unlockSecond, errSecond := LockRootTargetContext(ctx, secondRoot, "auth.json")
	if unlockSecond != nil {
		_ = unlockSecond()
		t.Fatal("canceled target lock returned an unlock function")
	}
	if !errors.Is(errSecond, context.DeadlineExceeded) {
		t.Fatalf("target lock error = %v, want deadline exceeded", errSecond)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("target lock cancellation took %s", elapsed)
	}
}

func TestLockContextStopsWaitingAfterCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	unlock := Lock(path)
	defer unlock()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	startedAt := time.Now()
	secondUnlock, errLock := LockContext(ctx, path)
	if secondUnlock != nil {
		secondUnlock()
		t.Fatal("LockContext() returned an unlock function after cancellation")
	}
	if !errors.Is(errLock, context.DeadlineExceeded) {
		t.Fatalf("LockContext() error = %v, want deadline exceeded", errLock)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("path lock cancellation took %s", elapsed)
	}
}

func TestLockRootRebuildContextStopsWaitingForMutation(t *testing.T) {
	dir := t.TempDir()
	mutationRoot, errMutationRoot := os.OpenRoot(dir)
	if errMutationRoot != nil {
		t.Fatal(errMutationRoot)
	}
	defer mutationRoot.Close()
	rebuildRoot, errRebuildRoot := os.OpenRoot(dir)
	if errRebuildRoot != nil {
		t.Fatal(errRebuildRoot)
	}
	defer rebuildRoot.Close()

	unlockMutation, errMutation := LockRootMutation(mutationRoot)
	if errMutation != nil {
		t.Fatal(errMutation)
	}
	defer unlockMutation()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	unlockRebuild, errRebuild := LockRootRebuildContext(ctx, rebuildRoot)
	if unlockRebuild != nil {
		_ = unlockRebuild()
		t.Fatal("canceled rebuild lock returned an unlock function")
	}
	if !errors.Is(errRebuild, context.Canceled) {
		t.Fatalf("rebuild lock error = %v, want context canceled", errRebuild)
	}
}

func TestLockRootRebuildContextCancellationReleasesWriterTurnstile(t *testing.T) {
	dir := t.TempDir()
	mutationRoot, errMutationRoot := os.OpenRoot(dir)
	if errMutationRoot != nil {
		t.Fatal(errMutationRoot)
	}
	defer mutationRoot.Close()
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

	unlockMutation, errMutation := LockRootMutation(mutationRoot)
	if errMutation != nil {
		t.Fatal(errMutation)
	}
	mutationLocked := true
	defer func() {
		if mutationLocked {
			_ = unlockMutation()
		}
	}()

	ctx, cancel := context.WithCancel(t.Context())
	type lockResult struct {
		unlock func() error
		err    error
	}
	result := make(chan lockResult, 1)
	go func() {
		unlock, errLock := LockRootRebuildContext(ctx, rebuildRoot)
		result <- lockResult{unlock: unlock, err: errLock}
	}()
	waitForPersistentProcessWriter(t, rebuildRoot, rootWriterTurnstileFileName)
	cancel()
	select {
	case canceled := <-result:
		if canceled.unlock != nil {
			_ = canceled.unlock()
			t.Fatal("canceled rebuild returned an unlock function")
		}
		if !errors.Is(canceled.err, context.Canceled) {
			t.Fatalf("rebuild error = %v, want context canceled", canceled.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("rebuild did not stop after cancellation")
	}
	if errUnlock := unlockMutation(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
	mutationLocked = false

	lateCtx, cancelLate := context.WithTimeout(t.Context(), time.Second)
	defer cancelLate()
	unlockLate, errLate := LockRootMutationContext(lateCtx, lateRoot)
	if errLate != nil {
		t.Fatalf("mutation remained blocked by canceled writer: %v", errLate)
	}
	if errUnlock := unlockLate(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
}

func TestLockRootTargetContextDoesNotRequireTargetParent(t *testing.T) {
	dir := t.TempDir()
	firstRoot, errFirstRoot := os.OpenRoot(dir)
	if errFirstRoot != nil {
		t.Fatal(errFirstRoot)
	}
	defer firstRoot.Close()
	secondRoot, errSecondRoot := os.OpenRoot(dir)
	if errSecondRoot != nil {
		t.Fatal(errSecondRoot)
	}
	defer secondRoot.Close()

	unlockNested, errNested := LockRootTarget(firstRoot, "missing/auth.json")
	if errNested != nil {
		t.Fatal(errNested)
	}
	defer unlockNested()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	unlockLeaf, errLeaf := LockRootTargetContext(ctx, secondRoot, "missing/auth.json")
	if unlockLeaf != nil {
		_ = unlockLeaf()
		t.Fatal("equivalent missing-parent lock returned an unlock function")
	}
	if !errors.Is(errLeaf, context.DeadlineExceeded) {
		t.Fatalf("equivalent missing-parent lock error = %v, want deadline exceeded", errLeaf)
	}
}

func TestLockRootTargetContextDoesNotSerializeDifferentRelativePaths(t *testing.T) {
	dir := t.TempDir()
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	unlockFirst, errFirst := LockRootTarget(root, filepath.Join("first", "auth.json"))
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	defer unlockFirst()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	unlockSecond, errSecond := LockRootTargetContext(ctx, root, filepath.Join("second", "auth.json"))
	if errSecond != nil {
		t.Fatalf("unrelated target lock blocked: %v", errSecond)
	}
	if errUnlock := unlockSecond(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
}

func TestLockRootTargetContextKeepsLexicalSymlinkPathsDistinct(t *testing.T) {
	dir := t.TempDir()
	if errMkdir := os.Mkdir(filepath.Join(dir, "real"), 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errSymlink := os.Symlink("real", filepath.Join(dir, "alias")); errSymlink != nil {
		t.Skipf("directory symlink unavailable: %v", errSymlink)
	}
	root, errRoot := os.OpenRoot(dir)
	if errRoot != nil {
		t.Fatal(errRoot)
	}
	defer root.Close()

	unlockReal, errReal := LockRootTarget(root, filepath.Join("real", "auth.json"))
	if errReal != nil {
		t.Fatal(errReal)
	}
	defer unlockReal()

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	unlockAlias, errAlias := LockRootTargetContext(ctx, root, filepath.Join("alias", "auth.json"))
	if errAlias != nil {
		t.Fatalf("lexically distinct alias lock blocked: %v", errAlias)
	}
	if errUnlock := unlockAlias(); errUnlock != nil {
		t.Fatal(errUnlock)
	}
}

func TestLockRootTargetContextNormalizesDarwinPathCase(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("Darwin case normalization only")
	}
	dir := t.TempDir()
	firstRoot, errFirstRoot := os.OpenRoot(dir)
	if errFirstRoot != nil {
		t.Fatal(errFirstRoot)
	}
	defer firstRoot.Close()
	secondRoot, errSecondRoot := os.OpenRoot(dir)
	if errSecondRoot != nil {
		t.Fatal(errSecondRoot)
	}
	defer secondRoot.Close()

	unlockFirst, errFirst := LockRootTarget(firstRoot, "Auth.JSON")
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	defer unlockFirst()

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	unlockSecond, errSecond := LockRootTargetContext(ctx, secondRoot, "auth.json")
	if unlockSecond != nil {
		_ = unlockSecond()
		t.Fatal("case-variant target lock returned an unlock function")
	}
	if !errors.Is(errSecond, context.DeadlineExceeded) {
		t.Fatalf("case-variant target lock error = %v, want deadline exceeded", errSecond)
	}
}
