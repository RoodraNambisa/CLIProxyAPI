//go:build !windows

package authfileguard

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestNormalizePathKeyResolvesSymlinkRootAliasForMissingFile(t *testing.T) {
	realRoot := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realRoot, aliasRoot); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}

	realPath := filepath.Join(realRoot, "missing", "auth.json")
	aliasPath := filepath.Join(aliasRoot, "missing", "auth.json")
	if realKey, aliasKey := normalizePathKey(realPath), normalizePathKey(aliasPath); realKey != aliasKey {
		t.Fatalf("symlink root alias keys differ: real=%q alias=%q", realKey, aliasKey)
	}

	ClearRetired(realPath)
	t.Cleanup(func() {
		ClearRetired(realPath)
		ClearRetired(aliasPath)
	})
	MarkRetired(aliasPath)
	if !IsRetired(realPath) {
		t.Fatal("retired marker was not shared across the symlink root alias")
	}
}

func TestLockSerializesLexicalAndResolvedPathKeys(t *testing.T) {
	realRoot := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realRoot, aliasRoot); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	aliasPath := filepath.Join(aliasRoot, "auth.json")
	realPath := filepath.Join(realRoot, "auth.json")

	unlockAlias := Lock(aliasPath)
	acquired := make(chan func(), 1)
	go func() {
		acquired <- Lock(realPath)
	}()
	select {
	case unlockReal := <-acquired:
		unlockReal()
		unlockAlias()
		t.Fatal("resolved path lock did not wait for the symlink alias lock")
	case <-time.After(100 * time.Millisecond):
	}
	unlockAlias()
	select {
	case unlockReal := <-acquired:
		unlockReal()
	case <-time.After(5 * time.Second):
		t.Fatal("resolved path lock remained blocked after alias unlock")
	}
}

func TestLockRevalidatesRedirectedSymlinkAfterWaiting(t *testing.T) {
	tests := []struct {
		name              string
		realRootAName     string
		aliasRootName     string
		realRootBName     string
		entityBeforeAlias bool
	}{
		{
			name:              "entity_key_before_lexical_key",
			realRootAName:     "z-real-a",
			aliasRootName:     "m-alias",
			realRootBName:     "a-real-b",
			entityBeforeAlias: true,
		},
		{
			name:              "lexical_key_before_entity_key",
			realRootAName:     "a-real-a",
			aliasRootName:     "m-alias",
			realRootBName:     "z-real-b",
			entityBeforeAlias: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, errEval := filepath.EvalSymlinks(t.TempDir())
			if errEval != nil {
				t.Fatalf("resolve temporary root: %v", errEval)
			}
			realRootA := filepath.Join(root, test.realRootAName)
			realRootB := filepath.Join(root, test.realRootBName)
			aliasRoot := filepath.Join(root, test.aliasRootName)
			if errMkdir := os.Mkdir(realRootA, 0o755); errMkdir != nil {
				t.Fatalf("create real root A: %v", errMkdir)
			}
			if errMkdir := os.Mkdir(realRootB, 0o755); errMkdir != nil {
				t.Fatalf("create real root B: %v", errMkdir)
			}
			if errSymlink := os.Symlink(realRootA, aliasRoot); errSymlink != nil {
				t.Skipf("symlink is unavailable: %v", errSymlink)
			}

			aliasPath := filepath.Join(aliasRoot, "auth.json")
			realPathB := filepath.Join(realRootB, "auth.json")
			lexicalKey := pathLexicalKey(aliasPath)
			entityBKey := normalizePathKey(realPathB)
			if got := entityBKey < lexicalKey; got != test.entityBeforeAlias {
				t.Fatalf("unexpected retry key order: entity=%q lexical=%q", entityBKey, lexicalKey)
			}

			unlockAlias := Lock(aliasPath)
			var unlockB func()
			waiterAcquired := make(chan struct{})
			releaseWaiter := make(chan struct{})
			waiterDone := make(chan struct{})
			go func() {
				unlockWaiter := Lock(aliasPath)
				close(waiterAcquired)
				<-releaseWaiter
				unlockWaiter()
				close(waiterDone)
			}()
			defer func() {
				if unlockAlias != nil {
					unlockAlias()
				}
				if unlockB != nil {
					unlockB()
				}
				close(releaseWaiter)
				select {
				case <-waiterDone:
				case <-time.After(5 * time.Second):
					t.Error("redirected alias lock waiter did not exit")
				}
			}()

			waitForPathLockRefs(t, pathIdentityKeys(aliasPath), 2, nil)
			if errRemove := os.Remove(aliasRoot); errRemove != nil {
				t.Fatalf("remove symlink alias: %v", errRemove)
			}
			if errSymlink := os.Symlink(realRootB, aliasRoot); errSymlink != nil {
				t.Fatalf("redirect symlink alias: %v", errSymlink)
			}

			unlockB = Lock(realPathB)
			unlockAlias()
			unlockAlias = nil

			waitForPathLockRefs(t, []string{entityBKey}, 2, waiterAcquired)
			select {
			case <-waiterAcquired:
				t.Fatal("redirected alias lock acquired while the new entity was locked directly")
			default:
			}

			unlockB()
			unlockB = nil
			select {
			case <-waiterAcquired:
			case <-time.After(5 * time.Second):
				t.Fatal("redirected alias lock remained blocked after the new entity was unlocked")
			}
		})
	}
}

func waitForPathLockRefs(t *testing.T, keys []string, want int, acquired <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ready := true
		pathLocks.Lock()
		for _, key := range keys {
			entry := pathLocks.entries[key]
			if entry == nil || entry.refs < want {
				ready = false
				break
			}
		}
		pathLocks.Unlock()
		if ready {
			return
		}
		if acquired != nil {
			select {
			case <-acquired:
				t.Fatal("lock acquired before registering the expected path keys")
			default:
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("path lock references did not reach %d for %q", want, keys)
		}
		runtime.Gosched()
	}
}

func TestRetiredMarkerSurvivesSymlinkRedirectAndClearsSavedGeneration(t *testing.T) {
	realRootA := t.TempDir()
	realRootB := t.TempDir()
	aliasRoot := filepath.Join(t.TempDir(), "auths")
	if errSymlink := os.Symlink(realRootA, aliasRoot); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	aliasPath := filepath.Join(aliasRoot, "auth.json")
	realPathA := filepath.Join(realRootA, "auth.json")
	realPathB := filepath.Join(realRootB, "auth.json")
	t.Cleanup(func() {
		ClearRetired(aliasPath)
		ClearRetired(realPathA)
		ClearRetired(realPathB)
	})

	MarkRetired(aliasPath)
	initialSnapshot := CaptureRetired(aliasPath)
	if !IsRetired(realPathA) {
		t.Fatal("initial resolved entity is not retired")
	}
	if errRemove := os.Remove(aliasRoot); errRemove != nil {
		t.Fatalf("remove symlink alias: %v", errRemove)
	}
	if errSymlink := os.Symlink(realRootB, aliasRoot); errSymlink != nil {
		t.Fatalf("redirect symlink alias: %v", errSymlink)
	}
	if !IsRetired(aliasPath) {
		t.Fatal("redirected lexical path bypassed the retired marker")
	}

	MarkRetired(aliasPath)
	if !IsRetired(realPathB) {
		t.Fatal("redirected entity was not added to the retired generation")
	}
	ClearRetiredSnapshot(initialSnapshot)
	for _, path := range []string{aliasPath, realPathA, realPathB} {
		if !IsRetired(path) {
			t.Fatalf("clearing the pre-redirect snapshot released %q", path)
		}
	}
	ClearRetired(aliasPath)
	for _, path := range []string{aliasPath, realPathA, realPathB} {
		if IsRetired(path) {
			t.Fatalf("ClearRetired(%q) left a saved key retired", aliasPath)
		}
	}
}
