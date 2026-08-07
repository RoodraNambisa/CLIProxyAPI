package authfileguard

import (
	"path/filepath"
	"reflect"
	"testing"
)

func TestRetiredPathLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	ClearRetired(path)
	if IsRetired(path) {
		t.Fatal("new path is unexpectedly retired")
	}
	MarkRetired(path)
	if !IsRetired(path) {
		t.Fatal("marked path is not retired")
	}
	ClearRetired(path)
	if IsRetired(path) {
		t.Fatal("cleared path remains retired")
	}
}

func TestEmptyRetiredPathIsIgnored(t *testing.T) {
	MarkRetired("")
	if IsRetired("") {
		t.Fatal("empty path must not be retired")
	}
}

func TestRetiredRevisionTracksCatalogChangesAndObservedRewrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retired.json")
	initial := RetiredRevision()
	snapshot, created := MarkRetired(path)
	if !created || RetiredRevision() <= initial {
		t.Fatalf("mark revision = %d, initial = %d, created = %t", RetiredRevision(), initial, created)
	}
	marked := RetiredRevision()
	if _, created = MarkRetired(path); created || RetiredRevision() != marked {
		t.Fatalf("repeated mark changed revision: revision=%d marked=%d created=%t", RetiredRevision(), marked, created)
	}
	NotifyRetiredFileChanged(path)
	if RetiredRevision() <= marked {
		t.Fatalf("rewrite revision = %d, marked = %d", RetiredRevision(), marked)
	}
	rewritten := RetiredRevision()
	ClearRetiredSnapshot(snapshot)
	if RetiredRevision() <= rewritten || IsRetired(path) {
		t.Fatalf("clear revision = %d, rewritten = %d, retired = %t", RetiredRevision(), rewritten, IsRetired(path))
	}
}

func TestClearRetiredSnapshotPreservesNewGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auth.json")
	ClearRetired(path)
	MarkRetired(path)
	snapshot := CaptureRetired(path)

	ClearRetired(path)
	MarkRetired(path)
	ClearRetiredSnapshot(snapshot)

	if !IsRetired(path) {
		t.Fatal("new retired generation was cleared by an older snapshot")
	}
	ClearRetired(path)
}

func TestSortedUniqueKeysUsesStableLockOrder(t *testing.T) {
	keys := sortedUniqueKeys([]string{"entity", "lexical", "entity", ""})
	if want := []string{"entity", "lexical"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("sortedUniqueKeys() = %q, want %q", keys, want)
	}
}
