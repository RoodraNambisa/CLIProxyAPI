package fillorder

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openTest(t *testing.T, path string) *Store {
	t.Helper()
	s, e := Open(path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}
func putTest(t *testing.T, s *Store, key string, priority int) Record {
	t.Helper()
	r, e := s.Upsert(Input{Key: key, Identity: "identity-" + key, Priority: priority})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestOrderLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "order")
	s := openTest(t, path)
	if e := s.Reconcile([]Input{{Key: "z", Identity: "z", Priority: 5}, {Key: "b", Identity: "b", Priority: 3}}); e != nil {
		t.Fatal(e)
	}
	b, _ := s.Lookup("b")
	z, _ := s.Lookup("z")
	if b.Sequence != 1 || z.Sequence != 2 {
		t.Fatal(s.Records())
	}
	a := putTest(t, s, "a", 5)
	if a.Sequence != 3 || a.Position != 3 {
		t.Fatal(a)
	}
	changed, e := s.Upsert(Input{Key: "b", Identity: "b", Priority: 5})
	if e != nil || changed.ID != b.ID || changed.Sequence != 1 {
		t.Fatal(changed, e)
	}
	p, e := s.Move(Move{RecordID: a.ID, Revision: s.Revision(), Direction: "up", Steps: 2})
	if e != nil || p.Record.Position != 1 || p.Record.Sequence != 3 {
		t.Fatal(p, e)
	}
	if _, e = s.Move(Move{RecordID: z.ID, Revision: "1", Direction: "up", Steps: 1}); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if e = s.Compact(); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTest(t, path)
	got, _ := s.Lookup("a")
	if got.Position != 1 || got.Sequence != 3 || got.ID != a.ID {
		t.Fatal(got)
	}
	if e = s.Delete("a"); e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	s = openTest(t, path)
	a = putTest(t, s, "a", 5)
	if a.Sequence != 4 || a.ID == got.ID || a.Position != 3 {
		t.Fatal(a)
	}
}
func TestIdentityReplacementAndRename(t *testing.T) {
	s := openTest(t, "")
	a := putTest(t, s, "a", 5)
	if e := s.Reconcile([]Input{{Key: "renamed", Identity: a.Identity, Priority: 5}}); e != nil {
		t.Fatal(e)
	}
	renamed, _ := s.Lookup("renamed")
	if renamed.ID != a.ID {
		t.Fatal(renamed)
	}
	r, e := s.Upsert(Input{Key: "renamed", Identity: "different-workspace", Priority: 5})
	if e != nil || r.ID == a.ID || r.Sequence != 2 {
		t.Fatal(r, e)
	}
	copy, e := s.Upsert(Input{Key: "copy", Identity: r.Identity, Priority: 5})
	if e != nil || copy.ID == r.ID || copy.Sequence != 3 {
		t.Fatal(copy, e)
	}
}
func TestRecoveryAndLock(t *testing.T) {
	p := filepath.Join(t.TempDir(), "order")
	s := openTest(t, p)
	r := putTest(t, s, "a", 5)
	if extra, e := Open(p); e == nil {
		_ = extra.Close()
		t.Fatal("accepted second writer")
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(p+".wal", os.O_APPEND|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = f.WriteString("partial")
	_ = f.Close()
	s = openTest(t, p)
	got, _ := s.Lookup("a")
	if got.ID != r.ID {
		t.Fatal(got)
	}
	putTest(t, s, "b", 5)
	_ = s.Close()
	data, _ := os.ReadFile(p + ".wal")
	data[20] ^= 1
	_ = os.WriteFile(p+".wal", data, 0600)
	if extra, e := Open(p); e == nil {
		_ = extra.Close()
		t.Fatal("accepted corrupt journal")
	}
}
func TestConcurrentAdds(t *testing.T) {
	s := openTest(t, "")
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, e := s.Upsert(Input{Key: string(rune(1000 + i)), Priority: 5})
			if e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	seen := map[uint64]bool{}
	for _, r := range s.Records() {
		if seen[r.Sequence] {
			t.Fatal("duplicate")
		}
		seen[r.Sequence] = true
	}
	if len(seen) != 100 {
		t.Fatal(len(seen))
	}
}
func TestMoveCountsWholePriority(t *testing.T) {
	s := openTest(t, "")
	a := putTest(t, s, "a", 5)
	putTest(t, s, "other", 3)
	b := putTest(t, s, "b", 5)
	c := putTest(t, s, "c", 5)
	p, e := s.Preview(Move{RecordID: c.ID, Revision: s.Revision(), Direction: "up", Steps: 2})
	if e != nil || p.Before != nil || p.After.ID != a.ID {
		t.Fatal(p, e)
	}
	_, e = s.Move(Move{RecordID: c.ID, Revision: s.Revision(), BeforeID: b.ID})
	if e != nil {
		t.Fatal(e)
	}
	got, _ := s.Lookup("c")
	if got.Position != 3 {
		t.Fatal(got)
	}
}

func TestRankSurvivesDeletionAndPriorityUpdate(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "order"))
	putTest(t, s, "a", 3)
	b := putTest(t, s, "b", 3)
	c := putTest(t, s, "c", 3)
	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	updated, err := s.Upsert(Input{Key: "b", Identity: b.Identity, Priority: 5})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Rank != b.Rank || updated.Position != 1 {
		t.Fatal(updated)
	}
	if err = s.Compact(); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Lookup("c")
	if got.Rank != c.Rank || got.Position != 2 {
		t.Fatal(got)
	}
}
func TestOfflineRenameOverReplacedSource(t *testing.T) {
	s := openTest(t, "")
	original := putTest(t, s, "old", 3)
	putTest(t, s, "target", 3)
	if err := s.Reconcile([]Input{{Key: "target", Identity: original.Identity, Priority: 5}}); err != nil {
		t.Fatal(err)
	}
	rows := s.Records()
	if len(rows) != 1 || rows[0].ID != original.ID || rows[0].Sequence != 1 || rows[0].Priority != 5 {
		t.Fatal(rows)
	}
}
func TestJournalWriteFailureKeepsCommittedState(t *testing.T) {
	s := openTest(t, filepath.Join(t.TempDir(), "order"))
	a := putTest(t, s, "a", 3)
	rev := s.Revision()
	if err := s.journal.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Upsert(Input{Key: "b"}); err == nil {
		t.Fatal("write unexpectedly succeeded")
	}
	if err := s.Delete("a"); err == nil {
		t.Fatal("delete unexpectedly succeeded")
	}
	got, _ := s.Lookup("a")
	if got != a || s.Revision() != rev || len(s.Records()) != 1 {
		t.Fatal(got, s.Revision())
	}
}
func TestSequenceHighWaterAndCorruptSemanticJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "order")
	s := openTest(t, path)
	a := putTest(t, s, "a", 3)
	putTest(t, s, "b", 3)
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, path)
	c := putTest(t, s, "c", 3)
	if c.Sequence != 3 {
		t.Fatal(c)
	}
	rev := s.revision
	_ = s.Close()
	raw, err := encode(change{Revision: rev + 1, HighWater: 3, Upserts: []Record{{Input: Input{Key: "bad"}, ID: a.ID, Sequence: 3, Rank: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path+".wal", os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Write(append(raw, '\n'))
	_ = f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if extra, err := Open(path); err == nil {
		_ = extra.Close()
		t.Fatal("accepted duplicate identity/sequence")
	}
}
func TestTrustedRefreshEnrichesIdentity(t *testing.T) {
	s := openTest(t, "")
	a, err := s.Upsert(Input{Key: "a"})
	if err != nil {
		t.Fatal(err)
	}
	next, err := s.Upsert(Input{Key: "a", Identity: "known"}, true)
	if err != nil || next.ID != a.ID || next.Sequence != a.Sequence || next.Identity != "known" {
		t.Fatal(next, err)
	}
	replacement, err := s.Upsert(Input{Key: "a", Identity: "other"})
	if err != nil || replacement.ID == a.ID || replacement.Sequence != 2 {
		t.Fatal(replacement, err)
	}
}

func TestUnknownIdentityDoesNotCrossProviderScope(t *testing.T) {
	s := openTest(t, "")
	a, err := s.Upsert(Input{Key: "a", Scope: "codex-official"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Upsert(Input{Key: "a", Scope: "other-provider"})
	if err != nil || b.ID == a.ID || b.Sequence != a.Sequence+1 {
		t.Fatal(b, err)
	}
}
func TestSequenceRetainsIntegerPrecisionAndRejectsOverflow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "order")
	s := openTest(t, path)
	s.highWater = 9007199254740993
	if err := s.Compact(); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	s = openTest(t, path)
	r := putTest(t, s, "a", 0)
	if r.Sequence != 9007199254740994 {
		t.Fatal(r.Sequence)
	}
	raw, err := os.ReadFile(path + ".wal")
	if err != nil || !strings.Contains(string(raw), `"sequence":"9007199254740994"`) {
		t.Fatal(string(raw), err)
	}
	s.highWater = math.MaxUint64
	rev := s.Revision()
	if _, err = s.Upsert(Input{Key: "b"}); err == nil {
		t.Fatal("overflow accepted")
	}
	if s.Revision() != rev {
		t.Fatal("failed allocation published")
	}
}
