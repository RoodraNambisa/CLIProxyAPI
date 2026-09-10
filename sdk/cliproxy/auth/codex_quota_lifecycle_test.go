package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type quotaObservationStore struct{ writes atomic.Int64 }

func (s *quotaObservationStore) Save(context.Context, *Auth) (string, error) {
	s.writes.Add(1)
	return "", nil
}
func (s *quotaObservationStore) Delete(context.Context, string) error { s.writes.Add(1); return nil }
func (*quotaObservationStore) List(context.Context) ([]*Auth, error)  { return nil, nil }

func TestCodexQuotaObservationDoesNotChangeRuntimeOrPersistence(t *testing.T) {
	store := &quotaObservationStore{}
	manager := NewManager(store, nil, nil)
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := manager.GetByID(installed.ID)
	revision, writes := manager.authIndexRevision, store.writes.Load()
	at := time.Unix(100, 0)
	if !manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", http.Header{"X-Codex-Plan-Type": {"pro"}, "Retry-After": {"20"}}, at) {
		t.Fatal("current credential observation was rejected")
	}
	after, _ := manager.GetByID(installed.ID)
	snapshot := after.CodexQuotaSnapshot()
	if snapshot == nil || !snapshot.ObservedAt.Equal(at) || snapshot.Signals["Retry-After"] != "20" {
		t.Fatal("current credential observation was not published")
	}
	encoded, err := json.Marshal(after)
	if err != nil || strings.Contains(string(encoded), "observed_at") || strings.Contains(string(encoded), "signals") {
		t.Fatal("passive observation entered the serialized credential")
	}
	if after.CloneWithoutRuntimeInstance().CodexQuotaSnapshot() != nil {
		t.Fatal("a new credential clone retained a previous observation")
	}
	after.codexQuotaObservation = nil
	if !reflect.DeepEqual(before, after) || manager.authIndexRevision != revision || store.writes.Load() != writes {
		t.Fatal("passive quota observation changed runtime/index state or invoked storage")
	}
	snapshot.Signals["Retry-After"] = "changed"
	current, _ := manager.GetByID(installed.ID)
	if current.CodexQuotaSnapshot().Signals["Retry-After"] != "20" || installed.CodexQuotaSnapshot() != nil {
		t.Fatal("returned quota maps share credential storage")
	}
}

func TestCodexQuotaObservationSnapshotOrderingAndReplacement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	// A source generation identifies the same file-backed credential on update.
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota", Provider: "codex", Attributes: map[string]string{SourceHashAttributeKey: "unchanged-source"}})
	if err != nil {
		t.Fatal(err)
	}
	instance := installed.RuntimeInstanceID()
	first := time.Unix(100, 0)
	manager.recordCodexQuotaObservation(installed.ID, instance, "http", http.Header{"X-Codex-Plan-Type": {"pro"}, "Retry-After": {"20"}}, first)
	if manager.recordCodexQuotaObservation(installed.ID, instance, "http", nil, first.Add(time.Second)) || manager.recordCodexQuotaObservation(installed.ID, instance, "http", http.Header{"X-Codex-Plan-Type": {"old"}}, first.Add(-time.Second)) {
		t.Fatal("missing or older signals replaced a usable observation")
	}
	manager.recordCodexQuotaObservation(installed.ID, instance, "websocket", http.Header{"X-Codex-Primary-Used-Percent": {"15"}}, first.Add(time.Second))
	current, _ := manager.GetByID(installed.ID)
	snapshot := current.CodexQuotaSnapshot()
	if len(snapshot.Signals) != 1 || snapshot.Source != "websocket" || snapshot.Signals["Retry-After"] != "" {
		t.Fatal("new observations must replace rather than accumulate stale signal keys")
	}
	// A management update from an older clone must preserve a newer observation.
	updated, err := manager.Update(t.Context(), installed)
	if err != nil || updated.RuntimeInstanceID() != instance || updated.CodexQuotaSnapshot() == nil || !updated.CodexQuotaSnapshot().ObservedAt.Equal(first.Add(time.Second)) {
		t.Fatal("same-instance installation lost the latest observation")
	}
	if err := manager.Delete(t.Context(), installed.ID); err != nil {
		t.Fatal(err)
	}
	replacement, err := manager.Register(t.Context(), installed.CloneWithoutRuntimeInstance())
	if err != nil || replacement.RuntimeInstanceID() == instance || replacement.CodexQuotaSnapshot() != nil {
		t.Fatal("reuploaded credential inherited an old observation")
	}
	if manager.recordCodexQuotaObservation(installed.ID, instance, "http", http.Header{"X-Codex-Plan-Type": {"late"}}, first.Add(time.Hour)) {
		t.Fatal("a late old-instance response updated a reuploaded credential")
	}
}

func TestCodexQuotaObservationRejectsInvalidCredentialScope(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	other, err := manager.Register(t.Context(), &Auth{ID: "other", Provider: "claude"})
	if err != nil {
		t.Fatal(err)
	}
	retired, err := manager.Register(t.Context(), &Auth{ID: "retired", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	retired.instanceState.retired.Store(true)
	for _, scope := range []struct{ id, instance string }{
		{"", "instance"}, {"missing", "instance"}, {other.ID, other.RuntimeInstanceID()}, {retired.ID, retired.RuntimeInstanceID()}, {retired.ID, ""},
	} {
		if manager.recordCodexQuotaObservation(scope.id, scope.instance, "http", http.Header{"X-Codex-Plan-Type": {"pro"}}, time.Unix(100, 0)) {
			t.Fatal("quota observation crossed an invalid credential scope")
		}
	}
}

func TestCodexQuotaObservationConcurrentReadersKeepLatestSnapshot(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Go(func() {
			manager.recordCodexQuotaObservation(installed.ID, installed.RuntimeInstanceID(), "http", http.Header{"X-Codex-Primary-Used-Percent": {"15"}}, time.Unix(int64(100+index), 0))
			current, _ := manager.GetByID(installed.ID)
			if snapshot := current.CodexQuotaSnapshot(); snapshot != nil {
				snapshot.Signals["X-Codex-Primary-Used-Percent"] = "reader"
			}
		})
	}
	workers.Wait()
	current, _ := manager.GetByID(installed.ID)
	snapshot := current.CodexQuotaSnapshot()
	if snapshot == nil || snapshot.ObservedAt.Unix() != 131 || snapshot.Signals["X-Codex-Primary-Used-Percent"] != "15" {
		t.Fatal("concurrent publication lost the latest snapshot or shared a reader map")
	}
}
