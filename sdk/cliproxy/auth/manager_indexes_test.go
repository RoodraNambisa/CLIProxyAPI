package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

type blockingPersistedSnapshotStore struct {
	*chatGPTWebDependencyTestStore
	listCalls atomic.Int32
	started   chan struct{}
	release   chan struct{}
	once      sync.Once
}

type saveOutcomePersistedSnapshotStore struct {
	*chatGPTWebDependencyTestStore
	saveErr error
}

type listErrorPersistedSnapshotStore struct {
	*chatGPTWebDependencyTestStore
	listErr error
}

func (store *saveOutcomePersistedSnapshotStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if store.saveErr != nil {
		return "", store.saveErr
	}
	return store.chatGPTWebDependencyTestStore.Save(ctx, auth)
}

func (store *listErrorPersistedSnapshotStore) List(context.Context) ([]*Auth, error) {
	return nil, store.listErr
}

func (store *blockingPersistedSnapshotStore) List(ctx context.Context) ([]*Auth, error) {
	store.listCalls.Add(1)
	store.once.Do(func() { close(store.started) })
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-store.release:
	}
	return store.chatGPTWebDependencyTestStore.List(ctx)
}

func TestManagerBackingPathIndexTracksSharedPathsAndReplacement(t *testing.T) {
	firstRoot := t.TempDir()
	secondRoot := t.TempDir()
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{AuthDir: firstRoot})
	register := func(id, path string) {
		t.Helper()
		_, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
			ID:         id,
			Provider:   "claude",
			FileName:   id + ".json",
			Status:     StatusActive,
			Attributes: map[string]string{"path": path},
		})
		if errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	register("first", "shared.json")
	register("second", "shared.json")

	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "shared.json"), "first", "second")
	first, _ := manager.GetByID("first")
	first.Attributes["path"] = "moved.json"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), first); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "shared.json"), "second")
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "moved.json"), "first")

	manager.SetConfig(&internalconfig.Config{AuthDir: secondRoot})
	assertAuthIDsForPath(t, manager, filepath.Join(firstRoot, "moved.json"))
	assertAuthIDsForPath(t, manager, filepath.Join(secondRoot, "moved.json"), "first")
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), "first"); errDelete != nil {
		t.Fatal(errDelete)
	}
	assertAuthIDsForPath(t, manager, filepath.Join(secondRoot, "moved.json"))
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerAuthsForProvidersTracksReplacementAndDelete(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	for _, auth := range []*Auth{
		{ID: "web-b", Provider: "chatgpt-web", Status: StatusActive},
		{ID: "codex", Provider: "codex", Status: StatusActive},
		{ID: "web-a", Provider: "ChatGPT-Web", Status: StatusActive},
	} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	assertAuthIDs(t, manager.AuthsForProviders("CHATGPT-WEB"), "web-a", "web-b")

	replacement, _ := manager.GetByID("web-b")
	replacement.Provider = "codex"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	assertAuthIDs(t, manager.AuthsForProviders("chatgpt-web"), "web-a")
	assertAuthIDs(t, manager.AuthsForProviders("codex"), "codex", "web-b")

	if errDelete := manager.Delete(WithSkipPersist(t.Context()), "web-a"); errDelete != nil {
		t.Fatal(errDelete)
	}
	assertAuthIDs(t, manager.AuthsForProviders("chatgpt-web"))
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerDirectLookupIndexesTrackReplacementAndDelete(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	for _, auth := range []*Auth{
		{ID: "auth-b", Index: "shared-index", Provider: "codex", FileName: "nested/auth-b.json", Status: StatusActive},
		{ID: "auth-a", Index: "shared-index", Provider: "chatgpt-web", FileName: "auth-a.json", Status: StatusActive},
	} {
		auth.indexAssigned = true
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	indexed, ok := manager.GetByAuthIndex("shared-index")
	if !ok || indexed.ID != "auth-a" {
		t.Fatalf("GetByAuthIndex() = %#v, %v", indexed, ok)
	}
	assertAuthIDs(t, manager.AuthsForManagedFileName("auth-b.json"), "auth-b")
	byIDs := manager.GetByIDs([]string{"auth-b", "missing", "auth-a"})
	if len(byIDs) != 2 || byIDs[0].ID != "auth-b" || byIDs[1].ID != "auth-a" {
		t.Fatalf("GetByIDs() = %#v", byIDs)
	}
	if ids := manager.AuthIDsForProviders("CODEX"); !slices.Equal(ids, []string{"auth-b"}) {
		t.Fatalf("AuthIDsForProviders() = %v", ids)
	}

	replacement, _ := manager.GetByID("auth-a")
	replacement.Index = "replacement-index"
	replacement.FileName = "replacement.json"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	indexed, ok = manager.GetByAuthIndex("shared-index")
	if !ok || indexed.ID != "auth-b" {
		t.Fatalf("shared index after replacement = %#v, %v", indexed, ok)
	}
	if _, ok = manager.GetByAuthIndex("replacement-index"); !ok {
		t.Fatal("replacement index was not installed")
	}
	assertAuthIDs(t, manager.AuthsForManagedFileName("auth-a.json"))
	assertAuthIDs(t, manager.AuthsForManagedFileName("replacement.json"), "auth-a")

	if errDelete := manager.Delete(WithSkipPersist(t.Context()), "auth-b"); errDelete != nil {
		t.Fatal(errDelete)
	}
	if _, ok = manager.GetByAuthIndex("shared-index"); ok {
		t.Fatal("deleted auth remained in reverse auth-index lookup")
	}
	assertAuthIDs(t, manager.AuthsForManagedFileName("auth-b.json"))
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerManagementCatalogIsSafeAndOnlyRevisesForVisibleChanges(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "catalog-auth",
		Provider: "codex",
		FileName: "catalog.json",
		Label:    "Catalog",
		Status:   StatusActive,
		Attributes: map[string]string{
			"path":    "catalog.json",
			"api_key": "attribute-secret",
		},
		Metadata: map[string]any{
			"access_token":   "metadata-secret",
			"session_cookie": "cookie-secret",
			"email":          "catalog@example.com",
			"plan_type":      "plus",
			"note":           map[string]any{"secret": "nested-secret"},
		},
		LastError: &Error{Code: "unsafe code with spaces", Message: "error-message-secret"},
	}
	auth.EnsureIndex()
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	revision, catalog := manager.ManagementAuthCatalogSnapshot()
	if revision == 0 || len(catalog) != 1 {
		t.Fatalf("catalog revision=%d entries=%d", revision, len(catalog))
	}
	payload, errMarshal := json.Marshal(catalog)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	for _, secret := range []string{"attribute-secret", "metadata-secret", "cookie-secret", "nested-secret", "error-message-secret", "access_token", "session_cookie", "api_key"} {
		if strings.Contains(string(payload), secret) {
			t.Fatalf("management catalog leaked %q: %s", secret, payload)
		}
	}
	if got := catalog[0].Metadata["email"]; got != "catalog@example.com" {
		t.Fatalf("safe email = %#v", got)
	}

	tokenOnly, _ := manager.GetByID(auth.ID)
	tokenOnly.Metadata["access_token"] = "rotated-secret"
	tokenOnly.Metadata["session_cookie"] = "rotated-cookie"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), tokenOnly); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if got := manager.ManagementAuthCatalogRevision(); got != revision {
		t.Fatalf("token-only update changed catalog revision from %d to %d", revision, got)
	}

	visible, _ := manager.GetByID(auth.ID)
	visible.Label = "Updated Catalog"
	if _, errUpdate := manager.Update(WithSkipPersist(t.Context()), visible); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	if got := manager.ManagementAuthCatalogRevision(); got <= revision {
		t.Fatalf("visible update did not advance catalog revision: before=%d after=%d", revision, got)
	}
	usageRevision, usageCatalog := manager.UsageAuthCatalogSnapshot()
	if usageRevision != manager.ManagementAuthCatalogRevision() || len(usageCatalog) != 1 || usageCatalog[0].Label != "Updated Catalog" {
		t.Fatalf("usage catalog revision=%d entries=%#v", usageRevision, usageCatalog)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerDependencyGraphForAuthsExcludesUnrelatedCredentials(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source := dependencyTestCodexAuth("source", "source-uid")
	dependent := dependencyTestWebAuth("dependent", "source-uid")
	unrelatedSource := dependencyTestCodexAuth("unrelated-source", "unrelated-uid")
	unrelatedDependent := dependencyTestWebAuth("unrelated-dependent", "unrelated-uid")
	for _, auth := range []*Auth{source, dependent, unrelatedSource, unrelatedDependent} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}

	target, ok := manager.GetByID(source.ID)
	if !ok {
		t.Fatal("source auth not found")
	}
	graph, complete := manager.ChatGPTWebDependencyGraphForAuths([]*Auth{target})
	if complete {
		t.Fatal("runtime-only dependency graph unexpectedly reported complete")
	}
	resolved, ambiguous := graph.SourceByUID("source-uid")
	if ambiguous || resolved == nil || resolved.ID != source.ID {
		t.Fatalf("source = %#v, ambiguous = %v", resolved, ambiguous)
	}
	dependents, ambiguous := graph.DependentsForSource(target)
	if ambiguous || len(dependents) != 1 || dependents[0].ID != dependent.ID {
		t.Fatalf("dependents = %#v, ambiguous = %v", dependents, ambiguous)
	}
	if unrelated, ambiguousUnrelated := graph.SourceByUID("unrelated-uid"); unrelated != nil || ambiguousUnrelated {
		t.Fatalf("unrelated source leaked into page graph: %#v, ambiguous = %v", unrelated, ambiguousUnrelated)
	}
}

func TestManagerLoadDoesNotHoldMainLockDuringStoreList(t *testing.T) {
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(
			&Auth{ID: "loaded-auth", Provider: "codex", Status: StatusActive},
		),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{ID: "existing-auth", Provider: "codex", Status: StatusActive}); errRegister != nil {
		t.Fatal(errRegister)
	}

	loadDone := make(chan error, 1)
	go func() { loadDone <- manager.Load(t.Context()) }()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("Load() did not start Store.List")
	}

	readDone := make(chan error, 1)
	go func() {
		if manager.Count() != 1 {
			readDone <- errors.New("unexpected auth count while load was blocked")
			return
		}
		if auth, ok := manager.GetByID("existing-auth"); !ok || auth == nil {
			readDone <- errors.New("existing auth unavailable while load was blocked")
			return
		}
		readDone <- nil
	}()
	select {
	case errRead := <-readDone:
		if errRead != nil {
			t.Fatal(errRead)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Manager read blocked behind Store.List")
	}

	close(store.release)
	if errLoad := <-loadDone; errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, ok := manager.GetByID("loaded-auth"); !ok {
		t.Fatal("loaded auth was not installed")
	}
	if _, ok := manager.GetByID("existing-auth"); ok {
		t.Fatal("stale runtime auth remained after load")
	}
}

func TestManagerLoadFailurePreservesCurrentState(t *testing.T) {
	store := &listErrorPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(),
		listErr:                       errors.New("list failed"),
	}
	manager := NewManager(store, nil, nil)
	existing := &Auth{ID: "existing-auth", Index: "existing-index", Provider: "codex", FileName: "existing.json", Status: StatusActive}
	existing.indexAssigned = true
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), existing); errRegister != nil {
		t.Fatal(errRegister)
	}

	if errLoad := manager.Load(t.Context()); !errors.Is(errLoad, store.listErr) {
		t.Fatalf("Load() error = %v, want %v", errLoad, store.listErr)
	}
	if auth, ok := manager.GetByID(existing.ID); !ok || auth == nil || auth.Index != existing.Index {
		t.Fatalf("current auth changed after failed load: %#v, %v", auth, ok)
	}
	if indexed, ok := manager.GetByAuthIndex(existing.Index); !ok || indexed.ID != existing.ID {
		t.Fatalf("auth index changed after failed load: %#v, %v", indexed, ok)
	}
	assertAuthIDs(t, manager.AuthsForManagedFileName(existing.FileName), existing.ID)
	assertManagerAuthIndexesConsistent(t, manager)
}

func assertAuthIDs(t *testing.T, auths []*Auth, want ...string) {
	t.Helper()
	got := make([]string, 0, len(auths))
	for _, auth := range auths {
		if auth != nil {
			got = append(got, auth.ID)
		}
	}
	sort.Strings(want)
	if !slices.Equal(got, want) {
		t.Fatalf("auth IDs = %v, want %v", got, want)
	}
}

func TestManagerDependencyIndexIncludesPersistedOnlyAuthsAndDirtyState(t *testing.T) {
	authDir := t.TempDir()
	source := dependencyTestCodexAuth("source", "uid-a")
	source.Attributes = map[string]string{"path": filepath.Join(authDir, source.FileName)}
	web := dependencyTestWebAuth("web", "uid-a")
	web.Attributes = map[string]string{"path": filepath.Join(authDir, web.FileName)}
	store := newChatGPTWebDependencyTestStore(source, web)
	manager := NewManager(store, nil, nil)
	manager.SetConfig(&internalconfig.Config{AuthDir: authDir})

	authfileguard.MarkQuarantined(source.Attributes["path"])
	t.Cleanup(func() { authfileguard.ClearQuarantined(source.Attributes["path"]) })
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, loaded := manager.GetByID(source.ID); loaded {
		t.Fatal("quarantined source unexpectedly entered the runtime view")
	}
	graph, complete := manager.ChatGPTWebDependencyIndexSnapshot()
	indexedSource, ambiguous := graph.SourceByUID("uid-a")
	if !complete || ambiguous || indexedSource == nil || indexedSource.ID != source.ID {
		t.Fatalf("dependency index source = %#v, complete = %v, ambiguous = %v", indexedSource, complete, ambiguous)
	}
	dependents, ambiguous := graph.DependentsForSource(source)
	if ambiguous || len(dependents) != 1 || dependents[0].ID != web.ID {
		t.Fatalf("dependency index dependents = %#v, ambiguous = %v", dependents, ambiguous)
	}
	directSource, ambiguous, directComplete := manager.ChatGPTWebSourceByCredentialUID("uid-a")
	if !directComplete || ambiguous || directSource == nil || directSource.ID != source.ID {
		t.Fatalf("direct source = %#v, complete = %v, ambiguous = %v", directSource, directComplete, ambiguous)
	}
	directDependents, ambiguous, directComplete := manager.ChatGPTWebDependentsForSource(source)
	if !directComplete || ambiguous || len(directDependents) != 1 || directDependents[0].ID != web.ID {
		t.Fatalf("direct dependents = %#v, complete = %v, ambiguous = %v", directDependents, directComplete, ambiguous)
	}
	assertAuthIDsForPath(t, manager, source.Attributes["path"])

	manager.MarkChatGPTWebDependencyIndexDirty()
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("dependency index remained complete after being marked dirty")
	}
	if _, errSnapshot := manager.PersistedAuthSnapshot(t.Context()); errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("successful persisted snapshot did not restore completeness")
	}
	manager.SetStore(store)
	if _, complete = manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("replacing the store did not mark the dependency index dirty")
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerPersistedAuthSnapshotSharesConcurrentRefresh(t *testing.T) {
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(
			dependencyTestWebAuth("shared-snapshot", "source-uid"),
		),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)

	const callers = 32
	results := make(chan error, callers)
	for range callers {
		go func() {
			auths, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
			if errSnapshot == nil && (len(auths) != 1 || auths[0].ID != "shared-snapshot") {
				errSnapshot = errors.New("unexpected persisted snapshot")
			}
			results <- errSnapshot
		}()
	}
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}
	close(store.release)
	for range callers {
		if errSnapshot := <-results; errSnapshot != nil {
			t.Fatal(errSnapshot)
		}
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want 1", calls)
	}
}

func TestManagerPersistedAuthSnapshotSerializesConcurrentRuntimeOnlyMutation(t *testing.T) {
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(
			dependencyTestWebAuth("persisted", "source-uid"),
		),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)

	result := make(chan error, 1)
	go func() {
		_, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
		result <- errSnapshot
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}

	registerResult := make(chan error, 1)
	go func() {
		_, errRegister := manager.Register(WithSkipPersist(t.Context()), &Auth{
			ID:       "runtime-only",
			Provider: "claude",
			Status:   StatusActive,
		})
		registerResult <- errRegister
	}()
	select {
	case errRegister := <-registerResult:
		t.Fatalf("runtime mutation completed during persisted snapshot refresh: %v", errRegister)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if errSnapshot := <-result; errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errRegister := <-registerResult; errRegister != nil {
		t.Fatal(errRegister)
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want 1", calls)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("runtime-only mutation invalidated the refreshed persistence index")
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerPersistedAuthSnapshotDoesNotReuseReleasedMutationBarrier(t *testing.T) {
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(
			dependencyTestWebAuth("persisted", "source-uid"),
		),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := NewManager(store, nil, nil)
	lockedCtx, unlockMutation, errLock := manager.LockAuthMutation(t.Context(), &Auth{
		ID:       "released-mutation",
		Provider: "claude",
	})
	if errLock != nil {
		t.Fatal(errLock)
	}
	unlockMutation()

	snapshotResult := make(chan error, 1)
	go func() {
		_, errSnapshot := manager.PersistedAuthSnapshot(lockedCtx)
		snapshotResult <- errSnapshot
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}

	registerResult := make(chan error, 1)
	go func() {
		_, errRegister := manager.Register(t.Context(), &Auth{
			ID:       "durable",
			Provider: "claude",
			Metadata: map[string]any{"marker": "saved"},
		})
		registerResult <- errRegister
	}()
	select {
	case errRegister := <-registerResult:
		t.Fatalf("durable mutation bypassed the snapshot barrier through a released context: %v", errRegister)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	if errSnapshot := <-snapshotResult; errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errRegister := <-registerResult; errRegister != nil {
		t.Fatal(errRegister)
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want one stable refresh", calls)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerPersistedAuthSnapshotTracksDurableRuntimeMetadataMutation(t *testing.T) {
	auth := dependencyTestWebAuth("persisted-metadata", "source-uid")
	store := newChatGPTWebDependencyTestStore(auth)
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok || expected == nil {
		t.Fatal("loaded auth not found")
	}
	_, matched, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), expected, func(candidate *Auth) {
		candidate.Metadata["persisted_marker"] = "updated"
	})
	if errMutate != nil {
		t.Fatal(errMutate)
	}
	if !matched {
		t.Fatal("runtime metadata mutation did not match current auth")
	}
	persisted, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if len(persisted) != 1 || persisted[0].Metadata["persisted_marker"] != "updated" {
		t.Fatalf("persisted snapshot = %#v, want updated metadata", persisted)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerPersistedAuthSnapshotSerializesConcurrentDurableSave(t *testing.T) {
	auth := dependencyTestWebAuth("concurrent-save", "source-uid")
	baseStore := newChatGPTWebDependencyTestStore(auth)
	manager := NewManager(baseStore, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: baseStore,
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.SetStore(store)

	snapshotResult := make(chan error, 1)
	go func() {
		_, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
		snapshotResult <- errSnapshot
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}

	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("loaded auth not found")
	}
	current.Metadata["persisted_marker"] = "updated"
	updateResult := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), current)
		updateResult <- errUpdate
	}()
	select {
	case errUpdate := <-updateResult:
		t.Fatalf("durable save completed during persisted snapshot refresh: %v", errUpdate)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	if errSnapshot := <-snapshotResult; errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errUpdate := <-updateResult; errUpdate != nil {
		t.Fatal(errUpdate)
	}
	persisted, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if len(persisted) != 1 || persisted[0].Metadata["persisted_marker"] != "updated" {
		t.Fatalf("persisted snapshot = %#v, want updated metadata", persisted)
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want one stable refresh", calls)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerPersistedAuthSnapshotInvalidatesAfterUncertainSave(t *testing.T) {
	auth := dependencyTestWebAuth("uncertain-save", "source-uid")
	storeErr := errors.New("save outcome unavailable")
	store := &saveOutcomePersistedSnapshotStore{
		chatGPTWebDependencyTestStore: newChatGPTWebDependencyTestStore(auth),
		saveErr:                       NewSaveOutcomeError(SaveOutcomeUncertain, storeErr),
	}
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("loaded auth not found")
	}
	current.Metadata["note"] = "updated"
	if _, errUpdate := manager.Update(t.Context(), current); !errors.Is(errUpdate, storeErr) {
		t.Fatalf("Update() error = %v, want %v", errUpdate, storeErr)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("uncertain save left the persistence index authoritative")
	}
}

func TestManagerDeleteWithOperationRemovesPersistedSnapshotIndex(t *testing.T) {
	auth := dependencyTestWebAuth("operation-delete", "source-uid")
	store := newChatGPTWebDependencyTestStore(auth)
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if errDelete := manager.DeleteWithOperation(t.Context(), auth.ID, func(ctx context.Context) error {
		return store.Delete(ctx, auth.ID)
	}); errDelete != nil {
		t.Fatal(errDelete)
	}
	if persisted, complete := manager.PersistedAuthByID(auth.ID); persisted != nil || !complete {
		t.Fatalf("persisted auth after delete = %#v, complete=%v", persisted, complete)
	}
	auths, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
	if errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if len(auths) != 0 {
		t.Fatalf("persisted snapshot after delete = %#v", auths)
	}
}

func TestManagerPersistedAuthSnapshotRejectsConcurrentUncertainDelete(t *testing.T) {
	auth := dependencyTestWebAuth("uncertain-operation-delete", "source-uid")
	baseStore := newChatGPTWebDependencyTestStore(auth)
	manager := NewManager(baseStore, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: baseStore,
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.SetStore(store)

	result := make(chan error, 1)
	go func() {
		_, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
		result <- errSnapshot
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}

	storeErr := errors.New("delete outcome unavailable")
	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- manager.DeleteWithOperation(t.Context(), auth.ID, func(context.Context) error {
			return NewDeleteOutcomeError(DeleteOutcomeUncertain, storeErr)
		})
	}()
	select {
	case errDelete := <-deleteResult:
		t.Fatalf("uncertain delete completed during persisted snapshot refresh: %v", errDelete)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if errSnapshot := <-result; errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errDelete := <-deleteResult; !errors.Is(errDelete, storeErr) {
		t.Fatalf("DeleteWithOperation() error = %v, want %v", errDelete, storeErr)
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want one stable refresh", calls)
	}
	if persisted, complete := manager.PersistedAuthByID(auth.ID); persisted == nil || complete {
		t.Fatalf("persisted auth after uncertain delete = %#v, complete=%v", persisted, complete)
	}
}

func TestManagerPersistedAuthSnapshotRejectsConcurrentCommittedDelete(t *testing.T) {
	auth := dependencyTestWebAuth("committed-operation-delete", "source-uid")
	baseStore := newChatGPTWebDependencyTestStore(auth)
	manager := NewManager(baseStore, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	store := &blockingPersistedSnapshotStore{
		chatGPTWebDependencyTestStore: baseStore,
		started:                       make(chan struct{}),
		release:                       make(chan struct{}),
	}
	manager.SetStore(store)

	result := make(chan error, 1)
	go func() {
		_, errSnapshot := manager.PersistedAuthSnapshot(t.Context())
		result <- errSnapshot
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("persisted snapshot enumeration did not start")
	}

	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- manager.DeleteWithOperation(t.Context(), auth.ID, func(ctx context.Context) error {
			return baseStore.Delete(ctx, auth.ID)
		})
	}()
	select {
	case errDelete := <-deleteResult:
		t.Fatalf("committed delete completed during persisted snapshot refresh: %v", errDelete)
	case <-time.After(50 * time.Millisecond):
	}
	close(store.release)
	if errSnapshot := <-result; errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	if errDelete := <-deleteResult; errDelete != nil {
		t.Fatal(errDelete)
	}
	if calls := store.listCalls.Load(); calls != 1 {
		t.Fatalf("store List calls = %d, want one stable refresh", calls)
	}
	if persisted, complete := manager.PersistedAuthByID(auth.ID); persisted != nil || !complete {
		t.Fatalf("persisted auth after committed delete = %#v, complete=%v", persisted, complete)
	}
}

func TestManagerDependencyIndexTracksDuplicateUIDAndRuntimeChanges(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := dependencyTestCodexAuth("source-a", "duplicate")
	second := dependencyTestCodexAuth("source-b", "duplicate")
	web := dependencyTestWebAuth("web", "duplicate")
	for _, auth := range []*Auth{first, second, web} {
		if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	graph, complete := manager.ChatGPTWebDependencyIndexSnapshot()
	if complete {
		t.Fatal("runtime-only dependency index unexpectedly reported a complete persisted view")
	}
	if source, ambiguous := graph.SourceByUID("duplicate"); source != nil || !ambiguous {
		t.Fatalf("duplicate source = %#v, ambiguous = %v", source, ambiguous)
	}
	if source, ambiguous, _ := manager.ChatGPTWebSourceByCredentialUID("duplicate"); source != nil || !ambiguous {
		t.Fatalf("direct duplicate source = %#v, ambiguous = %v", source, ambiguous)
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), second.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	graph, _ = manager.ChatGPTWebDependencyIndexSnapshot()
	resolved, ambiguous := graph.SourceByUID("duplicate")
	if ambiguous || resolved == nil || resolved.ID != first.ID {
		t.Fatalf("resolved source = %#v, ambiguous = %v", resolved, ambiguous)
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerSkipPersistDeleteMarksDependencyIndexDirtyAndRestoresPersistedEntry(t *testing.T) {
	source := dependencyTestCodexAuth("source", "persisted-uid")
	store := newChatGPTWebDependencyTestStore(source)
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("loaded dependency index is incomplete")
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("skip-persist removal left the dependency index complete")
	}
	if _, errSnapshot := manager.PersistedAuthSnapshot(t.Context()); errSnapshot != nil {
		t.Fatal(errSnapshot)
	}
	restored, ambiguous, complete := manager.ChatGPTWebSourceByCredentialUID("persisted-uid")
	if !complete || ambiguous || restored == nil || restored.ID != source.ID {
		t.Fatalf("restored source = %#v, ambiguous=%v, complete=%v", restored, ambiguous, complete)
	}
}

func TestManagerUncertainPersistedDeleteInvalidatesDependencyIndex(t *testing.T) {
	storeErr := errors.New("delete outcome unavailable")
	store := &deleteOutcomeStore{deleteErr: NewDeleteOutcomeError(DeleteOutcomeUncertain, storeErr)}
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	source := dependencyTestCodexAuth("uncertain-source", "uncertain-uid")
	if _, errRegister := manager.Register(t.Context(), source); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); !complete {
		t.Fatal("loaded dependency index is incomplete before delete")
	}
	if errDelete := manager.Delete(t.Context(), source.ID); !errors.Is(errDelete, storeErr) {
		t.Fatalf("Delete() error = %v, want %v", errDelete, storeErr)
	}
	if _, complete := manager.ChatGPTWebDependencyIndexSnapshot(); complete {
		t.Fatal("uncertain persisted delete left dependency index authoritative")
	}
}

func TestManagerChatGPTWebIdentityIndexDropsDeletedRuntimeAuth(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	auth := chatGPTWebIdentityTestAuth("identity-index-delete", "account-a", "user-a")
	auth.Metadata["email"] = "identity-index-delete@example.com"
	if _, errRegister := manager.Register(t.Context(), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if matches, _ := manager.ChatGPTWebAuthsByEmail(chatGPTWebRegistrationEmail(auth)); len(matches) != 1 || matches[0].ID != auth.ID {
		t.Fatalf("email matches before delete = %+v", matches)
	}
	if errDelete := manager.Delete(WithSkipPersist(t.Context()), auth.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	if matches, _ := manager.ChatGPTWebAuthsByEmail(chatGPTWebRegistrationEmail(auth)); len(matches) != 0 {
		t.Fatalf("email matches after delete = %+v", matches)
	}
	if _, complete := manager.ChatGPTWebAuthsByEmail(chatGPTWebRegistrationEmail(auth)); complete {
		t.Fatal("skip-persist delete left the identity index authoritative")
	}
	assertManagerAuthIndexesConsistent(t, manager)
}

func TestManagerListMetadataSummariesIncludesOnlyRequestedMetadata(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:         "summary-auth",
		Provider:   "chatgpt-web",
		Index:      "summary-index",
		Status:     StatusActive,
		Attributes: map[string]string{"path": "summary-auth.json"},
		Metadata: map[string]any{
			"email":          "summary@example.com",
			"access_token":   "secret-access-token",
			"session_cookie": "secret-session-cookie",
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	summaries := manager.ListMetadataSummaries("email")
	if len(summaries) != 1 {
		t.Fatalf("summary count = %d, want 1", len(summaries))
	}
	summary := summaries[0]
	if summary.ID != auth.ID || summary.Index != auth.Index || summary.Metadata["email"] != "summary@example.com" {
		t.Fatalf("summary = %#v", summary)
	}
	if _, leaked := summary.Metadata["access_token"]; leaked {
		t.Fatal("summary leaked access token")
	}
	if _, leaked := summary.Metadata["session_cookie"]; leaked {
		t.Fatal("summary leaked session cookie")
	}
	summary.Attributes["path"] = "changed.json"
	current, ok := manager.GetByID(auth.ID)
	if !ok || current.Attributes["path"] != "summary-auth.json" {
		t.Fatal("mutating summary attributes changed Manager state")
	}
}

func TestManagerListMetadataSummariesUsesIndexedPlanWithoutReturningToken(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	claims := base64.RawURLEncoding.EncodeToString([]byte(`{"https://api.openai.com/auth":{"chatgpt_plan_type":"pro"}}`))
	auth := &Auth{
		ID:       "codex-summary-auth",
		Provider: "codex",
		Metadata: map[string]any{
			"id_token":     "e30." + claims + ".signature",
			"access_token": "secret-access-token",
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	summaries := manager.ListMetadataSummaries("plan_type")
	if len(summaries) != 1 || summaries[0].Metadata["plan_type"] != "pro" {
		t.Fatalf("summaries = %#v", summaries)
	}
	if _, leaked := summaries[0].Metadata["id_token"]; leaked {
		t.Fatal("summary leaked ID token")
	}
	if _, leaked := summaries[0].Metadata["access_token"]; leaked {
		t.Fatal("summary leaked access token")
	}
}

func assertAuthIDsForPath(t *testing.T, manager *Manager, path string, want ...string) {
	t.Helper()
	auths := manager.AuthsForBackingPath(path)
	got := make([]string, 0, len(auths))
	for _, auth := range auths {
		got = append(got, auth.ID)
	}
	sort.Strings(want)
	match := len(got) == len(want)
	for index := range got {
		if !match || got[index] != want[index] {
			match = false
			break
		}
	}
	if !match {
		t.Fatalf("auth IDs for %q = %v, want %v", path, got, want)
	}
}

func assertManagerAuthIndexesConsistent(t *testing.T, manager *Manager) {
	t.Helper()
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	expectedPaths := make(map[string]map[string]struct{})
	expectedByID := make(map[string]string)
	expectedProviders := make(map[string]map[string]struct{})
	expectedProviderByID := make(map[string]string)
	expectedProviderPrefixed := make(map[string]map[string]struct{})
	expectedProviderRetryByID := make(map[string]providerRequestRetryEntry)
	expectedProviderRetryAggregates := make(map[string]*providerRequestRetryAggregate)
	expectedImageBlockedIDs := make(map[string]string)
	expectedQuotaChecks := make(map[string]string)
	expectedAuthIndexes := make(map[string]string)
	expectedIDsByIndex := make(map[string]map[string]struct{})
	expectedManagedFiles := make(map[string]map[string]struct{})
	expectedManagedKeysByID := make(map[string][]string)
	expectedPlanTypes := make(map[string]string)
	expectedDependencies := make(map[string]*Auth)
	expectedIdentityKeys := make(map[string][]string)
	expectedManagementCatalog := make(map[string]*Auth)
	expectedUsageCatalog := make(map[string]UsageAuthInfo)
	for id, auth := range manager.auths {
		if index := strings.TrimSpace(auth.Index); index != "" {
			expectedAuthIndexes[id] = index
			addIDToDependencySet(expectedIDsByIndex, index, id)
		}
		if keys := authManagedFileIndexKeys(auth); len(keys) > 0 {
			expectedManagedKeysByID[id] = keys
			for _, key := range keys {
				addIDToDependencySet(expectedManagedFiles, key, id)
			}
		}
		if planType := strings.TrimSpace(internalcodex.EffectivePlanType(auth.Metadata)); planType != "" {
			expectedPlanTypes[id] = planType
		}
		if providerKey := strings.ToLower(strings.TrimSpace(auth.Provider)); providerKey != "" {
			ids := expectedProviders[providerKey]
			if ids == nil {
				ids = make(map[string]struct{})
				expectedProviders[providerKey] = ids
			}
			ids[id] = struct{}{}
			expectedProviderByID[id] = providerKey
			override, hasOverride := auth.RequestRetryOverride()
			if override < 0 {
				override = 0
			}
			expectedProviderRetryByID[id] = providerRequestRetryEntry{
				provider:    providerKey,
				override:    override,
				hasOverride: hasOverride,
			}
			aggregate := expectedProviderRetryAggregates[providerKey]
			if aggregate == nil {
				aggregate = &providerRequestRetryAggregate{overrideCounts: make(map[int]int)}
				expectedProviderRetryAggregates[providerKey] = aggregate
			}
			aggregate.authCount++
			if hasOverride {
				aggregate.overrideCounts[override]++
				if override > aggregate.maxOverride {
					aggregate.maxOverride = override
				}
			} else {
				aggregate.defaultCount++
			}
			if strings.TrimSpace(auth.Prefix) != "" {
				ids := expectedProviderPrefixed[providerKey]
				if ids == nil {
					ids = make(map[string]struct{})
					expectedProviderPrefixed[providerKey] = ids
				}
				ids[id] = struct{}{}
			}
		}
		if _, scheduled := chatGPTWebImageQuotaRefreshDueAt(auth, time.Now()); scheduled && !manager.sessionCleanupPendingLocked(id) {
			expectedQuotaChecks[id] = auth.RuntimeInstanceID()
		}
		if chatGPTWebImageMayBeBlocked(auth, time.Now()) && !manager.sessionCleanupPendingLocked(id) {
			expectedImageBlockedIDs[id] = auth.RuntimeInstanceID()
		}
		if key := authBackingPathKey(auth, manager.currentConfig()); key != "" {
			ids := expectedPaths[key]
			if ids == nil {
				ids = make(map[string]struct{})
				expectedPaths[key] = ids
			}
			ids[id] = struct{}{}
			expectedByID[id] = key
		}
		if authRelevantToChatGPTWebDependencyIndex(auth) {
			expectedDependencies[id] = auth
		}
		if keys := chatGPTWebIdentityIndexKeys(auth); len(keys) > 0 {
			expectedIdentityKeys[id] = keys
		}
		planType := strings.TrimSpace(internalcodex.EffectivePlanType(auth.Metadata))
		expectedManagementCatalog[id] = managementAuthCatalogSummary(auth, planType)
		expectedUsageCatalog[id] = usageAuthInfoLocked(auth, strings.TrimSpace(auth.Index))
	}
	if !reflect.DeepEqual(manager.backingPathAuthIDs, expectedPaths) || !reflect.DeepEqual(manager.backingPathByAuthID, expectedByID) {
		t.Fatalf("backing path index is inconsistent: paths=%v byID=%v", manager.backingPathAuthIDs, manager.backingPathByAuthID)
	}
	if !reflect.DeepEqual(manager.providerAuthIDs, expectedProviders) || !reflect.DeepEqual(manager.providerByAuthID, expectedProviderByID) {
		t.Fatalf("provider index is inconsistent: providers=%v byID=%v", manager.providerAuthIDs, manager.providerByAuthID)
	}
	if !reflect.DeepEqual(manager.providerPrefixedAuthIDs, expectedProviderPrefixed) {
		t.Fatalf("provider prefix index is inconsistent: got=%v want=%v", manager.providerPrefixedAuthIDs, expectedProviderPrefixed)
	}
	if !reflect.DeepEqual(manager.providerRetryByAuthID, expectedProviderRetryByID) ||
		!reflect.DeepEqual(manager.providerRetryAggregates, expectedProviderRetryAggregates) {
		t.Fatalf("provider retry indexes are inconsistent: byID=%v aggregates=%v", manager.providerRetryByAuthID, manager.providerRetryAggregates)
	}
	if !reflect.DeepEqual(manager.chatGPTWebImageBlockedIDs, expectedImageBlockedIDs) {
		t.Fatalf("image blocked index is inconsistent: got=%v want=%v", manager.chatGPTWebImageBlockedIDs, expectedImageBlockedIDs)
	}
	if len(manager.chatGPTWebImageQuotaChecks) != len(expectedQuotaChecks) {
		t.Fatalf("image quota schedule size = %d, want %d", len(manager.chatGPTWebImageQuotaChecks), len(expectedQuotaChecks))
	}
	for id, instanceID := range expectedQuotaChecks {
		check, ok := manager.chatGPTWebImageQuotaChecks[id]
		if !ok || check.instanceID != instanceID {
			t.Fatalf("image quota schedule for %q = %+v, %t; want instance %q", id, check, ok, instanceID)
		}
	}
	if !reflect.DeepEqual(manager.authIndexesByID, expectedAuthIndexes) {
		t.Fatalf("auth index map is inconsistent: got=%v want=%v", manager.authIndexesByID, expectedAuthIndexes)
	}
	if !reflect.DeepEqual(manager.authIDsByIndex, expectedIDsByIndex) {
		t.Fatalf("reverse auth index is inconsistent: got=%v want=%v", manager.authIDsByIndex, expectedIDsByIndex)
	}
	if !reflect.DeepEqual(manager.managedFileAuthIDs, expectedManagedFiles) || !reflect.DeepEqual(manager.managedFileKeysByAuthID, expectedManagedKeysByID) {
		t.Fatalf("managed filename index is inconsistent: files=%v keys=%v", manager.managedFileAuthIDs, manager.managedFileKeysByAuthID)
	}
	if !reflect.DeepEqual(manager.authPlanTypesByID, expectedPlanTypes) {
		t.Fatalf("auth plan type map is inconsistent: got=%v want=%v", manager.authPlanTypesByID, expectedPlanTypes)
	}
	if !reflect.DeepEqual(manager.managementAuthCatalog, expectedManagementCatalog) {
		t.Fatalf("management catalog is inconsistent: got=%v want=%v", manager.managementAuthCatalog, expectedManagementCatalog)
	}
	if !reflect.DeepEqual(manager.usageAuthCatalog, expectedUsageCatalog) {
		t.Fatalf("usage catalog is inconsistent: got=%v want=%v", manager.usageAuthCatalog, expectedUsageCatalog)
	}
	for id, auth := range expectedDependencies {
		indexed := manager.dependencyAuthsByID[id]
		if indexed == nil || indexed.Provider != auth.Provider || ChatGPTWebCredentialUID(indexed) != ChatGPTWebCredentialUID(auth) || ChatGPTWebLinkedSourceUID(indexed) != ChatGPTWebLinkedSourceUID(auth) {
			t.Fatalf("dependency index entry %q = %#v, want %#v", id, indexed, auth)
		}
	}
	for id := range manager.dependencyAuthsByID {
		if manager.auths[id] != nil && expectedDependencies[id] == nil {
			t.Fatalf("unexpected runtime dependency index entry %q", id)
		}
	}
	for id, keys := range expectedIdentityKeys {
		if !reflect.DeepEqual(manager.chatGPTWebIdentityKeysByID[id], keys) {
			t.Fatalf("identity keys for %q = %v, want %v", id, manager.chatGPTWebIdentityKeysByID[id], keys)
		}
	}
	for id := range manager.chatGPTWebIdentityKeysByID {
		if expectedIdentityKeys[id] == nil && manager.persistedAuthsByID[id] == nil {
			t.Fatalf("unexpected identity index entry %q", id)
		}
	}
}
