package auth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type chatGPTWebDependencyTestStore struct {
	mu          sync.Mutex
	records     map[string]*Auth
	blockMu     sync.Mutex
	blockSaveID string
	saveStarted chan struct{}
	saveRelease <-chan struct{}
}

func newChatGPTWebDependencyTestStore(auths ...*Auth) *chatGPTWebDependencyTestStore {
	store := &chatGPTWebDependencyTestStore{records: make(map[string]*Auth)}
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		clone := auth.Clone()
		store.stamp(clone)
		store.records[clone.ID] = clone
	}
	return store
}

func (store *chatGPTWebDependencyTestStore) List(context.Context) ([]*Auth, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	auths := make([]*Auth, 0, len(store.records))
	for _, auth := range store.records {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (store *chatGPTWebDependencyTestStore) Save(ctx context.Context, auth *Auth) (string, error) {
	store.blockMu.Lock()
	block := auth != nil && auth.ID == store.blockSaveID
	started := store.saveStarted
	release := store.saveRelease
	store.blockMu.Unlock()
	if block {
		if started != nil {
			select {
			case started <- struct{}{}:
			default:
			}
		}
		if release != nil {
			<-release
		}
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if auth == nil {
		return "", errors.New("auth is nil")
	}
	if expectedSourceHash, required := SourceHashSavePrecondition(ctx); required {
		current := store.records[auth.ID]
		if current == nil || authSourceHash(current) != expectedSourceHash {
			return "", NewSaveOutcomeError(SaveOutcomeRolledBack, errors.New("source generation changed"))
		}
	}
	store.stamp(auth)
	store.records[auth.ID] = auth.Clone()
	return auth.FileName, nil
}

func (store *chatGPTWebDependencyTestStore) SaveIfSourceHashMatches(ctx context.Context, auth *Auth, expectedSourceHash string) (string, error) {
	return store.Save(WithSourceHashSavePrecondition(ctx, expectedSourceHash), auth)
}

func (store *chatGPTWebDependencyTestStore) blockSave(id string, started chan struct{}, release <-chan struct{}) {
	store.blockMu.Lock()
	store.blockSaveID = id
	store.saveStarted = started
	store.saveRelease = release
	store.blockMu.Unlock()
}

func (store *chatGPTWebDependencyTestStore) Delete(_ context.Context, id string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.records, id)
	return nil
}

func (store *chatGPTWebDependencyTestStore) DeleteIfSourceHashMatches(_ context.Context, id, expectedSourceHash string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.records[id]
	if current == nil {
		return nil
	}
	if authSourceHash(current) != expectedSourceHash {
		return NewDeleteOutcomeError(DeleteOutcomeRolledBack, errors.New("source generation changed"))
	}
	delete(store.records, id)
	return nil
}

func (store *chatGPTWebDependencyTestStore) replaceOutsideManager(id string, mutate func(*Auth)) {
	store.mu.Lock()
	defer store.mu.Unlock()
	current := store.records[id]
	if current == nil {
		return
	}
	replacement := current.Clone()
	mutate(replacement)
	store.stamp(replacement)
	store.records[id] = replacement
}

func (store *chatGPTWebDependencyTestStore) has(id string) bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.records[id] != nil
}

func (store *chatGPTWebDependencyTestStore) stamp(auth *Auth) {
	data, errMarshal := CanonicalMetadataBytes(auth)
	if errMarshal != nil {
		panic(errMarshal)
	}
	SetSourceHashAttribute(auth, data)
}

func TestBuildChatGPTWebDependencyGraphIndexesOneToManyLinks(t *testing.T) {
	source := dependencyTestCodexAuth("source", "uid-a")
	first := dependencyTestWebAuth("web-a", "uid-a")
	second := dependencyTestWebAuth("web-b", "uid-a")
	graph := BuildChatGPTWebDependencyGraph([]*Auth{second, source, first})

	resolved, ambiguous := graph.SourceByUID("uid-a")
	if ambiguous || resolved == nil || resolved.ID != source.ID {
		t.Fatalf("source = %#v, ambiguous = %v", resolved, ambiguous)
	}
	dependents, ambiguous := graph.DependentsForSource(source)
	if ambiguous || len(dependents) != 2 || dependents[0].ID != "web-a" || dependents[1].ID != "web-b" {
		t.Fatalf("dependents = %#v, ambiguous = %v", dependents, ambiguous)
	}
}

func TestManagerDependencyReservationUsesSourceGenerationCAS(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	originalHash := authSourceHash(source)
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	store.blockSave(source.ID, started, release)
	type reservationResult struct {
		auth *Auth
		err  error
	}
	done := make(chan reservationResult, 1)
	go func() {
		reserved, _, errReserve := manager.ReserveChatGPTWebDependent(t.Context(), source, "web", "web-uid", time.Now())
		done <- reservationResult{auth: reserved, err: errReserve}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dependency reservation did not reach persistence")
	}
	if errDelete := store.DeleteIfSourceHashMatches(t.Context(), source.ID, originalHash); errDelete != nil {
		t.Fatal(errDelete)
	}
	close(release)
	result := <-done
	if result.err == nil || result.auth != nil {
		t.Fatalf("reservation after source deletion = %#v, %v", result.auth, result.err)
	}
	if store.has(source.ID) {
		t.Fatal("source deletion lost the source-generation race")
	}
}

func TestManagerDependencyReservationBlocksOrphanCleanupUntilReleased(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	reserved, reservation, errReserve := manager.ReserveChatGPTWebDependent(t.Context(), source, "web", "web-uid", time.Now())
	if errReserve != nil {
		t.Fatal(errReserve)
	}
	if active := ChatGPTWebActiveDependencyReservations(reserved, time.Now()); len(active) != 1 || active[0].AuthID != "web" {
		t.Fatalf("active reservations = %+v", active)
	}
	retained, current, errRetain := manager.UpdateIfCurrentSourceHash(t.Context(), reserved, RetainCodexAuthForChatGPTWebDependents(reserved, time.Now()))
	if errRetain != nil || !current || retained == nil {
		t.Fatalf("retain source: current=%v auth=%#v err=%v", current, retained, errRetain)
	}
	if deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(t.Context()); errReconcile != nil || len(deleted) != 0 {
		t.Fatalf("reconcile with active reservation = %v, %v", deleted, errReconcile)
	}
	if errRelease := manager.ReleaseChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", reservation, time.Now()); errRelease != nil {
		t.Fatal(errRelease)
	}
	deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(t.Context())
	if errReconcile != nil || len(deleted) != 1 || deleted[0] != source.ID {
		t.Fatalf("reconcile after reservation release = %v, %v", deleted, errReconcile)
	}
	if store.has(source.ID) {
		t.Fatal("orphaned retained source remained after reservation release")
	}
}

func TestManagerDependencyReservationRenewsAndFinalizesExactLease(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	startedAt := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	firstSource, first, errFirst := manager.ReserveChatGPTWebDependent(t.Context(), source, "web", "web-uid", startedAt)
	if errFirst != nil {
		t.Fatal(errFirst)
	}
	secondSource, second, errSecond := manager.ReserveChatGPTWebDependent(t.Context(), firstSource, "web", "web-uid", startedAt)
	if errSecond != nil {
		t.Fatal(errSecond)
	}
	if first.ID == "" || second.ID == "" || first.ID == second.ID {
		t.Fatalf("reservation IDs = %q / %q, want distinct", first.ID, second.ID)
	}
	if active := ChatGPTWebActiveDependencyReservations(secondSource, startedAt); len(active) != 2 {
		t.Fatalf("active reservations = %+v, want two independent leases", active)
	}
	renewAt := startedAt.Add(chatGPTWebDependencyReservationTTL / 2)
	renewedSource, renewed, errRenew := manager.RenewChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", first, renewAt)
	if errRenew != nil {
		t.Fatal(errRenew)
	}
	if renewed.ID != first.ID {
		t.Fatalf("renewed reservation ID = %q, want %q", renewed.ID, first.ID)
	}
	if active := ChatGPTWebActiveDependencyReservations(renewedSource, renewAt); len(active) != 2 {
		t.Fatalf("active reservations after renewal = %+v", active)
	}
	if errFinalize := manager.FinalizeChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", renewed, renewAt); errFinalize != nil {
		t.Fatal(errFinalize)
	}
	current, _ := manager.GetByID(source.ID)
	if chatGPTWebDependencyReservationExists(current, renewed) {
		t.Fatal("finalized reservation remained on source")
	}
}

func TestManagerDependencyReservationRejectsExpiredLease(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	startedAt := time.Date(2026, time.July, 19, 0, 0, 0, 0, time.UTC)
	_, reservation, errReserve := manager.ReserveChatGPTWebDependent(t.Context(), source, "web", "web-uid", startedAt)
	if errReserve != nil {
		t.Fatal(errReserve)
	}
	expiredAt := startedAt.Add(chatGPTWebDependencyReservationTTL)
	if _, _, errRenew := manager.RenewChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", reservation, expiredAt); errRenew == nil {
		t.Fatal("expired dependency reservation was renewed")
	}
	if errFinalize := manager.FinalizeChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", reservation, expiredAt); errFinalize == nil {
		t.Fatal("expired dependency reservation was finalized")
	}
}

func TestManagerDeleteIfCurrentSourceHashDoesNotDeleteReplacement(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	target := registerDependencyTestAuth(t, manager, dependencyTestWebAuth("web", "uid-a"))
	store.replaceOutsideManager(target.ID, func(auth *Auth) {
		auth.Metadata["note"] = "external replacement"
	})
	deleted, errDelete := manager.DeleteIfCurrentSourceHash(t.Context(), target)
	if errDelete == nil || deleted {
		t.Fatalf("DeleteIfCurrentSourceHash() = %v, %v, want stale rejection", deleted, errDelete)
	}
	if !store.has(target.ID) {
		t.Fatal("conditional rollback deleted a replacement credential")
	}
}

func TestBuildChatGPTWebDependencyGraphRejectsDuplicateSourceUID(t *testing.T) {
	first := dependencyTestCodexAuth("source-a", "duplicate")
	second := dependencyTestCodexAuth("source-b", "duplicate")
	graph := BuildChatGPTWebDependencyGraph([]*Auth{first, second, dependencyTestWebAuth("web", "duplicate")})

	if source, ambiguous := graph.SourceByUID("duplicate"); source != nil || !ambiguous {
		t.Fatalf("source = %#v, ambiguous = %v", source, ambiguous)
	}
	if dependents, ambiguous := graph.DependentsForSource(first); len(dependents) != 0 || !ambiguous {
		t.Fatalf("dependents = %#v, ambiguous = %v", dependents, ambiguous)
	}
}

func TestBuildChatGPTWebDependencyGraphRejectsMismatchedSourceID(t *testing.T) {
	source := dependencyTestCodexAuth("source", "uid-a")
	web := dependencyTestWebAuth("web", "uid-a")
	web.Metadata[chatGPTWebSourceAuthIDKey] = "replacement"
	graph := BuildChatGPTWebDependencyGraph([]*Auth{source, web})

	if dependents, ambiguous := graph.DependentsForSource(source); ambiguous || len(dependents) != 0 {
		t.Fatalf("dependents = %#v, ambiguous = %v, want mismatched source ID ignored", dependents, ambiguous)
	}
}

func TestBuildChatGPTWebDependencyGraphRejectsMismatchedSourceIdentity(t *testing.T) {
	source := dependencyTestCodexAuth("source", "uid-a")
	source.Metadata["account_id"] = "account-a"
	identitySource := source.Clone()
	identitySource.Provider = "chatgpt-web"
	web := dependencyTestWebAuth("web", "uid-a")
	web.Metadata["source_identity"] = ChatGPTWebCredentialReferenceValue(identitySource)

	replacement := source.Clone()
	replacement.Metadata = map[string]any{
		"type":                     "codex",
		chatGPTWebCredentialUIDKey: "uid-a",
		"access_token":             "replacement-token",
		"account_id":               "account-b",
	}
	graph := BuildChatGPTWebDependencyGraph([]*Auth{replacement, web})
	if dependents, ambiguous := graph.DependentsForSource(replacement); ambiguous || len(dependents) != 0 {
		t.Fatalf("dependents = %#v, ambiguous = %v, want mismatched source identity ignored", dependents, ambiguous)
	}
}

func TestDependencyMutationStaleContextReacquiresLock(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	staleCtx, unlockStale := manager.lockChatGPTWebDependencyMutationContext(t.Context(), "", nil, true)
	unlockStale()
	_, unlockHeld := manager.lockChatGPTWebDependencyMutationContext(t.Context(), "", nil, true)
	acquired := make(chan struct{})
	go func() {
		_ = manager.WithChatGPTWebDependencyMutation(staleCtx, func(context.Context) error {
			close(acquired)
			return nil
		})
	}()
	select {
	case <-acquired:
		unlockHeld()
		t.Fatal("stale dependency context bypassed the active lock")
	case <-time.After(50 * time.Millisecond):
	}
	unlockHeld()
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("stale dependency context did not reacquire after unlock")
	}
}

func TestManagerReconcileRetainedCodexSourceWaitsForLastDependent(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	first := registerDependencyTestAuth(t, manager, dependencyTestWebAuth("web-a", "uid-a"))
	second := registerDependencyTestAuth(t, manager, dependencyTestWebAuth("web-b", "uid-a"))
	retained, current, errRetain := manager.UpdateIfCurrent(context.Background(), source, RetainCodexAuthForChatGPTWebDependents(source, time.Now()))
	if errRetain != nil || !current || retained == nil {
		t.Fatalf("retain source: current=%v auth=%#v err=%v", current, retained, errRetain)
	}

	deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(context.Background())
	if errReconcile != nil || len(deleted) != 0 {
		t.Fatalf("reconcile with dependents = %v, %v", deleted, errReconcile)
	}
	if errDelete := manager.Delete(context.Background(), first.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	deleted, errReconcile = manager.ReconcileChatGPTWebDependencies(context.Background())
	if errReconcile != nil || len(deleted) != 0 {
		t.Fatalf("reconcile with one dependent = %v, %v", deleted, errReconcile)
	}
	if errDelete := manager.Delete(context.Background(), second.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	deleted, errReconcile = manager.ReconcileChatGPTWebDependencies(context.Background())
	if errReconcile != nil || len(deleted) != 1 || deleted[0] != source.ID {
		t.Fatalf("final reconcile = %v, %v", deleted, errReconcile)
	}
	if _, ok := manager.GetByID(source.ID); ok || store.has(source.ID) {
		t.Fatal("orphaned retained source was not removed")
	}
}

func TestManagerReconcileRetainedCodexSourceRejectsExternalReplacement(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	retained, current, errRetain := manager.UpdateIfCurrent(context.Background(), source, RetainCodexAuthForChatGPTWebDependents(source, time.Now()))
	if errRetain != nil || !current || retained == nil {
		t.Fatalf("retain source: current=%v auth=%#v err=%v", current, retained, errRetain)
	}
	store.replaceOutsideManager(source.ID, func(replacement *Auth) {
		replacement.Metadata["access_token"] = "external-replacement"
	})

	deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(context.Background())
	if errReconcile == nil || len(deleted) != 0 {
		t.Fatalf("reconcile after replacement = %v, %v", deleted, errReconcile)
	}
	if _, ok := manager.GetByID(source.ID); !ok || !store.has(source.ID) {
		t.Fatal("externally replaced source was deleted")
	}
}

func TestManagerReconcileWaitsForConcurrentLinkedWebUpdate(t *testing.T) {
	store := newChatGPTWebDependencyTestStore()
	manager := NewManager(store, nil, nil)
	source := registerDependencyTestAuth(t, manager, dependencyTestCodexAuth("source", "uid-a"))
	web := dependencyTestWebAuth("web", "")
	web.Metadata[chatGPTWebRefreshStrategyKey] = "token_only"
	delete(web.Metadata, chatGPTWebSourceCredentialUIDKey)
	web = registerDependencyTestAuth(t, manager, web)
	retained, current, errRetain := manager.UpdateIfCurrent(t.Context(), source, RetainCodexAuthForChatGPTWebDependents(source, time.Now()))
	if errRetain != nil || !current || retained == nil {
		t.Fatalf("retain source: current=%v auth=%#v err=%v", current, retained, errRetain)
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	store.blockSave(web.ID, started, release)
	updated := web.Clone()
	updated.Metadata[chatGPTWebRefreshStrategyKey] = "codex_source"
	updated.Metadata[chatGPTWebSourceCredentialUIDKey] = "uid-a"
	updateDone := make(chan error, 1)
	go func() {
		_, errUpdate := manager.Update(t.Context(), updated)
		updateDone <- errUpdate
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("linked Web update did not reach persistence")
	}
	reconcileDone := make(chan struct {
		deleted []string
		err     error
	}, 1)
	go func() {
		deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(t.Context())
		reconcileDone <- struct {
			deleted []string
			err     error
		}{deleted: deleted, err: errReconcile}
	}()
	select {
	case result := <-reconcileDone:
		t.Fatalf("reconcile returned before linked update committed: deleted=%v err=%v", result.deleted, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if errUpdate := <-updateDone; errUpdate != nil {
		t.Fatal(errUpdate)
	}
	result := <-reconcileDone
	if result.err != nil || len(result.deleted) != 0 {
		t.Fatalf("reconcile after linked update = %v, %v", result.deleted, result.err)
	}
	if _, ok := manager.GetByID(source.ID); !ok || !store.has(source.ID) {
		t.Fatal("retained source was deleted during linked Web update")
	}
}

func TestManagerLoadForcesRetainedCodexSourceDisabled(t *testing.T) {
	source := dependencyTestCodexAuth("source", "uid-a")
	source.Metadata[chatGPTWebDeletionStateKey] = ChatGPTWebDeletionStateRetained
	source.Metadata["disabled"] = false
	source.Disabled = false
	source.Status = StatusActive
	manager := NewManager(newChatGPTWebDependencyTestStore(source), nil, nil)
	if errLoad := manager.Load(context.Background()); errLoad != nil {
		t.Fatal(errLoad)
	}
	loaded, ok := manager.GetByID(source.ID)
	if !ok || loaded == nil || !loaded.Disabled || loaded.Status != StatusDisabled || !ChatGPTWebAuthRetainedForDependents(loaded) {
		t.Fatalf("loaded retained source = %#v", loaded)
	}
}

func TestManagerReconcileUsesPersistedDependenciesIgnoredByRuntimeDeduplication(t *testing.T) {
	source := RetainCodexAuthForChatGPTWebDependents(dependencyTestCodexAuth("source", "uid-a"), time.Now())
	linked := dependencyTestWebAuth("z-linked-web", "uid-a")
	linked.Metadata["email"] = "duplicate@example.com"
	preferred := dependencyTestWebAuth("a-preferred-web", "")
	preferred.Metadata["email"] = "duplicate@example.com"
	preferred.Metadata[chatGPTWebRefreshStrategyKey] = "token_only"
	delete(preferred.Metadata, chatGPTWebSourceAuthIDKey)
	delete(preferred.Metadata, chatGPTWebSourceCredentialUIDKey)
	store := newChatGPTWebDependencyTestStore(source, linked, preferred)
	manager := NewManager(store, nil, nil)
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	if _, loaded := manager.GetByID(linked.ID); loaded {
		t.Fatal("linked duplicate unexpectedly remained in the deduplicated runtime set")
	}

	deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(t.Context())
	if errReconcile != nil || len(deleted) != 0 {
		t.Fatalf("reconcile with persisted duplicate dependency = %v, %v", deleted, errReconcile)
	}
	if _, loaded := manager.GetByID(source.ID); !loaded || !store.has(source.ID) {
		t.Fatal("retained source was deleted despite a persisted linked Web credential")
	}
}

func TestRestoreCodexAuthFromRetentionPreservesCooldown(t *testing.T) {
	now := time.Now().UTC()
	retryAt := now.Add(20 * time.Minute)
	source := dependencyTestCodexAuth("source", "uid-a")
	source.Unavailable = true
	source.NextRetryAfter = retryAt
	source.CooldownScope = "auth"
	source.Status = StatusError
	retained := RetainCodexAuthForChatGPTWebDependents(source, now)
	restored, ok := RestoreCodexAuthFromChatGPTWebRetention(retained, now.Add(time.Minute))
	if !ok || restored.Disabled || restored.Status != StatusError || !restored.Unavailable || !restored.NextRetryAfter.Equal(retryAt) || restored.CooldownScope != "auth" {
		t.Fatalf("restored source = %#v", restored)
	}
}

func dependencyTestCodexAuth(id, uid string) *Auth {
	return &Auth{
		ID:       id,
		Provider: "codex",
		FileName: id + ".json",
		Status:   StatusActive,
		Metadata: map[string]any{"type": "codex", chatGPTWebCredentialUIDKey: uid, "access_token": "token"},
	}
}

func dependencyTestWebAuth(id, sourceUID string) *Auth {
	return &Auth{
		ID:       id,
		Provider: "chatgpt-web",
		FileName: id + ".json",
		Status:   StatusActive,
		Metadata: map[string]any{
			"type":                           "chatgpt-web",
			chatGPTWebRefreshStrategyKey:     "codex_source",
			chatGPTWebSourceAuthIDKey:        "source",
			chatGPTWebSourceCredentialUIDKey: sourceUID,
			"access_token":                   "token",
		},
	}
}

func registerDependencyTestAuth(t *testing.T, manager *Manager, auth *Auth) *Auth {
	t.Helper()
	installed, errRegister := manager.Register(context.Background(), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return installed
}
