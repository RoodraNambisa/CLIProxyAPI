package auth

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type resultPersistenceTestStore struct {
	mu sync.Mutex

	records          map[string]*Auth
	concurrency      int
	admission        int
	blockSaves       bool
	releaseSave      <-chan struct{}
	saveStarted      chan string
	forceFirstStale  bool
	staleObserved    chan struct{}
	staleObservedOne sync.Once
	saveCalls        int
	activeSaves      atomic.Int64
	peakSaves        atomic.Int64
}

type resultPersistencePlainStore struct {
	inner *resultPersistenceTestStore
}

func newResultPersistenceTestStore(concurrency, admission int, auths ...*Auth) *resultPersistenceTestStore {
	store := &resultPersistenceTestStore{
		records:     make(map[string]*Auth),
		concurrency: concurrency,
		admission:   admission,
	}
	for _, auth := range auths {
		store.seed(auth)
	}
	return store
}

func (store *resultPersistenceTestStore) seed(auth *Auth) {
	if store == nil || auth == nil {
		return
	}
	clone := auth.Clone()
	resultPersistenceStampAuth(clone)
	store.mu.Lock()
	store.records[clone.ID] = clone
	store.mu.Unlock()
}

func resultPersistenceStampAuth(auth *Auth) {
	raw, err := CanonicalMetadataBytes(auth)
	if err != nil {
		panic(err)
	}
	SetSourceHashAttribute(auth, raw)
}

func (store *resultPersistenceTestStore) List(context.Context) ([]*Auth, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	auths := make([]*Auth, 0, len(store.records))
	for _, auth := range store.records {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (store *resultPersistenceTestStore) Save(ctx context.Context, auth *Auth) (string, error) {
	if auth == nil {
		return "", errors.New("auth is nil")
	}
	call := 0
	store.mu.Lock()
	store.saveCalls++
	call = store.saveCalls
	store.mu.Unlock()
	active := store.activeSaves.Add(1)
	defer store.activeSaves.Add(-1)
	for {
		peak := store.peakSaves.Load()
		if active <= peak || store.peakSaves.CompareAndSwap(peak, active) {
			break
		}
	}
	if store.saveStarted != nil {
		select {
		case store.saveStarted <- auth.ID:
		default:
		}
	}
	if store.blockSaves && store.releaseSave != nil {
		select {
		case <-store.releaseSave:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if call == 1 && store.forceFirstStale {
		current := store.records[auth.ID]
		if current != nil {
			replacement := current.Clone()
			replacement.Metadata["access_token"] = "fresh-external-token"
			resultPersistenceStampAuth(replacement)
			store.records[auth.ID] = replacement
		}
		store.staleObservedOne.Do(func() {
			if store.staleObserved != nil {
				close(store.staleObserved)
			}
		})
		return "", NewSaveOutcomeError(SaveOutcomeRolledBack, errors.New("source generation changed"))
	}
	if expectedSourceHash, required := SourceHashSavePrecondition(ctx); required {
		current := store.records[auth.ID]
		if current == nil || authSourceHash(current) != expectedSourceHash {
			return "", NewSaveOutcomeError(SaveOutcomeRolledBack, errors.New("source generation changed"))
		}
	}
	resultPersistenceStampAuth(auth)
	store.records[auth.ID] = auth.Clone()
	return auth.FileName, nil
}

func (store *resultPersistenceTestStore) SaveIfSourceHashMatches(ctx context.Context, auth *Auth, expectedSourceHash string) (string, error) {
	return store.Save(WithSourceHashSavePrecondition(ctx, expectedSourceHash), auth)
}

func (store *resultPersistenceTestStore) Delete(_ context.Context, id string) error {
	store.mu.Lock()
	delete(store.records, id)
	store.mu.Unlock()
	return nil
}

func (store *resultPersistenceTestStore) RefreshPersistenceConcurrency() int {
	return store.concurrency
}

func (store *resultPersistenceTestStore) RefreshPersistenceAdmissionConcurrency() int {
	return store.admission
}

func (store *resultPersistencePlainStore) List(ctx context.Context) ([]*Auth, error) {
	return store.inner.List(ctx)
}

func (store *resultPersistencePlainStore) Save(ctx context.Context, auth *Auth) (string, error) {
	return store.inner.Save(ctx, auth)
}

func (store *resultPersistencePlainStore) Delete(ctx context.Context, id string) error {
	return store.inner.Delete(ctx, id)
}

func (store *resultPersistencePlainStore) RefreshPersistenceConcurrency() int {
	return store.inner.RefreshPersistenceConcurrency()
}

func (store *resultPersistenceTestStore) snapshot(id string) *Auth {
	store.mu.Lock()
	defer store.mu.Unlock()
	if auth := store.records[id]; auth != nil {
		return auth.Clone()
	}
	return nil
}

func (store *resultPersistenceTestStore) calls() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.saveCalls
}

func resultPersistenceTestAuth(id string, quota int) *Auth {
	return &Auth{
		ID:       id,
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":          "stale",
			"refresh_token":         "refresh-token",
			"lifecycle_state":       LifecycleStateActive,
			"image_quota_remaining": quota,
		},
	}
}

func registerResultPersistenceTestAuth(t *testing.T, manager *Manager, store *resultPersistenceTestStore, id string) *Auth {
	t.Helper()
	seeded := store.snapshot(id)
	if seeded == nil {
		t.Fatalf("store auth %s is missing", id)
	}
	registered, err := manager.Register(WithSkipPersist(t.Context()), seeded)
	if err != nil {
		t.Fatalf("Register(%s) error: %v", id, err)
	}
	return registered
}

func projectResultPersistenceImageQuota(manager *Manager, authID string, count int64) {
	auth, _ := manager.GetByID(authID)
	result := resultForAuth(auth, "chatgpt-web", "gpt-image-2", false)
	result.imageSuccessCount = count
	result.imageSuccessModels = []string{"gpt-image-2"}
	result.quotaProjectionOnly = true
	manager.markExecutionResult(context.Background(), result)
}

func waitForResultPersistenceCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal(description)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestResultPersistenceDoesNotBlockResultMutationAndCoalescesLatestState(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-coalesce", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 8)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer func() {
		release()
		_ = manager.CloseExecutors()
	}()

	started := time.Now()
	projectResultPersistenceImageQuota(manager, registered.ID, 1)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("result mutation waited %s for durable persistence", elapsed)
	}
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("background result persistence did not start")
	}
	for range 3 {
		projectResultPersistenceImageQuota(manager, registered.ID, 1)
	}
	release()
	waitForResultPersistenceCondition(t, 3*time.Second, "latest coalesced quota was not persisted", func() bool {
		persisted := store.snapshot(registered.ID)
		return persisted != nil && metadataInt(persisted.Metadata["image_quota_remaining"]) != nil &&
			*metadataInt(persisted.Metadata["image_quota_remaining"]) == 6
	})
	if calls := store.calls(); calls > 2 {
		t.Fatalf("coalesced result writes = %d, want at most 2", calls)
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.Coalesced < 3 || metrics.Persisted == 0 {
		t.Fatalf("result persistence metrics = %#v", metrics)
	}
}

func TestResultPersistenceRetriesStaleSourceAgainstCurrentRuntimeGeneration(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-stale-source", 10)
	store := newResultPersistenceTestStore(1, 1, auth)
	store.forceFirstStale = true
	store.staleObserved = make(chan struct{})
	manager := NewManager(store, &FillFirstSelector{}, nil)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	t.Cleanup(func() { _ = manager.CloseExecutors() })

	projectResultPersistenceImageQuota(manager, registered.ID, 1)
	select {
	case <-store.staleObserved:
	case <-time.After(time.Second):
		t.Fatal("stale source save was not observed")
	}
	if calls := store.calls(); calls != 1 {
		t.Fatalf("save calls after stale result = %d, want 1", calls)
	}
	time.Sleep(100 * time.Millisecond)
	if calls := store.calls(); calls != 1 {
		t.Fatalf("stale result retried in a busy loop: %d calls", calls)
	}

	external := store.snapshot(registered.ID)
	if external == nil || authAccessToken(external) != "fresh-external-token" {
		t.Fatalf("external replacement = %#v", external)
	}
	unlockResult := manager.lockResultMutation(registered.ID)
	manager.mu.Lock()
	current := manager.auths[registered.ID]
	current.Metadata["access_token"] = authAccessToken(external)
	current.Attributes[SourceHashAttributeKey] = authSourceHash(external)
	manager.mu.Unlock()
	unlockResult()

	waitForResultPersistenceCondition(t, 4*time.Second, "stale result was not replayed on the fresh source generation", func() bool {
		persisted := store.snapshot(registered.ID)
		if persisted == nil {
			return false
		}
		quota := metadataInt(persisted.Metadata["image_quota_remaining"])
		return authAccessToken(persisted) == "fresh-external-token" && quota != nil && *quota == 9
	})
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.Retries == 0 || metrics.Terminal != 0 {
		t.Fatalf("stale result persistence metrics = %#v", metrics)
	}
}

func TestResultPersistenceSerializesWithCredentialRefreshWithoutOverwritingFreshState(t *testing.T) {
	for _, testCase := range []struct {
		name         string
		refreshFirst bool
	}{
		{name: "refresh_holds_durable_lock_first", refreshFirst: true},
		{name: "result_holds_durable_lock_first", refreshFirst: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			auth := resultPersistenceTestAuth("result-persistence-refresh-interleave-"+testCase.name, 10)
			auth.Metadata["access_token"] = "old-access-token"
			auth.Metadata["refresh_token"] = "old-refresh-token"
			auth.Metadata["session_id"] = "old-session"
			releaseSave := make(chan struct{})
			store := newResultPersistenceTestStore(1, 1, auth)
			store.blockSaves = true
			store.releaseSave = releaseSave
			store.saveStarted = make(chan string, 8)
			manager := NewManager(store, &FillFirstSelector{}, nil)
			registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
			defer func() {
				release()
				_ = manager.CloseExecutors()
			}()

			manager.mu.RLock()
			expected := manager.auths[registered.ID]
			baseline := expected.Clone()
			manager.mu.RUnlock()
			updated := baseline.Clone()
			updated.Metadata["access_token"] = "fresh-access-token"
			updated.Metadata["refresh_token"] = "fresh-refresh-token"
			updated.Metadata["session_id"] = "fresh-session"

			type refreshResult struct {
				auth *Auth
				err  error
			}
			refreshDone := make(chan refreshResult, 1)
			startRefresh := func() {
				go func() {
					saved, errRefresh := manager.applyRefreshedAuthDurably(
						context.Background(),
						"chatgpt-web",
						expected,
						baseline,
						updated,
						time.Time{},
					)
					refreshDone <- refreshResult{auth: saved, err: errRefresh}
				}()
			}
			waitForFirstSave := func() {
				select {
				case startedID := <-store.saveStarted:
					if startedID != registered.ID {
						t.Fatalf("first persisted auth = %q, want %q", startedID, registered.ID)
					}
				case <-time.After(time.Second):
					t.Fatal("first interleaved persistence did not start")
				}
			}

			if testCase.refreshFirst {
				startRefresh()
				waitForFirstSave()
				projectResultPersistenceImageQuota(manager, registered.ID, 1)
			} else {
				projectResultPersistenceImageQuota(manager, registered.ID, 1)
				waitForFirstSave()
				startRefresh()
			}
			release()

			select {
			case result := <-refreshDone:
				if result.err != nil || result.auth == nil {
					t.Fatalf("credential refresh = (%#v, %v)", result.auth, result.err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("credential refresh did not finish")
			}
			waitForResultPersistenceCondition(t, 4*time.Second, "fresh credential and latest quota were not durably merged", func() bool {
				persisted := store.snapshot(registered.ID)
				if persisted == nil {
					return false
				}
				quota := metadataInt(persisted.Metadata["image_quota_remaining"])
				return authAccessToken(persisted) == "fresh-access-token" &&
					chatGPTWebIdentityMetadataString(persisted.Metadata, "refresh_token") == "fresh-refresh-token" &&
					chatGPTWebIdentityMetadataString(persisted.Metadata, "session_id") == "fresh-session" &&
					quota != nil && *quota == 9
			})

			current, ok := manager.GetByID(registered.ID)
			if !ok || current == nil {
				t.Fatal("refreshed runtime credential is missing")
			}
			quota := metadataInt(current.Metadata["image_quota_remaining"])
			if authAccessToken(current) != "fresh-access-token" ||
				chatGPTWebIdentityMetadataString(current.Metadata, "refresh_token") != "fresh-refresh-token" ||
				chatGPTWebIdentityMetadataString(current.Metadata, "session_id") != "fresh-session" ||
				quota == nil || *quota != 9 {
				t.Fatalf("refreshed runtime state = %#v", current.Metadata)
			}
		})
	}
}

func TestResultPersistenceDoesNotRestoreStaleSchedulerInstance(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-stale-scheduler", 10)
	store := newResultPersistenceTestStore(1, 1, auth)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(registered.ID)
		_ = manager.CloseExecutors()
	})

	stale := registered.Clone()
	manager.mu.Lock()
	current := manager.auths[registered.ID].Clone()
	current.Metadata["access_token"] = "fresh-access-token"
	current.UpdatedAt = stale.UpdatedAt.Add(time.Second)
	current.installationID = "fresh-installation"
	current.instanceID = "fresh-instance"
	current.instanceState = &authInstanceState{}
	manager.installAuthLocked(current.ID, current)
	manager.mu.Unlock()
	manager.scheduler.upsertAuth(current.Clone())

	manager.upsertCurrentResultAuthState(stale)
	selected, errPick := manager.scheduler.pickSingle(t.Context(), registered.Provider, model, cliproxyexecutor.Options{}, nil)
	if errPick != nil || selected == nil {
		t.Fatalf("pick current scheduler auth = %#v, %v", selected, errPick)
	}
	if selected.instanceID != current.instanceID || authAccessToken(selected) != "fresh-access-token" {
		t.Fatalf("stale result restored scheduler auth = %#v", selected)
	}
}

func TestResultPersistenceUsesSharedFileStoreAdmission(t *testing.T) {
	first := resultPersistenceTestAuth("result-persistence-file-first", 10)
	second := resultPersistenceTestAuth("result-persistence-file-second", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, first, second)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 8)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	registerResultPersistenceTestAuth(t, manager, store, first.ID)
	registerResultPersistenceTestAuth(t, manager, store, second.ID)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer func() {
		release()
		_ = manager.CloseExecutors()
	}()

	projectResultPersistenceImageQuota(manager, first.ID, 1)
	projectResultPersistenceImageQuota(manager, second.ID, 1)
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("first file-store result persistence did not start")
	}
	time.Sleep(50 * time.Millisecond)
	if calls := store.calls(); calls != 1 {
		t.Fatalf("file store received %d concurrent result writes, want 1", calls)
	}
	if peak := store.peakSaves.Load(); peak != 1 {
		t.Fatalf("file store peak result writes = %d, want 1", peak)
	}
	release()
	for _, id := range []string{first.ID, second.ID} {
		id := id
		waitForResultPersistenceCondition(t, 3*time.Second, "file-store result was not persisted for "+id, func() bool {
			persisted := store.snapshot(id)
			if persisted == nil {
				return false
			}
			quota := metadataInt(persisted.Metadata["image_quota_remaining"])
			return quota != nil && *quota == 9
		})
	}
}

func TestResultPersistenceReplenishesIdleWorkersWhileAnotherSaveIsActive(t *testing.T) {
	first := resultPersistenceTestAuth("result-persistence-worker-active", 10)
	second := resultPersistenceTestAuth("result-persistence-worker-replenished", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(2, 2, first, second)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 4)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultPersistence.workerLimit = 2
	registerResultPersistenceTestAuth(t, manager, store, first.ID)
	registerResultPersistenceTestAuth(t, manager, store, second.ID)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer func() {
		release()
		_ = manager.CloseExecutors()
	}()

	projectResultPersistenceImageQuota(manager, first.ID, 1)
	select {
	case startedID := <-store.saveStarted:
		if startedID != first.ID {
			t.Fatalf("first active save = %q, want %q", startedID, first.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("first result persistence did not start")
	}
	waitForResultPersistenceCondition(t, 2*time.Second, "idle result worker did not exit beside the active save", func() bool {
		manager.resultPersistence.mu.Lock()
		workers := manager.resultPersistence.liveWorkers
		manager.resultPersistence.mu.Unlock()
		return workers == 1
	})

	projectResultPersistenceImageQuota(manager, second.ID, 1)
	select {
	case startedID := <-store.saveStarted:
		if startedID != second.ID {
			t.Fatalf("replenished worker save = %q, want %q", startedID, second.ID)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("result worker pool was not replenished while another save remained active")
	}
	if peak := store.peakSaves.Load(); peak != 2 {
		t.Fatalf("peak concurrent saves = %d, want 2 after worker replenishment", peak)
	}
	release()
}

func TestResultPersistenceReacquiresAdmissionAfterStoreReplacement(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-store-replacement", 10)
	initialStore := newResultPersistenceTestStore(1, 1, auth)
	replacementInner := newResultPersistenceTestStore(1, 1, auth)
	replacementStore := &resultPersistencePlainStore{inner: replacementInner}
	manager := NewManager(initialStore, &FillFirstSelector{}, nil)
	registered := registerResultPersistenceTestAuth(t, manager, initialStore, auth.ID)
	reachedBeforeLock := make(chan struct{})
	releaseBeforeLock := make(chan struct{})
	var reachedOnce sync.Once
	manager.resultPersistence.beforeLock = func(string) {
		reachedOnce.Do(func() { close(reachedBeforeLock) })
		<-releaseBeforeLock
	}
	defer func() {
		select {
		case <-releaseBeforeLock:
		default:
			close(releaseBeforeLock)
		}
		_ = manager.CloseExecutors()
	}()

	projectResultPersistenceImageQuota(manager, registered.ID, 1)
	select {
	case <-reachedBeforeLock:
	case <-time.After(time.Second):
		t.Fatal("result persistence did not reach the admission/store replacement barrier")
	}
	manager.SetStore(replacementStore)
	close(releaseBeforeLock)
	waitForResultPersistenceCondition(t, 3*time.Second, "result state was not persisted to the replacement store", func() bool {
		persisted := replacementInner.snapshot(registered.ID)
		if persisted == nil {
			return false
		}
		quota := metadataInt(persisted.Metadata["image_quota_remaining"])
		return quota != nil && *quota == 9
	})
	if calls := initialStore.calls(); calls != 0 {
		t.Fatalf("superseded store save calls = %d, want zero", calls)
	}
	if calls := replacementInner.calls(); calls != 1 {
		t.Fatalf("replacement store save calls = %d, want one", calls)
	}
}

func TestResultPersistenceOverflowRescanSkipsTrackedPrefix(t *testing.T) {
	first := resultPersistenceTestAuth("a-result-persistence-tracked-prefix", 10)
	overflow := resultPersistenceTestAuth("z-result-persistence-overflow", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, first, overflow)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 8)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultPersistence.queueLimit = 1
	manager.resultPersistence.workerLimit = 1
	registerResultPersistenceTestAuth(t, manager, store, first.ID)
	registerResultPersistenceTestAuth(t, manager, store, overflow.ID)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer func() {
		release()
		_ = manager.CloseExecutors()
	}()

	projectResultPersistenceImageQuota(manager, first.ID, 1)
	select {
	case startedID := <-store.saveStarted:
		if startedID != first.ID {
			t.Fatalf("first persisted auth = %q, want tracked prefix %q", startedID, first.ID)
		}
	case <-time.After(time.Second):
		t.Fatal("tracked prefix persistence did not start")
	}
	projectResultPersistenceImageQuota(manager, overflow.ID, 1)
	if metrics := manager.RefreshPersistenceMetrics().ResultPersistence; metrics.BackpressureEvents != 1 {
		t.Fatalf("overflow backpressure metrics = %#v", metrics)
	}
	release()
	for _, id := range []string{first.ID, overflow.ID} {
		id := id
		waitForResultPersistenceCondition(t, 4*time.Second, "overflow result was not persisted for "+id, func() bool {
			persisted := store.snapshot(id)
			if persisted == nil {
				return false
			}
			quota := metadataInt(persisted.Metadata["image_quota_remaining"])
			return quota != nil && *quota == 9
		})
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.Rescans == 0 || metrics.Rescans > 4 {
		t.Fatalf("overflow rescan count = %d, want bounded progress", metrics.Rescans)
	}
}

type resultPersistenceBlockingExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (executor *resultPersistenceBlockingExecutor) Execute(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.once.Do(func() { close(executor.started) })
	select {
	case <-executor.release:
	case <-ctx.Done():
		return cliproxyexecutor.Response{}, ctx.Err()
	}
	state, _ := opts.Metadata[cliproxyexecutor.ImageGenerationResultStateMetadataKey].(*cliproxyexecutor.ImageGenerationResultState)
	state.AddProduced(1)
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (*resultPersistenceBlockingExecutor) Close() error { return nil }

func TestCloseExecutorsDrainsResultProducedByInFlightExecution(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-close-inflight", 10)
	store := newResultPersistenceTestStore(1, 1, auth)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	releaseExecution := make(chan struct{})
	executor := &resultPersistenceBlockingExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{},
		started:                               make(chan struct{}),
		release:                               releaseExecution,
	}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{
			Metadata: map[string]any{cliproxyexecutor.ImageGenerationMaxResultsMetadataKey: 1},
		})
		executeDone <- errExecute
	}()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("blocking execution did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.CloseExecutors() }()
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before the in-flight result: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseExecution)
	if errExecute := <-executeDone; errExecute != nil {
		t.Fatalf("Execute() error: %v", errExecute)
	}
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseExecutors() did not drain the in-flight result")
	}
	persisted := store.snapshot(registered.ID)
	if persisted == nil {
		t.Fatal("persisted in-flight result is missing")
	}
	quota := metadataInt(persisted.Metadata["image_quota_remaining"])
	if quota == nil || *quota != 9 {
		t.Fatalf("persisted in-flight result = %#v", persisted)
	}
}

type resultPersistenceHangingStreamExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	remaining chan cliproxyexecutor.StreamChunk
}

func TestDiscardStreamChunksHasBoundedCleanupForUncooperativeSources(t *testing.T) {
	t.Run("shutdown cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		remaining := make(chan cliproxyexecutor.StreamChunk)
		done := discardStreamChunksWithin(ctx, remaining, time.Hour)
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stream discard goroutine did not stop after shutdown cancellation")
		}
	})

	t.Run("fallback is asynchronous and bounded", func(t *testing.T) {
		remaining := make(chan cliproxyexecutor.StreamChunk)
		ctx, cancel := context.WithCancel(context.Background())
		returned := make(chan (<-chan struct{}), 1)
		go func() {
			returned <- discardStreamChunksWithin(ctx, remaining, time.Hour)
		}()
		var asynchronousDone <-chan struct{}
		select {
		case asynchronousDone = <-returned:
		case <-time.After(time.Second):
			cancel()
			t.Fatal("stream discard waited for cleanup before returning")
		}
		cancel()
		select {
		case <-asynchronousDone:
		case <-time.After(time.Second):
			t.Fatal("asynchronous stream discard did not stop after cancellation")
		}

		done := discardStreamChunksWithin(context.Background(), remaining, 20*time.Millisecond)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stream discard goroutine exceeded its fallback cleanup bound")
		}
	})
}

func (executor *resultPersistenceHangingStreamExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	executor.remaining <- cliproxyexecutor.StreamChunk{Payload: []byte("data: started\n\n")}
	return &cliproxyexecutor.StreamResult{Chunks: executor.remaining}, nil
}

func (*resultPersistenceHangingStreamExecutor) Close() error { return nil }

func TestCloseExecutorsCancelsUnconsumedStreamProducer(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-close-stream", 10)
	store := newResultPersistenceTestStore(1, 1, auth)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultProducerWaitTimeout = 20 * time.Millisecond
	manager.resultProducerCancelWait = 500 * time.Millisecond
	executor := &resultPersistenceHangingStreamExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{},
		remaining:                             make(chan cliproxyexecutor.StreamChunk, 1),
	}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })

	stream, errStream := manager.ExecuteStream(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
	if errStream != nil || stream == nil {
		t.Fatalf("ExecuteStream() = %#v, %v", stream, errStream)
	}
	started := time.Now()
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("CloseExecutors() took %s for an unconsumed stream", elapsed)
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.ProducerActive != 0 || metrics.ProducerTimeouts != 1 || metrics.ProducerAbandoned != 0 {
		t.Fatalf("stream producer close metrics = %#v", metrics)
	}
}

type resultPersistenceSuccessfulStreamExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
}

func (*resultPersistenceSuccessfulStreamExecutor) ExecuteStream(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	state, _ := opts.Metadata[cliproxyexecutor.ImageGenerationResultStateMetadataKey].(*cliproxyexecutor.ImageGenerationResultState)
	state.AddProduced(1)
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: completed\n\n")}
	chunks <- cliproxyexecutor.SuccessfulStreamTerminalChunk()
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*resultPersistenceSuccessfulStreamExecutor) Close() error { return nil }

func TestCloseExecutorsDrainsCompletedStreamResult(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-close-completed-stream", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 8)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &resultPersistenceSuccessfulStreamExecutor{chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{}}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer release()

	stream, errStream := manager.ExecuteStream(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ImageGenerationMaxResultsMetadataKey: 1},
	})
	if errStream != nil || stream == nil {
		t.Fatalf("ExecuteStream() = %#v, %v", stream, errStream)
	}
	for range stream.Chunks {
	}
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("completed stream result did not reach background persistence")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.CloseExecutors() }()
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before completed stream persistence: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CloseExecutors() did not drain completed stream persistence")
	}
	persisted := store.snapshot(registered.ID)
	if persisted == nil {
		t.Fatal("persisted completed stream result is missing")
	}
	quota := metadataInt(persisted.Metadata["image_quota_remaining"])
	if quota == nil || *quota != 9 {
		t.Fatalf("persisted completed stream result = %#v", persisted)
	}
}

func TestCloseExecutorsDrainsSlowResultPersistenceWithinConfiguredBudget(t *testing.T) {
	if resultPersistenceDrainTimeout < 40*time.Second {
		t.Fatalf("default result persistence drain timeout = %s, want at least one Git transaction plus verification", resultPersistenceDrainTimeout)
	}
	auth := resultPersistenceTestAuth("result-persistence-close-slow-save", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 1)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultPersistence.drainTimeout = 250 * time.Millisecond
	manager.resultPersistence.cancelTimeout = 100 * time.Millisecond
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	defer release()

	projectResultPersistenceImageQuota(manager, registered.ID, 1)
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("slow result persistence did not start")
	}
	closeDone := make(chan error, 1)
	started := time.Now()
	go func() { closeDone <- manager.CloseExecutors() }()
	time.Sleep(75 * time.Millisecond)
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before the legal slow save completed: %v", errClose)
	default:
	}
	release()
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("CloseExecutors() did not finish after the slow save completed")
	}
	if elapsed := time.Since(started); elapsed < 75*time.Millisecond || elapsed >= 250*time.Millisecond {
		t.Fatalf("CloseExecutors() elapsed = %s, want successful drain within configured budget", elapsed)
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.DrainTimeouts != 0 || metrics.CancelTimeouts != 0 || metrics.Abandoned != 0 {
		t.Fatalf("slow save close metrics = %#v", metrics)
	}
	persisted := store.snapshot(registered.ID)
	quota := metadataInt(persisted.Metadata["image_quota_remaining"])
	if quota == nil || *quota != 9 {
		t.Fatalf("slow save persisted state = %#v", persisted)
	}
}

func TestCloseExecutorsAbandonsResultPersistenceOnlyAfterConfiguredBudget(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-close-over-budget", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 1)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultPersistence.drainTimeout = 40 * time.Millisecond
	manager.resultPersistence.cancelTimeout = 250 * time.Millisecond
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	defer close(releaseSave)

	projectResultPersistenceImageQuota(manager, registered.ID, 1)
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("over-budget result persistence did not start")
	}
	started := time.Now()
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}
	elapsed := time.Since(started)
	if elapsed < 40*time.Millisecond || elapsed >= time.Second {
		t.Fatalf("CloseExecutors() elapsed = %s, want bounded cancellation after drain budget", elapsed)
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.DrainTimeouts != 1 || metrics.CancelTimeouts != 0 || metrics.Abandoned != 1 {
		t.Fatalf("over-budget close metrics = %#v", metrics)
	}
	persisted := store.snapshot(registered.ID)
	quota := metadataInt(persisted.Metadata["image_quota_remaining"])
	if quota == nil || *quota != 10 {
		t.Fatalf("over-budget save unexpectedly reported durable completion: %#v", persisted)
	}
}

type resultPersistenceIgnoringCancelExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (executor *resultPersistenceIgnoringCancelExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	executor.once.Do(func() { close(executor.started) })
	<-executor.release
	return cliproxyexecutor.Response{Payload: []byte("late")}, nil
}

func (*resultPersistenceIgnoringCancelExecutor) Close() error { return nil }

func TestCloseExecutorsAbandonsProducerThatIgnoresCancellation(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-close-abandoned", 10)
	store := newResultPersistenceTestStore(1, 1, auth)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	manager.resultProducerWaitTimeout = 20 * time.Millisecond
	manager.resultProducerCancelWait = 20 * time.Millisecond
	releaseExecution := make(chan struct{})
	executor := &resultPersistenceIgnoringCancelExecutor{
		chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{},
		started:                               make(chan struct{}),
		release:                               releaseExecution,
	}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(context.Background(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		executeDone <- errExecute
	}()
	select {
	case <-executor.started:
	case <-time.After(time.Second):
		t.Fatal("cancel-ignoring execution did not start")
	}
	started := time.Now()
	if errClose := manager.CloseExecutors(); errClose != nil {
		t.Fatalf("CloseExecutors() error: %v", errClose)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("CloseExecutors() took %s for a cancel-ignoring producer", elapsed)
	}
	metrics := manager.RefreshPersistenceMetrics().ResultPersistence
	if metrics.ProducerActive != 0 || metrics.ProducerTimeouts != 1 || metrics.ProducerCancelTimeouts != 1 || metrics.ProducerAbandoned != 1 {
		t.Fatalf("abandoned producer close metrics = %#v", metrics)
	}
	close(releaseExecution)
	select {
	case <-executeDone:
	case <-time.After(time.Second):
		t.Fatal("cancel-ignoring execution did not exit after test release")
	}
}

type resultPersistenceImmediateSuccessExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
}

func (*resultPersistenceImmediateSuccessExecutor) Execute(_ context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	state, _ := opts.Metadata[cliproxyexecutor.ImageGenerationResultStateMetadataKey].(*cliproxyexecutor.ImageGenerationResultState)
	if state == nil {
		return cliproxyexecutor.Response{}, errors.New("image generation result state is missing")
	}
	state.AddProduced(1)
	return cliproxyexecutor.Response{Payload: []byte("successful-image-response")}, nil
}

func (*resultPersistenceImmediateSuccessExecutor) Close() error { return nil }

func TestChatGPTWebSuccessfulExecuteDoesNotWaitForBlockingResultSave(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-success-response", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 1)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &resultPersistenceImmediateSuccessExecutor{chatGPTWebUnauthorizedRefreshExecutor: &chatGPTWebUnauthorizedRefreshExecutor{}}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	t.Cleanup(func() {
		release()
		_ = manager.CloseExecutors()
		registry.GetGlobalRegistry().UnregisterClient(registered.ID)
	})

	type executeResult struct {
		response cliproxyexecutor.Response
		err      error
	}
	executeDone := make(chan executeResult, 1)
	go func() {
		response, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{
			Metadata: map[string]any{cliproxyexecutor.ImageGenerationMaxResultsMetadataKey: 1},
		})
		executeDone <- executeResult{response: response, err: errExecute}
	}()
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("successful result did not reach background persistence")
	}
	select {
	case result := <-executeDone:
		if result.err != nil {
			t.Fatalf("Execute() error: %v", result.err)
		}
		if string(result.response.Payload) != "successful-image-response" {
			t.Fatalf("Execute() payload = %q", result.response.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("successful response waited for background result persistence")
	}

	release()
	waitForResultPersistenceCondition(t, 3*time.Second, "successful response quota was not persisted", func() bool {
		persisted := store.snapshot(registered.ID)
		if persisted == nil {
			return false
		}
		quota := metadataInt(persisted.Metadata["image_quota_remaining"])
		return quota != nil && *quota == 9
	})
}

func TestChatGPTWebUnauthorizedResponseDoesNotWaitForBlockingRefreshSave(t *testing.T) {
	auth := resultPersistenceTestAuth("result-persistence-401", 10)
	releaseSave := make(chan struct{})
	store := newResultPersistenceTestStore(1, 1, auth)
	store.blockSaves = true
	store.releaseSave = releaseSave
	store.saveStarted = make(chan string, 8)
	manager := NewManager(store, &FillFirstSelector{}, nil)
	executor := &chatGPTWebUnauthorizedRefreshExecutor{}
	manager.RegisterExecutor(executor)
	registered := registerResultPersistenceTestAuth(t, manager, store, auth.ID)
	model := "gpt-image-2"
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSave) }) }
	t.Cleanup(func() {
		release()
		_ = manager.CloseExecutors()
		registry.GetGlobalRegistry().UnregisterClient(registered.ID)
	})

	requestDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		requestDone <- errExecute
	}()
	select {
	case errExecute := <-requestDone:
		if statusCodeFromError(errExecute) != http.StatusUnauthorized || errExecute.Error() != "invalid access token" {
			t.Fatalf("Execute() error = %v, want original 401", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("401 response waited for background refresh persistence")
	}
	select {
	case <-store.saveStarted:
	case <-time.After(time.Second):
		t.Fatal("background refresh did not reach the blocking store")
	}
	if blocks := chatGPTWebRequestRefreshBlockCount(manager, registered.ID); blocks != 1 {
		t.Fatalf("request refresh blocks = %d, want 1", blocks)
	}
	release()
	waitForResultPersistenceCondition(t, 3*time.Second, "background refresh was not persisted", func() bool {
		persisted := store.snapshot(registered.ID)
		return persisted != nil && authAccessToken(persisted) == "fresh"
	})
}
