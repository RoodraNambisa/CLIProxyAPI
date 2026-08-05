package cliproxy

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type serviceUncertainConditionalDeleteStore struct {
	conditionalDeleteCalls atomic.Int32
}

type serviceRollbackThenSuccessConditionalDeleteStore struct {
	path                 string
	conditionalCalls     atomic.Int32
	secondAttempt        atomic.Int32
	secondBindingAllowed atomic.Bool
}

func (*serviceUncertainConditionalDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*serviceUncertainConditionalDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	return auth.ID, nil
}

func (*serviceUncertainConditionalDeleteStore) Delete(context.Context, string) error {
	return errors.New("ordinary delete must not be used")
}

func (store *serviceUncertainConditionalDeleteStore) DeleteIfSourceHashMatches(context.Context, string, string) error {
	store.conditionalDeleteCalls.Add(1)
	return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, errors.New("conditional delete result unavailable"))
}

func (*serviceRollbackThenSuccessConditionalDeleteStore) List(context.Context) ([]*coreauth.Auth, error) {
	return nil, nil
}

func (*serviceRollbackThenSuccessConditionalDeleteStore) Save(_ context.Context, auth *coreauth.Auth) (string, error) {
	if auth == nil {
		return "", nil
	}
	return auth.ID, nil
}

func (*serviceRollbackThenSuccessConditionalDeleteStore) Delete(context.Context, string) error {
	return errors.New("ordinary delete must not be used")
}

func (store *serviceRollbackThenSuccessConditionalDeleteStore) DeleteIfSourceHashMatches(ctx context.Context, _ string, _ string) error {
	if store.conditionalCalls.Add(1) == 1 {
		return coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("backend was not reached"))
	}
	store.secondAttempt.Store(int32(authfileguard.DeleteAttempt(ctx)))
	store.secondBindingAllowed.Store(authfileguard.DeleteIdentityBindingAllowed(ctx))
	return os.Remove(store.path)
}

func TestAuthEligibleForChatGPTWebDeadDelete(t *testing.T) {
	policy := chatGPTWebDeadAuthDeletePolicy{enabled: true, priorities: []int{0, -1}}
	compat := chatGPTWebAutoDeleteTestAuth("compat.json", -1, coreauth.LifecycleStateDead)
	compat.Attributes["compat_name"] = "compat"
	tests := []struct {
		name   string
		auth   *coreauth.Auth
		want   bool
		reason string
	}{
		{
			name:   "matching dead auth",
			auth:   chatGPTWebAutoDeleteTestAuth("dead.json", -1, coreauth.LifecycleStateDead),
			want:   true,
			reason: "chatgpt_web_dead_account_deactivated",
		},
		{
			name: "different priority",
			auth: chatGPTWebAutoDeleteTestAuth("other-priority.json", 1, coreauth.LifecycleStateDead),
		},
		{
			name: "active auth",
			auth: chatGPTWebAutoDeleteTestAuth("active.json", -1, coreauth.LifecycleStateActive),
		},
		{
			name: "different provider",
			auth: &coreauth.Auth{
				ID: "codex.json", Provider: "codex",
				Attributes: map[string]string{"priority": "-1"},
				Metadata:   map[string]any{"lifecycle_state": coreauth.LifecycleStateDead},
			},
		},
		{
			name: "openai compatibility auth",
			auth: compat,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reason, got := authEligibleForChatGPTWebDeadDelete(test.auth, policy)
			if got != test.want || reason != test.reason {
				t.Fatalf("authEligibleForChatGPTWebDeadDelete() = (%q, %v), want (%q, %v)", reason, got, test.reason, test.want)
			}
		})
	}

	if _, ok := authEligibleForChatGPTWebDeadDelete(
		chatGPTWebAutoDeleteTestAuth("all-priorities.json", 7, coreauth.LifecycleStateDead),
		chatGPTWebDeadAuthDeletePolicy{enabled: true},
	); !ok {
		t.Fatal("empty priority list did not match every priority")
	}
}

func TestServiceScanChatGPTWebDeadAuthDeleteCandidatesWithoutGenericMaintenance(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	dead := chatGPTWebAutoDeleteTestAuth("dead.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if err := os.WriteFile(dead.FileName, []byte(`{"type":"chatgpt-web"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}

	maintenanceCfg, gotAuthDir := service.snapshotAuthMaintenanceConfig()
	candidates := service.scanAuthMaintenanceCandidatesWithPolicy(
		maintenanceCfg,
		service.snapshotChatGPTWebDeadAuthDeletePolicy(),
		gotAuthDir,
	)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %#v, want one", candidates)
	}
	if candidates[0].Path != dead.FileName || candidates[0].Reason != "chatgpt_web_dead_account_deactivated" {
		t.Fatalf("candidate = %#v", candidates[0])
	}
}

func TestServiceHandleResultQueuesMatchingChatGPTWebDeadAuth(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{0}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("dead-result.json", 0, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if err := os.WriteFile(dead.FileName, []byte(`{"type":"chatgpt-web"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	genericCandidate, ok := service.authMaintenanceCandidateForAuth(dead, authDir, "http_401")
	if !ok || !service.enqueueAuthMaintenanceCandidate(genericCandidate) {
		t.Fatal("enqueue generic candidate")
	}

	service.handleAuthMaintenanceResult(context.Background(), coreauth.Result{
		AuthID: dead.ID, Provider: dead.Provider, Success: false,
		Error: &coreauth.Error{HTTPStatus: 403, Code: "account_deactivated"},
	})

	current, ok := service.coreManager.GetByID(dead.ID)
	if !ok || current == nil || !authMaintenancePendingDelete(current) {
		t.Fatalf("current auth = %#v, want pending delete", current)
	}
	service.maintenanceMu.Lock()
	queueLength := len(service.maintenanceQueue)
	queuedReason := ""
	if queueLength == 1 {
		queuedReason = service.maintenanceQueue[0].Reason
	}
	service.maintenanceMu.Unlock()
	if queueLength != 1 {
		t.Fatalf("maintenance queue length = %d, want 1", queueLength)
	}
	if queuedReason != "chatgpt_web_dead_account_deactivated" {
		t.Fatalf("maintenance candidate reason = %q, want Web dead reason", queuedReason)
	}
}

func TestServiceDequeueEnabledAuthMaintenanceCandidateKeepsDisabledGenericPolicyQueued(t *testing.T) {
	service := &Service{}
	generic := authMaintenanceCandidate{Key: "generic", IDs: []string{"generic"}, Reason: "http_401"}
	webDead := authMaintenanceCandidate{
		Key:    "web-dead",
		IDs:    []string{"web-dead"},
		Reason: "chatgpt_web_dead_account_deleted",
	}
	if !service.enqueueAuthMaintenanceCandidate(generic) || !service.enqueueAuthMaintenanceCandidate(webDead) {
		t.Fatal("failed to enqueue test candidates")
	}

	got, ok := service.dequeueProcessableAuthMaintenanceCandidate(false)
	if !ok || got.Key != webDead.Key {
		t.Fatalf("dequeueProcessableAuthMaintenanceCandidate() = %#v, %v; want Web dead candidate", got, ok)
	}
	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 1 || service.maintenanceQueue[0].Key != generic.Key {
		t.Fatalf("remaining queue = %#v, want generic candidate", service.maintenanceQueue)
	}
}

func TestServiceDequeueAuthMaintenanceCandidatePrioritizesWebDeadDeletion(t *testing.T) {
	service := &Service{}
	generic := authMaintenanceCandidate{Key: "generic", IDs: []string{"generic"}, Reason: "http_401"}
	webDead := authMaintenanceCandidate{
		Key:    "web-dead",
		IDs:    []string{"web-dead"},
		Reason: "chatgpt_web_dead_account_deleted",
	}
	if !service.enqueueAuthMaintenanceCandidate(generic) || !service.enqueueAuthMaintenanceCandidate(webDead) {
		t.Fatal("failed to enqueue test candidates")
	}

	got, ok := service.dequeueProcessableAuthMaintenanceCandidate(true)
	if !ok || got.Key != webDead.Key {
		t.Fatalf("dequeueProcessableAuthMaintenanceCandidate() = %#v, %v; want Web dead candidate", got, ok)
	}
	service.maintenanceMu.Lock()
	defer service.maintenanceMu.Unlock()
	if len(service.maintenanceQueue) != 1 || service.maintenanceQueue[0].Key != generic.Key {
		t.Fatalf("remaining queue = %#v, want generic candidate", service.maintenanceQueue)
	}
}

func TestServiceAuthMaintenanceRetryUsesBackoffAndRetainsDeleteGeneration(t *testing.T) {
	service := &Service{}
	candidate := authMaintenanceCandidate{
		Key:    "retry-key",
		Path:   filepath.Join(t.TempDir(), "retry.json"),
		IDs:    []string{"retry.json"},
		Reason: "chatgpt_web_dead_account_deleted",
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("enqueue initial candidate")
	}
	dequeued, ok := service.dequeueProcessableAuthMaintenanceCandidate(false)
	if !ok {
		t.Fatal("dequeue initial candidate")
	}
	generation, created, errGeneration := service.authMaintenanceDeleteGeneration(dequeued, "source-hash")
	if errGeneration != nil {
		t.Fatal(errGeneration)
	}
	if !created {
		t.Fatal("initial delete generation was not reported as new")
	}
	if !service.requeueAuthMaintenanceCandidate(dequeued) {
		t.Fatal("requeue failed candidate")
	}
	if _, ok = service.dequeueProcessableAuthMaintenanceCandidate(false); ok {
		t.Fatal("retry became runnable before its one-second backoff")
	}
	service.maintenanceMu.Lock()
	if len(service.maintenanceQueue) != 1 {
		service.maintenanceMu.Unlock()
		t.Fatalf("retry queue length = %d, want 1", len(service.maintenanceQueue))
	}
	service.maintenanceQueue[0].NextAttemptAt = time.Now().Add(-time.Millisecond)
	service.maintenanceMu.Unlock()
	retry, ok := service.dequeueProcessableAuthMaintenanceCandidate(false)
	if !ok || retry.Attempts != 1 {
		t.Fatalf("retry candidate = %#v, ok=%v", retry, ok)
	}
	reused, created, errGeneration := service.authMaintenanceDeleteGeneration(retry, "source-hash")
	if errGeneration != nil || reused != generation || created {
		t.Fatalf("retry generation = %p, want %p, created=%v, err=%v", reused, generation, created, errGeneration)
	}
	wantDelays := []time.Duration{time.Second, 5 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 5 * time.Minute}
	for index, want := range wantDelays {
		if got := authMaintenanceDeleteRetryDelay(index + 1); got != want {
			t.Fatalf("retry delay %d = %v, want %v", index+1, got, want)
		}
	}
}

func TestServiceAuthMaintenanceRolledBackDeleteStartsRetryWithFreshGeneration(t *testing.T) {
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	path := filepath.Join(authDir, "rolled-back.json")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	raw := []byte(`{"type":"chatgpt-web","access_token":"test-token"}`)
	if errWrite := os.WriteFile(path, raw, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	store := &serviceRollbackThenSuccessConditionalDeleteStore{path: path}
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		configPath:  filepath.Join(root, "config.yaml"),
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	auth := chatGPTWebAutoDeleteTestAuth("rolled-back.json", -1, coreauth.LifecycleStateDead)
	auth.FileName = path
	auth.Attributes["path"] = path
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(auth, raw); errSync != nil {
		t.Fatal(errSync)
	}
	installed, errRegister := service.coreManager.Register(t.Context(), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(installed, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok {
		t.Fatal("build maintenance candidate")
	}
	candidate.Installations = map[string]string{installed.ID: installed.RuntimeInstallationID()}

	deleted, errDelete := service.deleteAuthMaintenanceCandidateUnchecked(t.Context(), candidate)
	if deleted || errDelete == nil {
		t.Fatalf("first delete = (%v, %v), want rolled back", deleted, errDelete)
	}
	service.maintenanceMu.Lock()
	retainedGeneration := service.maintenanceDeleteGenerations[candidate.Key]
	service.maintenanceMu.Unlock()
	if retainedGeneration != nil || authfileguard.IsQuarantined(path) {
		t.Fatal("rolled-back delete retained its quarantine generation")
	}
	if _, exists := service.coreManager.GetByID(installed.ID); !exists {
		t.Fatal("rolled-back delete removed the runtime credential")
	}

	candidate.Attempts = 1
	deleted, errDelete = service.deleteAuthMaintenanceCandidateUnchecked(t.Context(), candidate)
	if errDelete != nil || !deleted {
		t.Fatalf("retry delete = (%v, %v), want success", deleted, errDelete)
	}
	if got := store.secondAttempt.Load(); got != 0 {
		t.Fatalf("fresh retry backend attempt = %d, want 0", got)
	}
	if !store.secondBindingAllowed.Load() {
		t.Fatal("fresh retry could not bind its backend identity")
	}
}

func TestServiceCancelDoesNotClearInFlightDeleteQuarantine(t *testing.T) {
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	path := filepath.Join(authDir, "in-flight.json")
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	service := &Service{cfg: &config.Config{AuthDir: authDir}, configPath: filepath.Join(root, "config.yaml")}
	candidate := authMaintenanceCandidate{Key: path, Path: path, IDs: []string{"in-flight"}}
	generation, created, errGeneration := service.authMaintenanceDeleteGeneration(candidate, "source-hash")
	if errGeneration != nil || !created {
		t.Fatalf("create generation: created=%v err=%v", created, errGeneration)
	}
	if errPersist := watcher.PersistAuthDeleteQuarantine(service.configPath, authDir, path, generation); errPersist != nil {
		t.Fatal(errPersist)
	}
	service.cancelAuthMaintenanceCandidate(candidate)
	service.maintenanceMu.Lock()
	retained := service.maintenanceDeleteGenerations[candidate.Key]
	service.maintenanceMu.Unlock()
	if retained != generation || !authfileguard.IsQuarantined(path) {
		t.Fatal("cancel cleared an in-flight delete quarantine")
	}

	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("enqueue retry candidate")
	}
	service.cancelAuthMaintenanceCandidate(candidate)
	service.maintenanceMu.Lock()
	retained = service.maintenanceDeleteGenerations[candidate.Key]
	service.maintenanceMu.Unlock()
	if retained != nil || authfileguard.IsQuarantined(path) {
		t.Fatal("cancel did not clear an idle queued retry quarantine")
	}
}

func TestServiceDeleteGenerationWaitsForCanceledQuarantineClear(t *testing.T) {
	service := &Service{}
	candidate := authMaintenanceCandidate{Key: "clearing", Path: "clearing.json"}
	generation := authfileguard.NewDeleteGeneration("source-hash")
	service.ensureAuthMaintenanceQueue()
	service.maintenanceMu.Lock()
	service.maintenanceDeleteClearing[candidate.Key] = generation
	service.maintenanceMu.Unlock()

	got, created, errGeneration := service.authMaintenanceDeleteGeneration(candidate, "source-hash")
	if got != nil || created || !errors.Is(errGeneration, authfileguard.ErrDeleteGenerationUncertain) {
		t.Fatalf("delete generation while clearing = (%p, %v, %v)", got, created, errGeneration)
	}
}

func TestServiceChatGPTWebDeadDeleteRechecksDisabledPolicyBeforeDeleting(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("recheck-disabled.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(dead, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
		t.Fatal("failed to stage dead auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)

	cfg.ChatGPTWeb.AutoDeleteDeadAuths = false
	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate() error = %v", err)
	}
	if deleted {
		t.Fatal("candidate deleted after auto-delete was disabled")
	}
	if _, err = os.Stat(dead.FileName); err != nil {
		t.Fatalf("dead auth file was removed: %v", err)
	}
	current, exists := service.coreManager.GetByID(dead.ID)
	if !exists || current == nil {
		t.Fatal("dead auth was removed from runtime")
	}
	if authMaintenancePendingDelete(current) {
		t.Fatal("stale pending-delete state was not cleared")
	}
	if current.Disabled {
		t.Fatal("credential disabled state was not restored")
	}
}

func TestServiceAuthMaintenanceWorkerClearsQueuedWebDeleteAfterPolicyDisabled(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("worker-policy-disabled.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(dead, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
		t.Fatal("failed to stage dead auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("failed to enqueue dead auth candidate")
	}

	ctx, cancel := context.WithCancel(context.Background())
	service.startAuthMaintenance(ctx)
	defer func() {
		cancel()
		service.stopAuthMaintenance()
	}()
	service.wakeAuthMaintenance()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, exists := service.coreManager.GetByID(dead.ID)
		if exists && current != nil && !authMaintenancePendingDelete(current) {
			if _, err := os.Stat(dead.FileName); err != nil {
				t.Fatalf("auth file was removed: %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("maintenance worker did not clear stale Web pending-delete state")
}

func TestServiceAuthMaintenanceWorkerCountsSuccessfulWebDeadDeletes(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("counted.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if err := os.WriteFile(dead.FileName, []byte(`{"type":"chatgpt-web"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	installed, err := service.coreManager.Register(context.Background(), dead)
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(installed, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
		t.Fatal("failed to stage dead auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("failed to enqueue dead auth candidate")
	}

	ctx, cancel := context.WithCancel(context.Background())
	service.startAuthMaintenance(ctx)
	defer func() {
		cancel()
		service.stopAuthMaintenance()
	}()
	service.wakeAuthMaintenance()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if service.chatGPTWebDeadAuthDeletedCount.Load() == 1 {
			if _, exists := service.coreManager.GetByID(dead.ID); exists {
				t.Fatal("count advanced before the runtime credential was removed")
			}
			if _, errStat := os.Stat(dead.FileName); !os.IsNotExist(errStat) {
				t.Fatalf("count advanced before the auth file was removed: %v", errStat)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("successful deletion count = %d, want 1", service.chatGPTWebDeadAuthDeletedCount.Load())
}

func TestServiceAuthMaintenanceWorkerDeletesWebDeadQueueWithoutGenericInterval(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{
		AuthDir: authDir,
		AuthMaintenance: config.AuthMaintenanceConfig{
			DeleteIntervalSeconds: 60,
			ScanIntervalSeconds:   60,
		},
	}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{cfg: cfg, coreManager: coreauth.NewManager(store, nil, nil)}

	const candidateCount = 100
	for index := range candidateCount {
		id := "web-dead-immediate-" + strconv.Itoa(index) + ".json"
		dead := chatGPTWebAutoDeleteTestAuth(id, -1, coreauth.LifecycleStateDead)
		dead.FileName = filepath.Join(authDir, id)
		dead.Attributes["path"] = dead.FileName
		installed, errRegister := service.coreManager.Register(t.Context(), dead)
		if errRegister != nil {
			t.Fatalf("register auth %d: %v", index, errRegister)
		}
		candidate, ok := service.authMaintenanceCandidateForAuth(installed, authDir, "chatgpt_web_dead_account_deactivated")
		if !ok || !service.disableAuthMaintenanceCandidate(t.Context(), candidate, true) {
			t.Fatalf("stage candidate %d", index)
		}
		candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
		if !service.enqueueAuthMaintenanceCandidate(candidate) {
			t.Fatalf("enqueue candidate %d", index)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	service.startAuthMaintenance(ctx)
	defer func() {
		cancel()
		service.stopAuthMaintenance()
	}()
	service.wakeAuthMaintenance()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if service.chatGPTWebDeadAuthDeletedCount.Load() == candidateCount {
			if service.hasEnabledAuthMaintenanceCandidates(false, true) {
				t.Fatal("delete count completed while Web candidates remained queued")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("deleted %d/%d Web credentials; generic 60-second interval leaked into automatic deletion", service.chatGPTWebDeadAuthDeletedCount.Load(), candidateCount)
}

func TestServiceAuthMaintenanceWorkerRetainsUncertainDeleteQuarantineWithoutResend(t *testing.T) {
	root := t.TempDir()
	authDir := filepath.Join(root, "auths")
	store := &serviceUncertainConditionalDeleteStore{}
	cfg := &config.Config{
		AuthDir: authDir,
		AuthMaintenance: config.AuthMaintenanceConfig{
			DeleteIntervalSeconds: 60,
			ScanIntervalSeconds:   1,
		},
	}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{
		cfg:         cfg,
		configPath:  filepath.Join(root, "config.yaml"),
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	dead := chatGPTWebAutoDeleteTestAuth("web-dead-uncertain.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	raw := []byte(`{"type":"chatgpt-web","lifecycle_state":"dead","lifecycle_reason":"account_deactivated","access_token":"test-token"}`)
	if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
		t.Fatal(errMkdir)
	}
	if errWrite := os.WriteFile(dead.FileName, raw, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(dead, raw); errSync != nil {
		t.Fatal(errSync)
	}
	installed, errRegister := service.coreManager.Register(t.Context(), dead)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	shadow := chatGPTWebAutoDeleteTestAuth("web-dead-uncertain-shadow", -1, coreauth.LifecycleStateDead)
	shadow.FileName = dead.FileName
	shadow.Attributes["path"] = dead.FileName
	shadow.Attributes[coreauth.SourceHashAttributeKey] = installed.Attributes[coreauth.SourceHashAttributeKey]
	if _, errRegister = service.coreManager.Register(coreauth.WithSkipPersist(t.Context()), shadow); errRegister != nil {
		t.Fatal(errRegister)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(installed, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok || !service.disableAuthMaintenanceCandidate(t.Context(), candidate, true) {
		t.Fatal("stage uncertain delete candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	if !service.chatGPTWebDeadMaintenanceCandidateStillEligible(candidate, service.snapshotChatGPTWebDeadAuthDeletePolicy(), authDir) {
		t.Fatalf("shared uncertain candidate is not eligible: candidate=%#v auths=%#v", candidate, service.coreManager.AuthsForBackingPath(dead.FileName))
	}
	if !service.enqueueAuthMaintenanceCandidate(candidate) {
		t.Fatal("enqueue uncertain delete candidate")
	}

	ctx, cancel := context.WithCancel(t.Context())
	service.startAuthMaintenance(ctx)
	defer func() {
		cancel()
		service.stopAuthMaintenance()
		service.maintenanceMu.Lock()
		generation := service.maintenanceDeleteGenerations[candidate.Key]
		service.maintenanceMu.Unlock()
		if generation != nil {
			_ = watcher.ClearAuthDeleteQuarantine(service.configPath, authDir, candidate.Path, generation)
		}
		authfileguard.ClearQuarantined(candidate.Path)
	}()
	service.wakeAuthMaintenance()

	deadline := time.Now().Add(2 * time.Second)
	for store.conditionalDeleteCalls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if calls := store.conditionalDeleteCalls.Load(); calls != 1 {
		t.Fatalf("conditional delete calls = %d, want 1", calls)
	}
	if _, exists := service.coreManager.GetByID(dead.ID); exists {
		t.Fatal("uncertain delete left the credential schedulable")
	}
	if _, exists := service.coreManager.GetByID(shadow.ID); exists {
		t.Fatal("uncertain shared-file delete left a sibling runtime credential schedulable")
	}
	if !authfileguard.IsQuarantined(candidate.Path) {
		t.Fatal("uncertain delete did not retain the credential quarantine")
	}
	tombstones, errReadDir := os.ReadDir(filepath.Join(root, ".cliproxy-delete-quarantine"))
	if errReadDir != nil || len(tombstones) != 1 {
		t.Fatalf("delete tombstones = %d, err=%v; want 1", len(tombstones), errReadDir)
	}

	time.Sleep(1200 * time.Millisecond)
	if calls := store.conditionalDeleteCalls.Load(); calls != 1 {
		t.Fatalf("uncertain delete was resent %d times", calls)
	}
}

func TestServiceChatGPTWebDeadDeleteDoesNotDeleteReplacementGeneration(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	cfg.ChatGPTWeb.AutoDeleteDeadPriorities = []int{-1}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("replacement.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register dead auth: %v", err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(dead, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
		t.Fatal("failed to stage dead auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)

	replacement := chatGPTWebAutoDeleteTestAuth(dead.ID, -1, coreauth.LifecycleStateActive)
	replacement.FileName = dead.FileName
	replacement.Attributes["path"] = dead.FileName
	if _, err := service.coreManager.Update(context.Background(), replacement); err != nil {
		t.Fatalf("install replacement auth: %v", err)
	}

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate() error = %v", err)
	}
	if deleted {
		t.Fatal("stale candidate deleted replacement auth")
	}
	current, exists := service.coreManager.GetByID(dead.ID)
	if !exists || current == nil || current.LifecycleState() != coreauth.LifecycleStateActive {
		t.Fatalf("current auth = %#v, want active replacement", current)
	}
	if _, err = os.Stat(dead.FileName); err != nil {
		t.Fatalf("replacement auth file was removed: %v", err)
	}
}

func TestServiceChatGPTWebDeadDeleteRequiresPersistedSourceHash(t *testing.T) {
	authDir := t.TempDir()
	service := &Service{
		cfg:         &config.Config{AuthDir: authDir},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("missing-source-hash.json", -1, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if err := os.WriteFile(dead.FileName, []byte(`{"type":"chatgpt-web"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	installed, err := service.coreManager.Register(context.Background(), dead)
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(installed, authDir, "chatgpt_web_dead_account_deactivated")
	if !ok {
		t.Fatal("build dead auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	if service.chatGPTWebDeadMaintenanceCandidateStillEligible(
		candidate,
		chatGPTWebDeadAuthDeletePolicy{enabled: true, priorities: []int{-1}},
		authDir,
	) {
		t.Fatal("file-backed candidate without a source hash was considered safe to delete")
	}
}

func TestServiceWebDeadCandidateReplacesDisabledQueuedPolicyForSamePath(t *testing.T) {
	for _, genericEnabled := range []bool{false, true} {
		t.Run("generic-enabled="+strconv.FormatBool(genericEnabled), func(t *testing.T) {
			service := &Service{}
			generic := authMaintenanceCandidate{Key: "same-path", IDs: []string{"auth"}, Reason: "http_401"}
			webDead := authMaintenanceCandidate{
				Key:    "same-path",
				IDs:    []string{"auth"},
				Reason: "chatgpt_web_dead_account_deleted",
			}
			if !service.enqueueAuthMaintenanceCandidate(generic) {
				t.Fatal("failed to enqueue generic candidate")
			}
			if service.authMaintenanceCandidateQueuedForEnabledPolicy(webDead, genericEnabled, true) {
				t.Fatal("generic candidate blocked enabled Web candidate")
			}
			if !service.enqueueAuthMaintenanceCandidate(webDead) {
				t.Fatal("failed to enqueue replacement Web candidate")
			}
			got, ok := service.dequeueProcessableAuthMaintenanceCandidate(genericEnabled)
			if !ok || got.Reason != webDead.Reason {
				t.Fatalf("dequeued candidate = %#v, %v; want Web dead candidate", got, ok)
			}
		})
	}
}

func TestServiceWebDeadCandidateCancelsDequeuedGenericCandidate(t *testing.T) {
	service := &Service{}
	generic := authMaintenanceCandidate{Key: "same-path", IDs: []string{"auth"}, Reason: "http_401"}
	if !service.enqueueAuthMaintenanceCandidate(generic) {
		t.Fatal("failed to enqueue generic candidate")
	}
	inFlight, ok := service.dequeueProcessableAuthMaintenanceCandidate(true)
	if !ok {
		t.Fatal("failed to dequeue generic candidate")
	}

	webDead := authMaintenanceCandidate{
		Key:    "same-path",
		IDs:    []string{"auth"},
		Reason: "chatgpt_web_dead_account_deleted",
	}
	if service.authMaintenanceCandidateQueuedForEnabledPolicy(webDead, true, true) {
		t.Fatal("dequeued generic candidate blocked Web candidate")
	}
	if !service.enqueueAuthMaintenanceCandidate(webDead) {
		t.Fatal("failed to enqueue Web candidate")
	}
	if !service.authMaintenanceCandidateCanceled(inFlight) {
		t.Fatal("in-flight generic candidate was not canceled")
	}
	next, ok := service.dequeueProcessableAuthMaintenanceCandidate(true)
	if !ok || next.Reason != webDead.Reason || service.authMaintenanceCandidateCanceled(next) {
		t.Fatalf("replacement candidate = %#v, ok=%v, canceled=%v", next, ok, service.authMaintenanceCandidateCanceled(next))
	}
}

func TestServiceWebDeadCandidateReplacementPreservesOriginalDisabledState(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}
	dead := chatGPTWebAutoDeleteTestAuth("replacement-disabled-state.json", 0, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	if _, err := service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	generic, ok := service.authMaintenanceCandidateForAuth(dead, authDir, "http_401")
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), generic, true) {
		t.Fatal("failed to stage generic candidate")
	}
	generic = snapshotChatGPTWebAutoDeleteCandidate(t, service, generic, authDir)
	if !service.enqueueAuthMaintenanceCandidate(generic) {
		t.Fatal("failed to enqueue generic candidate")
	}
	webDead := generic
	webDead.Reason = "chatgpt_web_dead_account_deactivated"
	if service.authMaintenanceCandidateQueuedForEnabledPolicy(webDead, false, true) {
		t.Fatal("generic candidate blocked Web candidate")
	}
	if !service.disableAuthMaintenanceCandidate(context.Background(), webDead, true) {
		t.Fatal("failed to stage Web candidate")
	}
	webDead = snapshotChatGPTWebAutoDeleteCandidate(t, service, webDead, authDir)

	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), webDead)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate() error = %v", err)
	}
	if deleted {
		t.Fatal("disabled policy deleted Web candidate")
	}
	current, exists := service.coreManager.GetByID(dead.ID)
	if !exists || current == nil {
		t.Fatal("auth was removed")
	}
	if current.Disabled {
		t.Fatal("candidate replacement lost original enabled state")
	}
}

func TestServiceAuthMaintenanceDependencyReconcilePendingState(t *testing.T) {
	service := &Service{}
	service.setAuthMaintenanceDependencyReconcilePending(true)
	if !service.authMaintenanceDependencyReconcileRequired() {
		t.Fatal("dependency reconcile retry was not recorded")
	}
	service.setAuthMaintenanceDependencyReconcilePending(false)
	if service.authMaintenanceDependencyReconcileRequired() {
		t.Fatal("dependency reconcile retry was not cleared")
	}
}

func TestServiceChatGPTWebDeadDeleteReconcilesRetainedCodexSource(t *testing.T) {
	testServiceChatGPTWebDeleteReconcilesRetainedCodexSource(t, "chatgpt_web_dead_account_deactivated")
}

func TestServiceGenericMaintenanceDeleteReconcilesRetainedCodexSource(t *testing.T) {
	testServiceChatGPTWebDeleteReconcilesRetainedCodexSource(t, "http_401")
}

func testServiceChatGPTWebDeleteReconcilesRetainedCodexSource(t *testing.T, reason string) {
	t.Helper()
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	service := &Service{
		cfg:         cfg,
		coreManager: coreauth.NewManager(store, nil, nil),
	}

	source := &coreauth.Auth{
		ID:       "source.json",
		Provider: "codex",
		FileName: filepath.Join(authDir, "source.json"),
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"path": filepath.Join(authDir, "source.json"),
		},
		Metadata: map[string]any{
			"type":           "codex",
			"credential_uid": "source-uid",
			"access_token":   "source-token",
		},
	}
	installedSource, err := service.coreManager.Register(context.Background(), source)
	if err != nil {
		t.Fatalf("register source auth: %v", err)
	}

	dead := chatGPTWebAutoDeleteTestAuth("dependent.json", 0, coreauth.LifecycleStateDead)
	dead.FileName = filepath.Join(authDir, dead.ID)
	dead.Attributes["path"] = dead.FileName
	dead.Metadata["refresh_strategy"] = "codex_source"
	dead.Metadata["source_auth_id"] = installedSource.ID
	dead.Metadata["source_credential_uid"] = "source-uid"
	if _, err = service.coreManager.Register(context.Background(), dead); err != nil {
		t.Fatalf("register dependent auth: %v", err)
	}
	retained, current, err := service.coreManager.UpdateIfCurrent(
		context.Background(),
		installedSource,
		coreauth.RetainCodexAuthForChatGPTWebDependents(installedSource, time.Now()),
	)
	if err != nil || !current || retained == nil {
		t.Fatalf("retain source: current=%v auth=%#v err=%v", current, retained, err)
	}

	candidate, ok := service.authMaintenanceCandidateForAuth(dead, authDir, reason)
	if !ok || !service.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
		t.Fatal("failed to stage dependent auth candidate")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	deleted, err := service.deleteAuthMaintenanceCandidate(context.Background(), candidate)
	if err != nil {
		t.Fatalf("deleteAuthMaintenanceCandidate() error = %v", err)
	}
	if !deleted {
		t.Fatal("dependent auth was not deleted")
	}
	service.runAuthMaintenanceCheckpoint(context.Background(), "test queue drained")
	if _, exists := service.coreManager.GetByID(installedSource.ID); exists {
		t.Fatal("orphaned retained Codex source remained after dependent deletion")
	}
	if _, err = os.Stat(source.FileName); !os.IsNotExist(err) {
		t.Fatalf("retained Codex source file still exists: %v", err)
	}
}

func chatGPTWebAutoDeleteTestAuth(id string, priority int, lifecycle string) *coreauth.Auth {
	return &coreauth.Auth{
		ID:       id,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.RuntimeStatusForLifecycle(lifecycle),
		Attributes: map[string]string{
			"priority": strconv.Itoa(priority),
		},
		Metadata: map[string]any{
			"type":             chatgptwebauth.Provider,
			"lifecycle_state":  lifecycle,
			"lifecycle_reason": "account_deactivated",
			"access_token":     "test-token",
		},
	}
}

func snapshotChatGPTWebAutoDeleteCandidate(t *testing.T, service *Service, candidate authMaintenanceCandidate, authDir string) authMaintenanceCandidate {
	t.Helper()
	snapshot, ok := service.snapshotAuthMaintenanceCandidateInstallations(candidate, authDir)
	if !ok {
		t.Fatalf("snapshot auth maintenance candidate = %#v, false", candidate)
	}
	return snapshot
}
