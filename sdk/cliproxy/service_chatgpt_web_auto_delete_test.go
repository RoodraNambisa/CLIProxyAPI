package cliproxy

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

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
