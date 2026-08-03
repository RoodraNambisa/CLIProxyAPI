package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebAccountInfoRuntimeStartsAfterExecutorPublication(t *testing.T) {
	executor := &ChatGPTWebExecutor{}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})

	runtime.mu.Lock()
	startedBeforePublication := runtime.started
	workersBeforePublication := len(runtime.workers)
	runtime.mu.Unlock()
	if startedBeforePublication || workersBeforePublication != 0 {
		t.Fatalf(
			"runtime before publication: started=%v workers=%d, want false/0",
			startedBeforePublication,
			workersBeforePublication,
		)
	}

	executor.accountInfo = runtime
	runtime.start()
	t.Cleanup(runtime.close)

	runtime.mu.Lock()
	started := runtime.started
	workers := len(runtime.workers)
	runtime.mu.Unlock()
	if !started || workers == 0 {
		t.Fatalf("runtime after start: started=%v workers=%d, want true/non-zero", started, workers)
	}
	if executor.accountInfo != runtime {
		t.Fatal("executor accountInfo was not published before runtime start")
	}
}

func TestChatGPTWebAccountInfoRuntimeClampsUnvalidatedConfig(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:         accountInfoTestInt(maxInt),
		RefreshQueueSize:       accountInfoTestInt(maxInt),
		RefreshTTLMinutes:      accountInfoTestInt(-1),
		PeriodicRefreshMinutes: accountInfoTestInt(maxInt),
		RecoveryJitterSeconds:  accountInfoTestInt(maxInt),
		MaxRetries:             accountInfoTestInt(-1),
	}}}
	executor := NewChatGPTWebExecutor(cfg, nil)
	t.Cleanup(func() { _ = executor.Close() })

	runtime := executor.accountInfo
	runtime.mu.Lock()
	resolved := runtime.cfg
	workers := len(runtime.workers)
	runtime.mu.Unlock()
	if resolved.RefreshWorkers != config.MaxChatGPTWebAccountInfoWorkers ||
		resolved.RefreshQueueSize != config.MaxChatGPTWebAccountInfoQueueSize ||
		resolved.RefreshTTLMinutes != 1 ||
		resolved.PeriodicRefreshMinutes != config.MaxChatGPTWebAccountInfoPeriodMinutes ||
		resolved.RecoveryJitterSeconds != config.MaxChatGPTWebAccountInfoJitterSeconds ||
		resolved.MaxRetries != 0 {
		t.Fatalf("clamped config = %+v", resolved)
	}
	if workers != config.MaxChatGPTWebAccountInfoWorkers {
		t.Fatalf("workers = %d, want %d", workers, config.MaxChatGPTWebAccountInfoWorkers)
	}

	cfg.ChatGPTWeb.AccountInfo = config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:         accountInfoTestInt(-1),
		RefreshQueueSize:       accountInfoTestInt(-1),
		RefreshTTLMinutes:      accountInfoTestInt(maxInt),
		PeriodicRefreshMinutes: accountInfoTestInt(-1),
		RecoveryJitterSeconds:  accountInfoTestInt(-1),
		MaxRetries:             accountInfoTestInt(maxInt),
	}
	executor.UpdateConfig(cfg)
	runtime.mu.Lock()
	resolved = runtime.cfg
	runtime.mu.Unlock()
	if resolved.RefreshWorkers != 1 ||
		resolved.RefreshQueueSize != 0 ||
		resolved.RefreshTTLMinutes != config.MaxChatGPTWebAccountInfoTTLMinutes ||
		resolved.PeriodicRefreshMinutes != 0 ||
		resolved.RecoveryJitterSeconds != 0 ||
		resolved.MaxRetries != config.MaxChatGPTWebAccountInfoRetries {
		t.Fatalf("updated clamped config = %+v", resolved)
	}
}

func TestChatGPTWebAccountInfoDiagnosticsAreSafeAndDetailed(t *testing.T) {
	enabled := true
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{DiagnosticsEnabled: &enabled},
	}}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	payload := []byte(`{
		"limits_progress":[{"feature_name":"image_gen","remaining":-1,"reset_after":"2026-08-03T13:23:29Z"}],
		"error":{"code":"secret-token"},
		"access_token":"secret-token",
		"email":"person@example.com",
		"cookie":"secret-cookie"
	}`)
	response := &fhttp.Response{
		StatusCode:    http.StatusOK,
		Header:        fhttp.Header{"Content-Type": {"application/json; charset=utf-8"}, "Cf-Ray": {"ray-id"}},
		ContentLength: int64(len(payload)),
	}
	ctx := withChatGPTWebAccountInfoDiagnosticContext(context.Background(), "safe-auth-index", 3)
	executor.recordChatGPTWebAccountInfoDiagnostic(
		ctx,
		"quota",
		"parse",
		response,
		payload,
		errors.New("image quota remaining is invalid"),
	)
	snapshot := executor.AccountInfoDiagnosticsSnapshot()
	if !snapshot.Enabled || snapshot.UniqueCount != 1 || snapshot.TotalCount != 1 || len(snapshot.Records) != 1 {
		t.Fatalf("diagnostics snapshot = %+v", snapshot)
	}
	record := snapshot.Records[0]
	if record.LastAuthIndex != "safe-auth-index" || record.LastAttempt != 3 ||
		record.HTTPStatus != http.StatusOK || record.ContentType != "application/json" ||
		record.BodyKind != "json_object" || record.LimitsProgressCount != 1 ||
		record.LastRemaining == nil || *record.LastRemaining != int64(-1) ||
		record.ImageQuotaResetAfter != "2026-08-03T13:23:29Z" ||
		record.Reason != "quota_remaining_invalid" || record.UpstreamErrorCode != "" || !record.Cloudflare {
		t.Fatalf("diagnostic record = %#v", record)
	}
	encoded, errMarshal := json.Marshal(snapshot)
	if errMarshal != nil {
		t.Fatalf("marshal diagnostic fields: %v", errMarshal)
	}
	for _, secret := range []string{"secret-token", "person@example.com", "secret-cookie"} {
		if bytes.Contains(encoded, []byte(secret)) {
			t.Fatalf("diagnostics leaked %q: %s", secret, encoded)
		}
	}
}

func TestSafeChatGPTWebAccountInfoDiagnosticUpstreamCodeUsesAllowlist(t *testing.T) {
	if got := safeChatGPTWebAccountInfoDiagnosticUpstreamCode("RATE_LIMIT_EXCEEDED"); got != "rate_limit_exceeded" {
		t.Fatalf("safe upstream code = %q, want rate_limit_exceeded", got)
	}
	for _, value := range []string{"secret-token", "unknown_error", strings.Repeat("a", 40)} {
		if got := safeChatGPTWebAccountInfoDiagnosticUpstreamCode(value); got != "" {
			t.Fatalf("safe upstream code %q = %q, want empty", value, got)
		}
	}
}

func TestChatGPTWebAccountInfoDiagnosticsDefaultDisabledAndHotUpdated(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	executor.recordChatGPTWebAccountInfoDiagnostic(
		context.Background(),
		"quota",
		"parse",
		nil,
		[]byte(`{"limits_progress":[]}`),
		errors.New("image quota remaining is invalid"),
	)
	if snapshot := executor.AccountInfoDiagnosticsSnapshot(); snapshot.Enabled || snapshot.TotalCount != 0 {
		t.Fatalf("disabled diagnostics = %+v", snapshot)
	}
	enabled := true
	executor.UpdateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{DiagnosticsEnabled: &enabled},
	}})
	executor.recordChatGPTWebAccountInfoDiagnostic(
		context.Background(),
		"quota",
		"parse",
		nil,
		[]byte(`{"limits_progress":[]}`),
		errors.New("image quota remaining is invalid"),
	)
	if snapshot := executor.AccountInfoDiagnosticsSnapshot(); !snapshot.Enabled || snapshot.TotalCount != 1 {
		t.Fatalf("enabled diagnostics = %+v", snapshot)
	}
}

func TestSafeChatGPTWebAccountInfoDiagnosticResetAfter(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "RFC3339", raw: `"2026-08-03T13:23:29Z"`, want: "2026-08-03T13:23:29Z"},
		{name: "numeric string", raw: `"1785763409000"`, want: "2026-08-03T13:23:29Z"},
		{name: "milliseconds", raw: `1785763409000`, want: "2026-08-03T13:23:29Z"},
		{name: "invalid", raw: `"tomorrow"`, want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := safeChatGPTWebAccountInfoDiagnosticResetAfter(json.RawMessage(test.raw)); got != test.want {
				t.Fatalf("reset_after = %q, want %q", got, test.want)
			}
		})
	}
}

func TestChatGPTWebAccountInfoPeriodicScheduleLifecycle(t *testing.T) {
	start := time.Date(2026, time.August, 2, 8, 0, 0, 0, time.UTC)
	var clock atomic.Int64
	clock.Store(start.UnixNano())
	period := 60
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			PeriodicRefreshMinutes: &period,
		}},
	})
	runtime.now = func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	runtime.start()
	t.Cleanup(runtime.close)

	runtime.mu.Lock()
	firstDue := runtime.periodicNextAt
	runtime.mu.Unlock()
	if want := start.Add(time.Hour); !firstDue.Equal(want) {
		t.Fatalf("first periodic due = %s, want %s", firstDue, want)
	}

	clock.Store(start.Add(5 * time.Minute).UnixNano())
	ttl := 30
	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{
			PeriodicRefreshMinutes: &period,
			RefreshTTLMinutes:      &ttl,
		},
	}})
	runtime.mu.Lock()
	unchangedDue := runtime.periodicNextAt
	runtime.mu.Unlock()
	if !unchangedDue.Equal(firstDue) {
		t.Fatalf("unrelated update moved periodic due from %s to %s", firstDue, unchangedDue)
	}

	period = 30
	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{PeriodicRefreshMinutes: &period},
	}})
	runtime.mu.Lock()
	changedDue := runtime.periodicNextAt
	runtime.mu.Unlock()
	if want := start.Add(35 * time.Minute); !changedDue.Equal(want) {
		t.Fatalf("changed periodic due = %s, want %s", changedDue, want)
	}
	runtime.mu.Lock()
	runtime.schedulePeriodicTargetsLocked([]chatgptwebauth.AccountInfoRefreshTarget{{
		AuthID: "pending-periodic", AuthInstanceID: "pending-instance",
	}}, time.Now().Add(24*time.Hour))
	if count := accountInfoPeriodicScheduleCountLocked(runtime); count != 1 {
		runtime.mu.Unlock()
		t.Fatalf("periodic schedules before disable = %d, want 1", count)
	}
	runtime.mu.Unlock()

	period = 0
	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{PeriodicRefreshMinutes: &period},
	}})
	runtime.mu.Lock()
	disabledDue := runtime.periodicNextAt
	pending := accountInfoPeriodicScheduleCountLocked(runtime)
	runtime.mu.Unlock()
	if !disabledDue.IsZero() || pending != 0 {
		t.Fatalf("disabled periodic state = due %s pending %d", disabledDue, pending)
	}

	period = 30
	autoRefresh := false
	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled:     &autoRefresh,
			PeriodicRefreshMinutes: &period,
		},
	}})
	runtime.mu.Lock()
	autoDisabledDue := runtime.periodicNextAt
	runtime.mu.Unlock()
	if !autoDisabledDue.IsZero() {
		t.Fatalf("automatic refresh disabled periodic due = %s", autoDisabledDue)
	}

	clock.Store(start.Add(10 * time.Minute).UnixNano())
	autoRefresh = true
	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled:     &autoRefresh,
			PeriodicRefreshMinutes: &period,
		},
	}})
	runtime.mu.Lock()
	reenabledDue := runtime.periodicNextAt
	runtime.mu.Unlock()
	if want := start.Add(40 * time.Minute); !reenabledDue.Equal(want) {
		t.Fatalf("re-enabled periodic due = %s, want %s", reenabledDue, want)
	}
}

func TestChatGPTWebAccountInfoPeriodicTargetsAreEligibleAndSorted(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	register := func(auth *cliproxyauth.Auth) {
		t.Helper()
		if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register %q: %v", auth.ID, errRegister)
		}
	}
	validB := chatGPTWebTestAuth("periodic-b")
	validA := chatGPTWebTestAuth("periodic-a")
	disabled := chatGPTWebTestAuth("periodic-disabled")
	disabled.Disabled = true
	dead := chatGPTWebTestAuth("periodic-dead")
	dead.Metadata["lifecycle_state"] = string(chatgptwebauth.LifecycleDead)
	malformed := chatGPTWebTestAuth("periodic-malformed")
	malformed.Metadata["refresh_strategy"] = "unsupported"
	wrongProvider := chatGPTWebTestAuth("periodic-codex")
	wrongProvider.Provider = "codex"
	for _, auth := range []*cliproxyauth.Auth{validB, disabled, malformed, validA, dead, wrongProvider} {
		register(auth)
	}

	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{manager: manager}, &config.Config{})
	targets := runtime.periodicRefreshTargets()
	if len(targets) != 2 || targets[0].AuthID != validA.ID || targets[1].AuthID != validB.ID {
		t.Fatalf("periodic targets = %+v", targets)
	}
}

func TestChatGPTWebAccountInfoPeriodicSchedulesBeyondQueueCapacity(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.started = true
	runtime.cfg = config.ResolvedChatGPTWebAccountInfoConfig{
		RefreshWorkers:         1,
		RefreshQueueSize:       2,
		PeriodicRefreshMinutes: 1,
	}
	targets := make([]chatgptwebauth.AccountInfoRefreshTarget, 300)
	for index := range targets {
		targets[index] = chatgptwebauth.AccountInfoRefreshTarget{
			AuthID:         fmt.Sprintf("periodic-%03d", index),
			AuthInstanceID: fmt.Sprintf("instance-%03d", index),
		}
	}

	runtime.mu.Lock()
	runtime.schedulePeriodicTargetsLocked(targets, time.Now())
	if got := accountInfoPeriodicScheduleCountLocked(runtime); got != len(targets) {
		runtime.mu.Unlock()
		t.Fatalf("periodic schedules = %d, want %d", got, len(targets))
	}
	for key, entry := range runtime.scheduled {
		if !strings.HasPrefix(key, chatGPTWebAccountInfoPeriodicSchedulePrefix) {
			continue
		}
		if entry.work.force || !entry.work.automatic {
			runtime.mu.Unlock()
			t.Fatalf("periodic work flags = force %v automatic %v", entry.work.force, entry.work.automatic)
		}
	}
	runtime.mu.Unlock()
}

func TestChatGPTWebAccountInfoPeriodicScanDoesNotDuplicateExistingWork(t *testing.T) {
	now := time.Now().UTC()
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.started = true
	runtime.cfg = config.ResolvedChatGPTWebAccountInfoConfig{
		RefreshWorkers:         1,
		RefreshQueueSize:       4,
		PeriodicRefreshMinutes: 1,
	}
	inflight := chatgptwebauth.AccountInfoRefreshTarget{AuthID: "inflight", AuthInstanceID: "one"}
	scheduled := chatgptwebauth.AccountInfoRefreshTarget{AuthID: "scheduled", AuthInstanceID: "two"}
	queued := chatgptwebauth.AccountInfoRefreshTarget{AuthID: "queued", AuthInstanceID: "three"}
	newTarget := chatgptwebauth.AccountInfoRefreshTarget{AuthID: "new", AuthInstanceID: "four"}

	runtime.mu.Lock()
	runtime.inflight[chatGPTWebAccountInfoTargetKey(inflight)] = 1
	if !runtime.scheduleLocked("retry:"+chatGPTWebAccountInfoTargetKey(scheduled), now.Add(time.Hour), chatGPTWebAccountInfoWork{
		target: scheduled, attempt: 2, automatic: true,
	}) {
		t.Fatal("schedule existing retry")
	}
	if !runtime.enqueueLocked(chatGPTWebAccountInfoWork{target: queued, attempt: 1}) {
		t.Fatal("queue existing work")
	}
	dueBefore := runtime.scheduled["retry:"+chatGPTWebAccountInfoTargetKey(scheduled)].due
	runtime.schedulePeriodicTargetsLocked([]chatgptwebauth.AccountInfoRefreshTarget{
		inflight, scheduled, queued, newTarget,
	}, now)
	periodicPending := accountInfoPeriodicScheduleCountLocked(runtime)
	queuedWork := append([]chatGPTWebAccountInfoWork(nil), runtime.queuedWorkLocked()...)
	dueAfter := runtime.scheduled["retry:"+chatGPTWebAccountInfoTargetKey(scheduled)].due
	periodic := runtime.scheduled[chatGPTWebAccountInfoPeriodicSchedulePrefix+chatGPTWebAccountInfoTargetKey(newTarget)]
	runtime.mu.Unlock()

	if periodicPending != 1 || !dueAfter.Equal(dueBefore) {
		t.Fatalf("periodic overlap = scheduled %d due %s, want 1/%s", periodicPending, dueAfter, dueBefore)
	}
	if periodic == nil || chatGPTWebAccountInfoTargetKey(periodic.work.target) != chatGPTWebAccountInfoTargetKey(newTarget) {
		t.Fatalf("new periodic schedule = %+v", periodic)
	}
	if len(queuedWork) != 1 || chatGPTWebAccountInfoTargetKey(queuedWork[0].target) != chatGPTWebAccountInfoTargetKey(queued) {
		t.Fatalf("queued periodic work = %+v", queuedWork)
	}
}

func TestChatGPTWebAccountInfoPeriodicPendingDropsRetiredInstance(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.started = true
	runtime.cfg = config.ResolvedChatGPTWebAccountInfoConfig{
		RefreshWorkers:         1,
		RefreshQueueSize:       0,
		PeriodicRefreshMinutes: 1,
	}
	target := chatgptwebauth.AccountInfoRefreshTarget{AuthID: "retired", AuthInstanceID: "old"}
	runtime.mu.Lock()
	runtime.schedulePeriodicTargetsLocked([]chatgptwebauth.AccountInfoRefreshTarget{target}, time.Now())
	if accountInfoPeriodicScheduleCountLocked(runtime) != 1 {
		runtime.mu.Unlock()
		t.Fatal("retired target was not scheduled")
	}
	runtime.mu.Unlock()
	runtime.removeAuthInstance(target.AuthID, target.AuthInstanceID)
	runtime.mu.Lock()
	pending := accountInfoPeriodicScheduleCountLocked(runtime)
	runtime.mu.Unlock()
	if pending != 0 {
		t.Fatalf("retired periodic pending = %d", pending)
	}
}

func TestChatGPTWebAccountInfoPeriodicScanRespectsTTL(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "periodic-stale-account",
					"plan_type":  "plus",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 5},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	now := time.Now().UTC()
	register := func(id string, updatedAt time.Time) {
		t.Helper()
		auth := chatGPTWebTestAuth(id)
		credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
		if errCredential != nil {
			t.Fatalf("parse %q: %v", id, errCredential)
		}
		remaining := 5
		credential.PlanType = "plus"
		credential.ProfileUpdatedAt = updatedAt.Format(time.RFC3339Nano)
		credential.ImageQuotaRemaining = &remaining
		credential.QuotaState = chatgptwebauth.QuotaStateAvailable
		credential.QuotaUpdatedAt = updatedAt.Format(time.RFC3339Nano)
		credential.QuotaStale = false
		credential.ApplyToMetadata(auth.Metadata)
		if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("register %q: %v", id, errRegister)
		}
	}
	register("periodic-fresh", now.Add(-time.Minute))
	register("periodic-stale", now.Add(-time.Hour))

	period := 1
	ttl := 15
	workers := 2
	queueSize := 4
	maxRetries := 0
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{
			PeriodicRefreshMinutes: &period,
			RefreshTTLMinutes:      &ttl,
			RefreshWorkers:         &workers,
			RefreshQueueSize:       &queueSize,
			MaxRetries:             &maxRetries,
		},
	}}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL
	runtime := executor.accountInfo

	expected := time.Now().Add(-time.Second)
	runtime.mu.Lock()
	runtime.periodicNextAt = expected
	runtime.mu.Unlock()
	runtime.runPeriodicScan(expected)

	waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
		runtime.mu.Lock()
		idle := runtime.busy == 0 && runtime.waiting == 0 && runtime.queueLengthLocked() == 0 &&
			accountInfoPeriodicScheduleCountLocked(runtime) == 0 && len(runtime.inflight) == 0
		runtime.mu.Unlock()
		return idle && profileCalls.Load() == 1 && quotaCalls.Load() == 1
	})
	if profileCalls.Load() != 1 || quotaCalls.Load() != 1 {
		t.Fatalf("periodic upstream calls = profile %d quota %d, want one stale credential", profileCalls.Load(), quotaCalls.Load())
	}
}

func TestChatGPTWebAccountInfoAutoRefreshDisabledKeepsManualRefresh(t *testing.T) {
	enabled := false
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled: &enabled,
		}},
	})
	t.Cleanup(runtime.close)
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: "web.json", AuthID: "web.json"}

	if _, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{target}, false); !errors.Is(
		errStart,
		chatgptwebauth.ErrAccountInfoAutoRefreshDisabled,
	) {
		t.Fatalf("automatic start error = %v", errStart)
	}
	task, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{target}, true)
	if errStart != nil {
		t.Fatalf("manual start error = %v", errStart)
	}
	if task == nil || !task.Force || task.Total != 1 {
		t.Fatalf("manual task = %+v", task)
	}
	if runtime.triggerAutomaticRecheck(target.AuthID) {
		t.Fatal("automatic trigger was accepted while disabled")
	}
	if runtime.triggerImageQuotaEvidenceRecheck(target.AuthID) {
		t.Fatal("explicit image quota trigger was accepted while disabled")
	}
	if runtime.triggerImageQuotaEvidenceRecheckForInstance(target.AuthID, "instance-a") {
		t.Fatal("instance-bound image quota trigger was accepted while disabled")
	}
	if runtime.triggerAmbiguousImageRecheck(target.AuthID) {
		t.Fatal("ambiguous image recheck was accepted while disabled")
	}
}

func TestChatGPTWebAmbiguousImageRecheckUsesPerCredentialCooldown(t *testing.T) {
	now := time.Now().UTC()
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{})
	runtime.now = func() time.Time { return now }
	t.Cleanup(runtime.close)

	drainQueue := func() {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		for {
			work, ok := runtime.dequeueLocked()
			if !ok {
				return
			}
			runtime.releaseWorkEpochLocked(work)
		}
	}

	if !runtime.triggerAmbiguousImageRecheck("web-a.json") {
		t.Fatal("first ambiguous image recheck was rejected")
	}
	drainQueue()
	if runtime.triggerAmbiguousImageRecheck("web-a.json") {
		t.Fatal("ambiguous image recheck bypassed its cooldown")
	}
	if !runtime.triggerAmbiguousImageRecheck("web-b.json") {
		t.Fatal("cooldown from another credential blocked recheck")
	}
	drainQueue()

	now = now.Add(chatGPTWebAmbiguousImageRecheckCooldown)
	if !runtime.triggerAmbiguousImageRecheck("web-a.json") {
		t.Fatal("ambiguous image recheck remained blocked after cooldown")
	}
	drainQueue()

	if !runtime.triggerAutomaticRecheck("web-a.json") {
		t.Fatal("explicit quota recheck was blocked by ambiguous-error cooldown")
	}
}

func TestChatGPTWebImageQuotaEvidenceRecheckRejectsReplacedInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("quota-evidence-instance")
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	oldInstanceID := installed.RuntimeInstanceID()

	replacementCandidate := installed.Clone()
	replacementCandidate.Metadata["access_token"] = "replacement-access-token"
	replacement, current, errUpdate := manager.UpdateIfCurrent(
		cliproxyauth.WithForceRuntimeReplacement(cliproxyauth.WithSkipPersist(context.Background())),
		installed,
		replacementCandidate,
	)
	if errUpdate != nil || !current || replacement == nil || replacement.RuntimeInstanceID() == oldInstanceID {
		t.Fatalf("replace auth = (%+v, current=%v, err=%v)", replacement, current, errUpdate)
	}

	executor := &ChatGPTWebExecutor{manager: manager}
	runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
	executor.accountInfo = runtime
	t.Cleanup(runtime.close)

	if executor.TriggerImageQuotaEvidenceAccountInfoRefresh(auth.ID, oldInstanceID) {
		t.Fatal("quota evidence refresh accepted the replaced runtime instance")
	}
	if !executor.TriggerImageQuotaEvidenceAccountInfoRefresh(auth.ID, replacement.RuntimeInstanceID()) {
		t.Fatal("quota evidence refresh rejected the current runtime instance")
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	queued := runtime.queuedWorkLocked()
	if len(queued) != 1 || !queued[0].force ||
		queued[0].target.AuthInstanceID != replacement.RuntimeInstanceID() {
		t.Fatalf("queued quota evidence refresh = %+v", queued)
	}
}

func TestChatGPTWebAmbiguousImageRecheckConcurrentTriggersOnlyOnce(t *testing.T) {
	now := time.Now().UTC()
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{})
	runtime.now = func() time.Time { return now }
	t.Cleanup(runtime.close)

	var accepted atomic.Int32
	var wait sync.WaitGroup
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if runtime.triggerAmbiguousImageRecheck("web-concurrent.json") {
				accepted.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted ambiguous image rechecks = %d, want 1", got)
	}
	runtime.mu.Lock()
	queued := runtime.queueLengthLocked()
	runtime.mu.Unlock()
	if queued != 1 {
		t.Fatalf("queued ambiguous image rechecks = %d, want 1", queued)
	}
}

func TestChatGPTWebAccountInfoDisableClearsQueuedAutomaticWork(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{})
	t.Cleanup(runtime.close)
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: "web.json", AuthID: "web.json"}
	task, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{target}, false)
	if errStart != nil {
		t.Fatalf("start automatic task: %v", errStart)
	}
	runtime.mu.Lock()
	if !runtime.scheduleRecoveryForTargetLocked(target, time.Now().Add(time.Hour)) {
		runtime.mu.Unlock()
		t.Fatal("schedule recovery = false")
	}
	callContext, cancelCall := context.WithCancel(context.Background())
	call := &chatGPTWebAccountInfoCall{
		ctx:        callContext,
		cancel:     cancelCall,
		done:       make(chan struct{}),
		authID:     target.AuthID,
		runtimeKey: target.AuthID,
		accepting:  true,
	}
	runtime.calls[target.AuthID] = call
	runtime.inflight[target.AuthID] = 1
	runtime.started = true
	runtime.mu.Unlock()

	enabled := false
	runtime.updateConfig(&config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled: &enabled,
		}},
	})

	snapshot, found := runtime.task(task.ID)
	if !found || snapshot.State != chatgptwebauth.AccountInfoTaskCanceled ||
		snapshot.Canceled != 1 || snapshot.Processed != 1 {
		t.Fatalf("disabled task = %+v found=%v", snapshot, found)
	}
	runtime.mu.Lock()
	queueLength := runtime.queueLengthLocked()
	scheduled := len(runtime.scheduled)
	runtime.mu.Unlock()
	if queueLength != 0 || scheduled != 0 {
		t.Fatalf("automatic work remains queued=%d scheduled=%d", queueLength, scheduled)
	}
	select {
	case <-callContext.Done():
	default:
		t.Fatal("independent automatic call remained active")
	}
}

func TestChatGPTWebAccountInfoReenableRestoresPersistedRecovery(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-reenable-recovery")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = -1
	auth.Metadata["image_quota_reset_at"] = time.Now().Add(time.Hour).Format(time.RFC3339Nano)
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	enabled := false
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		AutoRefreshEnabled: &enabled,
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	if state := executor.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() {
		t.Fatalf("disabled startup scheduled recovery: %+v", state)
	}

	enabled = true
	executor.UpdateConfig(cfg)
	if state := executor.AccountInfoAuthState(auth.ID); state.NextRefreshAt.IsZero() {
		t.Fatal("re-enabling automatic refresh did not restore persisted recovery")
	}
}

func TestChatGPTWebAccountInfoDisabledAutomaticRefreshDoesNotPollUnknownReset(t *testing.T) {
	enabled := false
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			AutoRefreshEnabled: &enabled,
		}},
	})
	t.Cleanup(runtime.close)
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: "unknown-reset.json", AuthID: "unknown-reset"}

	runtime.mu.Lock()
	runtime.syncRecoveryScheduleForTargetLocked(target, true, time.Time{}, 0)
	scheduled := len(runtime.scheduled)
	runtime.mu.Unlock()
	if scheduled != 0 {
		t.Fatalf("disabled automatic refresh scheduled unknown reset polling: %d", scheduled)
	}
}

func TestChatGPTWebAccountInfoDisabledAutomaticRefreshRequiresManualRecovery(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer access-token" {
			t.Errorf("Authorization = %q", request.Header.Get("Authorization"))
		}
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "account-1",
					"plan_type":  "free",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{
					"feature_name": "image_gen",
					"remaining":    3,
					"reset_after":  time.Now().UTC().Add(time.Hour).Format(time.RFC3339),
				},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("disabled-reset-recovery")
	auth.Metadata["account_id"] = "account-1"
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = -2
	auth.Metadata["image_quota_reset_at"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	auth.ModelStates = map[string]*cliproxyauth.ModelState{
		chatgptwebauth.ImageModel: {
			Status:      cliproxyauth.StatusError,
			Unavailable: true,
			Quota: cliproxyauth.QuotaState{
				Exceeded: true,
				Reason:   "chatgpt_web_image_quota",
			},
		},
	}
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	enabled := false
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		AutoRefreshEnabled:    &enabled,
		RecoveryJitterSeconds: accountInfoTestInt(0),
		MaxRetries:            accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, nil)
	executor.manager = manager
	executor.runtimeBaseURL = server.URL
	t.Cleanup(func() { _ = executor.Close() })
	executor.SyncAccountInfoRecovery(auth)

	if state := executor.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() {
		t.Fatalf("disabled automatic refresh scheduled reset recovery: %+v", state)
	}
	if profileCalls.Load() != 0 || quotaCalls.Load() != 0 {
		t.Fatalf("disabled automatic refresh queried account info: profile:%d quota:%d", profileCalls.Load(), quotaCalls.Load())
	}
	current, _ := manager.GetByID(auth.ID)
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil || credential.ImageQuotaRemaining == nil ||
		*credential.ImageQuotaRemaining != -2 || credential.QuotaState != chatgptwebauth.QuotaStateExhausted {
		t.Fatalf("disabled automatic refresh changed exhausted quota: credential=%+v error=%v", credential, errCredential)
	}

	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name:   auth.ID,
		AuthID: auth.ID,
	}}, true)
	if errStart != nil {
		t.Fatalf("start manual refresh: %v", errStart)
	}
	waitForAccountInfoTask(t, executor, task.ID)
	if profileCalls.Load() != 1 || quotaCalls.Load() != 1 {
		t.Fatalf("manual recovery calls = profile:%d quota:%d, want one each", profileCalls.Load(), quotaCalls.Load())
	}
	current, _ = manager.GetByID(auth.ID)
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || imageState.Status == cliproxyauth.StatusError ||
		imageState.Unavailable || imageState.Quota.Exceeded {
		t.Fatalf("manual refresh did not restore image capability: %+v", imageState)
	}
	if state := executor.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() {
		t.Fatalf("manual recovery left an automatic schedule while disabled: %+v", state)
	}
}

func TestChatGPTWebAccountInfoRefreshPersistsProfileQuotaAndUsesTTL(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	var planType atomic.Value
	planType.Store("plus")
	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			if request.Method != http.MethodGet {
				t.Errorf("account check method = %s", request.Method)
			}
			account := map[string]any{"account_id": "account-1"}
			if currentPlanType, _ := planType.Load().(string); currentPlanType != "" {
				account["plan_type"] = currentPlanType
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": account},
			}})
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			if request.Method != http.MethodPost {
				t.Errorf("conversation init method = %s", request.Method)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{
					"feature_name": "image_gen",
					"remaining":    7,
					"reset_after":  resetAt.Format(time.RFC3339),
				},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info")
	customImageModel := "custom-web-image"
	auth.ModelStates = map[string]*cliproxyauth.ModelState{
		chatgptwebauth.ImageModel: {
			Status:         cliproxyauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Hour),
			Quota: cliproxyauth.QuotaState{
				Exceeded: true,
				Reason:   "chatgpt_web_image_quota",
			},
		},
		customImageModel: {
			Status:         cliproxyauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Hour),
			Quota: cliproxyauth.QuotaState{
				Exceeded: true,
				Reason:   "chatgpt_web_image_quota",
			},
		},
	}
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{
		ID:         customImageModel,
		UpstreamID: chatgptwebauth.ImageModel,
		Type:       registry.OpenAIImageModelType,
	}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
	})
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:    accountInfoTestInt(2),
		RefreshQueueSize:  accountInfoTestInt(4),
		RefreshTTLMinutes: accountInfoTestInt(15),
		MaxRetries:        accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	})
	executor.runtimeBaseURL = server.URL

	targets := []chatgptwebauth.AccountInfoRefreshTarget{{Name: "account-info.json", AuthID: auth.ID}}
	first, errStart := executor.StartAccountInfoRefreshTask(targets, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	first = waitForAccountInfoTask(t, executor, first.ID)
	if first.State != chatgptwebauth.AccountInfoTaskCompleted ||
		len(first.Results) != 1 ||
		first.Results[0].Status != chatgptwebauth.AccountInfoResultUpdated {
		t.Fatalf("first task = %+v", first)
	}

	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.PlanType != "plus" || credential.AccountID != "account-1" {
		t.Fatalf("account profile = plan %q account %q", credential.PlanType, credential.AccountID)
	}
	if credential.ProfileUpdatedAt == "" || credential.QuotaUpdatedAt == "" {
		t.Fatalf("account info timestamps = profile %q quota %q", credential.ProfileUpdatedAt, credential.QuotaUpdatedAt)
	}
	if credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != 7 ||
		credential.QuotaState != chatgptwebauth.QuotaStateAvailable ||
		credential.QuotaStale {
		t.Fatalf("quota = %+v", credential)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || imageState.Status == cliproxyauth.StatusError ||
		imageState.Unavailable || imageState.Quota.Exceeded {
		t.Fatalf("available quota did not clear image quota state: %+v", imageState)
	}
	customImageState := current.ModelStates[customImageModel]
	if customImageState == nil || customImageState.Status == cliproxyauth.StatusError ||
		customImageState.Unavailable || customImageState.Quota.Exceeded {
		t.Fatalf("available quota did not clear custom image quota state: %+v", customImageState)
	}

	second, errStart := executor.StartAccountInfoRefreshTask(targets, false)
	if errStart != nil {
		t.Fatalf("second StartAccountInfoRefreshTask() error = %v", errStart)
	}
	second = waitForAccountInfoTask(t, executor, second.ID)
	if second.Results[0].Status != chatgptwebauth.AccountInfoResultFresh {
		t.Fatalf("second result = %+v", second.Results[0])
	}
	if profileCalls.Load() != 1 || quotaCalls.Load() != 1 {
		t.Fatalf("upstream calls = profile %d quota %d, want one each", profileCalls.Load(), quotaCalls.Load())
	}

	planType.Store("")
	third, errStart := executor.StartAccountInfoRefreshTask(targets, true)
	if errStart != nil {
		t.Fatalf("third StartAccountInfoRefreshTask() error = %v", errStart)
	}
	third = waitForAccountInfoTask(t, executor, third.ID)
	if third.Results[0].Status != chatgptwebauth.AccountInfoResultUpdated {
		t.Fatalf("third result = %+v, want updated plan type", third.Results[0])
	}
	current, _ = manager.GetByID(auth.ID)
	credential, errCredential = chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() after empty plan error = %v", errCredential)
	}
	if credential.PlanType != "" {
		t.Fatalf("plan type after empty profile = %q, want empty", credential.PlanType)
	}
	if profileCalls.Load() != 2 || quotaCalls.Load() != 2 {
		t.Fatalf("forced upstream calls = profile %d quota %d, want two each", profileCalls.Load(), quotaCalls.Load())
	}
}

func TestChatGPTWebAccountInfoQueuedForcedTasksShareOneRefresh(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			started <- struct{}{}
			<-release
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "shared-account",
					"plan_type":  "plus",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			started <- struct{}{}
			<-release
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 5},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("queued-force-singleflight")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(1),
		RefreshQueueSize: accountInfoTestInt(2),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL
	target := []chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "queued-force.json", AuthID: auth.ID,
	}}

	first, errStart := executor.StartAccountInfoRefreshTask(target, true)
	if errStart != nil {
		t.Fatalf("first StartAccountInfoRefreshTask() error = %v", errStart)
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("first forced refresh did not start both upstream requests")
		}
	}
	second, errStart := executor.StartAccountInfoRefreshTask(target, true)
	if errStart != nil {
		t.Fatalf("second StartAccountInfoRefreshTask() error = %v", errStart)
	}
	close(release)

	first = waitForAccountInfoTask(t, executor, first.ID)
	second = waitForAccountInfoTask(t, executor, second.ID)
	if first.Failed != 0 || second.Failed != 0 {
		t.Fatalf("forced tasks failed: first=%+v second=%+v", first, second)
	}
	if profileCalls.Load() != 1 || quotaCalls.Load() != 1 {
		t.Fatalf(
			"queued forced tasks made profile/quota calls %d/%d, want 1/1",
			profileCalls.Load(),
			quotaCalls.Load(),
		)
	}
}

func TestChatGPTWebAccountInfoDequeuedFollowerKeepsSharedCall(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	started := make(chan struct{}, 2)
	releaseUpstream := make(chan struct{})
	var releaseUpstreamOnce sync.Once
	t.Cleanup(func() {
		releaseUpstreamOnce.Do(func() { close(releaseUpstream) })
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			started <- struct{}{}
			<-releaseUpstream
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "shared-account",
					"plan_type":  "plus",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			started <- struct{}{}
			<-releaseUpstream
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 5},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("dequeued-follower-singleflight")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(2),
		RefreshQueueSize: accountInfoTestInt(2),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	ownerRegistered := make(chan struct{})
	releaseOwner := make(chan struct{})
	followerReserved := make(chan struct{})
	var ownerRegisteredOnce sync.Once
	var followerReservedOnce sync.Once
	var releaseOwnerOnce sync.Once
	t.Cleanup(func() {
		releaseOwnerOnce.Do(func() { close(releaseOwner) })
	})
	executor.accountInfo.mu.Lock()
	executor.accountInfo.beforeAccountInfoExecution = func(
		_ chatGPTWebAccountInfoWork,
		call *chatGPTWebAccountInfoCall,
		owner bool,
	) {
		if call == nil {
			return
		}
		if owner {
			ownerRegisteredOnce.Do(func() { close(ownerRegistered) })
			<-releaseOwner
			return
		}
		followerReservedOnce.Do(func() { close(followerReserved) })
	}
	executor.accountInfo.mu.Unlock()

	target := []chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "dequeued-follower.json", AuthID: auth.ID,
	}}
	first, errStart := executor.StartAccountInfoRefreshTask(target, true)
	if errStart != nil {
		t.Fatalf("first StartAccountInfoRefreshTask() error = %v", errStart)
	}
	select {
	case <-ownerRegistered:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not register its shared call")
	}
	second, errStart := executor.StartAccountInfoRefreshTask(target, true)
	if errStart != nil {
		t.Fatalf("second StartAccountInfoRefreshTask() error = %v", errStart)
	}
	select {
	case <-followerReserved:
	case <-time.After(2 * time.Second):
		t.Fatal("second worker did not reserve the running shared call")
	}

	releaseOwnerOnce.Do(func() { close(releaseOwner) })
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("shared refresh did not start both upstream requests")
		}
	}
	releaseUpstreamOnce.Do(func() { close(releaseUpstream) })
	first = waitForAccountInfoTask(t, executor, first.ID)
	second = waitForAccountInfoTask(t, executor, second.ID)

	if first.Failed != 0 || second.Failed != 0 {
		t.Fatalf("forced tasks failed: first=%+v second=%+v", first, second)
	}
	if profileCalls.Load() != 1 || quotaCalls.Load() != 1 {
		t.Fatalf(
			"dequeued follower made profile/quota calls %d/%d, want 1/1",
			profileCalls.Load(),
			quotaCalls.Load(),
		)
	}
}

func TestChatGPTWebAccountInfoFreshFollowerConsumesCompletedCall(t *testing.T) {
	now := time.Now().UTC()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("completed-call-follower")
	auth.Metadata["profile_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	done := make(chan struct{})
	close(done)
	call := &chatGPTWebAccountInfoCall{
		done:       done,
		authID:     auth.ID,
		runtimeKey: auth.ID,
		epoch:      1,
		outcome: chatGPTWebAccountInfoOutcome{
			status: chatgptwebauth.AccountInfoResultUpdated,
		},
		accepting: true,
		completed: true,
		force:     true,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		executor: &ChatGPTWebExecutor{
			manager: manager,
			now:     func() time.Time { return now },
		},
		authEpoch: map[string]uint64{auth.ID: 1},
		calls:     map[string]*chatGPTWebAccountInfoCall{auth.ID: call},
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: auth.ID},
		epoch:  1,
	}

	acquired, owner, immediate := runtime.prepareAccountInfoExecution(work)
	if acquired != call || owner || immediate != nil {
		t.Fatalf("completed call acquisition = call:%p owner:%t immediate:%+v", acquired, owner, immediate)
	}
	outcome := runtime.waitForAccountInfoCall(work, acquired, owner)
	if outcome.status != chatgptwebauth.AccountInfoResultUpdated {
		t.Fatalf("shared outcome = %+v", outcome)
	}
	if runtime.calls[auth.ID] != nil {
		t.Fatal("completed call remained after the fresh follower consumed it")
	}
}

func TestChatGPTWebAccountInfoRefreshFailurePreservesSuccessfulQuota(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			http.Error(w, "unauthorized", http.StatusUnauthorized)
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 0},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("refresh-failure-partial")
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	credential.RefreshStrategy = chatgptwebauth.RefreshStrategyTokenOnly
	credential.RefreshToken = ""
	credential.Password = ""
	credential.TOTPSecret = ""
	credential.ApplyToMetadata(auth.Metadata)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	outcome := executor.refreshChatGPTWebAccountInfo(t.Context(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultPartial {
		t.Fatalf("account info outcome = %+v, want partial", outcome)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential = chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() after refresh error = %v", errCredential)
	}
	if credential.ImageQuotaRemaining == nil ||
		*credential.ImageQuotaRemaining != 0 ||
		credential.QuotaState != chatgptwebauth.QuotaStateExhausted {
		t.Fatalf("successful quota half was discarded: %+v", credential)
	}
}

func TestChatGPTWebAccountInfoRejectsCrossOriginAccountCheckRedirect(t *testing.T) {
	var crossOriginCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		crossOriginCalls.Add(1)
		if request.Header.Get("Oai-Device-Id") != "" ||
			request.Header.Get("Oai-Session-Id") != "" ||
			request.Header.Get("Chatgpt-Account-Id") != "" {
			t.Error("cross-origin account check leaked identity headers")
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			http.Redirect(response, request, target.URL+"/capture", http.StatusTemporaryRedirect)
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(response).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 4},
			}})
		default:
			http.NotFound(response, request)
		}
	}))
	defer source.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-cross-origin")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = source.URL

	outcome := executor.refreshChatGPTWebAccountInfo(t.Context(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultPartial {
		t.Fatalf("account info outcome = %+v, want partial", outcome)
	}
	if calls := crossOriginCalls.Load(); calls != 0 {
		t.Fatalf("cross-origin account check calls = %d, want zero", calls)
	}
}

func TestChatGPTWebAccountInfoPersistenceContextStartsCanceled(t *testing.T) {
	runtimeContext, cancelRuntime := context.WithCancel(context.Background())
	runtime := &chatGPTWebAccountInfoRuntime{ctx: runtimeContext}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()

	persistContext, cancelPersist := runtime.persistenceContext(requestContext)
	defer cancelPersist()
	if !errors.Is(persistContext.Err(), context.Canceled) {
		t.Fatalf("persistence context error = %v, want context.Canceled", persistContext.Err())
	}

	cancelRuntime()
}

func TestChatGPTWebAccountInfoPersistenceCancelsWithTaskAndStopsOnClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath, chatgptwebauth.ConversationInitPath:
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	store := &accountInfoTestLifecycleStore{
		started:  make(chan context.Context, 1),
		finished: make(chan error, 1),
		release:  make(chan struct{}),
	}
	manager := cliproxyauth.NewManager(store, nil, nil)
	auth := chatGPTWebTestAuth("account-info-persistence-lifecycle")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(1),
		RefreshQueueSize: accountInfoTestInt(1),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() {
		store.releaseStore()
		_ = executor.Close()
	})
	executor.runtimeBaseURL = server.URL
	executor.accountInfoTimeout = 250 * time.Millisecond

	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "persistence-lifecycle.json", AuthID: auth.ID,
	}}, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	var persistContext context.Context
	select {
	case persistContext = <-store.started:
	case <-time.After(2 * time.Second):
		t.Fatal("persistence did not reach the blocking store")
	}
	if deadline, ok := persistContext.Deadline(); ok {
		t.Fatalf("persistence inherited acquisition deadline %s", deadline)
	}

	executor.accountInfo.mu.Lock()
	var call *chatGPTWebAccountInfoCall
	for _, currentCall := range executor.accountInfo.calls {
		call = currentCall
		break
	}
	executor.accountInfo.mu.Unlock()
	if call == nil {
		t.Fatal("account-info call disappeared while persistence was blocked")
	}
	select {
	case <-call.ctx.Done():
		if !errors.Is(call.ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("acquisition context error = %v, want deadline exceeded", call.ctx.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("acquisition context deadline did not expire while persistence was blocked")
	}
	if _, found := executor.CancelAccountInfoRefreshTask(task.ID); !found {
		t.Fatal("CancelAccountInfoRefreshTask() did not find task")
	}
	select {
	case <-persistContext.Done():
		if !errors.Is(persistContext.Err(), context.Canceled) {
			t.Fatalf("persistence context error = %v, want context canceled", persistContext.Err())
		}
	case <-time.After(time.Second):
		t.Fatal("task cancellation did not cancel persistence lock wait")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- executor.Close()
	}()
	select {
	case errPersist := <-store.finished:
		if !errors.Is(errPersist, context.Canceled) {
			t.Fatalf("blocking store stopped with %v, want context canceled", errPersist)
		}
	case <-time.After(time.Second):
		t.Fatal("executor close did not cancel blocking persistence")
	}
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("Close() error = %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("executor close did not finish after persistence cancellation")
	}
}

func TestChatGPTWebAccountInfoRefreshPreservesLastQuotaOnPartialFailure(t *testing.T) {
	var profileCalls atomic.Int32
	var quotaCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			profileCalls.Add(1)
			http.Error(w, "temporary profile failure", http.StatusBadGateway)
		case chatgptwebauth.ConversationInitPath:
			quotaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 0, "reset_after": time.Now().Add(time.Hour).Format(time.RFC3339)},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-partial")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	auth.Metadata["image_quota_remaining"] = 4
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(1),
		RefreshQueueSize: accountInfoTestInt(1),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "partial.json", AuthID: auth.ID,
	}}, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	task = waitForAccountInfoTask(t, executor, task.ID)
	if task.Results[0].Status != chatgptwebauth.AccountInfoResultPartial {
		t.Fatalf("task result = %+v", task.Results[0])
	}
	if task.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors || task.Partial != 1 {
		t.Fatalf("partial task summary = %+v", task)
	}
	current, _ := manager.GetByID(auth.ID)
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != 0 ||
		credential.QuotaState != chatgptwebauth.QuotaStateExhausted ||
		credential.QuotaStale {
		t.Fatalf("partial quota was not persisted: %+v", credential)
	}

	second, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "partial.json", AuthID: auth.ID,
	}}, false)
	if errStart != nil {
		t.Fatalf("second StartAccountInfoRefreshTask() error = %v", errStart)
	}
	second = waitForAccountInfoTask(t, executor, second.ID)
	if second.Results[0].Status != chatgptwebauth.AccountInfoResultPartial {
		t.Fatalf("second task result = %+v", second.Results[0])
	}
	if profileCalls.Load() != 2 || quotaCalls.Load() != 2 {
		t.Fatalf("partial account info was treated as fresh: profile=%d quota=%d", profileCalls.Load(), quotaCalls.Load())
	}
}

func TestChatGPTWebAccountInfoRejectsByteDifferentAccountIdentity(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "account-a",
					"plan_type":  "team",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 9},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	for _, testCase := range []struct {
		name      string
		accountID string
	}{
		{name: "case", accountID: "Account-A"},
		{name: "leading-space", accountID: " account-a"},
		{name: "trailing-space", accountID: "account-a "},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			auth := chatGPTWebTestAuth("account-info-byte-exact-identity-" + testCase.name)
			auth.Metadata["account_id"] = testCase.accountID
			auth.Metadata["plan_type"] = "free"
			auth.Metadata["image_quota_remaining"] = 4
			auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
			if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
				MaxRetries: accountInfoTestInt(0),
			}}}
			executor := NewChatGPTWebExecutor(cfg, manager)
			t.Cleanup(func() { _ = executor.Close() })
			executor.runtimeBaseURL = server.URL

			outcome := executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
			if outcome.status != chatgptwebauth.AccountInfoResultFailed || outcome.errorCode != "identity_mismatch" {
				t.Fatalf("account info outcome = %+v, want identity mismatch", outcome)
			}
			current, _ := manager.GetByID(auth.ID)
			credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
			if errCredential != nil {
				t.Fatalf("ParseCredential() error = %v", errCredential)
			}
			if credential.AccountID != testCase.accountID || credential.PlanType != "free" ||
				credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != 4 {
				t.Fatalf("identity mismatch changed account data: %+v", credential)
			}
			if !credential.QuotaStale || credential.QuotaLastError != "identity_mismatch" {
				t.Fatalf("identity mismatch failure state = %+v", credential)
			}
		})
	}
}

func TestChatGPTWebAccountInfoRuntimeUsesFiniteRetries(t *testing.T) {
	now := time.Now()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:        1,
			RefreshQueueSize:      1,
			RefreshTTLMinutes:     15,
			RecoveryJitterSeconds: 0,
			MaxRetries:            3,
		},
		tasks:     make(map[string]*chatGPTWebAccountInfoTaskState),
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:  make(map[string]int),
		workers:   map[int]chan struct{}{0: make(chan struct{})},
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
		random:    bytes.NewReader(make([]byte, 64)),
	}
	work := chatGPTWebAccountInfoWork{
		target:    chatgptwebauth.AccountInfoRefreshTarget{Name: "retry.json", AuthID: "retry"},
		attempt:   1,
		automatic: true,
	}
	outcome := chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "network_error",
		retryable: true,
	}
	for wantAttempt := 2; wantAttempt <= 4; wantAttempt++ {
		runtime.finishWorkLocked(work, outcome)
		entry := runtime.scheduled["retry:retry"]
		if entry == nil || entry.work.attempt != wantAttempt {
			t.Fatalf("scheduled retry = %+v, want attempt %d", entry, wantAttempt)
		}
		runtime.removeScheduleLocked(entry.key)
		work = entry.work
	}
	runtime.finishWorkLocked(work, outcome)
	if entry := runtime.scheduled["retry:retry"]; entry != nil {
		t.Fatalf("unexpected retry after max attempts: %+v", entry)
	}
	if runtime.retryCount != 3 || runtime.failedCount != 1 {
		t.Fatalf("retry/failed counts = %d/%d, want 3/1", runtime.retryCount, runtime.failedCount)
	}
}

func TestChatGPTWebAccountInfoPartialResultSurvivesRetryFailure(t *testing.T) {
	now := time.Now().UTC()
	taskCtx := context.Background()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
			MaxRetries:       1,
		},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"partial-task": {
				snapshot: chatgptwebauth.AccountInfoRefreshTask{
					ID:    "partial-task",
					Total: 1,
					State: chatgptwebauth.AccountInfoTaskRunning,
					Results: []chatgptwebauth.AccountInfoRefreshResult{{
						Name:   "partial.json",
						Status: chatgptwebauth.AccountInfoResultRunning,
					}},
				},
				ctx: taskCtx,
			},
		},
		states:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:      make(map[string]int),
		workers:       map[int]chan struct{}{0: make(chan struct{})},
		scheduled:     make(map[string]*chatGPTWebAccountInfoSchedule),
		authEpoch:     make(map[string]uint64),
		authEpochRefs: make(map[string]int),
		wake:          make(chan struct{}, 1),
		now:           func() time.Time { return now },
		random:        bytes.NewReader(make([]byte, 64)),
	}
	work := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{Name: "partial.json", AuthID: "partial"},
		taskID:  "partial-task",
		index:   0,
		attempt: 1,
	}
	runtime.finishWorkLocked(work, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultPartial,
		errorCode: "quota_refresh_failed",
		retryable: true,
	})
	retry := runtime.removeScheduleLocked("task:partial-task:0")
	if retry == nil || !retry.work.partialApplied {
		t.Fatalf("partial retry = %+v", retry)
	}
	runtime.finishWorkLocked(retry.work, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "network_error",
	})

	task := runtime.tasks["partial-task"].snapshot
	if task.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors ||
		task.Processed != 1 || task.Succeeded != 1 || task.Partial != 1 || task.Failed != 0 ||
		task.Results[0].Status != chatgptwebauth.AccountInfoResultPartial {
		t.Fatalf("task after retry failure = %+v", task)
	}
}

func TestChatGPTWebAccountInfoRetryUsesLaterUpstreamRetryAfter(t *testing.T) {
	now := time.Now()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
			MaxRetries:       1,
		},
		tasks:         make(map[string]*chatGPTWebAccountInfoTaskState),
		states:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:      make(map[string]int),
		workers:       map[int]chan struct{}{0: make(chan struct{})},
		scheduled:     make(map[string]*chatGPTWebAccountInfoSchedule),
		authEpoch:     make(map[string]uint64),
		authEpochRefs: make(map[string]int),
		wake:          make(chan struct{}, 1),
		now:           func() time.Time { return now },
		random:        bytes.NewReader(make([]byte, 64)),
	}
	work := chatGPTWebAccountInfoWork{
		target:    chatgptwebauth.AccountInfoRefreshTarget{Name: "retry-after.json", AuthID: "retry-after"},
		attempt:   1,
		automatic: true,
	}
	runtime.finishWorkLocked(work, chatGPTWebAccountInfoOutcome{
		status:     chatgptwebauth.AccountInfoResultFailed,
		errorCode:  "rate_limited",
		retryable:  true,
		retryAfter: 2 * time.Minute,
	})
	entry := runtime.scheduled["retry:retry-after"]
	if entry == nil || !entry.due.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("retry due = %+v, want %v", entry, now.Add(2*time.Minute))
	}

	secondsError := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		chatgptwebauth.ConversationInitPath,
		[]byte(`{"error":"rate limited"}`),
		fhttp.Header{"Retry-After": {"120"}},
	)
	if got := chatGPTWebAccountInfoRetryAfter(secondsError); got != 2*time.Minute {
		t.Fatalf("numeric Retry-After = %v, want 2m", got)
	}
	retryAt := time.Now().UTC().Add(3 * time.Minute).Truncate(time.Second)
	dateError := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		chatgptwebauth.AccountCheckPath,
		[]byte(`{"error":"rate limited"}`),
		fhttp.Header{"Retry-After": {retryAt.Format(http.TimeFormat)}},
	)
	if got := chatGPTWebAccountInfoRetryAfter(dateError); got < 2*time.Minute+58*time.Second || got > 3*time.Minute {
		t.Fatalf("HTTP-date Retry-After = %v, want about 3m", got)
	}
	longError := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		chatgptwebauth.ConversationInitPath,
		[]byte(`{"error":"rate limited"}`),
		fhttp.Header{"Retry-After": {"86400"}},
	)
	if got := chatGPTWebAccountInfoRetryAfter(longError); got != chatGPTWebAccountInfoMaxRetryAfter {
		t.Fatalf("long Retry-After = %v, want %v", got, chatGPTWebAccountInfoMaxRetryAfter)
	}
}

func TestChatGPTWebAccountInfoRetryAfterCapBoundsDelayedTaskSlot(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	taskCtx, cancelTask := context.WithCancel(context.Background())
	defer cancelTask()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
			MaxRetries:       1,
		},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task-a": {
				snapshot: chatgptwebauth.AccountInfoRefreshTask{
					ID:    "task-a",
					State: chatgptwebauth.AccountInfoTaskRunning,
					Total: 1,
					Results: []chatgptwebauth.AccountInfoRefreshResult{{
						Name:   "retry-after-cap.json",
						Status: chatgptwebauth.AccountInfoResultRunning,
					}},
				},
				ctx:    taskCtx,
				cancel: cancelTask,
			},
		},
		states:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		authInstances: make(map[string]string),
		authEpoch:     make(map[string]uint64),
		authEpochRefs: make(map[string]int),
		workers:       map[int]chan struct{}{0: make(chan struct{})},
		scheduled:     make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:          make(chan struct{}, 1),
		now:           func() time.Time { return now },
		random:        bytes.NewReader(make([]byte, 8)),
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{
			Name:           "retry-after-cap.json",
			AuthID:         "retry-after-cap",
			AuthInstanceID: "instance-a",
		},
		taskID:  "task-a",
		index:   0,
		attempt: 1,
	}
	runtime.finishWorkLocked(work, chatGPTWebAccountInfoOutcome{
		status:     chatgptwebauth.AccountInfoResultFailed,
		errorCode:  "rate_limited",
		retryable:  true,
		retryAfter: 24 * time.Hour,
	})

	entry := runtime.scheduled["task:task-a:0"]
	wantDue := now.Add(chatGPTWebAccountInfoMaxRetryAfter)
	if entry == nil || !entry.due.Equal(wantDue) || entry.work.attempt != 2 {
		t.Fatalf("bounded delayed task = %+v, want attempt 2 due %s", entry, wantDue)
	}
	if runtime.delayedTasks != 1 {
		t.Fatalf("delayed task slots = %d, want 1", runtime.delayedTasks)
	}
	result := runtime.tasks["task-a"].snapshot.Results[0]
	if result.Status != chatgptwebauth.AccountInfoResultRetrying || result.Attempts != 1 {
		t.Fatalf("retrying task result = %+v", result)
	}
}

func TestChatGPTWebAccountInfoAutomaticRecheckPreservesRetrySchedule(t *testing.T) {
	now := time.Now().UTC()
	retryAt := now.Add(10 * time.Minute)
	const authID = "automatic-retry"
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:            make(chan struct{}, 1),
		now:             func() time.Time { return now },
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	work := chatGPTWebAccountInfoWork{
		target:    chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID},
		force:     false,
		attempt:   2,
		automatic: true,
	}
	if !runtime.scheduleLocked("retry:"+authID, retryAt, work) {
		t.Fatal("scheduleLocked() rejected retry")
	}

	for index := 0; index < 3; index++ {
		if !runtime.triggerAutomaticRecheck(authID) {
			t.Fatalf("automatic recheck %d was rejected", index)
		}
	}
	retry := runtime.scheduled["retry:"+authID]
	if retry == nil || !retry.due.Equal(retryAt) || !retry.work.force || len(runtime.queue) != 0 {
		t.Fatalf("automatic recheck changed retry: entry=%+v queue=%+v", retry, runtime.queue)
	}
	if state := runtime.states[authID]; !state.NextRefreshAt.Equal(retryAt) {
		t.Fatalf("NextRefreshAt = %v, want %v", state.NextRefreshAt, retryAt)
	}

	runtime.inflight[authID] = 1
	if !runtime.triggerAutomaticRecheck(authID) ||
		runtime.pendingTriggers[authID] != chatGPTWebAccountInfoTriggerAutomaticRecheck {
		t.Fatalf("pending automatic trigger = %v", runtime.pendingTriggers)
	}
	delete(runtime.inflight, authID)
	runtime.enqueuePendingTriggerLocked(authID)
	retry = runtime.scheduled["retry:"+authID]
	if retry == nil || !retry.due.Equal(retryAt) ||
		runtime.pendingTriggers[authID] != chatGPTWebAccountInfoTriggerNone {
		t.Fatalf("pending automatic recheck changed retry: entry=%+v pending=%v", retry, runtime.pendingTriggers)
	}

	if !runtime.trigger(authID, true) {
		t.Fatal("explicit force trigger was rejected")
	}
	if retry = runtime.scheduled["retry:"+authID]; retry == nil || !retry.due.Equal(now) {
		t.Fatalf("explicit force did not promote retry: %+v", retry)
	}
}

func TestChatGPTWebAccountInfoAutomaticRecheckForcesTaskRetryWithoutPromotingIt(t *testing.T) {
	now := time.Now().UTC()
	retryAt := now.Add(10 * time.Minute)
	const authID = "automatic-task-retry"
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		states:            make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:          make(map[string]int),
		inflightForce:     make(map[string]int),
		pendingTriggers:   make(map[string]chatGPTWebAccountInfoTriggerMode),
		scheduled:         make(map[string]*chatGPTWebAccountInfoSchedule),
		scheduledByTarget: make(map[string]map[string]*chatGPTWebAccountInfoSchedule),
		wake:              make(chan struct{}, 1),
		now:               func() time.Time { return now },
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	work := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID},
		taskID:  "task-a",
		index:   0,
		attempt: 2,
	}
	if !runtime.scheduleLocked("task:task-a:0", retryAt, work) {
		t.Fatal("scheduleLocked() rejected task retry")
	}

	if !runtime.triggerAutomaticRecheck(authID) {
		t.Fatal("automatic recheck was rejected")
	}
	retry := runtime.scheduled["task:task-a:0"]
	if retry == nil || !retry.due.Equal(retryAt) || !retry.work.force ||
		retry.work.independentTrigger != chatGPTWebAccountInfoTriggerAutomaticRecheck {
		t.Fatalf("automatic task retry = %+v, want original due with forced recheck", retry)
	}
}

func TestChatGPTWebAccountInfoCloseReleasesRuntimeIndexes(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{})
	runtime.tasks["task"] = &chatGPTWebAccountInfoTaskState{}
	runtime.states["auth"] = chatgptwebauth.AccountInfoAuthRuntimeState{Refreshing: true}
	runtime.pendingTriggers["auth"] = chatGPTWebAccountInfoTriggerForce
	runtime.ambiguousImageRecheckAfter["auth"] = time.Now().Add(time.Minute)
	runtime.scheduled["retry:auth"] = &chatGPTWebAccountInfoSchedule{}
	runtime.scheduledByTarget["auth"] = map[string]*chatGPTWebAccountInfoSchedule{
		"retry:auth": runtime.scheduled["retry:auth"],
	}
	runtime.calls["auth"] = &chatGPTWebAccountInfoCall{}

	runtime.close()

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.tasks) != 0 || len(runtime.states) != 0 || len(runtime.pendingTriggers) != 0 ||
		len(runtime.ambiguousImageRecheckAfter) != 0 ||
		len(runtime.scheduled) != 0 || len(runtime.scheduledByTarget) != 0 || len(runtime.calls) != 0 {
		t.Fatalf(
			"runtime indexes after close: tasks=%d states=%d pending=%d ambiguous=%d scheduled=%d reverse=%d calls=%d",
			len(runtime.tasks),
			len(runtime.states),
			len(runtime.pendingTriggers),
			len(runtime.ambiguousImageRecheckAfter),
			len(runtime.scheduled),
			len(runtime.scheduledByTarget),
			len(runtime.calls),
		)
	}
}

func TestChatGPTWebAccountInfoTaskReportsQueueCapacityFailure(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 0,
		},
		tasks:     make(map[string]*chatGPTWebAccountInfoTaskState),
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:  make(map[string]int),
		workers:   map[int]chan struct{}{0: make(chan struct{})},
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		ctx:       context.Background(),
		busy:      1,
		now:       time.Now,
	}

	task, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "queue-full.json", AuthID: "queue-full",
	}}, true)
	if errStart != nil {
		t.Fatalf("startTask() error = %v", errStart)
	}
	if task.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors ||
		len(task.Results) != 1 ||
		task.Results[0].Status != chatgptwebauth.AccountInfoResultFailed ||
		task.Results[0].Error != "refresh_queue_full" {
		t.Fatalf("queue-full task = %+v", task)
	}
}

func TestChatGPTWebAccountInfoForcedTriggerDoesNotBypassQueueCapacity(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 0,
		},
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
		workers:         map[int]chan struct{}{0: make(chan struct{})},
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:            make(chan struct{}, 1),
		busy:            1,
		now:             time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: "overflow.json", AuthID: "overflow"}

	if runtime.triggerTargetLocked(target, chatGPTWebAccountInfoTriggerForce) {
		t.Fatal("forced trigger bypassed the bounded runnable queue")
	}
	if len(runtime.queue) != 0 || len(runtime.scheduled) != 0 {
		t.Fatalf("overflow trigger created work: queue=%+v scheduled=%+v", runtime.queue, runtime.scheduled)
	}
}

func TestChatGPTWebAccountInfoQueueHeadPreservesFIFOAndCompacts(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{}
	for index := 0; index < 2050; index++ {
		runtime.queue = append(runtime.queue, chatGPTWebAccountInfoWork{
			target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: fmt.Sprintf("auth-%d", index)},
		})
	}
	for index := 0; index < 1025; index++ {
		work, ok := runtime.dequeueLocked()
		if !ok || work.target.AuthID != fmt.Sprintf("auth-%d", index) {
			t.Fatalf("dequeue %d = (%+v, %v)", index, work, ok)
		}
	}
	if runtime.queueHead != 0 || len(runtime.queue) != 1025 {
		t.Fatalf("compacted queue = head:%d len:%d", runtime.queueHead, len(runtime.queue))
	}
	for index := 1025; index < 2050; index++ {
		work, ok := runtime.dequeueLocked()
		if !ok || work.target.AuthID != fmt.Sprintf("auth-%d", index) {
			t.Fatalf("dequeue %d = (%+v, %v)", index, work, ok)
		}
	}
	if runtime.queueHead != 0 || len(runtime.queue) != 0 {
		t.Fatalf("drained queue = head:%d len:%d", runtime.queueHead, len(runtime.queue))
	}
}

func TestChatGPTWebAccountInfoTaskLimitRejectsBeforeCreatingLargeTask(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{
		tasks: make(map[string]*chatGPTWebAccountInfoTaskState, chatGPTWebAccountInfoTaskMaxKept),
		ctx:   context.Background(),
		now:   time.Now,
	}
	for index := 0; index < chatGPTWebAccountInfoTaskMaxKept; index++ {
		id := fmt.Sprintf("active-%d", index)
		runtime.tasks[id] = &chatGPTWebAccountInfoTaskState{
			snapshot: chatgptwebauth.AccountInfoRefreshTask{ID: id, CreatedAt: time.Now()},
		}
	}
	targets := make([]chatgptwebauth.AccountInfoRefreshTarget, chatgptwebauth.AccountInfoMaxTargets)
	for index := range targets {
		targets[index] = chatgptwebauth.AccountInfoRefreshTarget{
			Name:   fmt.Sprintf("auth-%d.json", index),
			AuthID: fmt.Sprintf("auth-%d", index),
		}
	}

	task, errStart := runtime.startTask(targets, true)
	if errStart == nil || !errors.Is(errStart, chatgptwebauth.ErrAccountInfoTaskLimitReached) ||
		!strings.Contains(errStart.Error(), "active task limit reached") {
		t.Fatalf("startTask() = (%v, %v), want explicit task limit error", task, errStart)
	}
	if task != nil {
		t.Fatalf("startTask() task = %+v, want nil", task)
	}
	if len(runtime.tasks) != chatGPTWebAccountInfoTaskMaxKept || runtime.taskReservations != 0 {
		t.Fatalf("task limit changed runtime: tasks=%d reservations=%d", len(runtime.tasks), runtime.taskReservations)
	}
}

func TestChatGPTWebAccountInfoConcurrentStartsRespectTaskLimit(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		tasks:     make(map[string]*chatGPTWebAccountInfoTaskState, chatGPTWebAccountInfoTaskMaxKept),
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:  make(map[string]int),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		ctx:       context.Background(),
		now:       time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	for index := 0; index < chatGPTWebAccountInfoTaskMaxKept-1; index++ {
		id := fmt.Sprintf("active-%d", index)
		runtime.tasks[id] = &chatGPTWebAccountInfoTaskState{
			snapshot: chatgptwebauth.AccountInfoRefreshTask{ID: id, CreatedAt: time.Now()},
		}
	}

	const starts = 32
	start := make(chan struct{})
	results := make(chan error, starts)
	var workers sync.WaitGroup
	for index := 0; index < starts; index++ {
		index := index
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			_, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{{
				Name:   fmt.Sprintf("concurrent-%d.json", index),
				AuthID: fmt.Sprintf("concurrent-%d", index),
			}}, true)
			results <- errStart
		}()
	}
	close(start)
	workers.Wait()
	close(results)

	succeeded := 0
	rejected := 0
	for errStart := range results {
		if errStart == nil {
			succeeded++
			continue
		}
		if !strings.Contains(errStart.Error(), "active task limit reached") {
			t.Fatalf("unexpected start error: %v", errStart)
		}
		rejected++
	}
	if succeeded != 1 || rejected != starts-1 {
		t.Fatalf("concurrent starts succeeded/rejected = %d/%d, want 1/%d", succeeded, rejected, starts-1)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if len(runtime.tasks) != chatGPTWebAccountInfoTaskMaxKept || runtime.taskReservations != 0 {
		t.Fatalf("task bound = tasks:%d reservations:%d", len(runtime.tasks), runtime.taskReservations)
	}
}

func TestChatGPTWebAccountInfoPastResetSchedulesImmediateRecheck(t *testing.T) {
	now := time.Now().UTC()
	resetAt := now.Add(-time.Minute)
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: 0,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	runtime.cond = sync.NewCond(&runtime.mu)

	runtime.scheduleRecoveryLocked("quota-reset", resetAt)

	entry := runtime.scheduled["recovery:quota-reset"]
	if entry == nil {
		t.Fatal("past quota reset did not schedule a recovery refresh")
	}
	if !entry.due.Equal(now) {
		t.Fatalf("recovery due = %s, want %s", entry.due, now)
	}
	if !entry.work.quotaStateKnown || !entry.work.exhausted || !entry.work.quotaResetAt.Equal(resetAt) {
		t.Fatalf("recovery evidence = known:%v exhausted:%v reset:%s", entry.work.quotaStateKnown, entry.work.exhausted, entry.work.quotaResetAt)
	}
	if state := runtime.states["quota-reset"]; !state.NextRefreshAt.Equal(now) {
		t.Fatalf("NextRefreshAt = %s, want %s", state.NextRefreshAt, now)
	}
	if !runtime.trigger("quota-reset", true) {
		t.Fatal("scheduled recovery was not recognized as active")
	}
	if len(runtime.queue) != 0 {
		t.Fatalf("scheduled recovery was duplicated in the queue: %+v", runtime.queue)
	}
}

func TestChatGPTWebAccountInfoUnknownResetUsesBoundedRecoveryBackoff(t *testing.T) {
	now := time.Now().UTC()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: 0,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: "unknown-reset.json", AuthID: "unknown-reset"}

	runtime.syncRecoveryScheduleForTargetLocked(target, true, time.Time{}, 0)
	entry := runtime.scheduled["recovery:unknown-reset"]
	wantDue := now.Add(chatGPTWebAccountInfoExpiredRecoveryBackoff)
	if entry == nil || !entry.due.Equal(wantDue) || !entry.work.quotaResetAt.IsZero() {
		t.Fatalf("unknown reset recovery = %+v, want due %s", entry, wantDue)
	}
	runtime.syncRecoveryScheduleForTargetLocked(target, true, time.Time{}, 0)
	if current := runtime.scheduled["recovery:unknown-reset"]; current != entry || !current.due.Equal(wantDue) {
		t.Fatalf("repeated unknown reset changed recovery = %+v", current)
	}
	if state := runtime.states["unknown-reset"]; !state.NextRefreshAt.Equal(wantDue) {
		t.Fatalf("NextRefreshAt = %s, want %s", state.NextRefreshAt, wantDue)
	}
}

func TestChatGPTWebAccountInfoAutomaticRecoveryStopsWithoutFutureReset(t *testing.T) {
	now := time.Now().UTC()
	for _, testCase := range []struct {
		name    string
		resetAt time.Time
	}{
		{name: "missing"},
		{name: "expired", resetAt: now.Add(-time.Minute)},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			runtime := &chatGPTWebAccountInfoRuntime{
				cfg:       config.ResolvedChatGPTWebAccountInfoConfig{RecoveryJitterSeconds: 0},
				states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
				scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
				wake:      make(chan struct{}, 1),
				now:       func() time.Time { return now },
			}
			runtime.cond = sync.NewCond(&runtime.mu)
			target := chatgptwebauth.AccountInfoRefreshTarget{
				Name:   testCase.name + ".json",
				AuthID: testCase.name,
			}

			runtime.finishWorkLocked(
				chatGPTWebAccountInfoWork{
					target:    target,
					attempt:   1,
					automatic: true,
				},
				chatGPTWebAccountInfoOutcome{
					status:          chatgptwebauth.AccountInfoResultUnchanged,
					quotaStateKnown: true,
					exhausted:       true,
					quotaResetAt:    testCase.resetAt,
				},
			)

			if entry := runtime.scheduled["recovery:"+testCase.name]; entry != nil {
				t.Fatalf("automatic recovery was rescheduled without a future reset: %+v", entry)
			}
			if state := runtime.states[testCase.name]; !state.NextRefreshAt.IsZero() {
				t.Fatalf("NextRefreshAt = %s, want zero", state.NextRefreshAt)
			}
		})
	}
}

func TestChatGPTWebAccountInfoSameRecoveryResetKeepsEarlierDue(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	resetAt := now.Add(time.Hour)
	randomBytes := append(make([]byte, 8), bytes.Repeat([]byte{0xff}, 8)...)
	randomReader := bytes.NewReader(randomBytes)
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: 60,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
		random:    randomReader,
	}
	target := chatgptwebauth.AccountInfoRefreshTarget{
		Name:           "stable-recovery.json",
		AuthID:         "stable-recovery",
		AuthInstanceID: "instance-a",
	}
	if !runtime.scheduleRecoveryForTargetLocked(target, resetAt) {
		t.Fatal("initial recovery schedule was rejected")
	}
	key := "recovery:" + chatGPTWebAccountInfoTargetKey(target)
	entry := runtime.scheduled[key]
	if entry == nil || !entry.due.Equal(resetAt) {
		t.Fatalf("initial recovery = %+v, want due %s", entry, resetAt)
	}
	remainingRandom := randomReader.Len()

	if !runtime.scheduleRecoveryForTargetLocked(target, resetAt) {
		t.Fatal("same-reset recovery sync was rejected")
	}
	if current := runtime.scheduled[key]; current != entry || !current.due.Equal(resetAt) {
		t.Fatalf("same-reset recovery moved from %s: %+v", resetAt, current)
	}
	if randomReader.Len() != remainingRandom {
		t.Fatalf("same-reset recovery consumed another jitter sample: before=%d after=%d", remainingRandom, randomReader.Len())
	}

	changedResetAt := resetAt.Add(time.Hour)
	if !runtime.scheduleRecoveryForTargetLocked(target, changedResetAt) {
		t.Fatal("changed-reset recovery sync was rejected")
	}
	entry = runtime.scheduled[key]
	if entry == nil || !entry.work.quotaResetAt.Equal(changedResetAt) || !entry.due.After(changedResetAt) {
		t.Fatalf("changed-reset recovery was not rescheduled with jitter: %+v", entry)
	}
}

func TestChatGPTWebAccountInfoForceTriggerPromotesScheduledAndInflightWork(t *testing.T) {
	now := time.Now().UTC()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:            make(chan struct{}, 1),
		now:             func() time.Time { return now },
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	if !runtime.scheduleRecoveryLocked("scheduled", now.Add(time.Hour)) {
		t.Fatal("scheduleRecoveryLocked() = false")
	}
	if !runtime.trigger("scheduled", true) {
		t.Fatal("force trigger did not promote scheduled work")
	}
	scheduled := runtime.scheduled["recovery:scheduled"]
	if scheduled == nil || !scheduled.work.force || !scheduled.due.Equal(now) {
		t.Fatalf("promoted schedule = %+v, want forced work due now", scheduled)
	}
	if !scheduled.work.quotaStateKnown || !scheduled.work.exhausted ||
		!scheduled.work.quotaResetAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("promoted recovery lost quota evidence: %+v", scheduled.work)
	}

	runtime.inflight["running"] = 1
	if !runtime.trigger("running", true) ||
		runtime.pendingTriggers["running"] != chatGPTWebAccountInfoTriggerForce {
		t.Fatal("force trigger did not remember an in-flight follow-up")
	}
	if snapshot := runtime.snapshot(); snapshot.Queued != 1 || snapshot.Inflight != 1 {
		t.Fatalf("pending follow-up snapshot = %+v, want one queued and one inflight", snapshot)
	}
	delete(runtime.inflight, "running")
	runtime.enqueuePendingTriggerLocked("running")
	if runtime.pendingTriggers["running"] != chatGPTWebAccountInfoTriggerNone || len(runtime.queue) != 1 ||
		runtime.queue[0].target.AuthID != "running" || !runtime.queue[0].force {
		t.Fatalf("pending forced follow-up = pending:%v queue:%+v", runtime.pendingTriggers, runtime.queue)
	}
}

func TestChatGPTWebImageQuotaEvidenceRecheckReusesInstanceWork(t *testing.T) {
	now := time.Now().UTC()
	runtime := newChatGPTWebAccountInfoRuntime(&ChatGPTWebExecutor{}, &config.Config{
		ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: accountInfoTestInt(0),
		}},
	})
	runtime.now = func() time.Time { return now }
	t.Cleanup(runtime.close)

	scheduledTarget := chatgptwebauth.AccountInfoRefreshTarget{
		Name:           "scheduled.json",
		AuthID:         "scheduled",
		AuthInstanceID: "scheduled-instance",
	}
	resetAt := now.Add(time.Hour)
	runtime.mu.Lock()
	if !runtime.scheduleRecoveryForTargetLocked(scheduledTarget, resetAt) {
		runtime.mu.Unlock()
		t.Fatal("failed to schedule recovery")
	}
	runtime.mu.Unlock()
	if !runtime.triggerImageQuotaEvidenceRecheckForInstance(scheduledTarget.AuthID, scheduledTarget.AuthInstanceID) {
		t.Fatal("quota evidence trigger did not reuse scheduled recovery")
	}
	runtime.mu.Lock()
	scheduled := runtime.scheduled["recovery:"+chatGPTWebAccountInfoTargetKey(scheduledTarget)]
	runtime.mu.Unlock()
	if scheduled == nil || !scheduled.work.force || !scheduled.due.Equal(now) {
		t.Fatalf("promoted recovery = %+v, want forced work due now", scheduled)
	}

	queuedTarget := chatgptwebauth.AccountInfoRefreshTarget{
		Name:           "queued.json",
		AuthID:         "queued",
		AuthInstanceID: "queued-instance",
	}
	runtime.mu.Lock()
	enqueued, _ := runtime.enqueueForCurrentInstanceLocked(chatGPTWebAccountInfoWork{target: queuedTarget, attempt: 1})
	runtime.mu.Unlock()
	if !enqueued || !runtime.triggerImageQuotaEvidenceRecheckForInstance(queuedTarget.AuthID, queuedTarget.AuthInstanceID) {
		t.Fatal("quota evidence trigger did not reuse queued work")
	}
	runtime.mu.Lock()
	queued := runtime.queuedWorkLocked()
	var queuedForce bool
	for index := range queued {
		if chatGPTWebAccountInfoTargetKey(queued[index].target) == chatGPTWebAccountInfoTargetKey(queuedTarget) {
			queuedForce = queued[index].force
		}
	}
	runtime.mu.Unlock()
	if !queuedForce {
		t.Fatal("quota evidence trigger did not upgrade queued work to force")
	}

	inflightTarget := chatgptwebauth.AccountInfoRefreshTarget{
		Name:           "inflight.json",
		AuthID:         "inflight",
		AuthInstanceID: "inflight-instance",
	}
	inflightKey := chatGPTWebAccountInfoTargetKey(inflightTarget)
	runtime.mu.Lock()
	runtime.authInstances[inflightTarget.AuthID] = inflightTarget.AuthInstanceID
	runtime.inflight[inflightKey] = 1
	runtime.mu.Unlock()
	if !runtime.triggerImageQuotaEvidenceRecheckForInstance(inflightTarget.AuthID, inflightTarget.AuthInstanceID) {
		t.Fatal("quota evidence trigger did not reuse in-flight work")
	}
	runtime.mu.Lock()
	pending := runtime.pendingTriggers[inflightKey]
	runtime.mu.Unlock()
	if pending != chatGPTWebAccountInfoTriggerNone {
		t.Fatalf("in-flight quota evidence trigger queued duplicate mode %v", pending)
	}
}

func TestChatGPTWebAccountInfoPendingForceSurvivesCapacityShrink(t *testing.T) {
	const authID = "pending-after-shrink"
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 0,
		},
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: map[string]chatGPTWebAccountInfoTriggerMode{authID: chatGPTWebAccountInfoTriggerForce},
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:            make(chan struct{}, 1),
		busy:            2,
		now:             time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	target := chatgptwebauth.AccountInfoRefreshTarget{Name: authID, AuthID: authID}

	runtime.enqueuePendingTriggerForTargetLocked(target)
	if runtime.pendingTriggers[authID] != chatGPTWebAccountInfoTriggerForce ||
		len(runtime.queue) != 0 {
		t.Fatalf("capacity-constrained pending trigger = pending:%v queue:%+v", runtime.pendingTriggers, runtime.queue)
	}

	runtime.busy = 0
	runtime.drainPendingTriggersLocked()
	if runtime.pendingTriggers[authID] != chatGPTWebAccountInfoTriggerNone ||
		len(runtime.queue) != 1 ||
		runtime.queue[0].target.AuthID != authID ||
		!runtime.queue[0].force {
		t.Fatalf("drained pending trigger = pending:%v queue:%+v", runtime.pendingTriggers, runtime.queue)
	}
	if state := runtime.authState(authID); !state.Refreshing {
		t.Fatalf("queued account-info state = %+v, want refreshing", state)
	}
}

func TestChatGPTWebAccountInfoConfigExpansionDrainsPendingTrigger(t *testing.T) {
	const authID = "pending-after-expand"
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 0,
		},
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: map[string]chatGPTWebAccountInfoTriggerMode{authID: chatGPTWebAccountInfoTriggerForce},
		workers:         map[int]chan struct{}{1: make(chan struct{})},
		retiringWorkers: make(map[int]struct{}),
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:            make(chan struct{}, 1),
		ctx:             context.Background(),
		busy:            2,
		started:         true,
		now:             time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)

	runtime.updateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AccountInfo: config.ChatGPTWebAccountInfoConfig{
			RefreshWorkers:   accountInfoTestInt(1),
			RefreshQueueSize: accountInfoTestInt(2),
		},
	}})

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	queued := runtime.queuedWorkLocked()
	if len(queued) != 1 || queued[0].target.AuthID != authID || !queued[0].force {
		t.Fatalf("expanded capacity did not drain pending trigger: queue=%+v pending=%+v", queued, runtime.pendingTriggers)
	}
	if len(runtime.pendingTriggers) != 0 {
		t.Fatalf("drained trigger remained pending: %+v", runtime.pendingTriggers)
	}
}

func TestChatGPTWebAccountInfoForcedEarlyRecoveryRestoresFutureResetAfterRetries(t *testing.T) {
	now := time.Now().UTC()
	resetAt := now.Add(time.Hour)
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:        1,
			RefreshQueueSize:      1,
			RecoveryJitterSeconds: 0,
			MaxRetries:            1,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:  make(map[string]int),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
		random:    bytes.NewReader(make([]byte, 64)),
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	if !runtime.scheduleRecoveryLocked("forced-recovery", resetAt) {
		t.Fatal("scheduleRecoveryLocked() = false")
	}
	if !runtime.trigger("forced-recovery", true) {
		t.Fatal("trigger() = false")
	}
	recovery := runtime.removeScheduleLocked("recovery:forced-recovery")
	if recovery == nil || !recovery.due.Equal(now) {
		t.Fatalf("forced recovery = %+v, want due now", recovery)
	}
	failure := chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "network_error",
		retryable: true,
	}
	runtime.finishWorkLocked(recovery.work, failure)
	retry := runtime.removeScheduleLocked("retry:forced-recovery")
	if retry == nil || retry.work.attempt != 2 {
		t.Fatalf("retry work = %+v, want attempt 2", retry)
	}
	runtime.finishWorkLocked(retry.work, failure)

	restored := runtime.scheduled["recovery:forced-recovery"]
	if restored == nil || !restored.due.Equal(resetAt) {
		t.Fatalf("restored recovery = %+v, want reset %s", restored, resetAt)
	}
	if !restored.work.quotaStateKnown || !restored.work.exhausted ||
		!restored.work.quotaResetAt.Equal(resetAt) || restored.work.attempt != 1 {
		t.Fatalf("restored recovery evidence = %+v", restored.work)
	}
	if state := runtime.states["forced-recovery"]; !state.NextRefreshAt.Equal(resetAt) {
		t.Fatalf("NextRefreshAt = %s, want %s", state.NextRefreshAt, resetAt)
	}
	if runtime.retryCount != 1 || runtime.failedCount != 1 {
		t.Fatalf("retry/failed counts = %d/%d, want 1/1", runtime.retryCount, runtime.failedCount)
	}
}

func TestChatGPTWebAccountInfoScheduleReplacementDoesNotConsumeRunnableCapacity(t *testing.T) {
	now := time.Now().UTC()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		workers:   map[int]chan struct{}{0: make(chan struct{})},
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	first := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{AuthID: "bounded"},
		attempt: 1,
	}
	if !runtime.scheduleLocked("retry:bounded", now.Add(time.Minute), first) {
		t.Fatal("first delayed work was rejected")
	}
	replacement := first
	replacement.attempt = 2
	replacementDue := now.Add(2 * time.Minute)
	if !runtime.scheduleLocked("retry:bounded", replacementDue, replacement) {
		t.Fatal("replacement delayed work was rejected")
	}
	if len(runtime.scheduled) != 1 || len(runtime.schedules) != 1 {
		t.Fatalf("replacement grew delayed work: map=%d heap=%d", len(runtime.scheduled), len(runtime.schedules))
	}
	entry := runtime.scheduled["retry:bounded"]
	if entry == nil || entry.work.attempt != 2 || !entry.due.Equal(replacementDue) {
		t.Fatalf("replacement entry = %+v", entry)
	}
	runtime.busy = 1
	if !runtime.enqueueLocked(chatGPTWebAccountInfoWork{target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "queued"}}) {
		t.Fatal("delayed work consumed runnable queue capacity")
	}
	if runtime.enqueueLocked(chatGPTWebAccountInfoWork{target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "overflow"}}) {
		t.Fatal("runnable work exceeded worker plus queue capacity")
	}
}

func TestChatGPTWebAccountInfoTaskRetrySchedulesAreBounded(t *testing.T) {
	now := time.Now().UTC()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	for index := 0; index < 2; index++ {
		work := chatGPTWebAccountInfoWork{
			target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: fmt.Sprintf("auth-%d", index)},
			taskID: fmt.Sprintf("task-%d", index),
		}
		if !runtime.scheduleLocked(fmt.Sprintf("retry-%d", index), now.Add(time.Minute), work) {
			t.Fatalf("task retry %d was rejected before reaching capacity", index)
		}
	}
	if runtime.scheduleLocked("retry-overflow", now.Add(time.Minute), chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "overflow"},
		taskID: "task-overflow",
	}) {
		t.Fatal("task retry schedule exceeded its bound")
	}
	if !runtime.scheduleLocked("recovery:auth-recovery", now.Add(time.Minute), chatGPTWebAccountInfoWork{
		target:    chatgptwebauth.AccountInfoRefreshTarget{AuthID: "auth-recovery"},
		automatic: true,
	}) {
		t.Fatal("bounded task retries blocked a per-auth recovery schedule")
	}
	if runtime.delayedTasks != 2 {
		t.Fatalf("delayed task count = %d, want 2", runtime.delayedTasks)
	}
	runtime.removeScheduleLocked("retry-0")
	if !runtime.scheduleLocked("retry-replacement", now.Add(time.Minute), chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "replacement"},
		taskID: "task-replacement",
	}) {
		t.Fatal("removed task retry did not release delayed capacity")
	}
}

func TestChatGPTWebAccountInfoCanceledWaitersCancelOnlyLastSharedCall(t *testing.T) {
	callContext, cancel := context.WithCancel(context.Background())
	call := &chatGPTWebAccountInfoCall{
		ctx:       callContext,
		cancel:    cancel,
		done:      make(chan struct{}),
		authID:    "shared",
		waiters:   2,
		accepting: true,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		calls: map[string]*chatGPTWebAccountInfoCall{"shared": call},
	}
	if _, waitForCompletion := runtime.releaseAccountInfoCall(call, true, 1); waitForCompletion {
		t.Fatal("first canceled waiter waited for a shared call still used by another waiter")
	}
	if err := callContext.Err(); err != nil {
		t.Fatalf("first canceled waiter stopped shared call: %v", err)
	}
	if _, waitForCompletion := runtime.releaseAccountInfoCall(call, true, 1); !waitForCompletion {
		t.Fatal("last canceled waiter did not wait for the shared call to stop")
	}
	if !errors.Is(callContext.Err(), context.Canceled) {
		t.Fatalf("last canceled waiter left shared call running: %v", callContext.Err())
	}
	if call.accepting || runtime.calls["shared"] != call {
		t.Fatal("canceled shared call did not retain its non-joinable tombstone")
	}
}

func TestChatGPTWebAccountInfoCanceledWaiterRetainsCompletedQuotaEvidence(t *testing.T) {
	taskContext, cancelTask := context.WithCancel(context.Background())
	callContext, cancelCall := context.WithCancel(context.Background())
	t.Cleanup(cancelCall)
	call := &chatGPTWebAccountInfoCall{
		ctx:        callContext,
		cancel:     cancelCall,
		done:       make(chan struct{}),
		authID:     "canceled-quota",
		runtimeKey: "canceled-quota",
		waiters:    1,
		accepting:  true,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task": {ctx: taskContext},
		},
		calls: map[string]*chatGPTWebAccountInfoCall{"canceled-quota": call},
	}
	cancelTask()
	resetAt := time.Now().UTC().Add(time.Hour)
	result := make(chan chatGPTWebAccountInfoOutcome, 1)
	go func() {
		result <- runtime.waitForAccountInfoCall(chatGPTWebAccountInfoWork{
			target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "canceled-quota"},
			taskID: "task",
		}, call, true)
	}()

	select {
	case <-callContext.Done():
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not stop the sole shared call")
	}
	runtime.mu.Lock()
	call.outcome = chatGPTWebAccountInfoOutcome{
		status:          chatgptwebauth.AccountInfoResultUpdated,
		quotaStateKnown: true,
		exhausted:       true,
		quotaResetAt:    resetAt,
	}
	call.completed = true
	close(call.done)
	runtime.mu.Unlock()

	select {
	case outcome := <-result:
		if outcome.status != chatgptwebauth.AccountInfoResultCanceled ||
			!outcome.quotaStateKnown ||
			!outcome.exhausted ||
			!outcome.quotaResetAt.Equal(resetAt) {
			t.Fatalf("canceled outcome = %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return after the shared call completed")
	}
}

func TestChatGPTWebAccountInfoSharedFailureUsesOneRetryDeadline(t *testing.T) {
	now := time.Now().UTC()
	randomBytes := append(make([]byte, 8), bytes.Repeat([]byte{0xff}, 8)...)
	randomReader := bytes.NewReader(randomBytes)
	done := make(chan struct{})
	close(done)
	call := &chatGPTWebAccountInfoCall{
		done:       done,
		authID:     "shared-retry",
		runtimeKey: "shared-retry",
		outcome: chatGPTWebAccountInfoOutcome{
			status:    chatgptwebauth.AccountInfoResultFailed,
			errorCode: "network_error",
			retryable: true,
		},
		waiters:      2,
		accepting:    true,
		completed:    true,
		retryAttempt: 2,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		calls:  map[string]*chatGPTWebAccountInfoCall{"shared-retry": call},
		now:    func() time.Time { return now },
		random: randomReader,
	}

	first, _ := runtime.releaseAccountInfoCall(call, false, 1)
	second, _ := runtime.releaseAccountInfoCall(call, false, 2)

	if first.retryAt.IsZero() || !first.retryAt.Equal(second.retryAt) {
		t.Fatalf("shared retry deadlines = %v and %v", first.retryAt, second.retryAt)
	}
	if want := now.Add(time.Minute); !first.retryAt.Equal(want) {
		t.Fatalf("shared retry deadline = %v, want %v", first.retryAt, want)
	}
	if remaining := randomReader.Len(); remaining != 8 {
		t.Fatalf("shared retry consumed %d random bytes, want 8", len(randomBytes)-remaining)
	}
	if runtime.calls["shared-retry"] != nil {
		t.Fatal("completed failed call remained after all waiters released it")
	}
}

func TestChatGPTWebAccountInfoCanceledQueuedFollowerPrunesCompletedCall(t *testing.T) {
	taskContext, cancelTask := context.WithCancel(context.Background())
	runtime := newChatGPTWebAccountInfoTaskTestRuntime(taskContext, cancelTask)
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{
			Name:   "shared.json",
			AuthID: "shared",
		},
		taskID: "task-a",
		index:  0,
	}
	runtime.queue = []chatGPTWebAccountInfoWork{work}
	done := make(chan struct{})
	close(done)
	runtime.calls = map[string]*chatGPTWebAccountInfoCall{
		"shared": {
			done:       done,
			authID:     "shared",
			runtimeKey: "shared",
			outcome: chatGPTWebAccountInfoOutcome{
				status: chatgptwebauth.AccountInfoResultUpdated,
			},
			accepting: true,
			completed: true,
			force:     true,
		},
	}

	task, ok := runtime.cancelTask("task-a")
	if !ok || task == nil || task.Canceled != 1 {
		t.Fatalf("cancelTask() = %+v, %v", task, ok)
	}
	if runtime.calls["shared"] != nil {
		t.Fatal("completed call remained after its queued follower was canceled")
	}
}

func TestChatGPTWebAccountInfoCallRechecksFreshnessBeforeNetwork(t *testing.T) {
	now := time.Now().UTC()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("fresh-after-singleflight")
	auth.Metadata["profile_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	executor := &ChatGPTWebExecutor{
		manager: manager,
		now:     func() time.Time { return now },
	}
	callContext, cancelCall := context.WithCancel(context.Background())
	call := &chatGPTWebAccountInfoCall{
		ctx:        callContext,
		cancel:     cancelCall,
		done:       make(chan struct{}),
		authID:     auth.ID,
		runtimeKey: auth.ID,
		accepting:  true,
		checkFresh: true,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		executor: executor,
		calls:    map[string]*chatGPTWebAccountInfoCall{auth.ID: call},
	}

	runtime.wg.Add(1)
	runtime.runAccountInfoCall(call)

	if !call.completed || call.outcome.status != chatgptwebauth.AccountInfoResultFresh {
		t.Fatalf("fresh call outcome = %+v", call.outcome)
	}
	if runtime.calls[auth.ID] != nil {
		t.Fatal("fresh call remained registered")
	}
}

func TestChatGPTWebAccountInfoRetryUsesFreshCachedResult(t *testing.T) {
	now := time.Now().UTC()
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("fresh-before-retry")
	auth.Metadata["profile_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_updated_at"] = now.Format(time.RFC3339Nano)
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		executor: &ChatGPTWebExecutor{
			manager: manager,
			now:     func() time.Time { return now },
		},
	}

	call, owner, immediate := runtime.prepareAccountInfoExecution(chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{AuthID: auth.ID},
		attempt: 2,
	})

	if call != nil || owner {
		t.Fatalf("fresh retry created an upstream call: call=%v owner=%t", call, owner)
	}
	if immediate == nil || immediate.status != chatgptwebauth.AccountInfoResultFresh {
		t.Fatalf("fresh retry outcome = %+v", immediate)
	}
}

func TestChatGPTWebAccountInfoCanceledCallBlocksReplacementUntilCompletion(t *testing.T) {
	tombstoneContext, cancelTombstone := context.WithCancel(context.Background())
	t.Cleanup(cancelTombstone)
	tombstone := &chatGPTWebAccountInfoCall{
		ctx:        tombstoneContext,
		cancel:     cancelTombstone,
		done:       make(chan struct{}),
		authID:     "shared",
		runtimeKey: "shared",
		epoch:      1,
		accepting:  false,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		executor:  &ChatGPTWebExecutor{},
		authEpoch: map[string]uint64{"shared": 1},
		calls:     map[string]*chatGPTWebAccountInfoCall{"shared": tombstone},
		ctx:       context.Background(),
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "shared"},
		epoch:  1,
	}
	type acquiredCall struct {
		call  *chatGPTWebAccountInfoCall
		owner bool
	}
	acquired := make(chan acquiredCall, 1)
	go func() {
		call, owner := runtime.acquireAccountInfoCall(work)
		acquired <- acquiredCall{call: call, owner: owner}
	}()

	select {
	case result := <-acquired:
		t.Fatalf("replacement call started before tombstone completed: %+v", result)
	case <-time.After(25 * time.Millisecond):
	}

	runtime.mu.Lock()
	tombstone.completed = true
	close(tombstone.done)
	runtime.mu.Unlock()

	select {
	case result := <-acquired:
		if result.call == nil || !result.owner || result.call == tombstone {
			t.Fatalf("replacement call = %+v, want new owner call", result)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement call did not start after tombstone completed")
	}
	runtime.wg.Wait()
}

func TestChatGPTWebAccountInfoRejectsStaleEpochBeforeAcquiringCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runtime := &chatGPTWebAccountInfoRuntime{
		executor:      &ChatGPTWebExecutor{},
		authEpoch:     map[string]uint64{"same-id": 2},
		authEpochRefs: map[string]int{"same-id": 1},
		calls:         make(map[string]*chatGPTWebAccountInfoCall),
		ctx:           ctx,
	}
	work := chatGPTWebAccountInfoWork{
		target:   chatgptwebauth.AccountInfoRefreshTarget{AuthID: "same-id"},
		epoch:    1,
		epochRef: &chatGPTWebAccountInfoEpochRef{},
		force:    true,
	}

	call, owner := runtime.acquireAccountInfoCall(work)
	if call != nil || owner {
		t.Fatalf("stale work acquired call: call=%p owner=%v", call, owner)
	}
	if len(runtime.calls) != 0 {
		t.Fatalf("stale work created calls: %d", len(runtime.calls))
	}
}

func TestChatGPTWebAccountInfoLastCanceledWaiterKeepsWorkerUntilCallStops(t *testing.T) {
	taskContext, cancelTask := context.WithCancel(context.Background())
	defer cancelTask()
	callContext, cancelCall := context.WithCancel(context.Background())
	defer cancelCall()
	call := &chatGPTWebAccountInfoCall{
		ctx:       callContext,
		cancel:    cancelCall,
		done:      make(chan struct{}),
		authID:    "shared",
		epoch:     1,
		accepting: true,
	}
	runtime := &chatGPTWebAccountInfoRuntime{
		executor: &ChatGPTWebExecutor{},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task": {ctx: taskContext},
		},
		authEpoch: map[string]uint64{"shared": 1},
		calls:     map[string]*chatGPTWebAccountInfoCall{"shared": call},
	}
	result := make(chan chatGPTWebAccountInfoOutcome, 1)
	go func() {
		result <- runtime.execute(chatGPTWebAccountInfoWork{
			target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "shared"},
			taskID: "task",
		})
	}()

	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		waiters := call.waiters
		runtime.mu.Unlock()
		if waiters == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("worker did not join the shared call")
		}
		time.Sleep(time.Millisecond)
	}
	cancelTask()
	select {
	case outcome := <-result:
		t.Fatalf("worker returned before the canceled call stopped: %+v", outcome)
	case <-time.After(25 * time.Millisecond):
	}

	runtime.mu.Lock()
	call.completed = true
	close(call.done)
	runtime.mu.Unlock()
	select {
	case outcome := <-result:
		if outcome.status != chatgptwebauth.AccountInfoResultCanceled {
			t.Fatalf("outcome = %+v", outcome)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not return after the shared call stopped")
	}
}

func TestChatGPTWebAccountInfoSharedFollowerDoesNotOccupyWorker(t *testing.T) {
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(1),
		RefreshQueueSize: accountInfoTestInt(2),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, nil)
	t.Cleanup(func() { _ = executor.Close() })
	runtime := executor.accountInfo
	callContext, cancelCall := context.WithCancel(context.Background())
	call := &chatGPTWebAccountInfoCall{
		ctx:       callContext,
		cancel:    cancelCall,
		done:      make(chan struct{}),
		authID:    "shared",
		epoch:     1,
		waiters:   1,
		accepting: true,
	}
	runtime.mu.Lock()
	runtime.authEpoch["shared"] = call.epoch
	runtime.calls["shared"] = call
	runtime.mu.Unlock()

	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{
		{Name: "shared.json", AuthID: "shared"},
		{Name: "other.json", AuthID: "other"},
	}, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		current, found := executor.AccountInfoRefreshTask(task.ID)
		if !found {
			t.Fatal("refresh task disappeared")
		}
		if current.Processed == 1 && accountInfoResultTerminal(current.Results[1].Status) {
			if accountInfoResultTerminal(current.Results[0].Status) {
				t.Fatalf("shared follower completed before its call: %+v", current)
			}
			break
		}
		time.Sleep(time.Millisecond)
	}
	current, _ := executor.AccountInfoRefreshTask(task.ID)
	if current.Processed != 1 || !accountInfoResultTerminal(current.Results[1].Status) {
		t.Fatalf("other credential did not run while follower waited: %+v", current)
	}
	snapshot := executor.AccountInfoSnapshot()
	if snapshot.Busy != 0 || snapshot.Queued != 1 {
		t.Fatalf("runtime snapshot while follower waits = %+v", snapshot)
	}

	runtime.mu.Lock()
	call.outcome = chatGPTWebAccountInfoOutcome{status: chatgptwebauth.AccountInfoResultUpdated}
	call.completed = true
	delete(runtime.calls, call.authID)
	close(call.done)
	runtime.mu.Unlock()
	cancelCall()
	current = waitForAccountInfoTask(t, executor, task.ID)
	if current.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors {
		t.Fatalf("completed task = %+v", current)
	}
}

func TestChatGPTWebAccountInfoRecoverySkipsDisabledAndCleansRemovedAuth(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	t.Cleanup(func() { _ = executor.Close() })
	auth := chatGPTWebTestAuth("account-info-recovery-cleanup")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = time.Now().Add(time.Hour).Format(time.RFC3339Nano)

	executor.SyncAccountInfoRecovery(auth)
	if state := executor.AccountInfoAuthState(auth.ID); state.NextRefreshAt.IsZero() {
		t.Fatal("enabled exhausted auth did not schedule recovery")
	}
	auth.Disabled = true
	auth.Status = cliproxyauth.StatusDisabled
	executor.SyncAccountInfoRecovery(auth)
	if state := executor.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() {
		t.Fatalf("disabled auth retained recovery schedule: %+v", state)
	}
	auth.Disabled = false
	auth.Status = cliproxyauth.StatusActive
	executor.SyncAccountInfoRecovery(auth)
	if state := executor.AccountInfoAuthState(auth.ID); state.NextRefreshAt.IsZero() {
		t.Fatal("re-enabled exhausted auth did not restore recovery schedule")
	}

	executor.CloseAuthInstanceExecutionSessions(auth.ID, "", "auth_removed")
	if state := executor.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() ||
		state.Refreshing || state.LastError != "" {
		t.Fatalf("removed auth retained runtime state: %+v", state)
	}
	executor.accountInfo.mu.Lock()
	_, scheduled := executor.accountInfo.scheduled["recovery:"+auth.ID]
	executor.accountInfo.mu.Unlock()
	if scheduled {
		t.Fatal("removed auth retained recovery schedule")
	}
}

func TestChatGPTWebAccountInfoAuthEpochReclaimedAfterOldAndNewWorkExit(t *testing.T) {
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg:             config.ResolvedChatGPTWebAccountInfoConfig{},
		tasks:           make(map[string]*chatGPTWebAccountInfoTaskState),
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
		authEpoch:       map[string]uint64{"same-id": 1},
		authEpochRefs:   map[string]int{"same-id": 1},
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		calls:           make(map[string]*chatGPTWebAccountInfoCall),
		wake:            make(chan struct{}, 1),
		now:             time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	oldWork := chatGPTWebAccountInfoWork{
		target:   chatgptwebauth.AccountInfoRefreshTarget{AuthID: "same-id"},
		epoch:    1,
		epochRef: &chatGPTWebAccountInfoEpochRef{},
		attempt:  1,
	}

	runtime.removeAuth("same-id")
	runtime.mu.Lock()
	if runtime.authEpoch["same-id"] != 2 || runtime.authEpochRefs["same-id"] != 1 {
		runtime.mu.Unlock()
		t.Fatalf("old generation tombstone was not retained: epochs=%v refs=%v", runtime.authEpoch, runtime.authEpochRefs)
	}
	newWork := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{AuthID: "same-id"},
		attempt: 1,
	}
	runtime.assignWorkEpochLocked(&newWork)
	runtime.mu.Unlock()
	if newWork.epoch != 2 || newWork.epochRef == nil {
		t.Fatalf("new work epoch = %d ref=%p", newWork.epoch, newWork.epochRef)
	}

	runtime.mu.Lock()
	runtime.finishWorkLocked(oldWork, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "credential_unavailable",
	})
	if runtime.authEpoch["same-id"] != 2 || runtime.authEpochRefs["same-id"] != 1 {
		runtime.mu.Unlock()
		t.Fatalf("old completion damaged new generation: epochs=%v refs=%v", runtime.authEpoch, runtime.authEpochRefs)
	}
	runtime.finishWorkLocked(oldWork, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "credential_unavailable",
	})
	if runtime.authEpoch["same-id"] != 2 || runtime.authEpochRefs["same-id"] != 1 {
		runtime.mu.Unlock()
		t.Fatalf("duplicate old completion damaged new generation: epochs=%v refs=%v", runtime.authEpoch, runtime.authEpochRefs)
	}
	runtime.finishWorkLocked(newWork, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "credential_unavailable",
	})
	_, epochKept := runtime.authEpoch["same-id"]
	_, refsKept := runtime.authEpochRefs["same-id"]
	runtime.mu.Unlock()
	if epochKept || refsKept {
		t.Fatalf("quiescent generation tombstone retained: epoch=%v refs=%v", epochKept, refsKept)
	}
}

func TestChatGPTWebAccountInfoReplacementCleansPassiveCurrentExecutorState(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	first := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(first)

	resetAt := time.Now().UTC().Add(time.Hour)
	auth := chatGPTWebTestAuth("account-info-passive-replacement")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	first.SyncAccountInfoRecovery(installed)

	second := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(second)
	t.Cleanup(func() { _ = manager.CloseExecutors() })
	if state := second.AccountInfoAuthState(auth.ID); state.NextRefreshAt.IsZero() {
		t.Fatal("replacement executor did not restore recovery state")
	}

	if errDelete := manager.Delete(cliproxyauth.WithSkipPersist(context.Background()), auth.ID); errDelete != nil {
		t.Fatalf("delete auth: %v", errDelete)
	}
	if state := second.AccountInfoAuthState(auth.ID); !state.NextRefreshAt.IsZero() ||
		state.Refreshing || state.LastError != "" {
		t.Fatalf("replacement executor retained passive auth state: %+v", state)
	}
	second.accountInfo.mu.Lock()
	runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, installed.RuntimeInstanceID())
	_, scheduled := second.accountInfo.scheduled["recovery:"+runtimeKey]
	_, epochKept := second.accountInfo.authEpoch[runtimeKey]
	second.accountInfo.mu.Unlock()
	if scheduled || epochKept {
		t.Fatalf("replacement executor retained cleanup state: scheduled=%v epoch=%v", scheduled, epochKept)
	}
}

func TestChatGPTWebAccountInfoReplacementCleansPassiveNonExhaustedInstanceMapping(t *testing.T) {
	operations := []struct {
		name string
		run  func(*testing.T, *cliproxyauth.Manager, *cliproxyauth.Auth) string
	}{
		{
			name: "delete",
			run: func(t *testing.T, manager *cliproxyauth.Manager, installed *cliproxyauth.Auth) string {
				t.Helper()
				if errDelete := manager.Delete(cliproxyauth.WithSkipPersist(t.Context()), installed.ID); errDelete != nil {
					t.Fatalf("delete auth: %v", errDelete)
				}
				return ""
			},
		},
		{
			name: "replace",
			run: func(t *testing.T, manager *cliproxyauth.Manager, installed *cliproxyauth.Auth) string {
				t.Helper()
				replacement := installed.Clone()
				replacement.Metadata["access_token"] = "replacement-token"
				updated, current, errUpdate := manager.UpdateIfCurrent(
					cliproxyauth.WithForceRuntimeReplacement(cliproxyauth.WithSkipPersist(t.Context())),
					installed,
					replacement,
				)
				if errUpdate != nil {
					t.Fatalf("replace auth: %v", errUpdate)
				}
				if !current || updated == nil ||
					updated.RuntimeInstanceID() == installed.RuntimeInstanceID() {
					t.Fatalf("replacement = (%+v, current=%v), want new runtime instance", updated, current)
				}
				return updated.RuntimeInstanceID()
			},
		},
	}

	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			first := NewChatGPTWebExecutor(&config.Config{}, manager)
			manager.RegisterExecutor(first)

			auth := chatGPTWebTestAuth("account-info-passive-mapping-" + operation.name)
			auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
			auth.Metadata["image_quota_remaining"] = 1
			installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
			if errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}

			second := NewChatGPTWebExecutor(&config.Config{}, manager)
			manager.RegisterExecutor(second)
			t.Cleanup(func() { _ = manager.CloseExecutors() })
			oldInstanceID := installed.RuntimeInstanceID()
			if !second.HasPassiveAuthInstanceState(auth.ID, oldInstanceID) {
				t.Fatal("replacement executor did not report its instance mapping as passive state")
			}
			if second.HasPassiveAuthInstanceState(auth.ID, "different-instance") {
				t.Fatal("passive instance mapping matched a different runtime instance")
			}

			expectedInstanceID := operation.run(t, manager, installed)

			var mappedInstance string
			var mapped bool
			deadline := time.Now().Add(time.Second)
			for {
				second.accountInfo.mu.Lock()
				mappedInstance, mapped = second.accountInfo.authInstances[auth.ID]
				second.accountInfo.mu.Unlock()
				if !mapped || mappedInstance != oldInstanceID {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("replacement executor retained old instance mapping %q after %s", mappedInstance, operation.name)
				}
				time.Sleep(time.Millisecond)
			}
			if mappedInstance == oldInstanceID {
				t.Fatalf("replacement executor retained old instance mapping %q after %s", mappedInstance, operation.name)
			}
			if expectedInstanceID == "" && mapped {
				t.Fatalf("replacement executor retained instance mapping %q after delete", mappedInstance)
			}
			if expectedInstanceID != "" && mapped && mappedInstance != expectedInstanceID {
				t.Fatalf("replacement executor mapping = %q, want absent or new instance %q", mappedInstance, expectedInstanceID)
			}
		})
	}
}

func TestChatGPTWebAccountInfoPersistedRecoveryCannotReviveReplacedInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })

	resetAt := time.Now().UTC().Add(time.Hour)
	auth := chatGPTWebTestAuth("account-info-recovery-replacement-barrier")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	oldInstanceID := installed.RuntimeInstanceID()
	oldRuntimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, oldInstanceID)

	commitReached := make(chan struct{})
	releaseCommit := make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() {
		releaseOnce.Do(func() { close(releaseCommit) })
	}
	t.Cleanup(releaseBarrier)
	executor.accountInfo.beforePersistedRecoveryCommit = func() {
		close(commitReached)
		<-releaseCommit
	}
	syncDone := make(chan struct{})
	go func() {
		executor.SyncAccountInfoRecovery(installed)
		close(syncDone)
	}()
	select {
	case <-commitReached:
	case <-time.After(time.Second):
		t.Fatal("persisted recovery did not reach commit barrier")
	}

	updated := installed.Clone()
	updated.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	updated.Metadata["image_quota_remaining"] = 1
	delete(updated.Metadata, "image_quota_reset_at")
	replacement, current, errUpdate := manager.UpdateIfCurrent(
		cliproxyauth.WithForceRuntimeReplacement(cliproxyauth.WithSkipPersist(context.Background())),
		installed,
		updated,
	)
	if errUpdate != nil {
		t.Fatalf("replace auth: %v", errUpdate)
	}
	if !current || replacement == nil || replacement.RuntimeInstanceID() == oldInstanceID {
		t.Fatalf("replacement = (%+v, current=%v), want a new runtime instance", replacement, current)
	}

	releaseBarrier()
	select {
	case <-syncDone:
	case <-time.After(time.Second):
		t.Fatal("persisted recovery did not finish after barrier release")
	}

	executor.accountInfo.mu.Lock()
	defer executor.accountInfo.mu.Unlock()
	if executor.accountInfo.scheduleLocked(
		"retry:"+oldRuntimeKey,
		time.Now().Add(time.Minute),
		chatGPTWebAccountInfoWork{
			target: chatgptwebauth.AccountInfoRefreshTarget{
				AuthID:         auth.ID,
				AuthInstanceID: oldInstanceID,
			},
			attempt: 1,
		},
	) {
		t.Fatal("generic schedule commit accepted the replaced runtime instance")
	}
	if executor.accountInfo.authInstances[auth.ID] == oldInstanceID {
		t.Fatalf("old runtime instance %q was rebound after cleanup", oldInstanceID)
	}
	if _, exists := executor.accountInfo.scheduled["recovery:"+oldRuntimeKey]; exists {
		t.Fatal("old runtime recovery schedule was revived after replacement cleanup")
	}
	if _, exists := executor.accountInfo.states[oldRuntimeKey]; exists {
		t.Fatal("old runtime state was revived after replacement cleanup")
	}
	if executor.accountInfo.authEpochRefs[oldRuntimeKey] != 0 {
		t.Fatalf("old runtime epoch refs = %d, want 0", executor.accountInfo.authEpochRefs[oldRuntimeKey])
	}
}

func TestChatGPTWebAccountInfoPersistedRecoveryUsesLatestSameInstanceQuota(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })

	resetAt := time.Now().UTC().Add(time.Hour)
	auth := chatGPTWebTestAuth("account-info-recovery-same-instance-barrier")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, installed.RuntimeInstanceID())

	commitReached := make(chan struct{})
	releaseCommit := make(chan struct{})
	var releaseOnce sync.Once
	releaseBarrier := func() {
		releaseOnce.Do(func() { close(releaseCommit) })
	}
	t.Cleanup(releaseBarrier)
	executor.accountInfo.beforePersistedRecoveryCommit = func() {
		close(commitReached)
		<-releaseCommit
	}
	syncDone := make(chan struct{})
	go func() {
		executor.SyncAccountInfoRecovery(installed)
		close(syncDone)
	}()
	select {
	case <-commitReached:
	case <-time.After(time.Second):
		t.Fatal("persisted recovery did not reach commit barrier")
	}

	updated, current, errUpdate := manager.MutateRuntimeMetadataIfCurrent(
		cliproxyauth.WithSkipPersist(t.Context()),
		installed,
		func(current *cliproxyauth.Auth) {
			current.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
			current.Metadata["image_quota_remaining"] = 1
			delete(current.Metadata, "image_quota_reset_at")
		},
	)
	if errUpdate != nil {
		t.Fatalf("update quota: %v", errUpdate)
	}
	if !current || updated == nil || updated.RuntimeInstanceID() != installed.RuntimeInstanceID() {
		t.Fatalf("same-instance update = (%+v, current=%v)", updated, current)
	}

	releaseBarrier()
	select {
	case <-syncDone:
	case <-time.After(time.Second):
		t.Fatal("persisted recovery did not finish after barrier release")
	}

	executor.accountInfo.mu.Lock()
	defer executor.accountInfo.mu.Unlock()
	if entry := executor.accountInfo.scheduled["recovery:"+runtimeKey]; entry != nil {
		t.Fatalf("stale persisted quota revived recovery schedule: %+v", entry)
	}
}

func TestChatGPTWebAccountInfoPersistedRecoveryRejectsInactiveLifecycle(t *testing.T) {
	for _, lifecycle := range []string{
		cliproxyauth.LifecycleStateReauthRequired,
		cliproxyauth.LifecycleStateInteractionRequired,
		cliproxyauth.LifecycleStateDead,
	} {
		t.Run(lifecycle, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			executor := NewChatGPTWebExecutor(&config.Config{}, manager)
			manager.RegisterExecutor(executor)
			t.Cleanup(func() { _ = manager.CloseExecutors() })

			resetAt := time.Now().UTC().Add(time.Hour)
			auth := chatGPTWebTestAuth("account-info-recovery-" + lifecycle)
			auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
			auth.Metadata["image_quota_remaining"] = 0
			auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
			installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
			if errRegister != nil {
				t.Fatalf("register auth: %v", errRegister)
			}
			executor.SyncAccountInfoRecovery(installed)

			updated, current, errUpdate := manager.MutateRuntimeMetadataIfCurrent(
				cliproxyauth.WithSkipPersist(t.Context()),
				installed,
				func(current *cliproxyauth.Auth) {
					current.Metadata["lifecycle_state"] = lifecycle
				},
			)
			if errUpdate != nil || !current || updated == nil {
				t.Fatalf("update lifecycle = (%+v, current=%v, err=%v)", updated, current, errUpdate)
			}
			executor.SyncAccountInfoRecovery(updated)

			runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, installed.RuntimeInstanceID())
			executor.accountInfo.mu.Lock()
			entry := executor.accountInfo.scheduled["recovery:"+runtimeKey]
			executor.accountInfo.mu.Unlock()
			if entry != nil {
				t.Fatalf("inactive lifecycle retained recovery schedule: %+v", entry)
			}
		})
	}
}

func TestChatGPTWebAccountInfoOldInstanceCleanupPreservesNewInstanceWork(t *testing.T) {
	for _, reason := range []string{"auth_replaced", "auth_runtime_replaced", "auth_refreshed", "auth_delete_rolled_back"} {
		t.Run(reason, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			runtime := &chatGPTWebAccountInfoRuntime{
				cfg: config.ResolvedChatGPTWebAccountInfoConfig{
					RefreshWorkers:        1,
					RefreshQueueSize:      4,
					RecoveryJitterSeconds: 0,
				},
				tasks:           make(map[string]*chatGPTWebAccountInfoTaskState),
				states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
				inflight:        make(map[string]int),
				inflightForce:   make(map[string]int),
				pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
				authInstances:   make(map[string]string),
				authEpoch:       make(map[string]uint64),
				authEpochRefs:   make(map[string]int),
				scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
				calls:           make(map[string]*chatGPTWebAccountInfoCall),
				wake:            make(chan struct{}, 1),
				ctx:             ctx,
				now:             time.Now,
				random:          bytes.NewReader(make([]byte, 64)),
			}
			runtime.cond = sync.NewCond(&runtime.mu)
			executor := &ChatGPTWebExecutor{accountInfo: runtime}
			runtime.executor = executor

			const (
				authID      = "shared-auth"
				oldInstance = "old-instance"
				newInstance = "new-instance"
			)
			oldTarget := chatgptwebauth.AccountInfoRefreshTarget{
				Name:           "shared-auth.json",
				AuthID:         authID,
				AuthInstanceID: oldInstance,
			}
			newTarget := oldTarget
			newTarget.AuthInstanceID = newInstance
			oldKey := chatGPTWebAccountInfoTargetKey(oldTarget)
			newKey := chatGPTWebAccountInfoTargetKey(newTarget)
			resetAt := time.Now().Add(time.Hour)

			runtime.mu.Lock()
			runtime.authInstances[authID] = oldInstance
			runtime.states[oldKey] = chatgptwebauth.AccountInfoAuthRuntimeState{LastError: "old"}
			if !runtime.scheduleRecoveryForTargetLocked(oldTarget, resetAt) {
				runtime.mu.Unlock()
				t.Fatal("old instance recovery schedule was rejected")
			}
			runtime.mu.Unlock()

			task, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{newTarget}, true)
			if errStart != nil {
				t.Fatalf("start new instance task: %v", errStart)
			}
			runtime.mu.Lock()
			runtime.states[newKey] = chatgptwebauth.AccountInfoAuthRuntimeState{LastError: "new"}
			if !runtime.scheduleRecoveryForTargetLocked(newTarget, resetAt.Add(time.Minute)) {
				runtime.mu.Unlock()
				t.Fatal("new instance recovery schedule was rejected")
			}
			runtime.mu.Unlock()

			cleanupDone := make(chan struct{})
			go func() {
				executor.CloseAuthInstanceExecutionSessions(authID, oldInstance, reason)
				close(cleanupDone)
			}()
			select {
			case <-cleanupDone:
			case <-time.After(time.Second):
				t.Fatal("old instance cleanup did not finish")
			}

			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			if runtime.authInstances[authID] != newInstance {
				t.Fatalf("current instance = %q, want %q", runtime.authInstances[authID], newInstance)
			}
			if _, exists := runtime.states[oldKey]; exists {
				t.Fatal("old instance state was retained")
			}
			if state, exists := runtime.states[newKey]; !exists || state.LastError != "new" {
				t.Fatalf("new instance state = %+v, exists=%v", state, exists)
			}
			if _, exists := runtime.scheduled["recovery:"+oldKey]; exists {
				t.Fatal("old instance recovery schedule was retained")
			}
			if _, exists := runtime.scheduled["recovery:"+newKey]; !exists {
				t.Fatal("new instance recovery schedule was removed")
			}
			if len(runtime.queue) != 1 || chatGPTWebAccountInfoTargetKey(runtime.queue[0].target) != newKey {
				t.Fatalf("new instance queue = %+v", runtime.queue)
			}
			currentTask := runtime.tasks[task.ID]
			if currentTask == nil || currentTask.snapshot.Processed != 0 ||
				currentTask.snapshot.Results[0].Status != chatgptwebauth.AccountInfoResultQueued {
				t.Fatalf("new instance task = %+v", currentTask)
			}
		})
	}
}

func TestChatGPTWebAccountInfoReplacementCleanupRequiresInstanceID(t *testing.T) {
	const (
		authID          = "current-auth"
		currentInstance = "current-instance"
	)
	currentKey := chatGPTWebAccountInfoAuthInstanceKey(authID, currentInstance)
	runtime := &chatGPTWebAccountInfoRuntime{
		states: map[string]chatgptwebauth.AccountInfoAuthRuntimeState{
			currentKey: {LastError: "current"},
		},
		authInstances: map[string]string{authID: currentInstance},
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	executor := &ChatGPTWebExecutor{accountInfo: runtime}

	for _, reason := range []string{"auth_runtime_replaced", "auth_refreshed", "auth_delete_rolled_back"} {
		executor.CloseAuthInstanceExecutionSessions(authID, "", reason)
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.authInstances[authID] != currentInstance {
		t.Fatalf("current instance = %q, want %q", runtime.authInstances[authID], currentInstance)
	}
	if state, exists := runtime.states[currentKey]; !exists || state.LastError != "current" {
		t.Fatalf("current instance state = %+v, exists=%v", state, exists)
	}
}

func TestChatGPTWebAccountInfoCancelStopsRunningAcquisition(t *testing.T) {
	started := make(chan struct{}, 2)
	canceled := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		_ = request.Body.Close()
		started <- struct{}{}
		<-request.Context().Done()
		canceled <- struct{}{}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-cancel-running")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:   accountInfoTestInt(1),
		RefreshQueueSize: accountInfoTestInt(1),
		MaxRetries:       accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL
	executor.accountInfoTimeout = 250 * time.Millisecond

	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "cancel-running.json", AuthID: auth.ID,
	}}, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	for index := 0; index < 2; index++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("account info requests did not start")
		}
	}
	if _, found := executor.CancelAccountInfoRefreshTask(task.ID); !found {
		t.Fatal("CancelAccountInfoRefreshTask() did not find task")
	}
	task = waitForAccountInfoTask(t, executor, task.ID)
	if task.State != chatgptwebauth.AccountInfoTaskCanceled ||
		len(task.Results) != 1 ||
		task.Results[0].Status != chatgptwebauth.AccountInfoResultCanceled {
		t.Fatalf("canceled running task = %+v", task)
	}
	for index := 0; index < 2; index++ {
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("canceled acquisition did not terminate within its bounded timeout")
		}
	}
	runtime := executor.accountInfo
	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		busy := runtime.busy
		inflight := len(runtime.inflight)
		calls := len(runtime.calls)
		runtime.mu.Unlock()
		if busy == 0 && inflight == 0 && calls == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("canceled task retained runtime capacity: busy=%d inflight=%d calls=%d", busy, inflight, calls)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatGPTWebAccountInfoFailurePreservesRecoverySchedule(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	resetAt := time.Now().UTC().Add(time.Hour).Truncate(time.Second)
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-preserve-recovery")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		RefreshWorkers:        accountInfoTestInt(1),
		RefreshQueueSize:      accountInfoTestInt(1),
		RecoveryJitterSeconds: accountInfoTestInt(0),
		MaxRetries:            accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	before := executor.AccountInfoAuthState(auth.ID)
	if !before.NextRefreshAt.Equal(resetAt) {
		t.Fatalf("initial recovery = %v, want %v", before.NextRefreshAt, resetAt)
	}
	task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
		Name: "preserve-recovery.json", AuthID: auth.ID,
	}}, true)
	if errStart != nil {
		t.Fatalf("StartAccountInfoRefreshTask() error = %v", errStart)
	}
	task = waitForAccountInfoTask(t, executor, task.ID)
	if task.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors {
		t.Fatalf("failed task state = %q", task.State)
	}
	after := executor.AccountInfoAuthState(auth.ID)
	if !after.NextRefreshAt.Equal(resetAt) {
		t.Fatalf("recovery after failed refresh = %v, want %v", after.NextRefreshAt, resetAt)
	}
}

func TestChatGPTWebAccountInfoRejectsOversizedResponses(t *testing.T) {
	for _, target := range []string{"profile", "quota"} {
		for _, transfer := range []string{"content-length", "chunked"} {
			t.Run(target+"/"+transfer, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
					oversized := (target == "profile" && request.URL.Path == chatgptwebauth.AccountCheckPath) ||
						(target == "quota" && request.URL.Path == chatgptwebauth.ConversationInitPath)
					if oversized {
						if transfer == "content-length" {
							writer.Header().Set("Content-Length", strconv.Itoa(chatGPTWebAccountInfoMaxBodyBytes+1))
						} else {
							writer.Header().Set("Content-Type", "application/json")
							writer.WriteHeader(http.StatusOK)
							writer.(http.Flusher).Flush()
						}
						_, _ = io.CopyN(writer, accountInfoTestRepeatingReader{}, int64(chatGPTWebAccountInfoMaxBodyBytes+1))
						return
					}
					switch request.URL.Path {
					case chatgptwebauth.AccountCheckPath:
						_ = json.NewEncoder(writer).Encode(map[string]any{"accounts": map[string]any{
							"default": map[string]any{"account": map[string]any{
								"account_id": "account-1",
								"plan_type":  "plus",
							}},
						}})
					case chatgptwebauth.ConversationInitPath:
						_ = json.NewEncoder(writer).Encode(map[string]any{"limits_progress": []any{
							map[string]any{"feature_name": "image_gen", "remaining": 1},
						}})
					default:
						http.NotFound(writer, request)
					}
				}))
				defer server.Close()

				executor := NewChatGPTWebExecutor(&config.Config{}, nil)
				t.Cleanup(func() { _ = executor.Close() })
				executor.runtimeBaseURL = server.URL
				credential, errCredential := chatgptwebauth.ParseCredential(chatGPTWebTestAuth("oversized-" + target).Metadata)
				if errCredential != nil {
					t.Fatalf("ParseCredential() error = %v", errCredential)
				}
				profileClient, errProfileClient := chatgptwebauth.NewAcquisitionClient(
					credential.Persona,
					"",
					credential.Cookies,
					time.Second,
				)
				if errProfileClient != nil {
					t.Fatalf("NewAcquisitionClient(profile) error = %v", errProfileClient)
				}
				defer profileClient.CloseIdleConnections()
				quotaClient, errQuotaClient := chatgptwebauth.NewAcquisitionClient(
					credential.Persona,
					"",
					credential.Cookies,
					time.Second,
				)
				if errQuotaClient != nil {
					t.Fatalf("NewAcquisitionClient(quota) error = %v", errQuotaClient)
				}
				defer quotaClient.CloseIdleConnections()

				_, _, profileErr, quotaErr := executor.fetchChatGPTWebAccountInfoPair(
					context.Background(),
					profileClient,
					quotaClient,
					credential,
				)
				if target == "profile" {
					if profileErr == nil || !strings.Contains(profileErr.Error(), "response body exceeds") {
						t.Fatalf("profile error = %v, want bounded response error", profileErr)
					}
					if quotaErr != nil {
						t.Fatalf("quota error = %v", quotaErr)
					}
				} else {
					if quotaErr == nil || !strings.Contains(quotaErr.Error(), "response body exceeds") {
						t.Fatalf("quota error = %v, want bounded response error", quotaErr)
					}
					if profileErr != nil {
						t.Fatalf("profile error = %v", profileErr)
					}
				}
			})
		}
	}
}

func TestClassifyChatGPTWebAccountInfoErrorsPrioritizesAuthentication(t *testing.T) {
	code, retryable := classifyChatGPTWebAccountInfoErrors(
		statusErr{code: http.StatusUnauthorized, msg: "unauthorized"},
		statusErr{code: http.StatusServiceUnavailable, msg: "temporary"},
	)
	if code != "unauthorized" || retryable {
		t.Fatalf("classification = %q retryable=%v", code, retryable)
	}
}

type accountInfoTestRepeatingReader struct{}

func (accountInfoTestRepeatingReader) Read(buffer []byte) (int, error) {
	for index := range buffer {
		buffer[index] = 'x'
	}
	return len(buffer), nil
}

func TestChatGPTWebAccountInfoWorkerResizeDoesNotOversubscribe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   4,
			RefreshQueueSize: 1,
		},
		tasks:           make(map[string]*chatGPTWebAccountInfoTaskState),
		states:          make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:        make(map[string]int),
		inflightForce:   make(map[string]int),
		pendingTriggers: make(map[string]chatGPTWebAccountInfoTriggerMode),
		workers:         make(map[int]chan struct{}),
		retiringWorkers: make(map[int]struct{}),
		scheduled:       make(map[string]*chatGPTWebAccountInfoSchedule),
		calls:           make(map[string]*chatGPTWebAccountInfoCall),
		wake:            make(chan struct{}, 1),
		ctx:             ctx,
		cancel:          cancel,
		now:             time.Now,
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	runtime.mu.Lock()
	runtime.resizeWorkersLocked(4)
	runtime.resizeWorkersLocked(1)
	runtime.cond.Broadcast()
	runtime.resizeWorkersLocked(4)
	if got := len(runtime.workers); got > 4 {
		runtime.mu.Unlock()
		t.Fatalf("workers after rapid shrink/grow = %d, want at most 4", got)
	}
	runtime.cond.Broadcast()
	runtime.mu.Unlock()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		workers := len(runtime.workers)
		retiring := len(runtime.retiringWorkers)
		runtime.mu.Unlock()
		if workers > 4 {
			t.Fatalf("workers during resize = %d, want at most 4", workers)
		}
		if workers == 4 && retiring == 0 {
			runtime.close()
			return
		}
		time.Sleep(time.Millisecond)
	}
	runtime.close()
	t.Fatal("workers did not converge after resize")
}

func TestChatGPTWebAccountInfoCancelRemovesDelayedHeapEntry(t *testing.T) {
	now := time.Now().UTC()
	taskCtx, cancel := context.WithCancel(context.Background())
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task-a": {
				snapshot: chatgptwebauth.AccountInfoRefreshTask{
					ID:      "task-a",
					State:   chatgptwebauth.AccountInfoTaskRunning,
					Total:   1,
					Results: []chatgptwebauth.AccountInfoRefreshResult{{Name: "cancel.json", Status: chatgptwebauth.AccountInfoResultRetrying}},
				},
				ctx:    taskCtx,
				cancel: cancel,
			},
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		workers:   map[int]chan struct{}{0: make(chan struct{})},
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	work := chatGPTWebAccountInfoWork{
		target:   chatgptwebauth.AccountInfoRefreshTarget{AuthID: "cancel"},
		taskID:   "task-a",
		index:    0,
		attempt:  2,
		schedule: "task:task-a:0",
	}
	if !runtime.scheduleLocked(work.schedule, now.Add(time.Minute), work) {
		t.Fatal("scheduleLocked() rejected delayed task")
	}
	task, ok := runtime.cancelTask("task-a")
	if !ok || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceled {
		t.Fatalf("cancelTask() = %+v, %v", task, ok)
	}
	if len(runtime.scheduled) != 0 || len(runtime.schedules) != 0 {
		t.Fatalf("canceled delayed work remained: map=%d heap=%d", len(runtime.scheduled), len(runtime.schedules))
	}
}

func TestChatGPTWebAccountInfoIndependentTriggerSurvivesTaskCancellation(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		name := "queued"
		if scheduled {
			name = "scheduled"
		}
		t.Run(name, func(t *testing.T) {
			now := time.Now().UTC()
			taskCtx, cancel := context.WithCancel(context.Background())
			runtime := &chatGPTWebAccountInfoRuntime{
				cfg: config.ResolvedChatGPTWebAccountInfoConfig{
					RefreshWorkers:   1,
					RefreshQueueSize: 1,
				},
				tasks: map[string]*chatGPTWebAccountInfoTaskState{
					"task-a": {
						snapshot: chatgptwebauth.AccountInfoRefreshTask{
							ID:      "task-a",
							State:   chatgptwebauth.AccountInfoTaskRunning,
							Total:   1,
							Results: []chatgptwebauth.AccountInfoRefreshResult{{Name: "shared.json", Status: chatgptwebauth.AccountInfoResultRetrying}},
						},
						ctx:    taskCtx,
						cancel: cancel,
					},
				},
				states:            make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
				inflight:          make(map[string]int),
				inflightForce:     make(map[string]int),
				pendingTriggers:   make(map[string]chatGPTWebAccountInfoTriggerMode),
				scheduled:         make(map[string]*chatGPTWebAccountInfoSchedule),
				scheduledByTarget: make(map[string]map[string]*chatGPTWebAccountInfoSchedule),
				wake:              make(chan struct{}, 1),
				now:               func() time.Time { return now },
			}
			runtime.cond = sync.NewCond(&runtime.mu)
			work := chatGPTWebAccountInfoWork{
				target:  chatgptwebauth.AccountInfoRefreshTarget{Name: "shared.json", AuthID: "shared"},
				taskID:  "task-a",
				index:   0,
				attempt: 1,
			}

			runtime.mu.Lock()
			if scheduled {
				work.schedule = "task:task-a:0"
				if !runtime.scheduleLocked(work.schedule, now.Add(time.Minute), work) {
					runtime.mu.Unlock()
					t.Fatal("scheduleLocked() rejected task work")
				}
			} else if !runtime.enqueueLocked(work) {
				runtime.mu.Unlock()
				t.Fatal("enqueueLocked() rejected task work")
			}
			if !runtime.triggerTargetLocked(work.target, chatGPTWebAccountInfoTriggerForce) {
				runtime.mu.Unlock()
				t.Fatal("force trigger was not merged into task work")
			}
			runtime.mu.Unlock()

			task, ok := runtime.cancelTask("task-a")
			if !ok || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceled {
				t.Fatalf("cancelTask() = %+v, %v", task, ok)
			}
			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			queued := runtime.queuedWorkLocked()
			if len(queued) != 1 || queued[0].taskID != "" || !queued[0].automatic ||
				!queued[0].force || queued[0].target.AuthID != "shared" {
				t.Fatalf("independent trigger after cancellation = %+v", queued)
			}
			if len(runtime.pendingTriggers) != 0 {
				t.Fatalf("pending triggers were not drained: %+v", runtime.pendingTriggers)
			}
			if len(runtime.scheduled) != 0 || len(runtime.schedules) != 0 {
				t.Fatalf("canceled task schedule remained: map=%d heap=%d", len(runtime.scheduled), len(runtime.schedules))
			}
		})
	}
}

func TestChatGPTWebAccountInfoIndependentTriggerSurvivesInflightTaskCancellation(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		workForce     bool
		triggerMode   chatGPTWebAccountInfoTriggerMode
		expectedForce bool
	}{
		{name: "default trigger", triggerMode: chatGPTWebAccountInfoTriggerDefault},
		{
			name:          "force trigger while forced task runs",
			workForce:     true,
			triggerMode:   chatGPTWebAccountInfoTriggerForce,
			expectedForce: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			taskCtx, cancel := context.WithCancel(context.Background())
			runtime := newChatGPTWebAccountInfoTaskTestRuntime(taskCtx, cancel)
			work := chatGPTWebAccountInfoWork{
				target:  chatgptwebauth.AccountInfoRefreshTarget{Name: "shared.json", AuthID: "shared"},
				taskID:  "task-a",
				index:   0,
				force:   testCase.workForce,
				attempt: 1,
			}

			runtime.mu.Lock()
			if !runtime.enqueueLocked(work) {
				runtime.mu.Unlock()
				t.Fatal("enqueueLocked() rejected task work")
			}
			work, ok := runtime.dequeueLocked()
			if !ok {
				runtime.mu.Unlock()
				t.Fatal("task work was not queued")
			}
			runtime.beginAccountInfoWorkLocked(work)
			if !runtime.triggerTargetLocked(work.target, testCase.triggerMode) {
				runtime.mu.Unlock()
				t.Fatal("trigger was not attached to inflight task work")
			}
			runtime.mu.Unlock()

			task, found := runtime.cancelTask("task-a")
			if !found || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceling {
				t.Fatalf("cancelTask() = %+v, %v", task, found)
			}
			runtime.mu.Lock()
			runtime.completeAccountInfoWorkLocked(work, false, chatGPTWebAccountInfoOutcome{
				status: chatgptwebauth.AccountInfoResultUpdated,
			})
			runtime.mu.Unlock()

			task, found = runtime.task("task-a")
			if !found || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceled {
				t.Fatalf("completed canceled task = %+v, %v", task, found)
			}
			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			queued := runtime.queuedWorkLocked()
			if len(queued) != 1 || queued[0].taskID != "" || !queued[0].automatic ||
				queued[0].force != testCase.expectedForce || queued[0].target.AuthID != "shared" {
				t.Fatalf("independent trigger after inflight cancellation = %+v", queued)
			}
		})
	}
}

func TestChatGPTWebAccountInfoDisableDropsInflightAutomaticRecheck(t *testing.T) {
	taskCtx, cancel := context.WithCancel(context.Background())
	runtime := newChatGPTWebAccountInfoTaskTestRuntime(taskCtx, cancel)
	work := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{Name: "shared.json", AuthID: "shared"},
		taskID:  "task-a",
		index:   0,
		attempt: 1,
	}

	runtime.mu.Lock()
	runtime.assignWorkEpochLocked(&work)
	runtime.beginAccountInfoWorkLocked(work)
	if !runtime.triggerTargetLocked(work.target, chatGPTWebAccountInfoTriggerAutomaticRecheck) {
		runtime.mu.Unlock()
		t.Fatal("automatic recheck was not attached to inflight task")
	}
	enabled := false
	runtime.cfg = (config.ChatGPTWebAccountInfoConfig{AutoRefreshEnabled: &enabled}).Resolved()
	runtime.disableAutomaticRefreshLocked()
	runtime.completeAccountInfoWorkLocked(work, false, chatGPTWebAccountInfoOutcome{
		status: chatgptwebauth.AccountInfoResultUpdated,
	})
	state := runtime.states[chatGPTWebAccountInfoTargetKey(work.target)]
	pending := len(runtime.pendingTriggers)
	queued := runtime.queueLengthLocked()
	runtime.mu.Unlock()

	if state.Refreshing || pending != 0 || queued != 0 {
		t.Fatalf("disabled automatic recheck state=%+v pending=%d queued=%d", state, pending, queued)
	}
}

func TestChatGPTWebAccountInfoInflightTaskConsumesIndependentTriggerOnCompletion(t *testing.T) {
	taskCtx, cancel := context.WithCancel(context.Background())
	runtime := newChatGPTWebAccountInfoTaskTestRuntime(taskCtx, cancel)
	work := chatGPTWebAccountInfoWork{
		target:  chatgptwebauth.AccountInfoRefreshTarget{Name: "shared.json", AuthID: "shared"},
		taskID:  "task-a",
		index:   0,
		force:   true,
		attempt: 1,
	}

	runtime.mu.Lock()
	runtime.assignWorkEpochLocked(&work)
	runtime.beginAccountInfoWorkLocked(work)
	if !runtime.triggerTargetLocked(work.target, chatGPTWebAccountInfoTriggerForce) {
		runtime.mu.Unlock()
		t.Fatal("force trigger was not attached to inflight task work")
	}
	runtime.completeAccountInfoWorkLocked(work, false, chatGPTWebAccountInfoOutcome{
		status: chatgptwebauth.AccountInfoResultUpdated,
	})
	defer runtime.mu.Unlock()

	if len(runtime.queuedWorkLocked()) != 0 || len(runtime.pendingTriggers) != 0 {
		t.Fatalf("completed task created duplicate refresh: queue=%+v pending=%+v", runtime.queuedWorkLocked(), runtime.pendingTriggers)
	}
}

func newChatGPTWebAccountInfoTaskTestRuntime(
	taskCtx context.Context,
	cancel context.CancelFunc,
) *chatGPTWebAccountInfoRuntime {
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
		},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task-a": {
				snapshot: chatgptwebauth.AccountInfoRefreshTask{
					ID:    "task-a",
					State: chatgptwebauth.AccountInfoTaskRunning,
					Total: 1,
					Results: []chatgptwebauth.AccountInfoRefreshResult{{
						Name:   "shared.json",
						Status: chatgptwebauth.AccountInfoResultQueued,
					}},
				},
				ctx:    taskCtx,
				cancel: cancel,
			},
		},
		states:            make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:          make(map[string]int),
		inflightForce:     make(map[string]int),
		inflightTask:      make(map[string]int),
		inflightRecovery:  make(map[string]time.Time),
		pendingTriggers:   make(map[string]chatGPTWebAccountInfoTriggerMode),
		scheduled:         make(map[string]*chatGPTWebAccountInfoSchedule),
		scheduledByTarget: make(map[string]map[string]*chatGPTWebAccountInfoSchedule),
	}
	runtime.cond = sync.NewCond(&runtime.mu)
	return runtime
}

func TestChatGPTWebAccountInfoRecoveryDoesNotDuplicateActiveWork(t *testing.T) {
	for _, phase := range []string{"queued", "running", "retrying"} {
		t.Run(phase, func(t *testing.T) {
			now := time.Now().UTC()
			resetAt := now.Add(-time.Minute)
			runtime := &chatGPTWebAccountInfoRuntime{
				cfg: config.ResolvedChatGPTWebAccountInfoConfig{
					RefreshWorkers:   1,
					RefreshQueueSize: 1,
				},
				states:            make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
				inflight:          make(map[string]int),
				inflightForce:     make(map[string]int),
				inflightRecovery:  make(map[string]time.Time),
				pendingTriggers:   make(map[string]chatGPTWebAccountInfoTriggerMode),
				scheduled:         make(map[string]*chatGPTWebAccountInfoSchedule),
				scheduledByTarget: make(map[string]map[string]*chatGPTWebAccountInfoSchedule),
				wake:              make(chan struct{}, 1),
				now:               func() time.Time { return now },
			}
			runtime.cond = sync.NewCond(&runtime.mu)
			work := chatGPTWebAccountInfoWork{
				target:          chatgptwebauth.AccountInfoRefreshTarget{Name: "recovery.json", AuthID: "recovery"},
				force:           true,
				attempt:         1,
				automatic:       true,
				schedule:        "recovery:recovery",
				quotaStateKnown: true,
				exhausted:       true,
				quotaResetAt:    resetAt,
			}

			runtime.mu.Lock()
			switch phase {
			case "queued":
				if !runtime.enqueueLocked(work) {
					runtime.mu.Unlock()
					t.Fatal("enqueueLocked() rejected recovery")
				}
			case "running":
				runtime.assignWorkEpochLocked(&work)
				runtime.beginAccountInfoWorkLocked(work)
			case "retrying":
				work.schedule = "retry:recovery"
				if !runtime.scheduleLocked(work.schedule, now.Add(time.Minute), work) {
					runtime.mu.Unlock()
					t.Fatal("scheduleLocked() rejected recovery retry")
				}
			}
			existingSchedules := len(runtime.scheduled)
			if !runtime.scheduleRecoveryForTargetLocked(work.target, resetAt) {
				runtime.mu.Unlock()
				t.Fatal("matching recovery was not accepted")
			}
			if len(runtime.scheduled) != existingSchedules ||
				runtime.scheduled["recovery:recovery"] != nil {
				runtime.mu.Unlock()
				t.Fatalf("duplicate recovery was scheduled: %+v", runtime.scheduled)
			}
			if phase == "running" {
				runtime.completeAccountInfoWorkLocked(work, false, chatGPTWebAccountInfoOutcome{
					status: chatgptwebauth.AccountInfoResultFailed,
				})
				if len(runtime.inflightRecovery) != 0 {
					runtime.mu.Unlock()
					t.Fatalf("completed recovery remained active: %+v", runtime.inflightRecovery)
				}
			}
			runtime.mu.Unlock()
		})
	}
}

func TestChatGPTWebAccountInfoCancelPreservesPartialQuotaRecovery(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })

	resetAt := time.Now().UTC().Add(time.Hour)
	auth := chatGPTWebTestAuth("account-info-cancel-partial")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	taskContext, cancelTask := context.WithCancel(context.Background())
	runtime := executor.accountInfo
	runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, installed.RuntimeInstanceID())
	runtime.mu.Lock()
	if recovery := runtime.removeScheduleLocked("recovery:" + runtimeKey); recovery != nil {
		runtime.releaseWorkEpochLocked(recovery.work)
	}
	runtime.tasks["task-partial"] = &chatGPTWebAccountInfoTaskState{
		snapshot: chatgptwebauth.AccountInfoRefreshTask{
			ID:      "task-partial",
			State:   chatgptwebauth.AccountInfoTaskRunning,
			Total:   1,
			Results: []chatgptwebauth.AccountInfoRefreshResult{{Name: "partial.json", Status: chatgptwebauth.AccountInfoResultRetrying}},
		},
		ctx:    taskContext,
		cancel: cancelTask,
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{
			Name:           "partial.json",
			AuthID:         auth.ID,
			AuthInstanceID: installed.RuntimeInstanceID(),
		},
		taskID:          "task-partial",
		index:           0,
		attempt:         2,
		schedule:        "task:task-partial:0",
		partialApplied:  true,
		quotaStateKnown: true,
		exhausted:       true,
		quotaResetAt:    resetAt,
	}
	if !runtime.scheduleLocked(work.schedule, resetAt, work) {
		runtime.mu.Unlock()
		t.Fatal("scheduleLocked() rejected partial task retry")
	}
	runtime.mu.Unlock()

	task, ok := runtime.cancelTask("task-partial")
	if !ok || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceled {
		t.Fatalf("cancelTask() = %+v, %v", task, ok)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.scheduled["task:task-partial:0"] != nil {
		t.Fatal("canceled task retry remained scheduled")
	}
	recovery := runtime.scheduled["recovery:"+runtimeKey]
	if recovery == nil || !recovery.work.exhausted || !recovery.work.quotaResetAt.Equal(resetAt) {
		t.Fatalf("partial quota recovery = %+v", recovery)
	}
}

func TestChatGPTWebAccountInfoCancelPreservesQueuedPartialQuotaRecovery(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = manager.CloseExecutors() })

	resetAt := time.Now().UTC().Add(time.Hour)
	auth := chatGPTWebTestAuth("account-info-cancel-queued-partial")
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["image_quota_reset_at"] = resetAt.Format(time.RFC3339Nano)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	taskContext, cancelTask := context.WithCancel(context.Background())
	runtime := executor.accountInfo
	runtimeKey := chatGPTWebAccountInfoAuthInstanceKey(auth.ID, installed.RuntimeInstanceID())
	runtime.mu.Lock()
	if recovery := runtime.removeScheduleLocked("recovery:" + runtimeKey); recovery != nil {
		runtime.releaseWorkEpochLocked(recovery.work)
	}
	runtime.tasks["task-queued-partial"] = &chatGPTWebAccountInfoTaskState{
		snapshot: chatgptwebauth.AccountInfoRefreshTask{
			ID:      "task-queued-partial",
			State:   chatgptwebauth.AccountInfoTaskRunning,
			Total:   1,
			Results: []chatgptwebauth.AccountInfoRefreshResult{{Name: "queued-partial.json", Status: chatgptwebauth.AccountInfoResultRetrying}},
		},
		ctx:    taskContext,
		cancel: cancelTask,
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{
			Name:           "queued-partial.json",
			AuthID:         auth.ID,
			AuthInstanceID: installed.RuntimeInstanceID(),
		},
		taskID:          "task-queued-partial",
		index:           0,
		attempt:         2,
		partialApplied:  true,
		quotaStateKnown: true,
		exhausted:       true,
		quotaResetAt:    resetAt,
	}
	runtime.assignWorkEpochLocked(&work)
	runtime.queue = append(runtime.queue, work)
	runtime.mu.Unlock()

	task, ok := runtime.cancelTask("task-queued-partial")
	if !ok || task == nil || task.State != chatgptwebauth.AccountInfoTaskCanceled {
		t.Fatalf("cancelTask() = %+v, %v", task, ok)
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.queueLengthLocked() != 0 {
		t.Fatalf("canceled queued task remained: %+v", runtime.queuedWorkLocked())
	}
	recovery := runtime.scheduled["recovery:"+runtimeKey]
	if recovery == nil || !recovery.work.exhausted || !recovery.work.quotaResetAt.Equal(resetAt) {
		t.Fatalf("queued partial quota recovery = %+v", recovery)
	}
}

func TestChatGPTWebAccountInfoCanceledTaskCannotScheduleRetryAfterWorkerBarrier(t *testing.T) {
	now := time.Now().UTC()
	taskCtx, cancelTask := context.WithCancel(context.Background())
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:   1,
			RefreshQueueSize: 1,
			MaxRetries:       1,
		},
		tasks: map[string]*chatGPTWebAccountInfoTaskState{
			"task-a": {
				snapshot: chatgptwebauth.AccountInfoRefreshTask{
					ID:      "task-a",
					State:   chatgptwebauth.AccountInfoTaskRunning,
					Total:   1,
					Results: []chatgptwebauth.AccountInfoRefreshResult{{Name: "cancel.json", Status: chatgptwebauth.AccountInfoResultRunning}},
				},
				ctx:    taskCtx,
				cancel: cancelTask,
			},
		},
		states:        make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		authInstances: make(map[string]string),
		authEpoch:     make(map[string]uint64),
		authEpochRefs: make(map[string]int),
		workers:       map[int]chan struct{}{0: make(chan struct{})},
		scheduled:     make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:          make(chan struct{}, 1),
		now:           func() time.Time { return now },
		random:        bytes.NewReader(make([]byte, 64)),
	}
	work := chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{
			AuthID:         "cancel",
			AuthInstanceID: "instance-a",
		},
		taskID:  "task-a",
		index:   0,
		attempt: 1,
	}
	outcome := chatGPTWebAccountInfoOutcome{
		status:     chatgptwebauth.AccountInfoResultFailed,
		errorCode:  "rate_limited",
		retryable:  true,
		retryAfter: time.Minute,
	}

	workerReady := make(chan struct{})
	releaseWorker := make(chan struct{})
	workerDone := make(chan struct{})
	runtime.mu.Lock()
	go func() {
		close(workerReady)
		<-releaseWorker
		runtime.mu.Lock()
		runtime.finishWorkLocked(work, outcome)
		runtime.mu.Unlock()
		close(workerDone)
	}()
	<-workerReady
	cancelTask()
	close(releaseWorker)
	runtime.mu.Unlock()
	select {
	case <-workerDone:
	case <-time.After(time.Second):
		t.Fatal("worker did not finish after cancellation barrier")
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	task := runtime.tasks["task-a"]
	if task == nil || task.snapshot.State != chatgptwebauth.AccountInfoTaskCanceled ||
		task.snapshot.Processed != 1 || task.snapshot.Canceled != 1 ||
		task.snapshot.Results[0].Status != chatgptwebauth.AccountInfoResultCanceled {
		t.Fatalf("canceled task = %+v", task)
	}
	if len(runtime.scheduled) != 0 || len(runtime.schedules) != 0 || runtime.delayedTasks != 0 {
		t.Fatalf(
			"canceled task retained delayed work: map=%d heap=%d delayed=%d",
			len(runtime.scheduled),
			len(runtime.schedules),
			runtime.delayedTasks,
		)
	}
}

func TestChatGPTWebAccountInfoExpiredRecoveryStopsAfterFiniteRetries(t *testing.T) {
	now := time.Now().UTC()
	resetAt := now.Add(-time.Minute)
	retryAfter := 10 * time.Minute
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RefreshWorkers:        1,
			RefreshQueueSize:      1,
			RecoveryJitterSeconds: 0,
			MaxRetries:            1,
		},
		tasks:     make(map[string]*chatGPTWebAccountInfoTaskState),
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		inflight:  make(map[string]int),
		workers:   map[int]chan struct{}{0: make(chan struct{})},
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
		random:    bytes.NewReader(make([]byte, 64)),
	}
	work := chatGPTWebAccountInfoWork{
		target:          chatgptwebauth.AccountInfoRefreshTarget{AuthID: "expired"},
		attempt:         1,
		automatic:       true,
		quotaStateKnown: true,
		exhausted:       true,
		quotaResetAt:    resetAt,
	}
	outcome := chatGPTWebAccountInfoOutcome{
		status:     chatgptwebauth.AccountInfoResultFailed,
		errorCode:  "network_error",
		retryable:  true,
		retryAfter: retryAfter,
	}
	runtime.finishWorkLocked(work, outcome)
	entry := runtime.scheduled["retry:expired"]
	wantDue := now.Add(chatGPTWebAccountInfoMaxRetryAfter)
	if entry == nil || len(runtime.scheduled) != 1 || len(runtime.schedules) != 1 ||
		!entry.due.Equal(wantDue) {
		t.Fatalf("first failure did not create exactly one retry: map=%d heap=%d", len(runtime.scheduled), len(runtime.schedules))
	}
	runtime.removeScheduleLocked(entry.key)
	runtime.finishWorkLocked(entry.work, outcome)
	if recheck := runtime.scheduled["recovery:expired"]; recheck != nil ||
		len(runtime.scheduled) != 0 ||
		len(runtime.schedules) != 0 {
		t.Fatalf(
			"expired recovery reopened after finite retries: recovery=%+v map=%d heap=%d",
			recheck,
			len(runtime.scheduled),
			len(runtime.schedules),
		)
	}
	if state := runtime.states["expired"]; !state.NextRefreshAt.IsZero() ||
		state.LastError != "network_error" {
		t.Fatalf("expired recovery state = %+v, want stopped retry state", state)
	}
	if runtime.retryCount != 1 || runtime.failedCount != 1 {
		t.Fatalf("retry/failed counts = %d/%d, want 1/1", runtime.retryCount, runtime.failedCount)
	}
}

func TestChatGPTWebAccountInfoAvailableResultCancelsOldRecoverySchedule(t *testing.T) {
	now := time.Now().UTC()
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: 0,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	runtime.scheduleRecoveryLocked("quota-recovered", now.Add(time.Hour))

	runtime.finishWorkLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "quota-recovered"},
	}, chatGPTWebAccountInfoOutcome{
		status:          chatgptwebauth.AccountInfoResultUpdated,
		quotaStateKnown: true,
	})

	if entry := runtime.scheduled["recovery:quota-recovered"]; entry != nil {
		t.Fatalf("available quota retained recovery schedule: %+v", entry)
	}
	if state := runtime.states["quota-recovered"]; !state.NextRefreshAt.IsZero() {
		t.Fatalf("NextRefreshAt = %s, want zero", state.NextRefreshAt)
	}
}

func TestChatGPTWebAccountInfoSuccessfulRefreshClearsLatestGlobalError(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.cfg.RefreshQueueSize = 2
	runtime.mu.Lock()
	if !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "failed"},
	}) || !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "recovered"},
	}) {
		runtime.mu.Unlock()
		t.Fatal("failed to enqueue account-info work")
	}
	failed, _ := runtime.dequeueLocked()
	recovered, _ := runtime.dequeueLocked()
	runtime.finishWorkLocked(failed, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "network_error",
	})
	runtime.finishWorkLocked(recovered, chatGPTWebAccountInfoOutcome{
		status: chatgptwebauth.AccountInfoResultUpdated,
	})
	runtime.mu.Unlock()

	if snapshot := runtime.snapshot(); snapshot.LastError != "" {
		t.Fatalf("last error = %q, want cleared after newer success", snapshot.LastError)
	}
}

func TestChatGPTWebAccountInfoOlderSuccessDoesNotClearNewerGlobalError(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.cfg.RefreshQueueSize = 2
	runtime.mu.Lock()
	if !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "older-success"},
	}) || !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "newer-failure"},
	}) {
		runtime.mu.Unlock()
		t.Fatal("failed to enqueue account-info work")
	}
	olderSuccess, _ := runtime.dequeueLocked()
	newerFailure, _ := runtime.dequeueLocked()
	runtime.finishWorkLocked(newerFailure, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "rate_limited",
	})
	runtime.finishWorkLocked(olderSuccess, chatGPTWebAccountInfoOutcome{
		status: chatgptwebauth.AccountInfoResultUpdated,
	})
	runtime.mu.Unlock()

	if snapshot := runtime.snapshot(); snapshot.LastError != "rate_limited" {
		t.Fatalf("last error = %q, want newer failure preserved", snapshot.LastError)
	}
}

func TestChatGPTWebAccountInfoQueuedTargetIndexTracksQueueChanges(t *testing.T) {
	runtime := newChatGPTWebAccountInfoRuntime(nil, nil)
	runtime.cfg.RefreshWorkers = 1
	runtime.cfg.RefreshQueueSize = 4

	runtime.mu.Lock()
	if !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "first"},
	}) || !runtime.enqueueLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "second"},
	}) {
		runtime.mu.Unlock()
		t.Fatal("failed to enqueue account-info work")
	}
	if !runtime.targetQueuedLocked("first") || !runtime.targetQueuedLocked("second") {
		runtime.mu.Unlock()
		t.Fatalf("queued target index = %+v", runtime.queuedByTarget)
	}
	if _, ok := runtime.dequeueLocked(); !ok {
		runtime.mu.Unlock()
		t.Fatal("failed to dequeue first work")
	}
	if runtime.targetQueuedLocked("first") || !runtime.targetQueuedLocked("second") {
		runtime.mu.Unlock()
		t.Fatalf("queued target index after dequeue = %+v", runtime.queuedByTarget)
	}
	runtime.replaceQueuedWorkLocked([]chatGPTWebAccountInfoWork{{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "replacement"},
	}})
	if runtime.targetQueuedLocked("second") || !runtime.targetQueuedLocked("replacement") {
		runtime.mu.Unlock()
		t.Fatalf("queued target index after replace = %+v", runtime.queuedByTarget)
	}
	runtime.mu.Unlock()
}

func TestChatGPTWebAccountInfoFailedResultPreservesOldRecoverySchedule(t *testing.T) {
	now := time.Now().UTC()
	resetAt := now.Add(time.Hour)
	runtime := &chatGPTWebAccountInfoRuntime{
		cfg: config.ResolvedChatGPTWebAccountInfoConfig{
			RecoveryJitterSeconds: 0,
		},
		states:    make(map[string]chatgptwebauth.AccountInfoAuthRuntimeState),
		scheduled: make(map[string]*chatGPTWebAccountInfoSchedule),
		wake:      make(chan struct{}, 1),
		now:       func() time.Time { return now },
	}
	runtime.scheduleRecoveryLocked("quota-stale", resetAt)

	runtime.finishWorkLocked(chatGPTWebAccountInfoWork{
		target: chatgptwebauth.AccountInfoRefreshTarget{AuthID: "quota-stale"},
	}, chatGPTWebAccountInfoOutcome{
		status:    chatgptwebauth.AccountInfoResultFailed,
		errorCode: "network_error",
	})

	entry := runtime.scheduled["recovery:quota-stale"]
	if entry == nil || !entry.due.Equal(resetAt) {
		t.Fatalf("failed refresh changed recovery schedule: %+v", entry)
	}
}

func TestClassifyChatGPTWebAccountInfoDeadlineForRetry(t *testing.T) {
	code, retryable := classifyChatGPTWebAccountInfoError(context.DeadlineExceeded)
	if code != "network_error" || !retryable {
		t.Fatalf("deadline classification = %q retryable=%v", code, retryable)
	}
	code, retryable = classifyChatGPTWebAccountInfoError(context.Canceled)
	if code != "canceled" || retryable {
		t.Fatalf("canceled classification = %q retryable=%v", code, retryable)
	}
}

func TestChatGPTWebAccountInfoUnauthorizedRefreshUsesResolvedProxyAndInstallsTerminalState(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetProxyResolver(&accountInfoTestProxyResolver{url: "http://proxy.example:8080"})
	auth := chatGPTWebTestAuth("account-info-terminal")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	var refreshProxy string
	fake := &fakeChatGPTWebAuthService{
		refreshFn: func(_ context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
			refreshProxy = proxyURL
			credential.LifecycleState = chatgptwebauth.LifecycleDead
			return &credential, &chatgptwebauth.AuthError{
				Code:           "account_deactivated",
				State:          chatgptwebauth.LifecycleDead,
				LifecycleState: chatgptwebauth.LifecycleDead,
				Terminal:       true,
			}
		},
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	manager.RegisterExecutor(executor)
	t.Cleanup(func() { _ = executor.Close() })

	registered, _ := manager.GetByID(auth.ID)
	installed, errRefresh := executor.refreshChatGPTWebAccountInfoCredential(t.Context(), registered)
	if errRefresh == nil || installed == nil {
		t.Fatalf("refreshChatGPTWebAccountInfoCredential() = (%+v, %v)", installed, errRefresh)
	}
	code, retryable := classifyChatGPTWebAccountInfoError(errRefresh)
	if code != "unauthorized" || retryable {
		t.Fatalf("terminal classification = %q retryable=%v error=%v", code, retryable, errRefresh)
	}
	if refreshProxy != "http://proxy.example:8080" {
		t.Fatalf("refresh proxy = %q", refreshProxy)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil || current.LifecycleState() != cliproxyauth.LifecycleStateDead {
		t.Fatalf("terminal lifecycle was not installed: %+v", current)
	}
}

func TestChatGPTWebAccountInfoCanceledWaitStillPersistsRotatedRefreshToken(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-canceled-refresh")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseRefresh := func() { releaseOnce.Do(func() { close(release) }) }
	fake := &fakeChatGPTWebAuthService{
		refreshFn: func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
			close(started)
			<-release
			credential.AccessToken = "rotated-access"
			credential.RefreshToken = "rotated-refresh"
			return &credential, nil
		},
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	manager.RegisterExecutor(executor)
	t.Cleanup(func() {
		releaseRefresh()
		_ = executor.Close()
	})

	registered, _ := manager.GetByID(auth.ID)
	refreshContext, cancelRefresh := context.WithCancel(t.Context())
	refreshDone := make(chan error, 1)
	go func() {
		_, errRefresh := executor.refreshChatGPTWebAccountInfoCredential(
			refreshContext,
			registered,
		)
		refreshDone <- errRefresh
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("credential refresh did not start")
	}
	cancelRefresh()
	select {
	case errRefresh := <-refreshDone:
		if !errors.Is(errRefresh, context.Canceled) {
			t.Fatalf("canceled refresh error = %v, want context canceled", errRefresh)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled account-info waiter did not return")
	}

	releaseRefresh()
	deadline := time.Now().Add(time.Second)
	for {
		current, ok := manager.GetByID(auth.ID)
		if ok && current != nil {
			accessToken, _ := current.Metadata["access_token"].(string)
			refreshToken, _ := current.Metadata["refresh_token"].(string)
			if accessToken == "rotated-access" && refreshToken == "rotated-refresh" {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("rotated credential was not persisted after cancellation: %+v", current)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestChatGPTWebAccountInfoCookiePersistenceMergesConcurrentChanges(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	resetAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-release
		http.SetCookie(w, &http.Cookie{Name: "response-cookie", Value: "added", Path: "/"})
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{"account_id": "account-1", "plan_type": "plus"}},
			}})
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 3, "reset_after": resetAt},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-cookie-merge")
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok || expected == nil {
		t.Fatal("registered auth missing")
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.runtimeBaseURL = server.URL
	t.Cleanup(func() { _ = executor.Close() })

	done := make(chan chatGPTWebAccountInfoOutcome, 1)
	go func() {
		done <- executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
	}()
	<-started
	<-started
	_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), expected, func(currentAuth *cliproxyauth.Auth) {
		credential, errParse := chatgptwebauth.ParseCredential(currentAuth.Metadata)
		if errParse != nil {
			return
		}
		credential.Cookies = append(credential.Cookies, chatgptwebauth.Cookie{
			Name:  "concurrent-cookie",
			Value: "keep",
			Path:  "/",
		})
		credential.ApplyToMetadata(currentAuth.Metadata)
	})
	if errMutate != nil || !current {
		t.Fatalf("concurrent cookie mutation = current %v error %v", current, errMutate)
	}
	close(release)
	if outcome := <-done; outcome.status != chatgptwebauth.AccountInfoResultUpdated {
		t.Fatalf("account info outcome = %+v", outcome)
	}
	installed, _ := manager.GetByID(auth.ID)
	credential, errCredential := chatgptwebauth.ParseCredential(installed.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	cookieNames := make(map[string]bool)
	for _, cookie := range credential.Cookies {
		cookieNames[cookie.Name] = true
	}
	if !cookieNames["concurrent-cookie"] || !cookieNames["response-cookie"] {
		t.Fatalf("merged cookies = %+v", credential.Cookies)
	}
}

func TestChatGPTWebImageQuotaObservationDetectsLaterRemainingProjection(t *testing.T) {
	auth := chatGPTWebTestAuth("account-info-quota-observation")
	auth.Metadata["image_quota_remaining"] = 3
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	auth.Metadata["quota_updated_at"] = time.Now().UTC().Add(-time.Minute).Format(time.RFC3339Nano)
	observation := captureChatGPTWebImageQuotaObservation(auth)
	if !observation.matches(auth) {
		t.Fatal("unchanged quota observation did not match")
	}
	auth.Metadata["image_quota_remaining"] = 2
	if observation.matches(auth) {
		t.Fatal("later projected image success was not detected")
	}
}

func TestChatGPTWebAccountInfoAuthoritativeQuotaOverridesEarlierImageQuotaResult(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-stale-quota")
	auth.Metadata["account_id"] = "account-1"
	auth.Metadata["image_quota_remaining"] = -2
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_reset_at"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	resetAt := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			if got := request.Header.Get("Chatgpt-Account-Id"); got != "account-1" {
				t.Errorf("account check Chatgpt-Account-Id = %q, want account-1", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"default_account_id": "account-2",
				"accounts": map[string]any{
					"default": map[string]any{"account": map[string]any{
						"account_id": "account-2",
						"plan_type":  "free",
					}},
					"requested": map[string]any{"account": map[string]any{
						"account_id": "account-1",
						"plan_type":  "plus",
					}},
				},
			})
		case chatgptwebauth.ConversationInitPath:
			if got := request.Header.Get("Chatgpt-Account-Id"); got != "account-1" {
				t.Errorf("conversation init Chatgpt-Account-Id = %q, want account-1", got)
			}
			retryAfter := 2 * time.Minute
			manager.MarkResult(context.Background(), cliproxyauth.Result{
				AuthID:   auth.ID,
				Provider: chatgptwebauth.Provider,
				Model:    chatgptwebauth.ImageModel,
				Error: &cliproxyauth.Error{
					Code:       "chatgpt_web_image_quota",
					Message:    "new image quota result",
					HTTPStatus: http.StatusTooManyRequests,
				},
				RetryAfter: &retryAfter,
			})
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{
					"feature_name": "image_gen",
					"remaining":    7,
					"reset_after":  resetAt,
				},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		MaxRetries: accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	outcome := executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultUpdated ||
		outcome.errorCode != "" || outcome.retryable {
		t.Fatalf("account info outcome = %+v", outcome)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.PlanType != "plus" || credential.AccountID != "account-1" {
		t.Fatalf("profile = plan %q account %q", credential.PlanType, credential.AccountID)
	}
	if credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != 7 ||
		credential.QuotaState != chatgptwebauth.QuotaStateAvailable ||
		credential.QuotaUpdatedAt == "" {
		t.Fatalf("authoritative quota response was not applied: %+v", credential)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState != nil && (imageState.Unavailable || imageState.Quota.Exceeded || imageState.Quota.Reason != "") {
		t.Fatalf("authoritative positive quota did not clear the image cooldown: %+v", imageState)
	}
}

func TestChatGPTWebAccountInfoDoesNotOverwriteConcurrentAccountProfile(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-stale-profile")
	auth.Metadata["account_id"] = "account-1"
	auth.Metadata["plan_type"] = "free"
	auth.Metadata["profile_updated_at"] = "2026-07-27T00:00:00Z"
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok || expected == nil {
		t.Fatal("registered auth missing")
	}

	concurrentUpdatedAt := "2026-07-27T01:00:00Z"
	var mutateOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			mutateOnce.Do(func() {
				_, current, errMutate := manager.MutateRuntimeMetadataIfCurrent(
					context.Background(),
					expected,
					func(currentAuth *cliproxyauth.Auth) {
						credential, errParse := chatgptwebauth.ParseCredential(currentAuth.Metadata)
						if errParse != nil {
							return
						}
						credential.PlanType = "enterprise"
						credential.ProfileUpdatedAt = concurrentUpdatedAt
						credential.ApplyToMetadata(currentAuth.Metadata)
					},
				)
				if errMutate != nil || !current {
					t.Errorf("concurrent profile mutation = current %v error %v", current, errMutate)
				}
			})
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "account-1",
					"plan_type":  "plus",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{"feature_name": "image_gen", "remaining": 3},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	outcome := executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultUpdated &&
		outcome.status != chatgptwebauth.AccountInfoResultUnchanged {
		t.Fatalf("account info outcome = %+v", outcome)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.PlanType != "enterprise" || credential.ProfileUpdatedAt != concurrentUpdatedAt {
		t.Fatalf("concurrent profile was overwritten: %+v", credential)
	}
	if credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != 3 {
		t.Fatalf("independent quota update was not applied: %+v", credential)
	}
}

func TestChatGPTWebAccountInfoFailurePreservesExhaustedQuotaAfterConcurrentImageSuccess(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("account-info-failure-after-success")
	oldQuotaUpdatedAt := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano)
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateExhausted)
	auth.Metadata["image_quota_remaining"] = 0
	auth.Metadata["quota_updated_at"] = oldQuotaUpdatedAt
	auth.Metadata["quota_stale"] = false
	auth.ModelStates = map[string]*cliproxyauth.ModelState{
		chatgptwebauth.ImageModel: {
			Status:         cliproxyauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Hour),
			Quota: cliproxyauth.QuotaState{
				Exceeded: true,
				Reason:   "chatgpt_web_image_quota",
			},
		},
	}
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	var successOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		successOnce.Do(func() {
			manager.MarkResult(context.Background(), cliproxyauth.Result{
				AuthID:   auth.ID,
				Provider: chatgptwebauth.Provider,
				Model:    chatgptwebauth.ImageModel,
				Success:  true,
			})
		})
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		MaxRetries: accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	t.Cleanup(func() { _ = executor.Close() })
	executor.runtimeBaseURL = server.URL

	outcome := executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultFailed || !outcome.retryable {
		t.Fatalf("account info outcome = %+v", outcome)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.QuotaState != chatgptwebauth.QuotaStateExhausted ||
		credential.ImageQuotaRemaining == nil ||
		*credential.ImageQuotaRemaining != 0 ||
		!credential.QuotaStale ||
		credential.QuotaLastError == "" ||
		credential.QuotaUpdatedAt != oldQuotaUpdatedAt {
		t.Fatalf("failed refresh changed the last confirmed exhausted quota: %+v", credential)
	}
	imageState := current.ModelStates[chatgptwebauth.ImageModel]
	if imageState == nil || !imageState.Unavailable || !imageState.Quota.Exceeded ||
		imageState.Quota.Reason != "chatgpt_web_image_quota" {
		t.Fatalf("concurrent image success cleared the confirmed quota state: %+v", imageState)
	}
}

func TestChatGPTWebImageQuotaErrorPreservesResultProjectionAfterCommit(t *testing.T) {
	upstream := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		"/backend-api/f/conversation",
		[]byte(`{"error":{"message":"image quota exhausted"}}`),
		fhttp.Header{"Retry-After": {"17"}},
	)
	projected := chatGPTWebImageRequestError(upstream)
	committed := chatGPTWebCommittedRequestError(context.Background(), projected)

	var model interface{ ExecutionResultModel() string }
	if !errors.As(committed, &model) || model.ExecutionResultModel() != chatgptwebauth.ImageModel {
		t.Fatalf("result model projection missing: %v", committed)
	}
	var code interface{ ExecutionResultErrorCode() string }
	if !errors.As(committed, &code) || code.ExecutionResultErrorCode() != "chatgpt_web_image_quota" {
		t.Fatalf("result code projection missing: %v", committed)
	}
	var status interface{ StatusCode() int }
	if !errors.As(committed, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %v", committed)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(committed, &headers) || headers.Headers().Get("Retry-After") != "17" {
		t.Fatalf("Retry-After = %v", committed)
	}
	var skip interface{ SkipAuthResult() bool }
	if errors.As(committed, &skip) && skip.SkipAuthResult() {
		t.Fatalf("committed image quota error skipped auth result: %v", committed)
	}
	var marker interface{ RequestCommitted() bool }
	if !errors.As(committed, &marker) || !marker.RequestCommitted() {
		t.Fatalf("committed image quota error missing terminal marker: %v", committed)
	}
}

func TestChatGPTWebAccountInfoFreshRejectsFutureTimestamps(t *testing.T) {
	now := time.Now().UTC()
	if accountInfoFresh(now.Add(time.Minute).Format(time.RFC3339Nano), now, 15) {
		t.Fatal("future timestamp was treated as fresh")
	}
	if !accountInfoFresh(now.Add(-time.Minute).Format(time.RFC3339Nano), now, 15) {
		t.Fatal("recent timestamp was not treated as fresh")
	}
	if accountInfoFresh(now.Add(-16*time.Minute).Format(time.RFC3339Nano), now, 15) {
		t.Fatal("expired timestamp was treated as fresh")
	}
}

func waitForAccountInfoTask(t *testing.T, executor *ChatGPTWebExecutor, id string) *chatgptwebauth.AccountInfoRefreshTask {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, ok := executor.AccountInfoRefreshTask(id)
		if !ok {
			t.Fatalf("task %q disappeared", id)
		}
		if task.CompletedAt != nil {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %q did not complete", id)
	return nil
}

func accountInfoTestInt(value int) *int {
	return &value
}

func accountInfoPeriodicScheduleCountLocked(runtime *chatGPTWebAccountInfoRuntime) int {
	if runtime == nil {
		return 0
	}
	count := 0
	for key := range runtime.scheduled {
		if strings.HasPrefix(key, chatGPTWebAccountInfoPeriodicSchedulePrefix) {
			count++
		}
	}
	return count
}

type accountInfoTestProxyResolver struct {
	url string
}

type accountInfoTestLifecycleStore struct {
	started     chan context.Context
	finished    chan error
	release     chan struct{}
	releaseOnce sync.Once
}

func (*accountInfoTestLifecycleStore) List(context.Context) ([]*cliproxyauth.Auth, error) {
	return nil, nil
}

func (store *accountInfoTestLifecycleStore) Save(ctx context.Context, _ *cliproxyauth.Auth) (string, error) {
	store.started <- ctx
	var errSave error
	select {
	case <-ctx.Done():
		errSave = ctx.Err()
	case <-store.release:
		errSave = errors.New("account-info test store released")
	}
	store.finished <- errSave
	return "", errSave
}

func (*accountInfoTestLifecycleStore) Delete(context.Context, string) error {
	return nil
}

func (store *accountInfoTestLifecycleStore) releaseStore() {
	store.releaseOnce.Do(func() {
		close(store.release)
	})
}

func (resolver *accountInfoTestProxyResolver) Resolve(context.Context, *cliproxyauth.Auth) (cliproxyauth.ResolvedProxy, error) {
	return cliproxyauth.ResolvedProxy{URL: resolver.url, BindingID: "account-info-test"}, nil
}

func (*accountInfoTestProxyResolver) ReportFailure(_ context.Context, _ *cliproxyauth.Auth, err error) error {
	return err
}
