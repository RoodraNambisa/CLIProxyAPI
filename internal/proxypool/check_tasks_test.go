package proxypool

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestCheckProxyFallsBackAcrossConfiguredEndpoints(t *testing.T) {
	var primaryCalls atomic.Int64
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		primaryCalls.Add(1)
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer primary.Close()
	var backupCalls atomic.Int64
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		backupCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer backup.Close()

	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyHealthCheck = internalconfig.ProxyHealthCheckConfig{
		EndpointTimeoutSeconds: 1,
		Endpoints: []internalconfig.ProxyHealthCheckEndpointConfig{
			{Name: "primary", URL: primary.URL, Mode: internalconfig.ProxyHealthCheckModeHTTPStatus},
			{Name: "backup", URL: backup.URL, Mode: internalconfig.ProxyHealthCheckModeHTTPStatus},
		},
	}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	result := manager.checkProxy(t.Context(), "direct")
	if !result.OK || result.Endpoint != "backup" || primaryCalls.Load() != 1 || backupCalls.Load() != 1 {
		t.Fatalf("fallback result/calls = %+v/%d/%d", result, primaryCalls.Load(), backupCalls.Load())
	}
}

func TestScheduledChecksApplyFailureThresholdAndSuccessReset(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyHealthCheck.FailureThreshold = 2
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(t.Context(), proxyPoolTestAuth("threshold-auth")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.check = func(context.Context, string) TraceResult {
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}
	manager.CheckNow(t.Context())
	first := manager.health[binding.ID]
	if !first.OK || first.FailureStreak != 1 {
		t.Fatalf("first failed check health = %+v", first)
	}
	manager.CheckNow(t.Context())
	second := manager.health[binding.ID]
	if second.OK || second.FailureStreak != 2 {
		t.Fatalf("second failed check health = %+v", second)
	}
	manager.check = successfulTrace
	manager.CheckNow(t.Context())
	recovered := manager.health[binding.ID]
	if !recovered.OK || recovered.FailureStreak != 0 {
		t.Fatalf("recovered health = %+v", recovered)
	}
}

func TestScheduledFailureThresholdDoesNotImmediatelyRejectFreshBinding(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyHealthCheck.FailureThreshold = 2
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(t.Context(), proxyPoolTestAuth("threshold-fresh")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.deleteHealth(binding.ID)
	manager.check = func(context.Context, string) TraceResult {
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}
	manager.CheckNow(t.Context())
	health := manager.health[binding.ID]
	if !health.OK || health.FailureStreak != 1 {
		t.Fatalf("fresh binding after first scheduled failure = %+v", health)
	}
}

func TestAsyncCheckTaskContinuesWithoutRequestContextAndDeduplicatesPool(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	started := make(chan struct{})
	release := make(chan struct{})
	manager.check = func(context.Context, string) TraceResult {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		return successfulTrace(context.Background(), "")
	}
	task, errStart := manager.StartCheckTask("residential", 1)
	if errStart != nil || task.Status != CheckTaskStatusQueued {
		t.Fatalf("StartCheckTask() = %+v, %v", task, errStart)
	}
	<-started
	duplicate, errDuplicate := manager.StartCheckTask("residential", 1)
	var conflict *CheckTaskConflictError
	if !errors.As(errDuplicate, &conflict) || duplicate.ID != task.ID || conflict.TaskID != task.ID {
		t.Fatalf("duplicate task/error = %+v/%v", duplicate, errDuplicate)
	}
	close(release)
	waitForCondition(t, time.Second, func() bool {
		current, exists := manager.CheckTask("residential", task.ID)
		return exists && current.Status == CheckTaskStatusCompleted
	})
	completed, _ := manager.CheckTask("residential", task.ID)
	if completed.Total != 1 || completed.Completed != 1 || completed.Succeeded != 1 || completed.Running != 0 {
		t.Fatalf("completed task = %+v", completed)
	}
}

func TestCheckTaskStopCancelsQueuedProbeWithoutLeakingAdmission(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyHealthCheck.Concurrency = 1
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	started := make(chan struct{})
	manager.check = func(ctx context.Context, _ string) TraceResult {
		close(started)
		<-ctx.Done()
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}
	task, errStart := manager.StartCheckTask("residential", 2)
	if errStart != nil {
		t.Fatalf("StartCheckTask() error = %v", errStart)
	}
	<-started
	waitForCondition(t, time.Second, func() bool { return manager.CheckAdmissionSnapshot().Queued == 1 })
	manager.Stop()
	current, exists := manager.CheckTask("residential", task.ID)
	if !exists || current.Status != CheckTaskStatusFailed || current.ErrorCode != "proxy_check_canceled" {
		t.Fatalf("canceled task = %+v, exists=%t", current, exists)
	}
	if snapshot := manager.CheckAdmissionSnapshot(); snapshot.Active != 0 || snapshot.Queued != 0 {
		t.Fatalf("admission after stop = %+v", snapshot)
	}
}

func TestChecksAcrossPoolsShareGlobalAdmission(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3383")
	cfg.ProxyHealthCheck.Concurrency = 8
	secondary := cfg.ProxyPools[0]
	secondary.Name = "secondary"
	secondary.Entries = append([]internalconfig.ProxyPoolEntryConfig(nil), secondary.Entries...)
	secondary.Entries[0].ID = "secondary"
	secondary.Entries[0].URLTemplate = "http://user-session-{3}:password@secondary.example"
	cfg.ProxyPools = append(cfg.ProxyPools, secondary)
	cfg.ProxyRules = append(cfg.ProxyRules, internalconfig.ProxyRuleConfig{
		Name:       "secondary",
		Pool:       "secondary",
		Providers:  []string{"codex"},
		Priorities: []int{1},
	})
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	for index := 0; index < 100; index++ {
		auth := proxyPoolTestAuth("shared-admission-" + time.Unix(int64(index), 0).UTC().Format("150405"))
		if index >= 50 {
			auth.Attributes["priority"] = "1"
		}
		if _, errResolve := manager.Resolve(t.Context(), auth); errResolve != nil {
			t.Fatalf("Resolve(%d) error = %v", index, errResolve)
		}
	}

	var active atomic.Int64
	var peak atomic.Int64
	var calls atomic.Int64
	manager.check = func(context.Context, string) TraceResult {
		current := active.Add(1)
		for {
			previous := peak.Load()
			if current <= previous || peak.CompareAndSwap(previous, current) {
				break
			}
		}
		calls.Add(1)
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
		return successfulTrace(context.Background(), "")
	}

	bindingsByPool := map[string][]Binding{}
	for _, binding := range manager.SortedBindings() {
		bindingsByPool[binding.Pool] = append(bindingsByPool[binding.Pool], binding)
	}
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		if _, errCheck := manager.CheckPool(t.Context(), "residential", 0); errCheck != nil {
			t.Errorf("CheckPool() error = %v", errCheck)
		}
	}()
	go func() {
		defer wait.Done()
		manager.checkBoundPool(t.Context(), "secondary", bindingsByPool["secondary"], true)
	}()
	wait.Wait()

	if calls.Load() != 100 {
		t.Fatalf("probe calls = %d, want 100", calls.Load())
	}
	if peak.Load() > 8 || peak.Load() < 2 {
		t.Fatalf("peak shared concurrency = %d, want 2..8", peak.Load())
	}
	snapshot := manager.CheckAdmissionSnapshot()
	if snapshot.Active != 0 || snapshot.Queued != 0 || snapshot.Completed != 100 || snapshot.Succeeded != 100 {
		t.Fatalf("shared admission snapshot = %+v", snapshot)
	}
}

func TestCheckTaskReportsConfigurationChangeWithoutApplyingOldHealth(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	started := make(chan struct{})
	release := make(chan struct{})
	manager.check = func(context.Context, string) TraceResult {
		close(started)
		<-release
		return successfulTrace(context.Background(), "")
	}
	task, errStart := manager.StartCheckTask("residential", 1)
	if errStart != nil {
		t.Fatalf("StartCheckTask() error = %v", errStart)
	}
	<-started
	next := proxyPoolTestConfig("3334")
	next.ProxyHealthCheck.Concurrency = 9
	if errUpdate := manager.UpdateConfig(next); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	close(release)
	waitForCondition(t, time.Second, func() bool {
		current, exists := manager.CheckTask("residential", task.ID)
		return exists && current.Status == CheckTaskStatusFailed
	})
	current, _ := manager.CheckTask("residential", task.ID)
	if current.ErrorCode != "proxy_configuration_changed" {
		t.Fatalf("changed task = %+v", current)
	}
}
