package executor

import (
	"container/heap"
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebReloginQueueTenThousandDelayedTasksUseConstantGoroutines(t *testing.T) {
	baseline := runtime.NumGoroutine()
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	executor := &ChatGPTWebExecutor{
		now:             time.Now,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
	queue := newChatGPTWebReloginQueue(executor, lifecycleCtx, chatGPTWebBackgroundReloginConcurrency)
	executor.backgroundQueue = queue
	queue.setEnabled(true)
	t.Cleanup(queue.close)

	dueAt := time.Now().Add(time.Hour)
	queue.mu.Lock()
	for index := range 10_000 {
		key := "generation-" + strconv.Itoa(index)
		task := &chatGPTWebReloginQueueTask{
			authID:        "auth-" + strconv.Itoa(index),
			instanceID:    "instance-" + strconv.Itoa(index),
			generationKey: key,
			attempt:       1,
			dueAt:         dueAt,
			sequence:      uint64(index + 1),
			heapIndex:     -1,
		}
		queue.tasks[key] = task
		queue.indexTaskLocked(task)
		heap.Push(&queue.delayed, task)
	}
	queue.sequence = 10_000
	queue.mu.Unlock()
	queue.notify()

	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := queue.snapshot()
		return snapshot.Delayed == 10_000 && snapshot.Running == 0
	})
	if delta := runtime.NumGoroutine() - baseline; delta > chatGPTWebBackgroundReloginConcurrency+4 {
		t.Fatalf("goroutine delta = %d, want at most %d", delta, chatGPTWebBackgroundReloginConcurrency+4)
	}
}

func TestChatGPTWebReloginQueueDeduplicatesOneCredentialInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "queue-deduplicate")
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "deduplicated-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	for range 100 {
		executor.TriggerBackgroundRelogin(expected)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool { return fake.loginCalls.Load() == 1 })
	snapshot := executor.BackgroundReloginSnapshot()
	if snapshot.Deduplicated != 99 {
		t.Fatalf("deduplicated = %d, want 99", snapshot.Deduplicated)
	}
	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot = executor.BackgroundReloginSnapshot()
		return snapshot.Queued == 0 && snapshot.Delayed == 0 && snapshot.Running == 0
	})
}

func TestChatGPTWebReloginQueueCancelsQueuedCredentialInstance(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "queue-cancel-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	var queued *cliproxyauth.Auth
	for index := range chatGPTWebBackgroundReloginConcurrency + 1 {
		auth := registerChatGPTWebPendingAuth(t, manager, "queue-cancel-"+strconv.Itoa(index))
		executor.TriggerBackgroundRelogin(auth)
		queued = auth
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == chatGPTWebBackgroundReloginConcurrency
	})
	executor.CloseAuthInstanceExecutionSessions(queued.ID, queued.RuntimeInstanceID(), "auth_removed")
	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := executor.BackgroundReloginSnapshot()
		return snapshot.Queued == 0 && snapshot.Delayed == 0 && snapshot.Running == 0
	})
	if got := fake.loginCalls.Load(); got != chatGPTWebBackgroundReloginConcurrency {
		t.Fatalf("login calls = %d, want %d", got, chatGPTWebBackgroundReloginConcurrency)
	}
	if got := executor.BackgroundReloginSnapshot().Canceled; got == 0 {
		t.Fatal("expected queued credential cancellation to be counted")
	}
}

func TestChatGPTWebReloginQueuePromotesManualRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "queue-promote")
	fake := &fakeChatGPTWebAuthService{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := *input.Credential
		credential.AccessToken = "promoted-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	queue := executor.backgroundQueue
	task := &chatGPTWebReloginQueueTask{
		authID:        expected.ID,
		instanceID:    expected.RuntimeInstanceID(),
		generationKey: chatGPTWebReloginGenerationKey(expected),
		attempt:       1,
		dueAt:         time.Now().Add(time.Hour),
		heapIndex:     -1,
	}
	queue.mu.Lock()
	queue.sequence++
	task.sequence = queue.sequence
	queue.tasks[task.generationKey] = task
	queue.indexTaskLocked(task)
	heap.Push(&queue.delayed, task)
	queue.mu.Unlock()
	queue.notify()

	updated, installed, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil {
		t.Fatal(errRelogin)
	}
	if !installed || updated == nil {
		t.Fatalf("installed = %t, updated = %v", installed, updated)
	}
	snapshot := executor.BackgroundReloginSnapshot()
	if snapshot.Promoted != 1 {
		t.Fatalf("promoted = %d, want 1", snapshot.Promoted)
	}
	if snapshot.Queued != 0 || snapshot.Delayed != 0 || snapshot.Running != 0 {
		t.Fatalf("queue snapshot after manual re-login = %+v", snapshot)
	}
}

func TestChatGPTWebReloginQueueRemovalUsesAuthIndex(t *testing.T) {
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	executor := &ChatGPTWebExecutor{
		now:             time.Now,
		lifecycleCtx:    lifecycleCtx,
		lifecycleCancel: lifecycleCancel,
	}
	queue := newChatGPTWebReloginQueue(executor, lifecycleCtx, 1)
	executor.backgroundQueue = queue
	queue.setEnabled(true)
	t.Cleanup(queue.close)

	dueAt := time.Now().Add(time.Hour)
	queue.mu.Lock()
	for index := range 10_000 {
		authID := "auth-" + strconv.Itoa(index)
		key := "generation-" + strconv.Itoa(index)
		task := &chatGPTWebReloginQueueTask{
			authID:        authID,
			instanceID:    "instance-" + strconv.Itoa(index),
			generationKey: key,
			attempt:       1,
			dueAt:         dueAt,
			sequence:      uint64(index + 1),
			heapIndex:     -1,
		}
		queue.tasks[key] = task
		queue.indexTaskLocked(task)
		heap.Push(&queue.delayed, task)
	}
	queue.sequence = 10_000
	queue.mu.Unlock()

	queue.removeAuthInstance("auth-7319", "instance-7319")

	queue.mu.Lock()
	defer queue.mu.Unlock()
	if got := len(queue.tasks); got != 9_999 {
		t.Fatalf("queued tasks = %d, want 9999", got)
	}
	if _, exists := queue.tasks["generation-7319"]; exists {
		t.Fatal("target task remains queued")
	}
	if _, exists := queue.byAuthID["auth-7319"]; exists {
		t.Fatal("target auth index remains")
	}
	if got := len(queue.byAuthID); got != 9_999 {
		t.Fatalf("auth index entries = %d, want 9999", got)
	}
}

func TestChatGPTWebReloginQueueBackpressureIsBoundedAndPersistent(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	workers := 1
	queueSize := 1
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "bounded-queue-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:          true,
		AutoReloginWorkers:   &workers,
		AutoReloginQueueSize: &queueSize,
	}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	first := registerChatGPTWebPendingAuth(t, manager, "bounded-queue-first")
	second := registerChatGPTWebPendingAuth(t, manager, "bounded-queue-second")
	if !executor.TriggerBackgroundRelogin(first) {
		t.Fatal("first task was rejected")
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool { return fake.loginCalls.Load() == 1 })
	if executor.TriggerBackgroundRelogin(second) {
		t.Fatal("second task was accepted beyond the configured queue capacity")
	}
	current, ok := manager.GetByID(second.ID)
	if !ok || current == nil || current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending || chatGPTWebLifecycleReason(current) != "auto_relogin_backpressure" {
		t.Fatalf("backpressured credential = %#v; snapshot=%+v expected_key=%q current_key=%q", current, executor.BackgroundReloginSnapshot(), chatGPTWebReloginGenerationKey(second), chatGPTWebReloginGenerationKey(current))
	}
	snapshot := executor.BackgroundReloginSnapshot()
	if snapshot.QueueLimit != 1 || snapshot.Backpressured != 1 || snapshot.Running != 1 {
		t.Fatalf("bounded queue snapshot = %+v", snapshot)
	}

	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool { return fake.loginCalls.Load() == 2 })
}

func TestChatGPTWebReloginQueueHotResizeGrowsAndShrinksWithoutDroppingActiveTasks(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	workers := 1
	queueSize := 4
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var active atomic.Int32
	var maximum atomic.Int32
	fake := &fakeChatGPTWebAuthService{loginFn: func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "resized-queue-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}}
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:          true,
		AutoReloginWorkers:   &workers,
		AutoReloginQueueSize: &queueSize,
	}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	for index := range 3 {
		auth := registerChatGPTWebPendingAuth(t, manager, "resize-"+strconv.Itoa(index))
		if !executor.TriggerBackgroundRelogin(auth) {
			t.Fatalf("task %d was rejected", index)
		}
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool { return fake.loginCalls.Load() == 1 })

	workers = 3
	executor.UpdateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:          true,
		AutoReloginWorkers:   &workers,
		AutoReloginQueueSize: &queueSize,
	}})
	waitForChatGPTWebCondition(t, time.Second, func() bool { return fake.loginCalls.Load() == 3 })

	workers = 1
	executor.UpdateConfig(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		AutoRelogin:          true,
		AutoReloginWorkers:   &workers,
		AutoReloginQueueSize: &queueSize,
	}})
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := executor.BackgroundReloginSnapshot()
		return snapshot.WorkerLimit == 1 && snapshot.Shrinking && snapshot.Running == 3
	})
	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		snapshot := executor.BackgroundReloginSnapshot()
		return snapshot.Workers == 1 && snapshot.Running == 0 && snapshot.Queued == 0
	})
	if got := maximum.Load(); got != 3 {
		t.Fatalf("maximum concurrency = %d, want 3 after hot growth", got)
	}
}

func TestChatGPTWebReloginQueueBackpressureRefillsOnlyAtLowWater(t *testing.T) {
	first := &chatGPTWebReloginQueueTask{authID: "first", generationKey: "first"}
	second := &chatGPTWebReloginQueueTask{authID: "second", generationKey: "second"}
	queue := &chatGPTWebReloginQueue{
		executor:   &ChatGPTWebExecutor{},
		wake:       make(chan struct{}, 1),
		enabled:    true,
		queueLimit: 4,
		tasks: map[string]*chatGPTWebReloginQueueTask{
			"third":  {authID: "third", generationKey: "third"},
			"fourth": {authID: "fourth", generationKey: "fourth"},
		},
		active: map[string]*chatGPTWebReloginQueueActive{
			first.generationKey:  {task: first},
			second.generationKey: {task: second},
		},
		byAuthID: make(map[string]map[string]struct{}),
	}
	queue.reconcileNeeded.Store(true)

	queue.finish(first, false)
	if !queue.reconcileNeeded.Load() {
		t.Fatal("backpressure reconciliation ran before the queue reached its low-water mark")
	}

	queue.finish(second, false)
	if queue.reconcileNeeded.Load() {
		t.Fatal("backpressure reconciliation was not released at the low-water mark")
	}
}
