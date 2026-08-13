package executor

import (
	"container/heap"
	"context"
	"runtime"
	"strconv"
	"sync"
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
