package executor

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

type chatGPTWebImageTaskTestClock struct {
	now time.Time
}

func (clock *chatGPTWebImageTaskTestClock) Now() time.Time {
	return clock.now
}

func (clock *chatGPTWebImageTaskTestClock) Advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

func TestChatGPTWebImageTaskRegistryTracksOnlySafeDiagnostics(t *testing.T) {
	clock := &chatGPTWebImageTaskTestClock{now: time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)}
	registry := newChatGPTWebImageTaskRegistry(clock.Now)
	const authID = "chatgpt-web-sensitive-account@example.com.json"
	ctx, handle := registry.begin(t.Context(), authID)
	if chatGPTWebImageTaskHandleFromContext(ctx) != handle {
		t.Fatal("task handle was not attached to the request context")
	}
	handle.setStage("starting_generation")
	clock.Advance(2 * time.Minute)
	finishPoll := beginChatGPTWebImageTaskPoll(ctx)
	clock.Advance(3 * time.Second)
	finishPoll(false)

	snapshot := registry.snapshot()
	if snapshot.Active != 1 || len(snapshot.Tasks) != 1 {
		t.Fatalf("task snapshot = %#v", snapshot)
	}
	task := snapshot.Tasks[0]
	if task.ID != handle.id || task.Stage != "poll_wait" || task.PollsInFlight != 0 || task.LastPollCompletedAt == nil {
		t.Fatalf("task = %#v", task)
	}
	if task.CredentialFingerprint == "" || strings.Contains(task.CredentialFingerprint, "sensitive") {
		t.Fatalf("credential fingerprint = %q", task.CredentialFingerprint)
	}
	payload, errMarshal := json.Marshal(snapshot)
	if errMarshal != nil {
		t.Fatalf("json.Marshal() error = %v", errMarshal)
	}
	if strings.Contains(string(payload), authID) {
		t.Fatalf("task snapshot exposed auth ID: %s", payload)
	}
	handle.finish()
	if got := registry.snapshot().Active; got != 0 {
		t.Fatalf("active after finish = %d, want 0", got)
	}
}

func TestChatGPTWebImageTaskRegistryCancelIsIdempotent(t *testing.T) {
	clock := &chatGPTWebImageTaskTestClock{now: time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)}
	registry := newChatGPTWebImageTaskRegistry(clock.Now)
	ctx, handle := registry.begin(t.Context(), "auth-a")

	first, found := registry.cancelTask(handle.id)
	if !found || first.Status != "canceling" {
		t.Fatalf("first cancel = %#v, found=%t", first, found)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("task context was not canceled")
	}
	normalized := normalizeChatGPTWebImageTaskCancellation(ctx, context.Canceled)
	var canceledErr *chatGPTWebImageTaskCanceledError
	if !errors.As(normalized, &canceledErr) || !canceledErr.SkipAuthResult() || canceledErr.RetryOtherAuth() || canceledErr.StatusCode() != 499 {
		t.Fatalf("normalized cancellation = %T %v", normalized, normalized)
	}
	snapshot := registry.snapshot()
	if snapshot.Canceling != 1 || !snapshot.Tasks[0].Canceling || snapshot.Tasks[0].CancellationRequestedAt == nil {
		t.Fatalf("canceling snapshot = %#v", snapshot)
	}
	handle.finish()
	second, found := registry.cancelTask(handle.id)
	if !found || second.Status != "already_canceled" {
		t.Fatalf("repeated cancel = %#v, found=%t", second, found)
	}
	clock.Advance(chatGPTWebImageCanceledTombstoneTTL + time.Second)
	if _, found = registry.cancelTask(handle.id); found {
		t.Fatal("expired cancel tombstone remained addressable")
	}
}

func TestChatGPTWebImageTaskRegistryMarksLongRunningTasksWithoutCanceling(t *testing.T) {
	clock := &chatGPTWebImageTaskTestClock{now: time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)}
	registry := newChatGPTWebImageTaskRegistry(clock.Now)
	ctx, handle := registry.begin(t.Context(), "auth-b")
	clock.Advance(chatGPTWebImageTaskWarningAge + time.Second)
	snapshot := registry.snapshot()
	if snapshot.ActiveOver15Min != 1 || !snapshot.Tasks[0].Over15Minutes || snapshot.Tasks[0].Canceling {
		t.Fatalf("long-running snapshot = %#v", snapshot)
	}
	select {
	case <-ctx.Done():
		t.Fatal("diagnostic warning canceled the task")
	default:
	}
	handle.finish()
}

func TestChatGPTWebImageTaskRegistryBoundsCanceledTombstones(t *testing.T) {
	clock := &chatGPTWebImageTaskTestClock{now: time.Date(2026, 8, 26, 2, 0, 0, 0, time.UTC)}
	registry := newChatGPTWebImageTaskRegistry(clock.Now)
	for index := 0; index < chatGPTWebImageCanceledTombstoneMax+10; index++ {
		_, handle := registry.begin(t.Context(), "auth")
		if _, found := registry.cancelTask(handle.id); !found {
			t.Fatalf("cancel task %d was not found", index)
		}
		handle.finish()
	}
	if got := len(registry.canceled); got != chatGPTWebImageCanceledTombstoneMax {
		t.Fatalf("canceled tombstones = %d, want %d", got, chatGPTWebImageCanceledTombstoneMax)
	}
	if got := len(registry.canceledOrder); got != chatGPTWebImageCanceledTombstoneMax {
		t.Fatalf("canceled order = %d, want %d", got, chatGPTWebImageCanceledTombstoneMax)
	}
}

func TestChatGPTWebImageTaskPollContextRetainsOnlyTaskHandle(t *testing.T) {
	registry := newChatGPTWebImageTaskRegistry(time.Now)
	ctx, handle := registry.begin(t.Context(), "auth-c")
	pollCtx := newChatGPTWebImageTaskPollContext(ctx)
	if chatGPTWebImageTaskHandleFromContext(pollCtx) != handle {
		t.Fatal("poll context lost the image task handle")
	}
	handle.finish()
}
