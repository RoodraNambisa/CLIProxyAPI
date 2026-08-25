package executor

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

type imagePollBreakerClock struct {
	now time.Time
}

func (clock *imagePollBreakerClock) Now() time.Time {
	return clock.now
}

func (clock *imagePollBreakerClock) Advance(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}

func TestImagePollStallErrorUsesLocalAdmissionContract(t *testing.T) {
	err := &ImagePollStallError{}
	if err.StatusCode() != http.StatusServiceUnavailable || !err.SkipAuthResult() || err.RetryOtherAuth() {
		t.Fatalf("error contract = status:%d skip:%t retry:%t", err.StatusCode(), err.SkipAuthResult(), err.RetryOtherAuth())
	}
	if got := err.Headers().Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After = %q, want 30", got)
	}
	var payload map[string]any
	if errDecode := json.Unmarshal([]byte(err.Error()), &payload); errDecode != nil {
		t.Fatalf("json.Unmarshal() error = %v", errDecode)
	}
	errorPayload, _ := payload["error"].(map[string]any)
	if errorPayload["code"] != "chatgpt_web_image_poll_stalled" || errorPayload["failure_stage"] != "admission" {
		t.Fatalf("error payload = %#v", errorPayload)
	}
	if !IsImagePollStallError(err) {
		t.Fatal("IsImagePollStallError() = false")
	}
}

func TestImagePollStallBreakerRequiresContinuousFullSaturation(t *testing.T) {
	clock := &imagePollBreakerClock{now: time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)}
	breaker := newImagePollStallBreaker(clock.Now)
	breaker.configure(true, 120*time.Second)
	full := ImageExecutionAdmissionSnapshot{Limit: 2, Active: 2}
	partial := ImageExecutionAdmissionSnapshot{Limit: 2, Active: 1}

	breaker.observeAdmission(full)
	clock.Advance(119 * time.Second)
	if breaker.reject(full) {
		t.Fatal("breaker opened before stall interval")
	}
	breaker.observeAdmission(partial)
	clock.Advance(120 * time.Second)
	if breaker.reject(full) {
		t.Fatal("breaker retained saturation time across a non-full interval")
	}
	clock.Advance(120 * time.Second)
	if !breaker.reject(full) {
		t.Fatal("breaker did not open after continuous full-slot stagnation")
	}
	snapshot := breaker.snapshot(full)
	if !snapshot.Open || snapshot.OpenedAt == nil || snapshot.FullSince == nil || snapshot.Rejected != 1 {
		t.Fatalf("open snapshot = %#v", snapshot)
	}
}

func TestImagePollStallBreakerCompletionAndDrainRecovery(t *testing.T) {
	clock := &imagePollBreakerClock{now: time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)}
	breaker := newImagePollStallBreaker(clock.Now)
	breaker.configure(true, time.Minute)
	full := ImageExecutionAdmissionSnapshot{Limit: 1, Active: 1}

	breaker.observeAdmission(full)
	clock.Advance(time.Minute)
	if !breaker.reject(full) {
		t.Fatal("breaker did not open")
	}
	breaker.observeCompletion(true, full)
	if snapshot := breaker.snapshot(full); !snapshot.Open || snapshot.CanceledCompletions != 1 {
		t.Fatalf("canceled completion unexpectedly recovered breaker: %#v", snapshot)
	}
	breaker.observeCompletion(false, full)
	if snapshot := breaker.snapshot(full); snapshot.Open || snapshot.TransportCompletions != 1 || snapshot.LastCompletionAt == nil {
		t.Fatalf("transport completion did not recover breaker: %#v", snapshot)
	}

	clock.Advance(time.Minute)
	if !breaker.reject(full) {
		t.Fatal("breaker did not reopen after later full-slot stagnation")
	}
	breaker.observeCompletion(true, ImageExecutionAdmissionSnapshot{Limit: 1, Active: 0})
	if snapshot := breaker.snapshot(ImageExecutionAdmissionSnapshot{Limit: 1, Active: 0}); snapshot.Open || snapshot.FullSince != nil {
		t.Fatalf("drained canceled polls did not recover breaker: %#v", snapshot)
	}
}

func TestImagePollStallBreakerHotConfiguration(t *testing.T) {
	clock := &imagePollBreakerClock{now: time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)}
	breaker := newImagePollStallBreaker(clock.Now)
	full := ImageExecutionAdmissionSnapshot{Limit: 1, Active: 1}

	breaker.configure(false, time.Minute)
	breaker.observeAdmission(full)
	clock.Advance(time.Hour)
	if breaker.reject(full) {
		t.Fatal("disabled breaker rejected admission")
	}
	breaker.configure(true, 30*time.Second)
	if breaker.reject(full) {
		t.Fatal("enabling breaker reused disabled saturation history")
	}
	clock.Advance(30 * time.Second)
	if !breaker.reject(full) {
		t.Fatal("enabled breaker did not use hot-updated interval")
	}
	breaker.configure(false, 30*time.Second)
	if snapshot := breaker.snapshot(full); snapshot.Enabled || snapshot.Open || snapshot.FullSince != nil {
		t.Fatalf("disabled snapshot = %#v", snapshot)
	}
}
