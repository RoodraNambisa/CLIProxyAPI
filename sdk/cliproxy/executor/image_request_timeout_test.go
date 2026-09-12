package executor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

func TestImageRequestTimeoutBudget(t *testing.T) {
	if NewImageRequestBudget(t.Context(), time.Time{}, 0, 0) != nil {
		t.Fatal("disabled budget allocated")
	}
	b := NewImageRequestBudget(t.Context(), time.Now().Add(-2*time.Second), time.Second, 0)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	if err := b.Candidates([]string{"codex", "chatgpt-web"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Select("chatgpt-web"); err != nil {
		t.Fatalf("unlimited Web inherited Codex limit: %v", err)
	}
	if err := b.Select("codex"); !IsImageRequestTimeout(err) {
		t.Fatalf("expired start was reset: %v", err)
	}
	if !IsImageRequestTimeout(ImageRequestContextError(ctx, errors.New("socket closed"))) {
		t.Fatal("lost deadline cause")
	}
	if err := b.Select("chatgpt-web"); !IsImageRequestTimeout(err) {
		t.Fatal("timeout allowed another provider")
	}
	wrapped := fmt.Errorf("retained execution source: %w", b.Err())
	if ImageRequestContextError(ctx, wrapped) != wrapped {
		t.Fatal("timeout lost its wrapper")
	}
}

func TestImageRequestTimeoutPreflightIsolation(t *testing.T) {
	b := NewImageRequestBudget(t.Context(), time.Now().Add(-2*time.Second), 0, time.Second)
	defer b.Close()
	ctx, cancel := b.PreflightContext(t.Context(), "chatgpt-web")
	defer cancel()
	if !IsImageRequestTimeout(ImageRequestContextError(ctx, nil)) {
		t.Fatal("preflight deadline missing")
	}
	if err := b.Select("codex"); err != nil {
		t.Fatalf("Web preflight cancelled Codex: %v", err)
	}
	if b.Err() != nil {
		t.Fatal("preflight poisoned shared budget")
	}
}

func TestImageRequestTimeoutRebindAndCancellation(t *testing.T) {
	b := NewImageRequestBudget(t.Context(), time.Now(), time.Hour, time.Second)
	defer b.Close()
	first, release := b.Bind(t.Context())
	if err := b.Select("chatgpt-web"); err != nil {
		t.Fatal(err)
	}
	release()
	if first.Err() == nil || b.Err() != nil {
		t.Fatal("iteration cancellation affected logical budget")
	}
	second, releaseSecond := b.Bind(t.Context())
	defer releaseSecond()
	if second.Err() != nil {
		t.Fatal("new iteration inherited previous cancellation")
	}
	// Advance the shared start under the lock to avoid timing-dependent assertions.
	b.mu.Lock()
	b.started = time.Now().Add(-2 * time.Second)
	b.mu.Unlock()
	if err := b.Select("chatgpt-web"); !IsImageRequestTimeout(err) {
		t.Fatal("aggregation reset start")
	}
	select {
	case <-second.Done():
	case <-time.After(time.Second):
		t.Fatal("bound context not cancelled")
	}
	parent, cancel := context.WithCancel(t.Context())
	other := NewImageRequestBudget(parent, time.Now(), time.Hour, 0)
	defer other.Close()
	bound, releaseOther := other.Bind(parent)
	defer releaseOther()
	cancel()
	if IsImageRequestTimeout(ImageRequestContextError(bound, bound.Err())) {
		t.Fatal("caller cancellation relabeled as configured timeout")
	}
}

func TestImageRequestTimeoutConcurrentSelectClose(t *testing.T) {
	for range 50 {
		b := NewImageRequestBudget(t.Context(), time.Now(), time.Millisecond, 2*time.Millisecond)
		var wg sync.WaitGroup
		for _, p := range []string{"codex", "chatgpt-web", "other"} {
			wg.Go(func() { _ = b.Select(p); _ = b.Err() })
		}
		wg.Go(b.Close)
		wg.Wait()
		b.Close()
		if err := b.Select("other"); err == nil {
			t.Fatal("closed budget allowed new work")
		}
	}
}

func TestImageRequestTimeoutClosesBlockedBody(t *testing.T) {
	b := NewImageRequestBudget(t.Context(), time.Now(), time.Hour, 0)
	defer b.Close()
	ctx, cancel := b.Bind(t.Context())
	defer cancel()
	reader, writer := io.Pipe()
	defer writer.Close()
	body := GuardImageResponseBody(ctx, reader)
	defer body.Close()
	finished := make(chan error, 1)
	go func() { _, err := io.ReadAll(body); finished <- err }()
	b.mu.Lock()
	b.started = time.Now().Add(-2 * time.Hour)
	b.mu.Unlock()
	_ = b.Select("codex")
	select {
	case err := <-finished:
		if !IsImageRequestTimeout(err) {
			t.Fatalf("read error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("body read remained blocked")
	}
}
