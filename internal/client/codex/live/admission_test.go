package live

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLiveAdmissionDefaultsAndDisableGeneration(t *testing.T) {
	g := &admissionGate{}
	if _, err := g.begin(t.Context()); !errors.Is(err, errLiveDisabled) {
		t.Fatal("zero gate accepted live work")
	}
	g.update(true)
	pending, err := g.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer pending.close()
	g.update(false)
	if !errors.Is(context.Cause(pending.ctx), errLiveDisabled) {
		t.Fatal("disable did not cancel setup")
	}
	g.update(true)
	if err := pending.commit(); !errors.Is(err, errLiveDisabled) {
		t.Fatal("re-enabling admitted a stale setup")
	}
	fresh, err := g.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.close()
	g.update(true)
	if err := fresh.commit(); err != nil {
		t.Fatal("unchanged enabled state invalidated setup")
	}
	g.update(false)
	if fresh.ctx.Err() != nil {
		t.Fatal("disable canceled committed work")
	}
	if err := fresh.commit(); err != nil {
		t.Fatal("committed admission was not idempotent")
	}
}

func TestLiveAdmissionCancellationAndCleanup(t *testing.T) {
	g := &admissionGate{}
	g.update(true)
	ctx, cancel := context.WithCancel(t.Context())
	a, err := g.begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := a.commit(); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled caller committed")
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		g.mu.Lock()
		remaining := len(g.pending)
		g.mu.Unlock()
		if remaining == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("caller cancellation retained setup bookkeeping")
		}
		time.Sleep(time.Millisecond)
	}
	a.close()
	a.close()
	if _, err := g.begin(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("already canceled request was admitted")
	}
}

func TestLiveAdmissionConcurrentDisableAndCommit(t *testing.T) {
	for range 10 {
		g := &admissionGate{}
		g.update(true)
		attempts := make([]*liveAdmission, 64)
		for i := range attempts {
			a, err := g.begin(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			attempts[i] = a
		}
		results := make([]error, len(attempts))
		start := make(chan struct{})
		var wait sync.WaitGroup
		for i, a := range attempts {
			wait.Add(1)
			go func() { defer wait.Done(); <-start; results[i] = a.commit() }()
		}
		wait.Add(1)
		go func() { defer wait.Done(); <-start; g.update(false) }()
		close(start)
		wait.Wait()
		for i, a := range attempts {
			if results[i] == nil && a.ctx.Err() != nil {
				t.Fatal("disable canceled a committed setup")
			}
			if results[i] != nil && (!errors.Is(results[i], errLiveDisabled) || !errors.Is(context.Cause(a.ctx), errLiveDisabled)) {
				t.Fatal("uncommitted setup bypassed disabling")
			}
			a.close()
		}
		g.mu.Lock()
		remaining := len(g.pending)
		g.mu.Unlock()
		if remaining != 0 {
			t.Fatal("completed race retained pending work")
		}
	}
}

func TestLiveAdmissionCancellationDoesNotHoldGateLock(t *testing.T) {
	g := &admissionGate{}
	g.update(true)
	a, err := g.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	done := make(chan struct{})
	stop := context.AfterFunc(a.ctx, func() { g.mu.Lock(); g.mu.Unlock(); close(done) })
	defer stop()
	g.update(false)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("cleanup blocked on the admission lock")
	}
}

func TestLiveAdmissionCloseCannotRaceIntoCommit(t *testing.T) {
	g := &admissionGate{}
	g.update(true)
	a, err := g.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	cancel := a.cancel
	a.cancel = func(cause error) { close(entered); <-release; cancel(cause) }
	go func() { a.close(); close(closed) }()
	<-entered
	err = a.commit()
	close(release)
	<-closed
	if !errors.Is(err, context.Canceled) {
		t.Fatal("closed setup committed before cancellation completed")
	}
}
