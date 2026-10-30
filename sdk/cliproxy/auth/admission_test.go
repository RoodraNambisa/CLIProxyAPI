package auth

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/admission"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type admissionHoldingExecutor struct {
	schedulerTestExecutor
	entered chan string
	release chan struct{}
}

func (e *admissionHoldingExecutor) Execute(ctx context.Context, a *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	e.entered <- a.ID
	select {
	case <-e.release:
		return core.Response{}, nil
	case <-ctx.Done():
		return core.Response{}, ctx.Err()
	}
}
func (e *admissionHoldingExecutor) ExecuteStream(ctx context.Context, a *Auth, _ core.Request, _ core.Options) (*core.StreamResult, error) {
	chunks := make(chan core.StreamChunk, 1)
	chunks <- core.StreamChunk{Payload: []byte("data: ready\n\n")}
	e.entered <- a.ID
	go func() {
		defer close(chunks)
		select {
		case <-e.release:
		case <-ctx.Done():
		}
	}()
	return &core.StreamResult{Chunks: chunks}, nil
}
func admissionTestManager(t *testing.T, queue bool) (*Manager, *admissionHoldingExecutor) {
	t.Helper()
	m := NewManager(nil, &RoundRobinSelector{}, nil)
	e := &admissionHoldingExecutor{entered: make(chan string, 8), release: make(chan struct{})}
	m.RegisterExecutor(e)
	m.SetConfig(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"one", "two"}}, Routing: config.RoutingConfig{PerAuthConcurrency: 1, Queue: config.AdmissionQueue{Enabled: queue}}})
	_, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "a", Index: "a-index", Provider: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.admission.Close)
	return m, e
}
func admissionKeyContext(ctx context.Context, key string) context.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Set("apiKey", key)
	return context.WithValue(ctx, "gin", c)
}
func awaitAdmission(t *testing.T, condition func() bool) {
	t.Helper()
	end := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(end) {
			t.Fatal("admission state did not settle")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestAdmissionStreamHoldsCredentialAndLogicalLeaseUntilEnd(t *testing.T) {
	m, e := admissionTestManager(t, false)
	result, err := m.ExecuteStream(admissionKeyContext(t.Context(), "one"), []string{"test"}, core.Request{}, core.Options{})
	if err != nil {
		t.Fatal(err)
	}
	<-e.entered
	if a := m.admission.Overview(time.Now(), 60, false, false).Active; a != 1 {
		t.Fatalf("active=%d", a)
	}
	if _, err = m.Execute(admissionKeyContext(t.Context(), "two"), []string{"test"}, core.Request{}, core.Options{}); !isAuthRequestLimitedError(err) {
		t.Fatalf("second request bypassed concurrency: %v", err)
	}
	if s := m.admission.Traffic.Lookup("credential", "a").Snapshot(time.Now()); s.Active != 1 {
		t.Fatalf("early release: %+v", s)
	}
	close(e.release)
	for range result.Chunks {
	}
	awaitAdmission(t, func() bool { return m.admission.Overview(time.Now(), 60, false, false).Active == 0 })
	if s := m.admission.Traffic.Lookup("credential", "a").Snapshot(time.Now()); s.Active != 0 || s.RPM60 != 1 {
		t.Fatalf("leak/double count: %+v", s)
	}
}
func TestAdmissionSharedCredentialQueueCancellationAndGrant(t *testing.T) {
	m, e := admissionTestManager(t, true)
	first := make(chan error, 1)
	go func() {
		_, err := m.Execute(admissionKeyContext(t.Context(), "one"), []string{"test"}, core.Request{}, core.Options{})
		first <- err
	}()
	<-e.entered
	ctx, cancel := context.WithCancel(admissionKeyContext(t.Context(), "two"))
	second := make(chan error, 1)
	go func() { _, err := m.Execute(ctx, []string{"test"}, core.Request{}, core.Options{}); second <- err }()
	awaitAdmission(t, func() bool { n, _ := m.admission.Queue.Counts(""); return n == 1 })
	if active := m.admission.Overview(time.Now(), 60, false, false).Active; active != 2 {
		t.Fatalf("waiting request lost Key lease: %d", active)
	}
	cancel()
	if err := <-second; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel=%v", err)
	}
	close(e.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	awaitAdmission(t, func() bool { return m.admission.Overview(time.Now(), 60, false, false).Active == 0 })
	n, bytes := m.admission.Queue.Counts("")
	if n != 0 || bytes != 0 {
		t.Fatalf("queue leak: %d %d", n, bytes)
	}
}
func TestAdmissionReservationHotReloadAndLateRelease(t *testing.T) {
	limiter := newAuthRequestWindowLimiter()
	limiter.traffic = admission.NewController()
	defer limiter.traffic.Close()
	now := time.Now()
	p := authRequestLimitPolicy{concurrency: 2, limit: 3, windowMinutes: 1, requestSlot: &core.AuthRequestSlot{}}
	a, ok, _ := limiter.reserveAt("a", p, now)
	if !ok {
		t.Fatal("first")
	}
	a.Commit()
	b, ok, _ := limiter.reserveAt("a", p, now)
	if !ok {
		t.Fatal("second")
	}
	b.Commit()
	limiter.reset(2)
	p.generation = 2
	p.concurrency = 1
	p.limit = 2
	if _, ok, _ := limiter.reserveAt("a", p, now); ok {
		t.Fatal("shrink cancelled/forgot in-flight")
	}
	a.Finish()
	a.Finish()
	if _, ok, _ := limiter.reserveAt("a", p, now); ok {
		t.Fatal("active limit ignored")
	}
	b.Finish()
	if _, ok, block := limiter.reserveAt("a", p, now); ok || block.limit != 2 {
		t.Fatal("hot reload forgot RPM")
	}
	p.limit = 3
	r, ok, _ := limiter.reserveAt("a", p, now)
	if !ok {
		t.Fatal("raised limit unavailable")
	}
	r.Release()
}
func TestAdmissionTargetBypassOnlySkipsRPM(t *testing.T) {
	m, _ := admissionTestManager(t, false)
	a := &Auth{ID: "a", Provider: "test"}
	opts := core.Options{AuthRequestSlot: &core.AuthRequestSlot{}}
	if err := m.reserveTargetConcurrency(a, opts); err != nil {
		t.Fatal(err)
	}
	if err := m.reserveTargetConcurrency(a, core.Options{AuthRequestSlot: &core.AuthRequestSlot{}}); !isAuthRequestLimitedError(err) {
		t.Fatalf("target bypassed concurrency: %v", err)
	}
	opts.AuthRequestSlot.FinishReservation()()
}
func TestAdmissionConcurrentReleaseIsIdempotent(t *testing.T) {
	m, _ := admissionTestManager(t, false)
	slot := &core.AuthRequestSlot{}
	if err := m.reserveTargetConcurrency(&Auth{ID: "a", Provider: "test"}, core.Options{AuthRequestSlot: slot}); err != nil {
		t.Fatal(err)
	}
	slot.Commit()
	finish := slot.TransferReservation()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(finish)
	}
	wg.Wait()
	s := m.admission.Traffic.Lookup("credential", "a").Snapshot(time.Now())
	if s.Active != 0 || s.CompletedRPM60 != 1 {
		t.Fatalf("bad final state %+v", s)
	}
}

func TestAdmissionRoutingQueueWakesWhenAnotherPriorityBecomesAvailable(t *testing.T) {
	m, e := admissionTestManager(t, true)
	one := 1
	m.SetConfig(&config.Config{SDKConfig: config.SDKConfig{APIKeys: []string{"one", "two"}}, Routing: config.RoutingConfig{PerAuthConcurrency: 1, Queue: config.AdmissionQueue{Enabled: true}, PriorityOverrides: []config.RoutingPriorityOverride{{Priority: 0, PerAuthRequestLimit: &one}}}})
	a, _ := m.GetByID("a")
	a.Attributes = map[string]string{"priority": "5"}
	if _, err := m.Update(WithSkipPersist(t.Context()), a); err != nil {
		t.Fatal(err)
	}
	b, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "low", Provider: "test"})
	if err != nil {
		t.Fatal(err)
	}
	policy := m.routingAuthRequestLimitPolicyForAuth(b)
	if ok, _ := m.authRequestLimiter().tryAcquireAt(b.ID, policy, time.Now()); !ok {
		t.Fatal("seed lower-priority RPM")
	}
	first := make(chan error, 1)
	go func() {
		_, err := m.Execute(admissionKeyContext(t.Context(), "one"), []string{"test"}, core.Request{}, core.Options{})
		first <- err
	}()
	<-e.entered
	second := make(chan error, 1)
	go func() {
		_, err := m.Execute(admissionKeyContext(t.Context(), "two"), []string{"test"}, core.Request{}, core.Options{})
		second <- err
	}()
	awaitAdmission(t, func() bool { n, _ := m.admission.Queue.Counts("route:0"); return n == 1 })
	close(e.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queue slept until unrelated RPM reset")
	}
}
