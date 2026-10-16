package proxyregion

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const fixture = `{"success":true,"ip":"8.8.8.8","country_code":"US","region_code":"NY","timezone":{"id":"America/New_York"}}`

func testService(t *testing.T) (*Service, *atomic.Int64) {
	t.Helper()
	s := New(t.Context(), config.ProxyIPConfig{})
	t.Cleanup(s.Close)
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).Unix())
	s.now = func() time.Time { return time.Unix(clock.Load(), 0) }
	s.SetObservationSource(func(Route) Observation { return Observation{IP: "8.8.8.8", At: s.now(), Source: ChatGPTScope} }, nil)
	return s, clock
}
func idle(t *testing.T, s *Service, r Route) Snapshot {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v := s.Preview(r)
		if v.RefreshState == "idle" {
			return v
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("metadata job did not finish")
	return Snapshot{}
}
func TestSharedRouteAndIPQueries(t *testing.T) {
	s, _ := testService(t)
	var lookups atomic.Int64
	entered, release := make(chan struct{}, 1), make(chan struct{})
	s.probe = func(ctx context.Context, proxy, target string) ([]byte, int, string, error) {
		if target == ChatGPTScope {
			t.Error("metadata service attempted exit detection")
		}
		lookups.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, 0, "", ctx.Err()
		}
		return []byte(fixture), 200, "", nil
	}
	a, b := Route{ProxyURL: "http://one:8080", OwnerID: "web"}, Route{ProxyURL: "http://two:8080", OwnerID: "codex"}
	s.Preview(a)
	if lookups.Load() != 0 {
		t.Fatal("preview sent network request")
	}
	s.Resolve(a)
	<-entered
	for range 1000 {
		s.Resolve(a)
	}
	s.Resolve(b)
	close(release)
	for _, r := range []Route{a, b} {
		v := idle(t, s, r)
		if v.Timezone != "America/New_York" || v.SuggestedLanguage != "en-US" {
			t.Fatalf("bad snapshot %+v", v)
		}
	}
	if lookups.Load() != 1 {
		t.Fatalf("lookups=%d", lookups.Load())
	}
}
func TestStaleRateLimitChangedExitAndSourceIsolation(t *testing.T) {
	s, clock := testService(t)
	r := Route{ProxyURL: "socks5://proxy:1080"}
	var mode atomic.Int64
	s.SetObservationSource(func(Route) Observation {
		ip := "8.8.8.8"
		if mode.Load() == 2 {
			ip = "1.1.1.1"
		}
		return Observation{IP: ip, Source: ChatGPTScope, At: s.now()}
	}, nil)
	s.probe = func(_ context.Context, proxy, target string) ([]byte, int, string, error) {
		if mode.Load() > 0 && proxy == r.ProxyURL {
			return nil, 429, "7200", nil
		}
		return []byte(fixture), 200, "", nil
	}
	s.Resolve(r)
	idle(t, s, r)
	clock.Add(25 * 3600)
	mode.Store(1)
	if v := s.Resolve(r); v.CacheState != "stale" || v.Timezone == "" {
		t.Fatal("lost applicable old value")
	}
	v := idle(t, s, r)
	if v.Reason != "lookup_rate_limited" || s.Refresh(r) != "refresh_cooldown" {
		t.Fatalf("ignored rate limit %+v", v)
	}
	b := Route{ProxyURL: "http://other:8080"}
	s.Resolve(b)
	if v := idle(t, s, b); v.Timezone == "" {
		t.Fatal("target IP incorrectly used as rate-limit source")
	}
	mode.Store(2)
	if v := s.Preview(r); v.Timezone != "" {
		t.Fatal("old IP metadata leaked")
	}
}
func TestColdWaitCancelAndFallback(t *testing.T) {
	s, _ := testService(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	s.probe = func(ctx context.Context, _, _ string) ([]byte, int, string, error) {
		entered <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, 0, "", ctx.Err()
		}
		return []byte(fixture), 200, "", nil
	}
	r := Route{ProxyURL: "http://proxy:80"}
	if got := s.ResolveForRequest(t.Context(), r); got.Timezone != "" {
		t.Fatal("fallback blocked")
	}
	<-entered
	if err := s.UpdateConfig(config.ProxyIPConfig{ColdCacheMode: "wait"}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if v := s.ResolveForRequest(ctx, r); v.Reason != "lookup_wait_cancelled" {
		t.Fatalf("cancel %+v", v)
	}
	done := make(chan Snapshot, 1)
	go func() { done <- s.ResolveForRequest(t.Context(), r) }()
	close(release)
	if v := <-done; v.Timezone != "America/New_York" {
		t.Fatalf("cold wait did not align %+v", v)
	}
}
func TestScheduledBatchBoundAndDisabledOwner(t *testing.T) {
	s, clock := testService(t)
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
		return []byte(fixture), 200, "", nil
	}
	for i := range 20 {
		r := Route{ProxyURL: fmt.Sprintf("http://proxy%d:80", i), OwnerID: fmt.Sprint(i)}
		s.Resolve(r)
		idle(t, s, r)
	}
	clock.Add(25 * 3600)
	batch := 3
	interval := 86400
	if err := s.UpdateConfig(config.ProxyIPConfig{RefreshMode: "scheduled", RefreshBatchSize: &batch, RefreshIntervalSeconds: &interval}); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	s.probe = func(ctx context.Context, _, _ string) ([]byte, int, string, error) {
		calls.Add(1)
		<-ctx.Done()
		return nil, 0, "", ctx.Err()
	}
	s.SetOwnerValidator(func(id string) bool { return id != "0" })
	if n := s.RefreshBatch(); n > 3 {
		t.Fatalf("unbounded batch %d", n)
	}
	s.mu.Lock()
	pending := len(s.pending)
	s.mu.Unlock()
	if pending > 3 {
		t.Fatalf("pending=%d", pending)
	}
	if err := s.UpdateConfig(config.ProxyIPConfig{}); err != nil {
		t.Fatal(err)
	}
	if s.RefreshBatch() != 0 {
		t.Fatal("lazy mode scheduled refresh")
	}
}
func TestDefaultsLRUGenerationAndMissingRetryAfter(t *testing.T) {
	s, _ := testService(t)
	if s.settings.IPCacheSize != 100000 {
		t.Fatal("wrong default")
	}
	var ip atomic.Int64
	ip.Store(8)
	s.SetObservationSource(func(Route) Observation { return Observation{IP: fmt.Sprintf("8.8.8.%d", ip.Load()), At: s.now()} }, nil)
	s.probe = func(_ context.Context, _, _ string) ([]byte, int, string, error) {
		return []byte(fmt.Sprintf(`{"success":true,"ip":"8.8.8.%d","country_code":"US","timezone":{"id":"UTC"}}`, ip.Load())), 200, "", nil
	}
	r := Route{ProxyURL: "http://p:80"}
	s.Resolve(r)
	idle(t, s, r)
	ip.Store(9)
	s.Resolve(r)
	idle(t, s, r)
	size := 1
	if err := s.UpdateConfig(config.ProxyIPConfig{IPCacheSize: &size}); err != nil {
		t.Fatal(err)
	}
	_, _, ips := s.Status(0, 10)
	if ips != 1 {
		t.Fatal("LRU shrink")
	}
	if err := s.UpdateConfig(config.ProxyIPConfig{LookupURL: "https://other.test/{ip}"}); err != nil {
		t.Fatal(err)
	}
	if s.Preview(r).Timezone != "" {
		t.Fatal("query service generation reused")
	}
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) { return nil, 429, "broken", nil }
	s.Resolve(r)
	if v := idle(t, s, r); v.RetryAfterSeconds < 86400 {
		t.Fatalf("bad fallback pause %+v", v)
	}
}
func TestLateResultAndBindingForget(t *testing.T) {
	s, _ := testService(t)
	entered, release := make(chan struct{}), make(chan struct{})
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
		close(entered)
		<-release
		return []byte(fixture), 200, "", nil
	}
	r := Route{ProxyURL: "http://p:80", BindingID: "old"}
	s.Resolve(r)
	<-entered
	s.ForgetBinding("old")
	s.UpdateConfig(config.ProxyIPConfig{LookupURL: "https://other.test/{ip}"})
	close(release)
	s.Close()
	if v := s.Preview(r); v.Timezone != "" {
		t.Fatal("late result installed after close")
	}
}
func TestNoDirectAndRedirectProbe(t *testing.T) {
	var count atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1) }))
	defer server.Close()
	for _, proxy := range []string{"", "direct", "invalid"} {
		if _, _, _, err := probeRegion(t.Context(), proxy, server.URL); err == nil {
			t.Fatal("accepted direct")
		}
	}
	if count.Load() != 0 {
		t.Fatal("direct request leaked")
	}
}
func TestMetadataValidationAndRetryDates(t *testing.T) {
	for _, raw := range []string{`{}`, `{"success":true,"ip":"127.0.0.1"}`, `{"success":true,"ip":"8.8.8.8","country_code":"US","timezone":{"id":"Local"}}`} {
		if _, reason := regionResponse([]byte(raw), "8.8.8.8", "test", time.Now()); reason == "" {
			t.Fatal("invalid metadata accepted")
		}
	}
	now := time.Now().UTC().Truncate(time.Second)
	if regionRetryAfter(now.Add(time.Hour).Format(http.TimeFormat), now) != time.Hour {
		t.Fatal("date Retry-After")
	}
	if SuggestedLanguage("XX") != "" {
		t.Fatal("invented language")
	}
}
func TestConcurrentConsumersSameRoute(t *testing.T) {
	s, _ := testService(t)
	s.probe = func(context.Context, string, string) ([]byte, int, string, error) {
		return []byte(fixture), 200, "", nil
	}
	var wg sync.WaitGroup
	for i := range 200 {
		wg.Add(1)
		go func() { defer wg.Done(); s.Resolve(Route{ProxyURL: "http://shared:80", OwnerID: fmt.Sprint(i)}) }()
	}
	wg.Wait()
	idle(t, s, Route{ProxyURL: "http://shared:80"})
	_, routes, _ := s.Status(0, 50)
	if routes != 1 {
		t.Fatalf("provider-specific routes: %d", routes)
	}
}

func TestRegionTargetScopeAndProxyFailure(t *testing.T) {
	if ScopeForTarget("wss://chatgpt.com/backend-api/codex/responses") != ChatGPTScope {
		t.Fatal("WSS scope mismatch")
	}
	if ScopeForTarget("https://other.example/responses") == ChatGPTScope {
		t.Fatal("custom target reused ChatGPT observation")
	}
	var count atomic.Int64
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { count.Add(1) }))
	defer destination.Close()
	if _, _, _, err := probeRegion(t.Context(), "http://127.0.0.1:1", destination.URL); err == nil {
		t.Fatal("failed proxy accepted")
	}
	if count.Load() != 0 {
		t.Fatal("failed proxy fell back to direct")
	}
}
