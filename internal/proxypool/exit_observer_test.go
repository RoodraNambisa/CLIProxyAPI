package proxypool

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

func TestUnifiedExitTemporaryReuseScopeAndRefresh(t *testing.T) {
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), &config.Config{})
	t.Cleanup(m.Stop)
	var calls atomic.Int64
	var clock atomic.Int64
	clock.Store(time.Now().Unix())
	m.now = func() time.Time { return time.Unix(clock.Load(), 0) }
	m.check = func(context.Context, string) TraceResult {
		calls.Add(1)
		return TraceResult{OK: true, IP: "8.8.8.8", CheckedAt: m.now()}
	}
	r := proxyregion.Route{ProxyURL: "http://shared:8080", Scope: proxyregion.ChatGPTScope}
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := m.ensureRegionObservation(t.Context(), r)
			if v.IP != "8.8.8.8" {
				t.Error("missing observation")
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("checks=%d", calls.Load())
	}
	other := r
	other.Scope = "https://other.test/cdn-cgi/trace"
	if m.regionObservation(other).IP != "" {
		t.Fatal("target scope leak")
	}
	m.ensureRegionObservation(t.Context(), other)
	if calls.Load() != 2 {
		t.Fatal("did not isolate target")
	}
	clock.Add(301)
	m.ensureRegionObservation(t.Context(), r)
	if calls.Load() != 3 {
		t.Fatal("temporary interval not applied")
	}
}
func TestUnifiedExitBoundNotEvictedAndLateSwitch(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	size := 1
	cfg.ProxyHealthCheck.TemporaryRouteCacheSize = &size
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	m.check = successfulTrace
	a := proxyPoolTestAuth("a")
	resolved, err := m.Resolve(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	r := proxyregion.Route{ProxyURL: resolved.URL, BindingID: resolved.BindingID, OwnerID: a.ID}
	if m.ensureRegionObservation(t.Context(), r).IP == "" {
		t.Fatal("bound exit missing")
	}
	for _, url := range []string{"http://one:80", "http://two:80"} {
		m.ensureRegionObservation(t.Context(), proxyregion.Route{ProxyURL: url})
	}
	if m.regionObservation(r).IP == "" {
		t.Fatal("temporary LRU evicted bound observation")
	}
	m.mu.Lock()
	old := m.bindings[a.ID]
	next := old
	next.ID = "replacement"
	m.bindings[a.ID] = next
	m.mu.Unlock()
	snapshot := m.snapshot()
	m.exits.publish(&exitTask{ctx: t.Context(), route: normalizeExitRoute(r), snapshot: snapshot, owner: a.ID}, TraceResult{OK: true, IP: "1.1.1.1", CheckedAt: time.Now()})
	if m.regionObservation(r).IP != "" {
		t.Fatal("old binding used after replacement")
	}
	r.BindingID = next.ID
	if m.regionObservation(r).IP != "" {
		t.Fatal("late old response contaminated new binding")
	}
}
func TestLegacyRouteMigrationDoesNotChangePoolInterval(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	ttl, size := 21600, 9999
	cfg.ChatGPTWeb.BrowserRegion.CacheTTLSeconds = &ttl
	cfg.ChatGPTWeb.BrowserRegion.RouteCacheSize = &size
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	snapshot := m.snapshot()
	if *snapshot.health.TemporaryRouteCheckIntervalSeconds != ttl || *snapshot.health.TemporaryRouteCacheSize != size {
		t.Fatal("legacy temporary settings lost")
	}
	if snapshot.pools["residential"].config.CheckIntervalSeconds == ttl {
		t.Fatal("legacy setting changed pool interval")
	}
	explicit := 42
	cfg.ProxyHealthCheck.TemporaryRouteCheckIntervalSeconds = &explicit
	if err := m.UpdateConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if *m.snapshot().health.TemporaryRouteCheckIntervalSeconds != explicit {
		t.Fatal("new config did not win")
	}
}

func TestUnifiedExitReusesMatchingPoolHealthWithoutSecondCheck(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyHealthCheck.Endpoints = []config.ProxyHealthCheckEndpointConfig{{Name: "chatgpt", URL: proxyregion.ChatGPTScope, Mode: "cloudflare-trace"}}
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	var calls atomic.Int64
	m.check = func(context.Context, string) TraceResult { calls.Add(1); return TraceResult{OK: true, IP: "8.8.8.8"} }
	a := proxyPoolTestAuth("shared")
	resolved, err := m.Resolve(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	before := calls.Load()
	r := proxyregion.Route{ProxyURL: resolved.URL, BindingID: resolved.BindingID, OwnerID: a.ID}
	if got := m.ensureRegionObservation(t.Context(), r); got.IP != "8.8.8.8" {
		t.Fatal(got)
	}
	if calls.Load() != before {
		t.Fatalf("duplicate exit check: before%d after%d", before, calls.Load())
	}
}
