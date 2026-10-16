package proxypool

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

func TestExitFirstFailureRetriesBeforeLongHealthInterval(t *testing.T) {
	for _, bound := range []bool{false, true} {
		t.Run(map[bool]string{true: "bound", false: "temporary"}[bound], func(t *testing.T) {
			cfg := proxyPoolTestConfig("3334")
			interval := 100 * 86400
			cfg.ProxyPools[0].CheckIntervalSeconds = interval
			cfg.ProxyHealthCheck.TemporaryRouteCheckIntervalSeconds = &interval
			m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
			t.Cleanup(m.Stop)
			var clock, calls atomic.Int64
			var failing atomic.Bool
			clock.Store(time.Now().Unix())
			m.now = func() time.Time { return time.Unix(clock.Load(), 0) }
			m.check = func(context.Context, string) TraceResult {
				calls.Add(1)
				if failing.Load() {
					return TraceResult{Error: "fixture", CheckedAt: m.now()}
				}
				return TraceResult{OK: true, IP: "8.8.8.8", CheckedAt: m.now()}
			}
			r := proxyregion.Route{ProxyURL: "http://fixture:80", Scope: proxyregion.ChatGPTScope}
			if bound {
				a := proxyPoolTestAuth("enabled")
				v, e := m.Resolve(t.Context(), a)
				if e != nil {
					t.Fatal(e)
				}
				r.ProxyURL = v.URL
				r.BindingID = v.BindingID
				r.OwnerID = a.ID
			}
			failing.Store(true)
			v := m.ensureRegionObservation(t.Context(), r)
			if v.IP != "" {
				t.Fatal("failed probe returned IP")
			}
			before := calls.Load()
			failing.Store(false)
			clock.Add(10)
			m.ensureRegionObservation(t.Context(), r)
			if calls.Load() != before {
				t.Fatal("ignored failure backoff")
			}
			clock.Add(30)
			if v = m.ensureRegionObservation(t.Context(), r); v.IP == "" {
				t.Fatalf("long health interval suppressed retry %+v", v)
			}
		})
	}
}
func TestFailedCandidatesHaveNoLongRetryAfter(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyPools[0].CheckIntervalSeconds = 100 * 86400
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	m.check = func(context.Context, string) TraceResult { return TraceResult{Error: "fixture"} }
	_, e := m.Resolve(t.Context(), proxyPoolTestAuth("a"))
	v, ok := e.(*UnavailableError)
	if !ok || v.RetryAfter() != nil {
		t.Fatalf("unexpected retry %v", e)
	}
	if len(m.SortedBindings()) != 0 {
		t.Fatal("failed candidate was bound")
	}
}
func TestManualRegionSourceIsolation(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyURL = "http://global:80"
	cfg.ProxyRegion = config.ProxyRegionConfig{Timezone: "Asia/Tokyo", Language: "ja-JP"}
	cfg.ProxyPools[0].Region = config.ProxyRegionConfig{Timezone: "America/New_York", Language: "en-US"}
	cfg.ProxyPools[0].Entries[0].Region = config.ProxyRegionConfig{Timezone: "Europe/Paris"}
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	m.check = successfulTrace
	a := proxyPoolTestAuth("a")
	m.SetAuthSource(staticAuthSource{a.ID: a})
	v, e := m.Resolve(t.Context(), a)
	if e != nil {
		t.Fatal(e)
	}
	p, _ := m.manualRegion(proxyregion.Route{ProxyURL: v.URL, BindingID: v.BindingID, OwnerID: a.ID})
	if p.Timezone != "Europe/Paris" || p.Language != "en-US" {
		t.Fatal(p)
	}
	p, _ = m.manualRegion(proxyregion.Route{ProxyURL: "http://global:80"})
	if p.Timezone != "Asia/Tokyo" {
		t.Fatal(p)
	}
	p, _ = m.manualRegion(proxyregion.Route{ProxyURL: "http://environment:80"})
	if p.Timezone != "" {
		t.Fatal("global policy leaked to another route")
	}
}

func TestReadyRoutesRegisteredWithoutAllocatingOtherCredentials(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.Codex.TimeContext.Enabled = true
	cfg.ProxyIP = &config.ProxyIPConfig{ColdCacheMode: "wait"}
	m := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	t.Cleanup(m.Stop)
	m.check = successfulTrace
	a, b := proxyPoolTestAuth("used"), proxyPoolTestAuth("unused")
	m.SetAuthSource(staticAuthSource{a.ID: a, b.ID: b})
	if _, err := m.Resolve(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	m.ReconcileRegions()
	_, routes, _ := m.regions.Status(0, 10)
	if routes != 1 || len(m.SortedBindings()) != 1 {
		t.Fatalf("routes=%d bindings=%d", routes, len(m.SortedBindings()))
	}
}
