package chatgptweb

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

const regionFixture = `{"success":true,"ip":"8.8.8.8","country_code":"US","region_code":"NY","timezone":{"id":"America/New_York"}}`

func regionTestResolver(t *testing.T) (*BrowserRegionResolver, *atomic.Int64) {
	t.Helper()
	r := NewBrowserRegionResolver(t.Context())
	t.Cleanup(r.Close)
	clock := &atomic.Int64{}
	clock.Store(time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC).Unix())
	r.now = func() time.Time { return time.Unix(clock.Load(), 0) }
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: true, AutoLanguage: true}})
	return r, clock
}
func awaitRegionIdle(t *testing.T, r *BrowserRegionResolver, route string) BrowserRegionSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s := r.Preview(route, DefaultPersona())
		if s.RefreshState == "idle" {
			return s
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("region task did not finish")
	return BrowserRegionSnapshot{}
}
func TestBrowserRegionLazySharedRoutes(t *testing.T) {
	r, _ := regionTestResolver(t)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var calls atomic.Int64
	r.probe = func(ctx context.Context, proxy, target string) ([]byte, int, string, error) {
		calls.Add(1)
		if proxy != "socks5://fixture" {
			t.Error("resolved route changed")
		}
		if target == browserRegionTraceURL {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return nil, 0, "", ctx.Err()
			}
			return []byte("ip=8.8.8.8\ncolo=YUL"), 200, "", nil
		}
		return []byte(regionFixture), 200, "", nil
	}
	if s := r.Preview("socks5://fixture", DefaultPersona()); s.RefreshState != "idle" || calls.Load() != 0 {
		t.Fatal("preview performed work")
	}
	first := r.Resolve("socks5://fixture", DefaultPersona())
	if first.Timezone != "UTC" {
		t.Fatal("cold lookup did not return immediately with fallback")
	}
	<-entered
	for range 100000 {
		r.Resolve("socks5://fixture", DefaultPersona())
	}
	if calls.Load() != 1 {
		t.Fatalf("duplicate task count: %d", calls.Load())
	}
	close(release)
	s := awaitRegionIdle(t, r, "socks5://fixture")
	if s.Timezone != "America/New_York" || s.OffsetMinutes != 240 || s.Language != "en-US" || s.CacheState != "fresh" || calls.Load() != 2 {
		t.Fatalf("bad region: %+v calls=%d", s, calls.Load())
	}
}
func TestBrowserRegionStaleFailureAndChangedExit(t *testing.T) {
	r, clock := regionTestResolver(t)
	var mode atomic.Int64
	r.probe = func(_ context.Context, _, target string) ([]byte, int, string, error) {
		if target == browserRegionTraceURL {
			ip := "8.8.8.8"
			if mode.Load() == 2 {
				ip = "1.1.1.1"
			}
			return []byte("ip=" + ip), 200, "", nil
		}
		if mode.Load() > 0 {
			return nil, 429, "7200", nil
		}
		return []byte(regionFixture), 200, "", nil
	}
	r.Resolve("direct", DefaultPersona())
	awaitRegionIdle(t, r, "direct")
	clock.Add(int64((25 * time.Hour).Seconds()))
	mode.Store(1)
	s := r.Resolve("direct", DefaultPersona())
	if s.CacheState != "stale" || s.Timezone != "America/New_York" {
		t.Fatalf("stale value lost: %+v", s)
	}
	s = awaitRegionIdle(t, r, "direct")
	if s.CacheState != "stale" || s.Reason != "lookup_rate_limited" || s.RetryAfterSeconds < 7200 {
		t.Fatalf("failed refresh replaced last success: %+v", s)
	}
	if r.Refresh("direct") != "refresh_cooldown" {
		t.Fatal("manual refresh ignored Retry-After")
	}
	clock.Add(7201)
	mode.Store(2)
	r.Resolve("direct", DefaultPersona())
	s = awaitRegionIdle(t, r, "direct")
	if s.Timezone != "UTC" || s.CacheState != "missing" {
		t.Fatalf("old exit was reused after IP change: %+v", s)
	}
}
func TestBrowserRegionIndependentFlagsAndDST(t *testing.T) {
	r, clock := regionTestResolver(t)
	r.mu.Lock()
	r.routes.put(browserRegionRouteKey("direct"), browserRegionEntry{ObservedIP: "8.8.8.8", Geo: browserRegionGeo{IP: "8.8.8.8", Country: "JP", Timezone: "America/New_York", At: r.now()}, RefreshAt: r.now().Add(365 * 24 * time.Hour)}, 4096)
	r.mu.Unlock()
	for _, tc := range []struct {
		zone, lang         bool
		wantZone, wantLang string
		offset             int
	}{{false, false, "Asia/Tokyo", "fr-FR", 123}, {true, false, "America/New_York", "fr-FR", 240}, {false, true, "Asia/Tokyo", "ja-JP", 123}, {true, true, "America/New_York", "ja-JP", 240}} {
		offset := 123
		cfg := config.ChatGPTWebConfig{Timezone: "Asia/Tokyo", TimezoneOffsetMinutes: &offset, BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: tc.zone, AutoLanguage: tc.lang, Language: "fr-FR"}}
		r.UpdateConfig(cfg)
		s := r.Preview("direct", DefaultPersona())
		if s.Timezone != tc.wantZone || s.Language != tc.wantLang || s.OffsetMinutes != tc.offset {
			t.Fatalf("flags not independent: %+v", s)
		}
	}
	clock.Store(time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC).Unix())
	if r.Preview("direct", DefaultPersona()).OffsetMinutes != 300 {
		t.Fatal("cached timezone froze DST offset")
	}
	if regionDefaultLanguage("XX") != "" {
		t.Fatal("unknown region invented language")
	}
}
func TestBrowserRegionBoundsGenerationAndShutdown(t *testing.T) {
	r, _ := regionTestResolver(t)
	entered := make(chan struct{}, 2)
	r.probe = func(ctx context.Context, _, _ string) ([]byte, int, string, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, 0, "", ctx.Err()
	}
	r.Resolve("route-a", DefaultPersona())
	r.Resolve("route-b", DefaultPersona())
	<-entered
	<-entered
	for i := range 10000 {
		r.Resolve(fmt.Sprintf("route-%d", i), DefaultPersona())
	}
	r.mu.Lock()
	pending, queued := len(r.pending), len(r.queue)
	r.mu.Unlock()
	if pending > 258 || queued > 256 {
		t.Fatalf("unbounded work: pending=%d queued=%d", pending, queued)
	}
	size := 1
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{LookupURL: "https://example.test/", RouteCacheSize: &size, IPCacheSize: &size}})
	r.Close()
	if r.Refresh("route-c") != "closed" {
		t.Fatal("accepted after shutdown")
	}
	if s := r.Preview("route-a", DefaultPersona()); s.Country != "" {
		t.Fatal("old generation contaminated new state")
	}
}
func TestBrowserRegionIPSingleflightAndRootMode(t *testing.T) {
	r, _ := regionTestResolver(t)
	var lookups atomic.Int64
	r.probe = func(_ context.Context, _, target string) ([]byte, int, string, error) {
		if target == browserRegionTraceURL {
			return []byte("ip=8.8.8.8"), 200, "", nil
		}
		lookups.Add(1)
		time.Sleep(20 * time.Millisecond)
		return []byte(regionFixture), 200, "", nil
	}
	r.Resolve("route-a", DefaultPersona())
	r.Resolve("route-b", DefaultPersona())
	awaitRegionIdle(t, r, "route-a")
	awaitRegionIdle(t, r, "route-b")
	if lookups.Load() != 1 {
		t.Fatalf("same IP was looked up %d times", lookups.Load())
	}
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: true, LookupURL: "https://ipwho.is/"}})
	r.probe = func(_ context.Context, _, target string) ([]byte, int, string, error) {
		if target == browserRegionTraceURL {
			t.Error("root mode sent extra trace")
		}
		return []byte(regionFixture), 200, "", nil
	}
	r.Resolve("route-a", DefaultPersona())
	s := awaitRegionIdle(t, r, "route-a")
	if s.ObservationSource != "lookup_exit" {
		t.Fatalf("misrepresented observation: %+v", s)
	}
}
func TestBrowserRegionResponseValidationAndAnonymousProbe(t *testing.T) {
	for _, payload := range []string{`{`, strings.Replace(regionFixture, "America/New_York", "Invalid/Zone", 1), strings.Replace(regionFixture, "8.8.8.8", "127.0.0.1", 1), strings.Replace(regionFixture, "8.8.8.8", "1.1.1.1", 1)} {
		if _, reason := regionResponse([]byte(payload), "8.8.8.8", "official_trace", time.Now()); reason == "" {
			t.Fatal("invalid response accepted")
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"Authorization", "Cookie", "Oai-Device-Id", "Oai-Language", "Proxy-Authorization"} {
			if r.Header.Get(name) != "" {
				t.Errorf("metadata leaked %s", name)
			}
		}
		_, _ = w.Write([]byte(regionFixture))
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, status, _, err := probeBrowserRegion(ctx, "direct", server.URL); err != nil || status != 200 {
		t.Fatalf("anonymous probe failed: %v status=%d", err, status)
	}
}
