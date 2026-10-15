package chatgptweb

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestBrowserRegionHotTTLResizeAndClose(t *testing.T) {
	r, clock := regionTestResolver(t)
	r.mu.Lock()
	for _, route := range []string{"old", "new"} {
		entry := browserRegionEntry{ObservedIP: "8.8.8.8", ObservedAt: r.now(), Geo: browserRegionGeo{IP: "8.8.8.8", Timezone: "America/New_York", Country: "US", At: r.now()}, RefreshAt: r.now().Add(6 * time.Hour)}
		r.routes.put(browserRegionRouteKey(route), entry, 4096)
		r.ips.put(route, entry, 4096)
	}
	r.mu.Unlock()
	clock.Add(600)
	seconds, size := 60, 1
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{AutoTimezone: true, CacheTTLSeconds: &seconds, RouteCacheSize: &size, IPCacheSize: &size}})
	if s := r.Preview("new", DefaultPersona()); s.CacheState != "stale" || s.RefreshState != "idle" {
		t.Fatalf("TTL update did not remain lazy: %+v", s)
	}
	r.mu.Lock()
	routeCount, ipCount := len(r.routes.items), len(r.ips.items)
	r.mu.Unlock()
	if routeCount != 1 || ipCount != 1 || r.Preview("old", DefaultPersona()).CacheState != "missing" {
		t.Fatal("hot shrink did not retain only the most recent entry")
	}
	r.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) != 0 || len(r.queue) != 0 || len(r.routes.items) != 0 || len(r.ips.items) != 0 {
		t.Fatal("shutdown retained provider-owned work or caches")
	}
}
func TestBrowserRegionLateLookupCannotPublishToNewGeneration(t *testing.T) {
	r, _ := regionTestResolver(t)
	entered, release := make(chan struct{}), make(chan struct{})
	r.probe = func(_ context.Context, _, target string) ([]byte, int, string, error) {
		if target == browserRegionTraceURL {
			return []byte("ip=8.8.8.8"), 200, "", nil
		}
		close(entered)
		<-release
		return []byte(regionFixture), 200, "", nil
	}
	r.mu.Lock()
	job := &browserRegionJob{proxy: "old-route", generation: r.generation, settings: r.policy.settings}
	r.mu.Unlock()
	done := make(chan string, 1)
	go func() { _, _, reason, _ := r.lookup(t.Context(), job); done <- reason }()
	<-entered
	r.UpdateConfig(config.ChatGPTWebConfig{BrowserRegion: config.ChatGPTWebBrowserRegionConfig{LookupURL: "https://example.test/"}})
	close(release)
	if reason := <-done; reason != "configuration_changed" {
		t.Fatalf("late result accepted: %s", reason)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ips.items) != 0 {
		t.Fatal("old lookup contaminated the new cache")
	}
}
