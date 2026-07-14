package misc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func overrideAntigravityVersionURLForTest(t *testing.T, hubManifestURL string) func() {
	t.Helper()
	oldURL := antigravityHubLatestManifestURL
	antigravityHubLatestManifestURL = hubManifestURL
	return func() {
		antigravityHubLatestManifestURL = oldURL
	}
}

func overrideAntigravityVersionCacheForTest(t *testing.T, version string, expiry time.Time) func() {
	t.Helper()
	antigravityVersionMu.Lock()
	oldVersion := cachedAntigravityVersion
	oldExpiry := antigravityVersionExpiry
	cachedAntigravityVersion = version
	antigravityVersionExpiry = expiry
	antigravityVersionMu.Unlock()
	return func() {
		antigravityVersionMu.Lock()
		cachedAntigravityVersion = oldVersion
		antigravityVersionExpiry = oldExpiry
		antigravityVersionMu.Unlock()
	}
}

func TestAntigravityLatestVersionUsesCurrentHubFallback(t *testing.T) {
	restore := overrideAntigravityVersionCacheForTest(t, "", time.Time{})
	defer restore()
	if got := AntigravityLatestVersion(); got != "2.2.1" {
		t.Fatalf("AntigravityLatestVersion() = %q, want %q", got, "2.2.1")
	}
}

func TestAntigravityUserAgentUsesHubFamily(t *testing.T) {
	restore := overrideAntigravityVersionCacheForTest(t, "2.2.1", time.Now().Add(time.Hour))
	defer restore()
	want := "antigravity/hub/2.2.1 darwin/arm64"
	if got := AntigravityUserAgent(); got != want {
		t.Fatalf("AntigravityUserAgent() = %q, want %q", got, want)
	}
}

func TestAntigravityVersionFromUserAgent(t *testing.T) {
	if got := AntigravityVersionFromUserAgent("antigravity/hub/2.2.1 darwin/arm64"); got != "2.2.1" {
		t.Fatalf("hub version = %q", got)
	}
	if got := AntigravityVersionFromUserAgent("antigravity/1.23.2 windows/amd64"); got != "1.23.2" {
		t.Fatalf("legacy version = %q", got)
	}
}

func TestAntigravityControlPlaneUserAgents(t *testing.T) {
	restore := overrideAntigravityVersionCacheForTest(t, "2.2.1", time.Now().Add(time.Hour))
	defer restore()
	short := "antigravity/hub/2.2.1 darwin/arm64"
	if got := AntigravityLoadCodeAssistUserAgent(""); got != short {
		t.Fatalf("short UA = %q, want %q", got, short)
	}
	long := short + " google-api-nodejs-client/10.3.0"
	if got := AntigravityOnboardUserUserAgent(""); got != long {
		t.Fatalf("long UA = %q, want %q", got, long)
	}
}

func TestFetchAntigravityLatestVersionUsesHubManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("User-Agent"); got != "electron-builder" {
			t.Errorf("User-Agent = %q", got)
		}
		if got := r.Header.Get("Cache-Control"); got != "no-cache" {
			t.Errorf("Cache-Control = %q", got)
		}
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write([]byte("version: 2.2.1\npath: Antigravity-arm64-mac.zip\n"))
	}))
	defer server.Close()
	restore := overrideAntigravityVersionURLForTest(t, server.URL+"/hub/latest-arm64-mac.yml")
	defer restore()

	version, err := fetchAntigravityLatestVersion(context.Background())
	if err != nil {
		t.Fatalf("fetchAntigravityLatestVersion() error = %v", err)
	}
	if version != "2.2.1" {
		t.Fatalf("version = %q", version)
	}
}

func TestFetchAntigravityLatestVersionRejectsInvalidManifest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("version: latest\n"))
	}))
	defer server.Close()
	restore := overrideAntigravityVersionURLForTest(t, server.URL)
	defer restore()
	if _, err := fetchAntigravityLatestVersion(context.Background()); err == nil {
		t.Fatal("fetchAntigravityLatestVersion() error = nil, want error")
	}
}

func TestRefreshAntigravityVersionUpdatesCacheOnSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("version: 2.3.4\n"))
	}))
	defer server.Close()
	restoreURL := overrideAntigravityVersionURLForTest(t, server.URL)
	defer restoreURL()
	restoreCache := overrideAntigravityVersionCacheForTest(t, "old", time.Time{})
	defer restoreCache()

	refreshAntigravityVersion(context.Background())
	if got := AntigravityLatestVersion(); got != "2.3.4" {
		t.Fatalf("cached version = %q, want 2.3.4", got)
	}
}

func TestRefreshAntigravityVersionKeepsUnexpiredCacheOnFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	restoreURL := overrideAntigravityVersionURLForTest(t, server.URL)
	defer restoreURL()
	restoreCache := overrideAntigravityVersionCacheForTest(t, "2.3.0", time.Now().Add(time.Hour))
	defer restoreCache()

	refreshAntigravityVersion(context.Background())
	if got := AntigravityLatestVersion(); got != "2.3.0" {
		t.Fatalf("cached version = %q, want 2.3.0", got)
	}
}

func TestRefreshAntigravityVersionFallsBackWhenCacheExpired(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	restoreURL := overrideAntigravityVersionURLForTest(t, server.URL)
	defer restoreURL()
	restoreCache := overrideAntigravityVersionCacheForTest(t, "2.3.0", time.Now().Add(-time.Hour))
	defer restoreCache()

	refreshAntigravityVersion(context.Background())
	if got := AntigravityLatestVersion(); got != "2.2.1" {
		t.Fatalf("cached version = %q, want fallback 2.2.1", got)
	}
}
