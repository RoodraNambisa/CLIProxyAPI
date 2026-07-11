package util

import "strings"

// IsRetiredAmpPath reports whether path belonged to the removed Amp integration.
func IsRetiredAmpPath(path string) bool {
	for _, prefix := range []string{"/auth", "/threads", "/docs", "/settings"} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	for _, prefix := range []string{
		"/api/internal",
		"/api/user",
		"/api/auth",
		"/api/meta",
		"/api/ads",
		"/api/telemetry",
		"/api/threads",
		"/api/otel",
		"/api/tab",
		"/api/provider",
	} {
		if path == prefix || strings.HasPrefix(path, prefix+"/") {
			return true
		}
	}
	return path == "/threads.rss" || path == "/news.rss"
}
