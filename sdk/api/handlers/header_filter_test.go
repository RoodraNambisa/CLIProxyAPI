package handlers

import (
	"net/http"
	"testing"
)

func TestFilterUpstreamHeaders_RemovesConnectionScopedHeaders(t *testing.T) {
	src := http.Header{}
	src.Add("Connection", "keep-alive, x-hop-a, x-hop-b")
	src.Add("Connection", "x-hop-c")
	src.Set("Keep-Alive", "timeout=5")
	src.Set("X-Hop-A", "a")
	src.Set("X-Hop-B", "b")
	src.Set("X-Hop-C", "c")
	src.Set("X-Request-Id", "req-1")
	src.Set("Set-Cookie", "session=secret")

	filtered := FilterUpstreamHeaders(src)
	if filtered == nil {
		t.Fatalf("expected filtered headers, got nil")
	}

	requestID := filtered.Get("X-Request-Id")
	if requestID != "req-1" {
		t.Fatalf("expected X-Request-Id to be preserved, got %q", requestID)
	}

	blockedHeaderKeys := []string{
		"Connection",
		"Keep-Alive",
		"X-Hop-A",
		"X-Hop-B",
		"X-Hop-C",
		"Set-Cookie",
	}
	for _, key := range blockedHeaderKeys {
		value := filtered.Get(key)
		if value != "" {
			t.Fatalf("expected %s to be removed, got %q", key, value)
		}
	}
}

func TestFilterUpstreamHeaders_ReturnsNilWhenAllHeadersBlocked(t *testing.T) {
	src := http.Header{}
	src.Add("Connection", "x-hop-a")
	src.Set("X-Hop-A", "a")
	src.Set("Set-Cookie", "session=secret")

	filtered := FilterUpstreamHeaders(src)
	if filtered != nil {
		t.Fatalf("expected nil when all headers are filtered, got %#v", filtered)
	}
}

func TestClientUpstreamHeadersAlwaysPreservesCodexTurnState(t *testing.T) {
	src := http.Header{
		codexTurnStateResponseHeader: []string{"state-1"},
		"X-Request-Id":               []string{"request-1"},
	}

	withoutPassthrough := ClientUpstreamHeaders(src, false)
	if got := withoutPassthrough.Get(codexTurnStateResponseHeader); got != "state-1" {
		t.Fatalf("turn state = %q, want state-1", got)
	}
	if got := withoutPassthrough.Get("X-Request-Id"); got != "" {
		t.Fatalf("request ID = %q, want omitted", got)
	}

	withPassthrough := ClientUpstreamHeaders(src, true)
	if got := withPassthrough.Get(codexTurnStateResponseHeader); got != "state-1" {
		t.Fatalf("turn state with passthrough = %q, want state-1", got)
	}
	if got := withPassthrough.Get("X-Request-Id"); got != "request-1" {
		t.Fatalf("request ID with passthrough = %q, want request-1", got)
	}
}
