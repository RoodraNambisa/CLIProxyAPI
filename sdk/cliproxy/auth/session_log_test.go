package auth

import (
	"strings"
	"testing"
)

func TestSessionDiagnosticDoesNotExposeShortCacheKeyAliases(t *testing.T) {
	if truncateSessionID("") != "" {
		t.Fatal("empty diagnostic identity changed")
	}
	for _, identity := range []string{"header:a", "header:short-key", "codex:019e417b-e000-7000-8000-000000000001"} {
		first, second := truncateSessionID(identity), truncateSessionID(identity)
		if first != second || !strings.HasPrefix(first, "sha256:") || strings.Contains(first, identity) {
			t.Fatal("session diagnostic exposed an original identity or lost stable correlation")
		}
	}
	if truncateSessionID("header:a") == truncateSessionID("header:b") {
		t.Fatal("different sessions collapsed in diagnostics")
	}
}
