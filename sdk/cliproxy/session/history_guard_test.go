package session

import (
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"testing"
	"time"
)

func TestHistoryGuardInvalidationIsConditionalAndScoped(t *testing.T) {
	m := NewHistoryMatcher(time.Hour)
	h := FingerprintHistory(translator.FormatCodex, []byte(`{"input":[{"role":"user","content":"question"},{"role":"assistant","content":"answer"}]}`))
	m.Bind("current", h, "a")
	m.Bind("other", h, "a")
	clear := m.CaptureInvalidation("current", &h, "a")
	m.Bind("current", h, "replacement")
	clear()
	if match, ok := m.Match("current", h); !ok || match.AuthID != "replacement" {
		t.Fatal("late invalidation removed replacement")
	}
	if _, ok := m.Match("other", h); !ok {
		t.Fatal("cross-scope invalidation")
	}
	clear = m.CaptureInvalidation("other", &h, "a")
	clear()
	if _, ok := m.Match("other", h); ok {
		t.Fatal("current history preference survived")
	}
}
