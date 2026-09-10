package executor

import (
	"net/http"
	"testing"
)

func TestCodexQuotaObserverContextCanDisableInheritedCapture(t *testing.T) {
	base := t.Context()
	if WithCodexQuotaObserver(base, nil) != base || CodexQuotaObserverFromContext(nil) != nil {
		t.Fatal("disabled quota observation changed the default context")
	}
	calls := 0
	parent := WithCodexQuotaObserver(base, func(authID, instanceID, source string, headers http.Header) {
		calls++
		if authID != "auth" || instanceID != "instance" || source != "http" || headers.Get("Retry-After") != "1" {
			t.Fatal("observer arguments changed")
		}
	})
	CodexQuotaObserverFromContext(parent)("auth", "instance", "http", http.Header{"Retry-After": {"1"}})
	child := WithCodexQuotaObserver(parent, nil)
	if calls != 1 || CodexQuotaObserverFromContext(child) != nil || CodexQuotaObserverFromContext(parent) == nil || child.Err() != base.Err() {
		t.Fatal("disabling a child observer changed its parent or context lifetime")
	}
}
