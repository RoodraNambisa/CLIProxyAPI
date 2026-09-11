package configaccess

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestAPIKeyUsageAuthenticationAndLifecycle(t *testing.T) {
	var usage apiKeyUsage
	keys := []string{"used-fixture", "unused-fixture"}
	p := newProvider("", keys, nil)
	p.lastUsed = usage.reconcile(keys)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	request.Header.Set("Authorization", "Bearer invalid-fixture")
	if _, err := p.Authenticate(t.Context(), request); err == nil || len(usage.snapshot(keys)) != 0 {
		t.Fatal("failed authentication recorded key use")
	}
	request.Header.Set("Authorization", "Bearer used-fixture")
	if _, err := p.Authenticate(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	before := usage.snapshot(keys)
	if len(before) != 1 || before[keys[0]] == "" {
		t.Fatal("successful authentication did not record only the accepted key")
	}
	if _, err := time.Parse(time.RFC3339Nano, before[keys[0]]); err != nil {
		t.Fatal(err)
	}
	next := usage.reconcile(keys)
	if next[keys[0]] != p.lastUsed[keys[0]] || usage.snapshot(keys)[keys[0]] != before[keys[0]] {
		t.Fatal("hot reload lost the last-use time")
	}
	usage.reconcile(keys[1:])
	noteAPIKeyUse(p.lastUsed[keys[0]], time.Now().Add(time.Hour))
	usage.reconcile(keys)
	if len(usage.snapshot(keys)) != 0 {
		t.Fatal("retired provider restored a deleted or re-added key's old use time")
	}
	if fresh := (&apiKeyUsage{}).snapshot(keys); len(fresh) != 0 {
		t.Fatal("new runtime retained usage")
	}
}

func TestAPIKeyUsageConcurrentUpdatesNeverMoveBackwards(t *testing.T) {
	var usage apiKeyUsage
	keys := []string{"concurrent-fixture"}
	counter := usage.reconcile(keys)[keys[0]]
	var workers sync.WaitGroup
	for i := 1; i <= 100; i++ {
		workers.Go(func() {
			noteAPIKeyUse(counter, time.Unix(int64(i), 0))
			usage.snapshot(keys)
			usage.reconcile(keys)
		})
	}
	workers.Wait()
	if counter.Load() != time.Unix(100, 0).UnixNano() {
		t.Fatal("an older authentication overwrote a newer timestamp")
	}
}
