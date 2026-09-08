package live

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

var callFixtureSequence atomic.Uint64

func newCallFixture(t *testing.T, id string, owner callOwner) (*liveCall, *auth.Manager) {
	t.Helper()
	m := auth.NewManager(nil, nil, nil)
	cfg := &config.Config{}
	cfg.ProxyURL = "direct"
	m.SetConfig(cfg)
	m.RegisterExecutor(executor.NewCodexAutoExecutor(cfg))
	a := &auth.Auth{ID: fmt.Sprintf("call-%s-%d", t.Name(), callFixtureSequence.Add(1)), Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}
	if _, errRegister := m.Register(t.Context(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "codex", registry.GetCodexRealtimeModels())
	t.Cleanup(func() { reg.UnregisterClient(a.ID) })
	lease, errAcquire := m.AcquireCodexLiveSession(t.Context(), t.Context(), registry.CodexLiveModelID, core.Options{})
	if errAcquire != nil {
		t.Fatal(errAcquire)
	}
	t.Cleanup(lease.Close)
	return &liveCall{id: id, owner: owner, lease: lease}, m
}

func TestLiveCallStoreOwnershipClaimAndHangupLookup(t *testing.T) {
	s := newCallStore()
	defer s.close()
	owner := callOwner{1}
	call, _ := newCallFixture(t, "call_first", owner)
	if errPut := s.put(call); errPut != nil {
		t.Fatal(errPut)
	}
	if s.find(call.id, callOwner{2}) != nil {
		t.Fatal("foreign owner found a call")
	}
	if got, busy := s.claim(call.id, owner); got != call || busy {
		t.Fatal("owner could not claim call")
	}
	if got, busy := s.claim(call.id, callOwner{2}); got != nil || busy {
		t.Fatal("foreign owner learned busy state")
	}
	if got, busy := s.claim(call.id, owner); got != nil || !busy {
		t.Fatal("second claim was allowed")
	}
	if s.find(call.id, owner) != call {
		t.Fatal("sideband claim prevented hangup lookup")
	}
	s.release(call)
	if got, busy := s.claim(call.id, owner); got != call || busy {
		t.Fatal("failed join could not be retried")
	}
}

func TestLiveCallStoreStaleExpiryAndRemovalCannotDeleteNewCall(t *testing.T) {
	s := newCallStore()
	defer s.close()
	call, _ := newCallFixture(t, "call_reused", callOwner{1})
	if errPut := s.put(call); errPut != nil {
		t.Fatal(errPut)
	}
	s.mu.Lock()
	oldEntry, oldVersion := s.entries[call.id], s.entries[call.id].expiry
	s.mu.Unlock()
	s.claim(call.id, call.owner)
	s.release(call)
	s.expire(oldEntry, oldVersion)
	if s.find(call.id, call.owner) != call {
		t.Fatal("stopped timer removed a renewed call")
	}
	replacement, _ := newCallFixture(t, call.id, call.owner)
	if errPut := s.put(replacement); errPut == nil {
		t.Fatal("duplicate ID replaced an active call")
	}
	if call.lease.Context().Err() != nil {
		t.Fatal("duplicate request closed original credential")
	}
	s.remove(call)
	if errPut := s.put(replacement); errPut != nil {
		t.Fatal(errPut)
	}
	s.remove(call)
	s.expire(oldEntry, oldVersion)
	if s.find(replacement.id, replacement.owner) != replacement {
		t.Fatal("old completion removed replacement")
	}
}

func TestLiveCallStoreExpiryRetirementAndConcurrentCloseReleaseOnce(t *testing.T) {
	for _, reason := range []string{"expiry", "retirement", "shutdown"} {
		t.Run(reason, func(t *testing.T) {
			s := newCallStore()
			defer s.close()
			call, manager := newCallFixture(t, "call_close", callOwner{1})
			var closed atomic.Int32
			done := make(chan struct{})
			call.onClose = func() { closed.Add(1); s.find(call.id, call.owner); close(done) }
			if errPut := s.put(call); errPut != nil {
				t.Fatal(errPut)
			}
			switch reason {
			case "expiry":
				s.mu.Lock()
				entry := s.entries[call.id]
				version := entry.expiry
				s.mu.Unlock()
				s.expire(entry, version)
			case "retirement":
				if errDelete := manager.Delete(t.Context(), call.lease.CloneAuth().ID); errDelete != nil {
					t.Fatal(errDelete)
				}
			case "shutdown":
				var workers sync.WaitGroup
				for i := 0; i < 10; i++ {
					workers.Go(s.close)
					workers.Go(func() { s.remove(call) })
				}
				workers.Wait()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("call resources were not released")
			}
			s.remove(call)
			if closed.Load() != 1 || s.find(call.id, call.owner) != nil {
				t.Fatal("call cleanup was not idempotent")
			}
		})
	}
}

func TestLiveCallStoreBoundsAndCancelledLeases(t *testing.T) {
	s := newCallStore()
	defer s.close()
	s.capacity = 1
	call, _ := newCallFixture(t, "call_one", callOwner{1})
	second, _ := newCallFixture(t, "call_two", callOwner{1})
	if errPut := s.put(call); errPut != nil {
		t.Fatal(errPut)
	}
	if errPut := s.put(second); errPut == nil {
		t.Fatal("call registry exceeded capacity")
	}
	if second.lease.Context().Err() != nil {
		t.Fatal("failed insertion stole caller-owned cleanup")
	}
	s.remove(call)
	second.lease.Close()
	if errPut := s.put(second); errPut == nil {
		t.Fatal("cancelled credential entered registry")
	}
	s.close()
	if errPut := s.put(call); errPut == nil {
		t.Fatal("closed registry reopened")
	}
}

func TestLiveCallStoreOwnerDigestAndLocationParsing(t *testing.T) {
	c, _ := liveHandlerRequest(t.Context(), "caller-one", "")
	c.Set("accessProvider", "provider-one")
	one, valid := requestCallOwner(c)
	if !valid || one == (callOwner{}) {
		t.Fatal("authenticated owner was unavailable")
	}
	c.Set("accessProvider", "provider-two")
	two, _ := requestCallOwner(c)
	if one == two {
		t.Fatal("same caller in another auth provider shared ownership")
	}
	c.Set("apiKey", "")
	if _, valid := requestCallOwner(c); valid {
		t.Fatal("anonymous caller got ownership")
	}
	for _, location := range []string{"call_fixture", "/v1/live/call_fixture", "https://upstream.invalid/v1/realtime/calls/call_fixture", "/v1/realtime?call_id=call_fixture"} {
		if callIDFromLocation(location) != "call_fixture" {
			t.Fatalf("valid call location rejected: %s", location)
		}
	}
	for _, location := range []string{"", "/unrelated/call_fixture", "/v1/live/..", "/v1/live/a%2Fb", "/v1/live/"} {
		if callIDFromLocation(location) != "" {
			t.Fatalf("invalid call location accepted: %s", location)
		}
	}
}

func TestLiveHandlerCloseReleasesStoredCallsOutsideConfigLock(t *testing.T) {
	h := NewHandler(&config.Config{}, nil)
	call, _ := newCallFixture(t, "call_handler", callOwner{1})
	call.onClose = func() { h.UpdateConfig(&config.Config{}) }
	if errPut := h.calls.put(call); errPut != nil {
		t.Fatal(errPut)
	}
	done := make(chan struct{})
	go func() { h.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup held configuration lock")
	}
	if !h.closed || call.lease.Context().Err() != context.Canceled {
		t.Fatal("handler did not close stored lease")
	}
}
