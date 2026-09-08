package live

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func hangupHandlerContext(t *testing.T, id, caller string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, w := liveHandlerRequest(t.Context(), caller, "")
	c.Set("accessProvider", "fixture-access")
	c.Params = gin.Params{{Key: "call_id", Value: id}}
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/realtime/calls/"+id+"/hangup", strings.NewReader(`{}`))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("Authorization", "Bearer client-fixture")
	return c, w
}

func TestLiveHangupRemainsAvailableWhenDisabledAndSidebandClaimed(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	cfg.Routing.PerAuthRequestLimit, cfg.Routing.PerAuthRequestWindowMinutes = 1, 1
	var attempts atomic.Int32
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			if r.Header.Get("Authorization") != "Bearer fixture-oauth" || r.Header.Get("Chatgpt-Account-Id") != "fixture-account" {
				t.Error("hangup lost the original credential")
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_end")
		_, _ = w.Write([]byte("v=0"))
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	c, w := hangupHandlerContext(t, "call_end", "fixture-caller")
	owner, _ := requestCallOwner(c)
	call, busy := h.calls.claim("call_end", owner)
	if call == nil || busy {
		t.Fatal("call could not be claimed")
	}
	h.UpdateConfig(&config.Config{})
	h.HandleHangup(c)
	if w.Code != 204 || attempts.Load() != 2 || call.lease.Context().Err() == nil || h.calls.find(call.id, owner) != nil {
		t.Fatal("disabled cleanup reselected, failed to hang up or retained resources")
	}
}

func TestLiveHangupCannotActOnAnotherOwnerOrCredentialReplacement(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	var attempts atomic.Int32
	h, manager, _, authID := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.Header().Set("Location", "/v1/realtime/calls/call_owned")
		_, _ = w.Write([]byte("v=0"))
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	for _, tc := range []struct {
		caller, id string
		status     int
	}{{"other-caller", "call_owned", 404}, {"", "call_owned", 401}, {"fixture-caller", "../invalid", 400}, {"fixture-caller", "missing", 404}} {
		c, w := hangupHandlerContext(t, tc.id, tc.caller)
		h.HandleHangup(c)
		if w.Code != tc.status {
			t.Fatalf("ownership status=%d, want %d", w.Code, tc.status)
		}
	}
	a, _ := manager.GetByID(authID)
	a.Metadata["access_token"] = "replacement-fixture"
	if _, errUpdate := manager.Update(t.Context(), a); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	c, w := hangupHandlerContext(t, "call_owned", "fixture-caller")
	h.HandleHangup(c)
	if w.Code != 404 || attempts.Load() != 1 {
		t.Fatal("hangup reused a replacement credential or foreign ownership")
	}
}

func TestLiveHangupFailureKeepsCallForExplicitRetry(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	var hangups atomic.Int32
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			if hangups.Add(1) == 1 {
				w.Header().Set("Retry-After", "9")
				w.WriteHeader(429)
				_, _ = w.Write([]byte(`{"error":{"code":"fixture_rejected"}}`))
				return
			}
			w.WriteHeader(204)
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_retry")
		_, _ = w.Write([]byte("v=0"))
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	for _, status := range []int{429, 204} {
		c, w := hangupHandlerContext(t, "call_retry", "fixture-caller")
		h.HandleHangup(c)
		if w.Code != status {
			t.Fatalf("hangup status=%d, want %d", w.Code, status)
		}
		if status == 429 && (w.Header().Get("Retry-After") != "9" || !strings.Contains(w.Body.String(), "fixture_rejected") || hangups.Load() != 1) {
			t.Fatal("rejected cleanup lost its error or was retried")
		}
	}
}

func TestLiveHangupRequestCancellationDoesNotEndStoredSession(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/hangup") {
			_, _ = io.Copy(io.Discard, r.Body)
			close(started)
			<-r.Context().Done()
			close(cancelled)
			return
		}
		w.Header().Set("Location", "/v1/realtime/calls/call_cancel")
		_, _ = w.Write([]byte("v=0"))
	})
	if w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader("v=0")); w.Code != 200 {
		t.Fatal("creation failed")
	}
	c, w := hangupHandlerContext(t, "call_cancel", "fixture-caller")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c.Request = c.Request.WithContext(ctx)
	done := make(chan struct{})
	go func() { h.HandleHangup(c); close(done) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("hangup did not begin")
	}
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("hangup upstream was not cancelled")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hangup handler did not finish")
	}
	owner, _ := requestCallOwner(c)
	if w.Code != 499 || h.calls.find("call_cancel", owner) == nil {
		t.Fatal("cancelled cleanup invalidated the stored session")
	}
}
