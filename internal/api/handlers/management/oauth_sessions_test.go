package management

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestOAuthSessionStoreCancelPreservesTerminalStatus(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	store.Register("pending-state", "xai")

	if !store.Cancel("pending-state") {
		t.Fatal("Cancel() = false, want true")
	}
	if store.IsPending("pending-state", "xai") {
		t.Fatal("cancelled session remained pending")
	}
	session, ok := store.Get("pending-state")
	if !ok {
		t.Fatal("cancelled session was not retained")
	}
	if session.Status != oauthSessionCancelledStatus {
		t.Fatalf("cancelled status = %q, want %q", session.Status, oauthSessionCancelledStatus)
	}
	if store.Cancel("pending-state") {
		t.Fatal("second Cancel() = true, want false")
	}
}

func TestOAuthSessionBeginSaveExcludesCancellation(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	replaceOAuthSessionStoreForTest(t, store)
	store.Register("xai-save", "xai")

	if errBegin := beginOAuthSessionSave("xai-save", "xai"); errBegin != nil {
		t.Fatalf("begin save error = %v", errBegin)
	}
	if CancelOAuthSession("xai-save") {
		t.Fatal("CancelOAuthSession() = true after save began")
	}
	if errBegin := beginOAuthSessionSave("xai-save", "xai"); !errors.Is(errBegin, errOAuthSessionNotPending) {
		t.Fatalf("second begin error = %v, want %v", errBegin, errOAuthSessionNotPending)
	}

	store.Register("xai-cancel", "xai")
	if !CancelOAuthSession("xai-cancel") {
		t.Fatal("CancelOAuthSession() = false, want true")
	}
	if errBegin := beginOAuthSessionSave("xai-cancel", "xai"); !errors.Is(errBegin, errOAuthSessionNotPending) {
		t.Fatalf("cancelled begin error = %v, want %v", errBegin, errOAuthSessionNotPending)
	}
}

func TestOAuthSessionCompletionPreservesSiblingTerminalState(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	store.Register("cancelled-state", "codex")
	store.Register("successful-state", "codex")
	if !store.Cancel("cancelled-state") {
		t.Fatal("Cancel() = false, want true")
	}
	store.Complete("successful-state")

	session, ok := store.Get("cancelled-state")
	if !ok || session.Status != oauthSessionCancelledStatus {
		t.Fatalf("cancelled sibling = %#v, %t", session, ok)
	}
}

func TestOAuthSessionErrorDoesNotOverwriteTerminalState(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	store.Register("cancelled-state", "xai")
	if !store.Cancel("cancelled-state") {
		t.Fatal("Cancel() = false, want true")
	}
	store.SetError("cancelled-state", "late polling error")

	session, ok := store.Get("cancelled-state")
	if !ok || session.Status != oauthSessionCancelledStatus {
		t.Fatalf("cancelled session = %#v, %t", session, ok)
	}
}

func TestCancelAuthSessionHandler(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	replaceOAuthSessionStoreForTest(t, store)
	store.Register("device-state", "xai")

	handler := &Handler{}
	router := gin.New()
	router.DELETE("/oauth-session", handler.CancelAuthSession)

	assertCancelResponse(t, router, "/oauth-session", http.StatusBadRequest, false)
	assertCancelResponse(t, router, "/oauth-session?state=bad/state", http.StatusBadRequest, false)
	assertCancelResponse(t, router, "/oauth-session?state=device-state", http.StatusOK, true)
	assertCancelResponse(t, router, "/oauth-session?state=device-state", http.StatusOK, false)
}

func TestGetAuthStatusReportsCancelledSession(t *testing.T) {
	store := newOAuthSessionStore(time.Minute)
	replaceOAuthSessionStoreForTest(t, store)
	store.Register("cancelled-state", "xai")
	if !store.Cancel("cancelled-state") {
		t.Fatal("Cancel() = false, want true")
	}

	handler := &Handler{}
	router := gin.New()
	router.GET("/auth-status", handler.GetAuthStatus)
	req := httptest.NewRequest(http.MethodGet, "/auth-status?state=cancelled-state", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", w.Code, http.StatusOK)
	}
	var response struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if errDecode := json.Unmarshal(w.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode status response: %v", errDecode)
	}
	if response.Status != "error" || response.Error != oauthSessionCancelledStatus {
		t.Fatalf("status response = %#v", response)
	}
}

func TestNormalizeOAuthProviderSupportsXAI(t *testing.T) {
	for _, provider := range []string{"xai", "x-ai", "x.ai", "grok"} {
		normalized, err := NormalizeOAuthProvider(provider)
		if err != nil || normalized != "xai" {
			t.Fatalf("NormalizeOAuthProvider(%q) = %q, %v", provider, normalized, err)
		}
	}
}

func assertCancelResponse(t *testing.T, router http.Handler, path string, wantStatus int, wantCancelled bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != wantStatus {
		t.Fatalf("DELETE %s status = %d, want %d; body=%s", path, w.Code, wantStatus, w.Body.String())
	}
	if wantStatus != http.StatusOK {
		return
	}
	var response struct {
		Cancelled bool `json:"cancelled"`
	}
	if errDecode := json.Unmarshal(w.Body.Bytes(), &response); errDecode != nil {
		t.Fatalf("decode cancel response: %v", errDecode)
	}
	if response.Cancelled != wantCancelled {
		t.Fatalf("DELETE %s cancelled = %t, want %t", path, response.Cancelled, wantCancelled)
	}
}

func replaceOAuthSessionStoreForTest(t *testing.T, store *oauthSessionStore) {
	t.Helper()
	original := oauthSessions
	oauthSessions = store
	t.Cleanup(func() {
		oauthSessions = original
	})
}
