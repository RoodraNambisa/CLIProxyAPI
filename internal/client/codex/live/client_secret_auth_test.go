package live

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestLiveClientSecretAuthenticationSupportsBearerAndBrowserProtocols(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	token, a := preparedSecret(t, callOwner{1}, time.Now())
	if errPut := h.clientSecrets.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	for _, browser := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/v1/realtime", nil)
		if browser {
			request.Header.Set("Connection", "Upgrade")
			request.Header.Set("Upgrade", "websocket")
			request.Header.Set("Sec-WebSocket-Protocol", "realtime, openai-insecure-api-key."+token)
		} else {
			request.Header.Set("Authorization", "Bearer "+token)
		}
		got, handled, errAuth := h.AuthenticateClientSecret(request)
		if errAuth != nil || !handled || got.principal != a.principal || !h.clientSecrets.valid(got) {
			t.Fatal("valid local credential was not authenticated")
		}
		h.clientSecrets.remove(token, a.principal)
		if h.clientSecrets.valid(got) {
			t.Fatal("revoked authorization snapshot remained valid")
		}
		if errPut := h.clientSecrets.put(token, a); errPut != nil {
			t.Fatal(errPut)
		}
	}
	h.UpdateConfig(&config.Config{})
	request := httptest.NewRequest(http.MethodGet, "/v1/realtime", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	_, handled, errAuth := h.AuthenticateClientSecret(request)
	c, w := liveHandlerRequest(t.Context(), "", "")
	h.WriteClientSecretError(c, errAuth)
	if !handled || w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) || !c.IsAborted() {
		t.Fatal("old credential bypassed disabled admission")
	}
}

func TestLiveClientSecretAuthenticationRejectsAmbiguityAndNonWebsocketProtocols(t *testing.T) {
	token, _ := preparedSecret(t, callOwner{1}, time.Now())
	other, _ := preparedSecret(t, callOwner{1}, time.Now())
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		request := httptest.NewRequest(method, "/v1/realtime", nil)
		request.Header.Set("Sec-WebSocket-Protocol", "realtime, openai-insecure-api-key."+token)
		if _, handled, errToken := realtimeClientSecretToken(request); handled || errToken != nil {
			t.Fatal("ordinary HTTP request accepted WebSocket authentication")
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/v1/realtime", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Sec-WebSocket-Protocol", "realtime, openai-insecure-api-key."+other)
	if _, handled, errToken := realtimeClientSecretToken(request); !handled || errToken == nil {
		t.Fatal("conflicting credentials were accepted")
	}
	request.Header.Del("Sec-WebSocket-Protocol")
	request.Header.Add("Authorization", "Bearer "+other)
	if _, handled, errToken := realtimeClientSecretToken(request); !handled || errToken == nil {
		t.Fatal("conflicting bearer credentials were accepted")
	}
	request.Header.Set("Authorization", "Bearer configured-standard-key")
	if _, handled, errToken := realtimeClientSecretToken(request); handled || errToken != nil {
		t.Fatal("ordinary API-key authentication was intercepted")
	}
	request.Header.Set("Sec-WebSocket-Protocol", "realtime, openai-insecure-api-key."+token)
	if _, handled, errToken := realtimeClientSecretToken(request); !handled || errToken == nil {
		t.Fatal("a rejected standard credential was replaced by a conflicting browser credential")
	}
	request.Header.Del("Authorization")
	request.Header.Set("Sec-WebSocket-Protocol", "openai-insecure-api-key., openai-insecure-api-key."+token)
	if _, handled, errToken := realtimeClientSecretToken(request); !handled || errToken == nil {
		t.Fatal("an empty credential prefix was hidden by a later credential")
	}
}

func TestLiveClientSecretAuthorizationSnapshotExpiresAndCannotChangeScope(t *testing.T) {
	s := newClientSecretStore()
	defer s.close()
	now := time.Now()
	s.now = func() time.Time { return now }
	token, a := preparedSecret(t, callOwner{1}, now)
	if errPut := s.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	got, errAuth := s.authenticate(token)
	if errAuth != nil {
		t.Fatal(errAuth)
	}
	changed := got
	changed.model = "another-model"
	if s.valid(changed) {
		t.Fatal("modified authorization scope remained valid")
	}
	now = now.Add(clientSecretDefaultLifetime)
	if s.valid(got) {
		t.Fatal("authorization snapshot outlived its credential")
	}
}

func TestLiveClientSecretAuthenticationRejectsCancelledRequests(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	token, a := preparedSecret(t, callOwner{1}, time.Now())
	if errPut := h.clientSecrets.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	request := httptest.NewRequest(http.MethodGet, "/v1/realtime", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+token)
	_, handled, errAuth := h.AuthenticateClientSecret(request)
	if !handled || errAuth != context.Canceled {
		t.Fatal("cancelled authentication proceeded")
	}
	c, w := liveHandlerRequest(t.Context(), "", "")
	h.WriteClientSecretError(c, context.DeadlineExceeded)
	if w.Code != http.StatusRequestTimeout {
		t.Fatal("deadline authentication error changed status")
	}
}
