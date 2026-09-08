package live

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func grantedSecret(t *testing.T, h *Handler) (string, ClientSecretAuthorization) {
	t.Helper()
	token, a := preparedSecret(t, callOwner{1}, h.clientSecrets.now())
	if errPut := h.clientSecrets.put(token, a); errPut != nil {
		t.Fatal(errPut)
	}
	grant, errAuth := h.clientSecrets.authenticate(token)
	if errAuth != nil {
		t.Fatal(errAuth)
	}
	return token, grant
}

func TestLiveClientSecretAdmissionUsesIssuerScopeWithoutStoringToken(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	token, grant := grantedSecret(t, h)
	c, _ := liveHandlerRequest(t.Context(), "", "")
	if !h.ApplyClientSecretAuthorization(c, grant) {
		t.Fatal("valid grant was not installed")
	}
	if c.GetString("apiKey") == token || c.GetString("apiKey") != grant.principal {
		t.Fatal("actual token entered caller identity")
	}
	owner, ok := requestCallOwner(c)
	if !ok || owner != grant.owner {
		t.Fatal("call ownership lost original issuer scope")
	}
	r := h.begin(c)
	if r == nil {
		t.Fatal("scoped credential failed provider access")
	}
	defer r.finish()
	if errActive := r.active(); errActive != nil {
		t.Fatal(errActive)
	}
}

func TestLiveClientSecretAdmissionRejectsRevokedOrExpiredGrantAtCommit(t *testing.T) {
	for _, expired := range []bool{false, true} {
		cfg := &config.Config{}
		cfg.Codex.LiveEnabled = true
		h := NewHandler(cfg, nil)
		defer h.Close()
		now := time.Now()
		h.clientSecrets.now = func() time.Time { return now }
		token, grant := grantedSecret(t, h)
		c, w := liveHandlerRequest(t.Context(), "", "")
		if !h.ApplyClientSecretAuthorization(c, grant) {
			t.Fatal("grant failed")
		}
		r := h.begin(c)
		if r == nil {
			t.Fatal("request did not enter admission")
		}
		defer r.finish()
		if expired {
			now = now.Add(clientSecretDefaultLifetime)
		} else {
			h.clientSecrets.remove(token, grant.principal)
		}
		published := false
		errCommit := r.commitWith(func() error { published = true; return nil })
		if !errors.Is(errCommit, errInvalidClientSecret) || published {
			t.Fatal("expired or revoked snapshot published a new connection")
		}
		r.fail(c, errCommit, nil, 502, "fixture", "server_error", "fixture", nil, nil)
		if w.Code != http.StatusUnauthorized {
			t.Fatal("invalid local grant was reported as an upstream error")
		}
	}
}

func TestLiveClientSecretExpirationDoesNotCancelEstablishedRequest(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	token, grant := grantedSecret(t, h)
	c, _ := liveHandlerRequest(t.Context(), "", "")
	if !h.ApplyClientSecretAuthorization(c, grant) {
		t.Fatal("grant failed")
	}
	r := h.begin(c)
	if r == nil {
		t.Fatal("request failed")
	}
	defer r.finish()
	if errCommit := r.commit(); errCommit != nil {
		t.Fatal(errCommit)
	}
	h.clientSecrets.remove(token, grant.principal)
	if r.ctx.Err() != nil {
		t.Fatal("grant revocation cancelled an established connection")
	}
	other, w := liveHandlerRequest(t.Context(), "", "")
	if h.ApplyClientSecretAuthorization(other, grant) || w.Code != http.StatusUnauthorized {
		t.Fatal("revoked grant authenticated another request")
	}
}

func TestLiveClientSecretAdmissionCannotReplaceStandardCleanupAuthentication(t *testing.T) {
	cfg := &config.Config{}
	cfg.Codex.LiveEnabled = true
	h := NewHandler(cfg, nil)
	defer h.Close()
	_, grant := grantedSecret(t, h)
	c, w := liveHandlerRequest(t.Context(), "", "")
	if !h.ApplyClientSecretAuthorization(c, grant) {
		t.Fatal("grant failed")
	}
	if request := h.beginRequest(c, false); request != nil {
		request.finish()
		t.Fatal("temporary grant replaced standard cleanup authentication")
	}
	if w.Code != http.StatusUnauthorized {
		t.Fatal("wrong temporary cleanup rejection")
	}
}
