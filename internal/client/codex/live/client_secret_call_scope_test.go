package live

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestLiveClientSecretCallUsesBoundSessionAndModel(t *testing.T) {
	for _, kind := range []string{"application/json", "application/sdp"} {
		t.Run(kind, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Codex.LiveEnabled = true
			observed := make(chan []byte, 1)
			h, _, _, _ := newLiveCallsFixture(t, cfg, func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				observed <- body
				writeSidebandCall(w)
			})
			request, errParse := parseClientSecretRequest([]byte(`{"session":{"model":"gpt-realtime","instructions":"scoped","large":90071992547409931234,"audio":{"output":{"voice":"marin"}}}}`), false)
			if errParse != nil {
				t.Fatal(errParse)
			}
			issuer, _ := secretHandlerContext(t, `{}`)
			owner, _ := requestCallOwner(issuer)
			token, a, errPrepare := prepareClientSecret(h.secretRandom, time.Now(), request.session, request.model, owner, request.lifetime)
			if errPrepare != nil {
				t.Fatal(errPrepare)
			}
			if errPut := h.clientSecrets.put(token, a); errPut != nil {
				t.Fatal(errPut)
			}
			grant, errAuth := h.clientSecrets.authenticate(token)
			if errAuth != nil {
				t.Fatal(errAuth)
			}
			body := `{"sdp":"v=0","model":"another-model","session":{"model":"another-model","instructions":"unscoped"},"other":90071992547409931234}`
			if kind == "application/sdp" {
				body = "v=0"
			}
			c, w := liveHandlerRequest(t.Context(), "", "")
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/realtime/calls", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", kind)
			if !h.ApplyClientSecretAuthorization(c, grant) {
				t.Fatal("grant failed")
			}
			h.HandleCall(c)
			if w.Code != 200 {
				t.Fatal("bound WebRTC call failed")
			}
			wire := <-observed
			if bytes.Contains(wire, []byte("another-model")) || bytes.Contains(wire, []byte("unscoped")) || !bytes.Contains(wire, []byte(`"model":"gpt-live-1-codex"`)) || !bytes.Contains(wire, []byte("scoped")) || !bytes.Contains(wire, []byte("90071992547409931234")) {
				t.Fatal("client escaped the credential session or model")
			}
			call := h.calls.find("call_sideband", owner)
			if call == nil || call.secretPrincipal != grant.principal {
				t.Fatal("created call lost grant and issuer binding")
			}
		})
	}
}

func TestLiveClientSecretCallScopeDoesNotShareCallsWithSiblingCredentials(t *testing.T) {
	s := newCallStore()
	defer s.close()
	owner := callOwner{1}
	call, _ := newCallFixture(t, "call_scoped", owner)
	call.secretPrincipal = "sess_first"
	if errPut := s.put(call); errPut != nil {
		t.Fatal(errPut)
	}
	if got, busy := s.claimForGrant(call.id, owner, "sess_other"); got != nil || busy {
		t.Fatal("sibling credential claimed an unrelated call")
	}
	if got, busy := s.claimForGrant(call.id, owner, "sess_first"); got != call || busy {
		t.Fatal("original credential could not join its call")
	}
	if got, busy := s.claimForGrant(call.id, owner, "sess_other"); got != nil || busy {
		t.Fatal("sibling credential learned unrelated busy state")
	}
	s.release(call)
	if got, busy := s.claim(call.id, owner); got != call || busy {
		t.Fatal("issuer standard key lost control of its call")
	}
}

func TestLiveClientSecretPayloadKeepsGrantAndBusinessJSONIndependent(t *testing.T) {
	grant := ClientSecretAuthorization{model: "bound", session: json.RawMessage(`{"model":"bound","tools":[{"parameters":{"model":"business"}}]}`)}
	original := bytes.Clone(grant.session)
	payload, errPayload := prepareCallPayload([]byte(`{"sdp":"v=0","model":"other","extra":{"model":"business-extra"}}`), "application/json")
	if errPayload != nil {
		t.Fatal(errPayload)
	}
	bound, errBound := payload.withClientSecret(grant)
	if errBound != nil {
		t.Fatal(errBound)
	}
	if !bytes.Equal(grant.session, original) || !bytes.Contains(bound.body, []byte("business-extra")) || !bytes.Contains(bound.body, []byte(`"model":"business"`)) {
		t.Fatal("grant application changed unrelated data or original grant")
	}
}
