package live

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pion/webrtc/v4"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type mediaDirectProxyResolver struct{}

func (mediaDirectProxyResolver) Resolve(context.Context, *auth.Auth) (auth.ResolvedProxy, error) {
	return auth.ResolvedProxy{URL: "direct", BindingID: "fixture-media-pool"}, nil
}
func (mediaDirectProxyResolver) ReportFailure(_ context.Context, _ *auth.Auth, err error) error {
	return err
}

func TestLiveMediaUsesCredentialAndResolvedProxyBeforeGlobalSnapshot(t *testing.T) {
	for _, mode := range []string{"global-direct", "global-invalid", "credential-direct", "resolved-direct"} {
		t.Run(mode, func(t *testing.T) {
			offer := mediaClientOffer(t)
			upstream := localMediaPeer(t, localMediaAPI(t))
			localMediaTrack(t, upstream)
			upstream.OnDataChannel(func(*webrtc.DataChannel) {})
			var calls atomic.Int32
			h, manager, _, id := newLiveCallsFixture(t, mediaTestHandlerConfig(), func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				answer, ok := mediaFixtureAnswer(t, upstream, r)
				if !ok {
					w.WriteHeader(500)
					return
				}
				w.Header().Set("Location", "/v1/realtime/calls/call_proxy_scope")
				_, _ = w.Write([]byte(answer))
			})
			cfg := mediaTestHandlerConfig()
			cfg.ProxyURL = "unsupported://fixture.invalid"
			if mode == "global-direct" {
				cfg.ProxyURL = "direct"
			}
			h.UpdateConfig(cfg)
			useLocalHandlerMedia(t, h)
			if mode == "credential-direct" {
				a, _ := manager.GetByID(id)
				a.ProxyURL = "direct"
				if _, err := manager.Update(t.Context(), a); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "resolved-direct" {
				manager.SetProxyResolver(mediaDirectProxyResolver{})
			}
			w := callHandlerRequest(t, h, "/v1/live", "application/sdp", strings.NewReader(offer))
			if mode == "global-invalid" {
				if w.Code != 503 || !strings.Contains(w.Body.String(), "realtime_media_unavailable") || calls.Load() != 0 {
					t.Fatal("invalid global media proxy fell back to direct")
				}
			} else if w.Code != 200 || calls.Load() != 1 {
				t.Fatalf("media proxy precedence failed: status=%d calls=%d", w.Code, calls.Load())
			}
			h.Close()
			assertMediaHandlerReleased(t, h)
		})
	}
}

func TestLiveMediaTemporaryCredentialBindsAliasSessionAndIssuerAcrossDisable(t *testing.T) {
	for _, kind := range []string{"application/json", "application/sdp"} {
		t.Run(kind, func(t *testing.T) {
			offer := mediaClientOffer(t)
			upstream := localMediaPeer(t, localMediaAPI(t))
			localMediaTrack(t, upstream)
			upstream.OnDataChannel(func(*webrtc.DataChannel) {})
			var calls atomic.Int32
			h, manager, _, id := newLiveCallsFixture(t, mediaTestHandlerConfig(), func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if strings.HasSuffix(r.URL.Path, "/hangup") {
					w.WriteHeader(204)
					return
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(400)
					return
				}
				if !bytes.Contains(body["session"], []byte(`"model":"gpt-live-1-codex"`)) || !bytes.Contains(body["session"], []byte(`"instructions":"bound-session"`)) || !bytes.Contains(body["session"], []byte("90071992547409931234")) || bytes.Contains(body["session"], []byte("unbound")) {
					t.Error("media setup escaped its temporary session or alias")
				}
				encoded, _ := json.Marshal(body)
				r.Body = io.NopCloser(bytes.NewReader(encoded))
				answer, ok := mediaFixtureAnswer(t, upstream, r)
				if !ok {
					w.WriteHeader(500)
					return
				}
				w.Header().Set("Location", "/v1/realtime/calls/call_media_grant")
				_, _ = w.Write([]byte(answer))
			})
			useLocalHandlerMedia(t, h)
			a, _ := manager.GetByID(id)
			a.Prefix = "team"
			if _, err := manager.Update(t.Context(), a); err != nil {
				t.Fatal(err)
			}
			manager.SetOAuthModelAlias(map[string][]config.OAuthModelAlias{"codex": {{Name: registry.CodexLiveModelID, Alias: "voice"}}})
			registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "team/voice", UpstreamID: registry.CodexLiveModelID, Type: registry.CodexRealtimeModelType}})
			issuer, issued := secretHandlerContext(t, `{"session":{"model":"team/voice","instructions":"bound-session","future":90071992547409931234}}`)
			h.CreateClientSecret(issuer)
			if issued.Code != 200 || calls.Load() != 0 {
				t.Fatal("local credential issuance contacted upstream")
			}
			var secret struct {
				Value string `json:"value"`
			}
			if err := json.Unmarshal(issued.Body.Bytes(), &secret); err != nil {
				t.Fatal(err)
			}
			grant, err := h.clientSecrets.authenticate(secret.Value)
			if err != nil {
				t.Fatal(err)
			}
			body := []byte(offer)
			if kind == "application/json" {
				body, _ = json.Marshal(map[string]any{"sdp": offer, "model": "unbound-model", "session": map[string]string{"instructions": "unbound", "model": "unbound-model"}})
			}
			request := func() *httptest.ResponseRecorder {
				c, w := liveHandlerRequest(t.Context(), "", "")
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/realtime/calls", bytes.NewReader(body))
				c.Request.Header.Set("Content-Type", kind)
				if !h.ApplyClientSecretAuthorization(c, grant) {
					return w
				}
				h.HandleCall(c)
				return w
			}
			if w := request(); w.Code != 200 {
				t.Fatalf("bound media call status=%d", w.Code)
			}
			owner, _ := requestCallOwner(issuer)
			call := h.calls.find("call_media_grant", owner)
			if call == nil || call.secretPrincipal != grant.principal || call.lease.Model() != registry.CodexLiveModelID {
				t.Fatal("media call lost its issuer, temporary principal or model mapping")
			}
			h.UpdateConfig(&config.Config{})
			if w := request(); w.Code != 503 || !strings.Contains(w.Body.String(), liveDisabledCode) || calls.Load() != 1 {
				t.Fatal("previously issued credential bypassed disabled media admission")
			}
			c, w := hangupHandlerContext(t, "call_media_grant", "fixture-issuer")
			h.HandleHangup(c)
			if w.Code != 204 || calls.Load() != 2 {
				t.Fatal("issuer lost authenticated cleanup after disable")
			}
			assertMediaHandlerReleased(t, h)
		})
	}
}
