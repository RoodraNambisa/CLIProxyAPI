package chatgptweb

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestServiceLoginUsesOrdinaryProxyWithoutDedicatedLoginProxy(t *testing.T) {
	for _, relogin := range []bool{false, true} {
		name := "login"
		if relogin {
			name = "relogin"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newLoginFixture(t, http.StatusOK, "")
			var connects atomic.Int32
			proxy := newLoginConnectProxy(t, func(*http.Request) { connects.Add(1) })
			service := NewService(fixture.options(time.Now()))
			credential, err := service.Login(t.Context(), LoginInput{Email: "person@example.com", Password: "correct-password", Relogin: relogin, ProxyURL: proxy.URL})
			if err != nil {
				t.Fatalf("ordinary proxy login: %v", err)
			}
			if connects.Load() == 0 || credential.LifecycleState != LifecycleActive || credential.AccessToken == "" {
				t.Fatal("login did not complete through the configured proxy")
			}
		})
	}
}

func TestServiceRefreshUsesOrdinaryProxy(t *testing.T) {
	for _, session := range []bool{false, true} {
		name := "oauth"
		forbiddenCode := "authentication_forbidden"
		if session {
			name = "session"
			forbiddenCode = "session_refresh_forbidden"
		}
		t.Run(name, func(t *testing.T) {
			for _, test := range []struct {
				name     string
				status   int
				body     string
				wantCode string
			}{
				{name: "success", status: http.StatusOK},
				{name: "challenge", status: http.StatusForbidden, body: `<html><script>window._cf_chl_opt = {};</script></html>`, wantCode: "cloudflare_challenge"},
				{name: "unknown forbidden", status: http.StatusForbidden, body: `<html>Request refused</html>`, wantCode: forbiddenCode},
			} {
				t.Run(test.name, func(t *testing.T) {
					token := testJWT(time.Now().Add(time.Hour).Unix())
					server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
						wantPath := "/oauth/token"
						if session {
							wantPath = "/api/auth/session"
							cookie, errCookie := request.Cookie("next-auth.session-token")
							if errCookie != nil || cookie.Value != "session-value" {
								t.Error("session cookie was not forwarded")
							}
						} else if request.FormValue("refresh_token") != "refresh" {
							t.Error("refresh token was not forwarded")
						}
						if request.URL.Path != wantPath {
							t.Errorf("request path = %q, want %q", request.URL.Path, wantPath)
						}
						if test.body != "" {
							response.Header().Set("Content-Type", "text/html")
							response.WriteHeader(test.status)
							_, _ = io.WriteString(response, test.body)
							return
						}
						response.Header().Set("Content-Type", "application/json")
						if session {
							_, _ = fmt.Fprintf(response, `{"accessToken":%q}`, token)
						} else {
							_, _ = fmt.Fprintf(response, `{"access_token":%q,"refresh_token":"rotated"}`, token)
						}
					}))
					t.Cleanup(server.Close)
					serverURL, errURL := url.Parse(server.URL)
					if errURL != nil {
						t.Fatal(errURL)
					}
					var connects atomic.Int32
					proxy := newLoginConnectProxy(t, func(*http.Request) { connects.Add(1) })
					service := NewService(Options{AuthBaseURL: server.URL, SessionBaseURL: server.URL, Rand: zeroReader{}})
					input := Credential{
						AccessToken: "previous-token", RefreshToken: "refresh", Persona: DefaultPersona(),
						Cookies: []Cookie{{Name: "next-auth.session-token", Value: "session-value", Host: serverURL.Host, Path: "/", HTTPOnly: true}},
					}
					refresh := service.Refresh
					if session {
						refresh = service.RefreshSession
					}
					credential, err := refresh(t.Context(), input, proxy.URL)
					if connects.Load() == 0 {
						t.Fatal("refresh did not connect through the configured proxy")
					}
					if test.wantCode == "" {
						if err != nil {
							t.Fatalf("refresh: %v", err)
						}
						if credential.LifecycleState != LifecycleActive || credential.AccessToken != token {
							t.Fatal("refresh did not activate the new token")
						}
						return
					}
					authError, ok := AsAuthError(err)
					if !ok || authError.Code != test.wantCode || authError.Terminal || !authError.Retryable {
						t.Fatalf("refresh error = %#v, want transient %q", authError, test.wantCode)
					}
					if credential.LifecycleState == LifecycleDead || credential.AccessToken != input.AccessToken {
						t.Fatal("transient refresh failure changed the token or marked the credential dead")
					}
				})
			}
		})
	}
}
