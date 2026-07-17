package chatgptweb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestCookieRoundTripAcrossClients(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/set":
			http.SetCookie(response, &http.Cookie{Name: "session", Value: "cookie-value", Path: "/", HttpOnly: true, SameSite: http.SameSiteLaxMode})
			response.WriteHeader(http.StatusNoContent)
		case "/echo":
			cookie, err := request.Cookie("session")
			if err != nil {
				http.Error(response, err.Error(), http.StatusUnauthorized)
				return
			}
			_, _ = fmt.Fprint(response, cookie.Value)
		default:
			http.NotFound(response, request)
		}
	}))
	defer server.Close()

	first, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	if response, _, err := first.DoFollow(context.Background(), http.MethodGet, server.URL+"/set", nil, nil); err != nil {
		t.Fatal(err)
	} else if response.StatusCode != http.StatusNoContent {
		t.Fatalf("set cookie status = %d", response.StatusCode)
	}
	exported := first.ExportCookies()
	if len(exported) != 1 || exported[0].Name != "session" || !exported[0].HTTPOnly {
		t.Fatalf("ExportCookies() = %#v", exported)
	}

	second, err := NewClient(first.Persona(), "", exported)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	response, payload, err := second.DoNoRedirect(context.Background(), http.MethodGet, server.URL+"/echo", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(payload) != "cookie-value" {
		t.Fatalf("restored cookie response = %d %q", response.StatusCode, payload)
	}
}

func TestDomainCookieRoundTrip(t *testing.T) {
	t.Parallel()
	first, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	if err := first.SetCookie(AuthBaseURL, "oai-did", "device"); err != nil {
		t.Fatal(err)
	}
	exported := first.ExportCookies()
	second, err := NewClient(first.Persona(), "", exported)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	restored := second.ExportCookies()
	if len(restored) != 1 || restored[0].Name != "oai-did" || restored[0].Value != "device" || restored[0].Domain != "auth.openai.com" {
		t.Fatalf("restored domain cookies = %#v", restored)
	}
}

func TestDoJSONStreamDoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		targetCalls.Add(1)
		_, _ = io.Copy(io.Discard, request.Body)
		response.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Location", target.URL+"/leak")
		response.WriteHeader(http.StatusTemporaryRedirect)
	}))
	defer source.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.DoJSONStream(context.Background(), http.MethodPost, source.URL+"/stream", map[string]string{
		"openai-sentinel-proof-token": "secret-proof",
	}, map[string]string{"prompt": "secret-prompt"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			t.Errorf("close response body: %v", errClose)
		}
	}()
	if response.StatusCode != http.StatusTemporaryRedirect {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusTemporaryRedirect)
	}
	if calls := targetCalls.Load(); calls != 0 {
		t.Fatalf("redirect target calls = %d, want 0", calls)
	}
}

func TestDoFollowStreamLeavesBodyForCaller(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(response, "streamed")
	}))
	defer server.Close()

	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := client.DoFollowStream(context.Background(), http.MethodGet, server.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err = response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(payload) != "streamed" {
		t.Fatalf("stream payload = %q", payload)
	}
}

func TestDoSameOriginRedirectStreamFollowsOnlyExactOrigin(t *testing.T) {
	t.Run("same origin", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			switch request.URL.Path {
			case "/start":
				http.Redirect(response, request, "/done", http.StatusFound)
			case "/done":
				if got := request.Header.Get("Oai-Device-Id"); got != "device" {
					t.Fatalf("device header = %q", got)
				}
				response.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(response, request)
			}
		}))
		defer server.Close()
		client, err := NewClient(DefaultPersona(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, err := client.DoSameOriginRedirectStream(context.Background(), http.MethodGet, server.URL+"/start", map[string]string{
			"oai-device-id": "device",
		}, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatalf("status = %d", response.StatusCode)
		}
	})

	t.Run("cross origin", func(t *testing.T) {
		var targetCalls atomic.Int32
		target := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			targetCalls.Add(1)
			if request.Header.Get("Oai-Device-Id") != "" || request.Header.Get("Oai-Session-Id") != "" {
				t.Errorf("cross-origin request leaked identity headers")
			}
			response.WriteHeader(http.StatusNoContent)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
			http.Redirect(response, request, target.URL+"/capture", http.StatusTemporaryRedirect)
		}))
		defer source.Close()
		client, err := NewClient(DefaultPersona(), "", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, err := client.DoSameOriginRedirectStream(context.Background(), http.MethodGet, source.URL, map[string]string{
			"authorization":  "Bearer secret",
			"oai-device-id":  "device",
			"oai-session-id": "session",
		}, 5)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusTemporaryRedirect {
			t.Fatalf("status = %d", response.StatusCode)
		}
		if calls := targetCalls.Load(); calls != 0 {
			t.Fatalf("target calls = %d", calls)
		}
	})
}
