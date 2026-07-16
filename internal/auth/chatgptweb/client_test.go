package chatgptweb

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
