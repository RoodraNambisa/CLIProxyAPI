package chatgptweb

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync/atomic"
	"testing"
	"time"
)

func TestAcquisitionClientClosesActiveConnections(t *testing.T) {
	started := make(chan struct{}, 1)
	canceled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		started <- struct{}{}
		<-request.Context().Done()
		canceled <- struct{}{}
	}))
	defer server.Close()

	client, errClient := NewAcquisitionClient(DefaultPersona(), "", nil, time.Second)
	if errClient != nil {
		t.Fatalf("NewAcquisitionClient() error = %v", errClient)
	}
	done := make(chan error, 1)
	go func() {
		_, _, errRequest := client.DoFollow(context.Background(), http.MethodGet, server.URL, nil, nil)
		done <- errRequest
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("acquisition request did not start")
	}
	client.acquisitionTracker.mu.Lock()
	activeConnections := len(client.acquisitionTracker.connections)
	client.acquisitionTracker.mu.Unlock()
	if activeConnections != 1 {
		t.Fatalf("tracked acquisition connections = %d, want 1", activeConnections)
	}
	client.CloseActiveAcquisitionConnections()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("active acquisition connection remained open")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("acquisition request did not return after closing its connection")
	}
}

func TestAcquisitionClientAcceptsConfiguredProxyDialer(t *testing.T) {
	for _, proxyURL := range []string{
		"http://user:pass@127.0.0.1:18080",
		"socks5h://user:pass@127.0.0.1:1080",
	} {
		t.Run(proxyURL, func(t *testing.T) {
			client, errClient := NewAcquisitionClient(DefaultPersona(), proxyURL, nil, time.Second)
			if errClient != nil {
				t.Fatalf("NewAcquisitionClient() error = %v", errClient)
			}
			client.CloseIdleConnections()
		})
	}
}

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

func TestSameChatGPTWebOriginNormalizesDefaultPorts(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "https default port",
			left:  "https://chatgpt.com/backend-api/accounts/check",
			right: "https://CHATGPT.com:443/backend-api/accounts/check",
			want:  true,
		},
		{
			name:  "http default port",
			left:  "http://chatgpt.com/start",
			right: "http://chatgpt.com:080/done",
			want:  true,
		},
		{
			name:  "non-default port",
			left:  "https://chatgpt.com/start",
			right: "https://chatgpt.com:8443/done",
			want:  false,
		},
		{
			name:  "different scheme",
			left:  "https://chatgpt.com/start",
			right: "http://chatgpt.com:443/done",
			want:  false,
		},
		{
			name:  "different host",
			left:  "https://chatgpt.com/start",
			right: "https://example.com:443/done",
			want:  false,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			left, errLeft := url.Parse(testCase.left)
			if errLeft != nil {
				t.Fatalf("parse left URL: %v", errLeft)
			}
			right, errRight := url.Parse(testCase.right)
			if errRight != nil {
				t.Fatalf("parse right URL: %v", errRight)
			}
			if got := sameChatGPTWebOrigin(left, right); got != testCase.want {
				t.Fatalf("sameChatGPTWebOrigin(%q, %q) = %v, want %v", testCase.left, testCase.right, got, testCase.want)
			}
		})
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
