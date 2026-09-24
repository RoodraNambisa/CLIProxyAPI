package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	web "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
)

func TestChatGPTWebHomepageUsesDocumentHeadersWithoutAPIOverrides(t *testing.T) {
	e := NewChatGPTWebExecutor(nil, nil)
	t.Cleanup(func() { _ = e.Close() })
	credential := &web.Credential{AccessToken: "fixture-token", DeviceID: "device", SessionID: "session", Persona: web.DefaultPersona()}
	headers := e.chatGPTWebHeaders(credential, "/", map[string]string{
		"Authorization": "Bearer must-not-be-sent", "origin": "https://example.invalid", "oai-device-id": "must-not-be-sent",
	})
	for _, key := range []string{"authorization", "Authorization", "origin", "referer", "oai-device-id", "oai-session-id", "oai-client-version", "oai-client-build-number", "x-openai-target-path", "x-openai-target-route", "pragma"} {
		if _, ok := headers[key]; ok {
			t.Errorf("homepage has API header %s", key)
		}
	}
	for key, want := range map[string]string{"sec-fetch-dest": "document", "sec-fetch-mode": "navigate", "sec-fetch-site": "none", "sec-fetch-user": "?1", "upgrade-insecure-requests": "1", "priority": "u=0, i"} {
		if headers[key] != want {
			t.Errorf("%s = %s, want %s", key, headers[key], want)
		}
	}
	api := e.chatGPTWebHeaders(credential, "/backend-api/models", nil)
	if api["authorization"] != "Bearer fixture-token" || api["oai-device-id"] != "device" || api["sec-fetch-mode"] != "cors" || api["x-openai-target-path"] != "/backend-api/models" {
		t.Fatal("backend API headers were changed")
	}
}

func TestChatGPTWebHomepageRedirectPreservesCookiesWithoutAPIHeaders(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		for key := range r.Header {
			lower := strings.ToLower(key)
			if lower == "authorization" || lower == "origin" || lower == "referer" || strings.HasPrefix(lower, "oai-") || strings.HasPrefix(lower, "x-openai-") {
				t.Errorf("%s leaked %s", r.URL.Path, key)
			}
		}
		if cookie, err := r.Cookie("oai-did"); err != nil || cookie.Value != "device-id" {
			t.Error("document lost identity cookie")
		}
		if r.Header.Get("Sec-Fetch-Mode") != "navigate" || r.Header.Get("Sec-Fetch-User") != "?1" {
			t.Error("not a document navigation")
		}
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/landing", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "<html>ok</html>")
	}))
	defer server.Close()
	e := NewChatGPTWebExecutor(nil, nil)
	t.Cleanup(func() { _ = e.Close() })
	e.runtimeBaseURL = server.URL
	client, credential, err := e.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	response, err := e.doChatGPTWebBootstrapRequest(t.Context(), client, credential, server.URL+"/", e.chatGPTWebHeaders(credential, "/", nil))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if requests.Load() != 2 || response.StatusCode != 200 {
		t.Fatal("redirect flow changed")
	}
}
