package executor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexAuthorizationDiscardsStaleCallerValues(t *testing.T) {
	for _, entry := range []string{"prepare", "http", "sse", "compact", "images", "websocket"} {
		for _, tc := range []struct{ name, token, custom, want string }{
			{"empty", "", "", ""},
			{"whitespace", "  ", "", ""},
			{"credential", "fixture", "", "Bearer fixture"},
			{"explicit custom", "", "Custom fixture", "Custom fixture"},
			{"credential wins", "fixture", "Custom fixture", "Bearer fixture"},
		} {
			t.Run(entry+"/"+tc.name, func(t *testing.T) {
				credential := &auth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": tc.token}}
				if tc.custom != "" {
					credential.Attributes["header:Authorization"] = tc.custom
				}
				request := httptest.NewRequest(http.MethodPost, "https://example.test/responses", nil)
				request.Header = http.Header{"Authorization": {"stale one"}, "authorization": {"stale two"}}
				var err error
				switch entry {
				case "prepare":
					err = NewCodexExecutor(&config.Config{}).PrepareRequest(request, credential)
				case "images":
					err = applyCodexDirectImageHeaders(request, credential, tc.token, false, &config.Config{})
				case "websocket":
					request.Header, err = prepareCodexWebsocketHeadersForURL(t.Context(), request.Header, credential, tc.token, &config.Config{}, nil)
				default:
					err = applyCodexHeaders(request, credential, tc.token, entry == "sse", &config.Config{})
				}
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				for key, values := range request.Header {
					if strings.EqualFold(key, "Authorization") {
						count += len(values)
						for _, value := range values {
							if value != tc.want {
								t.Fatal("stale or incorrect authorization survived")
							}
						}
					}
				}
				wantCount := 1
				if tc.want == "" {
					wantCount = 0
				}
				if count != wantCount {
					t.Fatalf("got %d authentication values, want %d", count, wantCount)
				}
			})
		}
	}
}

func TestCodexHTTPRequestDoesNotForwardStaleAuthorization(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Header.Values("Authorization")) != 0 {
			t.Error("stale authorization reached upstream")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "stale fixture")
	response, err := NewCodexExecutor(&config.Config{}).HttpRequest(t.Context(), &auth.Auth{Provider: "codex", ProxyURL: "direct"}, request)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		t.Fatal("unexpected upstream result")
	}
}
