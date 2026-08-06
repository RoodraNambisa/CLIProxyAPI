package helps

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestClassifyChatGPTWebHTTPDiagnostic(t *testing.T) {
	tests := []struct {
		name           string
		status         int
		path           string
		body           string
		headers        http.Header
		wantStage      string
		wantCode       string
		wantType       string
		wantCloudflare bool
		wantRetryable  bool
	}{
		{
			name:           "cloudflare challenge",
			status:         http.StatusForbidden,
			path:           "/backend-api/files?token=secret",
			body:           "<!doctype html><title>Just a moment...</title><script src=/cdn-cgi/challenge-platform/x></script>",
			headers:        http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cf-Mitigated": {"challenge"}, "Cf-Ray": {"abc-SJC"}},
			wantStage:      "file_sign",
			wantCode:       "cloudflare_challenge",
			wantType:       "html",
			wantCloudflare: true,
			wantRetryable:  true,
		},
		{
			name:           "cf ray alone is not a challenge",
			status:         http.StatusForbidden,
			path:           "/backend-api/conversation",
			body:           `{"error":{"message":"forbidden"}}`,
			headers:        http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc-SJC"}},
			wantStage:      "conversation",
			wantCode:       "forbidden",
			wantType:       "json",
			wantCloudflare: false,
			wantRetryable:  false,
		},
		{
			name:          "non json upstream failure",
			status:        http.StatusBadGateway,
			path:          "/backend-api/conversation/poll",
			body:          "temporary gateway failure",
			headers:       http.Header{"Content-Type": {"text/plain"}},
			wantStage:     "conversation",
			wantCode:      "upstream_non_json",
			wantType:      "text",
			wantRetryable: true,
		},
		{
			name:           "passkey structured error",
			status:         http.StatusBadRequest,
			path:           "/api/accounts/passkey/verify",
			body:           `{"error":{"code":"invalid_passkey_response","message":"private text"}}`,
			headers:        http.Header{"Content-Type": {"application/json"}},
			wantStage:      "passkey_verify",
			wantCode:       "invalid_passkey_response",
			wantType:       "json",
			wantCloudflare: false,
			wantRetryable:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostic := ClassifyChatGPTWebHTTPDiagnostic(test.status, test.path, []byte(test.body), test.headers)
			if diagnostic.Stage != test.wantStage || diagnostic.Code != test.wantCode || diagnostic.ResponseType != test.wantType {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
			if diagnostic.Cloudflare != test.wantCloudflare || diagnostic.Retryable != test.wantRetryable {
				t.Fatalf("diagnostic classification = %#v", diagnostic)
			}
			if strings.Contains(diagnostic.TargetPath, "?") {
				t.Fatalf("target path retained a query: %q", diagnostic.TargetPath)
			}
			encoded, errMarshal := json.Marshal(diagnostic)
			if errMarshal != nil {
				t.Fatalf("marshal diagnostic: %v", errMarshal)
			}
			if strings.Contains(string(encoded), "private text") || strings.Contains(string(encoded), "secret") {
				t.Fatalf("diagnostic leaked response content: %s", encoded)
			}
		})
	}
}

func TestClassifyChatGPTWebHTTPDiagnosticRejectsUnsafeUpstreamCode(t *testing.T) {
	diagnostic := ClassifyChatGPTWebHTTPDiagnostic(
		http.StatusBadRequest,
		"/backend-api/models",
		[]byte(`{"error":{"code":"unsafe token=secret@example.com"}}`),
		http.Header{"Content-Type": {"application/json"}},
	)
	if diagnostic.Code != "upstream_request_error" {
		t.Fatalf("code = %q, want upstream_request_error", diagnostic.Code)
	}
}

func TestClassifyChatGPTWebTransportDiagnostic(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		path          string
		wantCode      string
		wantHost      string
		wantPath      string
		wantRetryable bool
	}{
		{
			name:          "dns",
			err:           &net.DNSError{Err: "no such host", Name: "secret.example"},
			path:          "https://files.oaiusercontent.com/object/image.png?sig=secret",
			wantCode:      "dns_error",
			wantHost:      "files.oaiusercontent.com",
			wantPath:      "/object/image.png",
			wantRetryable: true,
		},
		{
			name:          "canceled",
			err:           context.Canceled,
			path:          "/backend-api/conversation",
			wantCode:      "request_canceled",
			wantHost:      "chatgpt.com",
			wantPath:      "/backend-api/conversation",
			wantRetryable: false,
		},
		{
			name:          "untrusted target",
			err:           context.DeadlineExceeded,
			path:          "https://secret.internal/path?token=secret",
			wantCode:      "network_timeout",
			wantHost:      "external_asset",
			wantPath:      "/path",
			wantRetryable: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diagnostic := ClassifyChatGPTWebTransportDiagnostic(test.err, test.path)
			if diagnostic.Code != test.wantCode || diagnostic.TargetHost != test.wantHost ||
				diagnostic.TargetPath != test.wantPath || diagnostic.Retryable != test.wantRetryable {
				t.Fatalf("diagnostic = %#v", diagnostic)
			}
			encoded, errMarshal := json.Marshal(diagnostic)
			if errMarshal != nil {
				t.Fatal(errMarshal)
			}
			if strings.Contains(string(encoded), "secret") {
				t.Fatalf("transport diagnostic leaked input: %s", encoded)
			}
		})
	}
}
