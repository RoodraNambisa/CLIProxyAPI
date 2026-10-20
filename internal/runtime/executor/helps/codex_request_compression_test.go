package helps

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexRequestCompressionScopeAndReplay(t *testing.T) {
	body := []byte(`{"input":"` + strings.Repeat("compress this prompt ", 512) + `"}`)
	for _, tc := range []struct {
		name, url, host, mode string
		apiKey                bool
		want                  bool
	}{
		{"enabled", "https://chatgpt.com/backend-api/codex/responses", "", "zstd", false, true},
		{"compact", "https://chatgpt.com/backend-api/codex/responses/compact", "", "zstd", false, true},
		{"off", "https://chatgpt.com/backend-api/codex/responses", "", "", false, false},
		{"api-key", "https://chatgpt.com/backend-api/codex/responses", "", "zstd", true, false},
		{"custom", "https://relay.example/responses", "", "zstd", false, false},
		{"insecure", "http://chatgpt.com/backend-api/codex/responses", "", "zstd", false, false},
		{"lookalike", "https://chatgpt.com.example/backend-api/codex/responses", "", "zstd", false, false},
		{"port", "https://chatgpt.com:8443/backend-api/codex/responses", "", "zstd", false, false},
		{"host-override", "https://chatgpt.com/backend-api/codex/responses", "relay.example", "zstd", false, false},
		{"wrong-path", "https://chatgpt.com/v1/images/generations", "", "zstd", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, tc.url, bytes.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			if tc.host != "" {
				req.Host = tc.host
			}
			auth := &coreauth.Auth{Provider: "codex"}
			if tc.apiKey {
				auth.Attributes = map[string]string{"api_key": "fixture"}
			}
			cfg := &config.Config{Codex: config.CodexConfig{UpstreamRequestCompression: tc.mode}}
			if got := PrepareCodexRequestCompression(req, body, auth, cfg); got != tc.want {
				t.Fatalf("compressed=%t, want %t", got, tc.want)
			}
			wire, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			if errClose := req.Body.Close(); errClose != nil {
				t.Fatal(errClose)
			}
			if tc.want {
				decoder, err := zstd.NewReader(nil)
				if err != nil {
					t.Fatal(err)
				}
				defer decoder.Close()
				decoded, err := decoder.DecodeAll(wire, nil)
				if err != nil || !bytes.Equal(decoded, body) {
					t.Fatal("compressed payload differs from projected request")
				}
				if req.Header.Get("Content-Encoding") != "zstd" || req.ContentLength != int64(len(wire)) {
					t.Fatal("wire headers disagree with body")
				}
				replay, err := req.GetBody()
				if err != nil {
					t.Fatal(err)
				}
				replayed, err := io.ReadAll(replay)
				_ = replay.Close()
				if err != nil || !bytes.Equal(wire, replayed) {
					t.Fatal("redirect/replay body changed")
				}
			} else if !bytes.Equal(wire, body) || req.Header.Get("Content-Encoding") != "" {
				t.Fatal("ineligible request changed")
			}
		})
	}
}

func TestCodexCompressedRedirectCannotEscapeOfficialTarget(t *testing.T) {
	client := &http.Client{}
	owned := CodexCompressedHTTPClient(client, true)
	for _, target := range []string{"https://relay.example/responses", "https://chatgpt.com/other"} {
		req := httptest.NewRequest(http.MethodPost, target, nil)
		if owned.CheckRedirect(req, nil) != http.ErrUseLastResponse {
			t.Fatal("unverified redirect accepted")
		}
	}
	if client.CheckRedirect != nil || CodexCompressedHTTPClient(client, false) != client {
		t.Fatal("shared HTTP client changed")
	}
}

func TestCodexCompressionReleasesNativeBodyAfterUpload(t *testing.T) {
	body := []byte(strings.Repeat("compressible payload ", 1024))
	request, err := http.NewRequest(http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", coreexecutor.NewReleasableReadCloser(body, nil))
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Codex: config.CodexConfig{UpstreamRequestCompression: "zstd"}}
	if !PrepareCodexRequestCompression(request, body, &coreauth.Auth{Provider: "codex"}, cfg) {
		t.Fatal("request was not compressed")
	}
	if request.GetBody != nil {
		t.Fatal("compression introduced implicit replay retention")
	}
	if _, err := io.Copy(io.Discard, request.Body); err != nil {
		t.Fatal(err)
	}
	if request.Body.(*coreexecutor.ReleasableReadCloser).Len() != 0 {
		t.Fatal("uploaded compressed body is still retained")
	}
}
