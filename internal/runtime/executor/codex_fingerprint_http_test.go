package executor

import (
	"context"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCodexFingerprintShouldForceHTTP1(t *testing.T) {
	tests := []struct {
		name         string
		cfg          *config.Config
		imageRequest bool
		want         bool
	}{
		{
			name: "defaults disabled",
			cfg:  &config.Config{},
		},
		{
			name: "global force applies to text",
			cfg: &config.Config{
				CodexFingerprint: config.CodexFingerprintConfig{ForceHTTP1: true},
			},
			want: true,
		},
		{
			name: "image force skips text",
			cfg: &config.Config{
				CodexFingerprint: config.CodexFingerprintConfig{ImagesForceHTTP1: true},
			},
		},
		{
			name: "image force applies to image request",
			cfg: &config.Config{
				CodexFingerprint: config.CodexFingerprintConfig{ImagesForceHTTP1: true},
			},
			imageRequest: true,
			want:         true,
		},
		{
			name: "global and image force can both be enabled",
			cfg: &config.Config{
				CodexFingerprint: config.CodexFingerprintConfig{ForceHTTP1: true, ImagesForceHTTP1: true},
			},
			want: true,
		},
		{
			name: "codex tls fingerprint overrides force http1",
			cfg: &config.Config{
				CodexFingerprint: config.CodexFingerprintConfig{JA3: true, ForceHTTP1: true, ImagesForceHTTP1: true},
			},
			imageRequest: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := codexFingerprintShouldForceHTTP1(tt.cfg, tt.imageRequest)
			if got != tt.want {
				t.Fatalf("codexFingerprintShouldForceHTTP1() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCodexHTTPClientForceHTTP1(t *testing.T) {
	executor := &CodexExecutor{
		cfg: &config.Config{
			SDKConfig:        sdkconfig.SDKConfig{ProxyURL: "direct"},
			CodexFingerprint: config.CodexFingerprintConfig{ForceHTTP1: true},
		},
	}

	client := executor.newCodexHTTPClient(context.Background(), nil, false)

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport type = %T, want *http.Transport", client.Transport)
	}
	if transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = true, want false")
	}
	if transport.TLSNextProto == nil {
		t.Fatal("TLSNextProto = nil, want empty map to disable HTTP/2")
	}
}

func TestCodexHTTPClientImagesForceHTTP1OnlyForImageRequests(t *testing.T) {
	executor := &CodexExecutor{
		cfg: &config.Config{
			SDKConfig:        sdkconfig.SDKConfig{ProxyURL: "direct"},
			CodexFingerprint: config.CodexFingerprintConfig{ImagesForceHTTP1: true},
		},
	}

	textClient := executor.newCodexHTTPClient(context.Background(), nil, false)
	textTransport, ok := textClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("text transport type = %T, want *http.Transport", textClient.Transport)
	}
	// Go may initialize this shared transport before the assertion runs.
	if !textTransport.ForceAttemptHTTP2 || (textTransport.TLSNextProto != nil && textTransport.TLSNextProto["h2"] == nil) {
		t.Fatal("text transport disabled HTTP/2 without an image request")
	}

	imageClient := executor.newCodexHTTPClient(context.Background(), nil, true)
	imageTransport, ok := imageClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("image transport type = %T, want *http.Transport", imageClient.Transport)
	}
	if imageTransport.ForceAttemptHTTP2 {
		t.Fatal("image ForceAttemptHTTP2 = true, want false")
	}
	if imageTransport.TLSNextProto == nil || len(imageTransport.TLSNextProto) != 0 {
		t.Fatal("image TLSNextProto must be an explicit empty map to disable HTTP/2")
	}
}
