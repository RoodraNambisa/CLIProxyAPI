package gemini

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestGeminiCLIRequestAllowed(t *testing.T) {
	tests := []struct {
		name       string
		host       string
		remoteAddr string
		want       bool
	}{
		{
			name:       "ipv4 loopback",
			host:       "127.0.0.1:8080",
			remoteAddr: net.JoinHostPort("127.0.0.1", "34567"),
			want:       true,
		},
		{
			name:       "localhost host with loopback remote",
			host:       "localhost:8080",
			remoteAddr: net.JoinHostPort("127.0.0.1", "34567"),
			want:       true,
		},
		{
			name:       "ipv6 loopback",
			host:       net.JoinHostPort("::1", "8080"),
			remoteAddr: net.JoinHostPort("::1", "34567"),
			want:       true,
		},
		{
			name:       "reject remote non loopback",
			host:       "localhost:8080",
			remoteAddr: net.JoinHostPort("192.168.1.50", "34567"),
			want:       false,
		},
		{
			name:       "reject non local host",
			host:       "example.com:8080",
			remoteAddr: net.JoinHostPort("127.0.0.1", "34567"),
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "http://"+tt.host+"/v1internal:generateContent", strings.NewReader(`{}`))
			req.Host = tt.host
			req.RemoteAddr = tt.remoteAddr

			if got := geminiCLIRequestAllowed(req); got != tt.want {
				t.Fatalf("geminiCLIRequestAllowed() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGeminiCLIHandlerRejectsWhenEndpointDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/v1internal:generateContent", strings.NewReader(`{"model":"gemini-2.5-pro"}`))
	req.RemoteAddr = net.JoinHostPort("127.0.0.1", "34567")
	ctx.Request = req

	base := handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{EnableGeminiCLIEndpoint: false}, nil)
	handler := NewGeminiCLIAPIHandler(base)
	handler.CLIHandler(ctx)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
	if !strings.Contains(recorder.Body.String(), "Gemini CLI endpoint is disabled") {
		t.Fatalf("body = %s, want disabled error", recorder.Body.String())
	}
}
