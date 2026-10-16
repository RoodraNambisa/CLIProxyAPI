package executor

import (
	"context"
	stdtls "crypto/tls"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestChatGPTWebTLSHotUpdateRetainsFlowAndCredential(t *testing.T) {
	disabled := false
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{TLSTrustAnchorsEnabled: &disabled}}
	e := NewChatGPTWebExecutor(cfg, nil)
	defer func() { _ = e.Close() }()
	auth := chatGPTWebRuntimeAuth()
	before := auth.Clone()
	ctx := chatgptwebauth.WithTLSOptions(t.Context(), e.chatGPTWebTLSOptions(t.Context()))
	old, _, err := e.newRuntimeClientForRequest(ctx, auth)
	if err != nil {
		t.Fatal(err)
	}
	defer old.CloseIdleConnections()
	e.UpdateConfig(&config.Config{})
	for _, requestCtx := range []context.Context{ctx, context.WithoutCancel(ctx)} {
		continued, _, err := e.newRuntimeClientForRequest(requestCtx, auth)
		if err != nil {
			t.Fatal(err)
		}
		if continued.TLSOptions().TrustAnchorsEnabled {
			t.Fatal("in-flight flow changed after hot update")
		}
		continued.CloseIdleConnections()
	}
	next, _, err := e.newRuntimeClient(auth)
	if err != nil {
		t.Fatal(err)
	}
	defer next.CloseIdleConnections()
	if old.TLSOptions().TrustAnchorsEnabled || !next.TLSOptions().TrustAnchorsEnabled {
		t.Fatal("new/default TLS policy not applied")
	}
	if !reflect.DeepEqual(auth.Metadata, before.Metadata) {
		t.Fatal("credential changed")
	}
	if chatGPTWebSentinelTransportKey(old) == chatGPTWebSentinelTransportKey(next) {
		t.Fatal("SDK transport key omitted TLS")
	}
	// The SDK downloader must inherit the old client even after the executor updates.
	observed := make(chan []uint16, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted certificate accepted") }))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.TLS = &stdtls.Config{GetConfigForClient: func(h *stdtls.ClientHelloInfo) (*stdtls.Config, error) {
		observed <- slices.Clone(h.Extensions)
		return nil, nil
	}}
	server.StartTLS()
	defer server.Close()
	_, _, _, err = e.chatGPTWebSentinelSDKFetcher(old, nil)(t.Context(), server.URL, 1024)
	if err == nil {
		t.Fatal("certificate verification lost")
	}
	select {
	case extensions := <-observed:
		if slices.Contains(extensions, 0xca34) {
			t.Fatal("SDK download used new TLS policy")
		}
	default:
		t.Fatal("SDK ClientHello not observed")
	}
}
func TestChatGPTWebTLSLoginAndImportSnapshot(t *testing.T) {
	disabled := false
	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{TLSTrustAnchorsEnabled: &disabled}}
	e := NewChatGPTWebExecutor(cfg, nil)
	defer func() { _ = e.Close() }()
	calls := 0
	check := func(ctx context.Context) {
		t.Helper()
		calls++
		if chatgptwebauth.TLSOptionsFromContext(ctx).TrustAnchorsEnabled {
			t.Fatal("authentication flow did not receive TLS configuration")
		}
	}
	e.authService = &fakeChatGPTWebAuthService{
		loginFn: func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
			check(ctx)
			return &chatgptwebauth.Credential{}, nil
		},
		refreshFn: func(ctx context.Context, c chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
			check(ctx)
			return &c, nil
		},
	}
	if _, err := e.Login(t.Context(), chatgptwebauth.LoginInput{}); err != nil {
		t.Fatal(err)
	}
	credential := &chatgptwebauth.Credential{AccessToken: "test-token", RefreshStrategy: chatgptwebauth.RefreshStrategyWebOAuthRT}
	if _, err := e.NormalizeImportedCredential(t.Context(), credential, ""); err != nil {
		t.Fatal(err)
	}
	frozen := chatgptwebauth.WithTLSOptions(t.Context(), e.chatGPTWebTLSOptions(t.Context()))
	e.UpdateConfig(&config.Config{})
	auth := chatGPTWebRuntimeAuth()
	auth.Metadata["refresh_strategy"] = string(chatgptwebauth.RefreshStrategyWebOAuthRT)
	auth.Metadata["refresh_token"] = "test-refresh"
	if _, err := e.RefreshToCompletion(frozen, auth); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls=%d", calls)
	}
}
