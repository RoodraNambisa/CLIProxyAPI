package chatgptweb

import (
	"context"
	stdtls "crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"
)

func TestTLSTrustAnchorsDefaultAndContext(t *testing.T) {
	base := profiles.Chrome_146
	got, ok := findTLSProfile("chrome_146")
	if !ok || !reflect.DeepEqual(got.GetSettings(), base.GetSettings()) {
		t.Fatal("default template differs")
	}
	spec, err := got.GetClientHelloSpec()
	if err != nil {
		t.Fatal(err)
	}
	expected, err := base.GetClientHelloSpec()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(spec, expected) {
		t.Fatal("default TLS differs from original Chrome 146")
	}
	ctx := WithTLSOptions(context.Background(), TLSOptions{TrustAnchorsEnabled: false})
	if TLSOptionsFromContext(WithTLSOptions(ctx, TLSOptions{TrustAnchorsEnabled: true})).TrustAnchorsEnabled {
		t.Fatal("flow snapshot changed")
	}
	if !TLSOptionsFromContext(nil).TrustAnchorsEnabled {
		t.Fatal("missing context must use original")
	}
	client, err := NewClient(DefaultPersona(), "", nil, TLSOptionsFromContext(ctx))
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	copy := client.TLSOptions()
	copy.TrustAnchorsEnabled = true
	if client.TLSOptions().TrustAnchorsEnabled || !copy.TrustAnchorsEnabled {
		t.Fatal("client TLS options were mutable")
	}
	requestClient, err := NewRequestClient(WithTLSOptions(t.Context(), TLSOptions{TrustAnchorsEnabled: true}), DefaultPersona(), "", nil, false, client.TLSOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer requestClient.CloseIdleConnections()
	if requestClient.TLSOptions().TrustAnchorsEnabled {
		t.Fatal("explicit parent TLS snapshot lost to fetch context")
	}
	other, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.CloseIdleConnections()
	if (&Sentinel{client: client}).transportKey() == (&Sentinel{client: other}).transportKey() {
		t.Fatal("SDK transports share different TLS policies")
	}
	client.loginRetry = &loginClientRetry{selector: &loginProxySelector{fixed: "http://127.0.0.1:1"}, attempts: 2}
	if err := client.prepareLoginRequestRetry(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if client.TLSOptions().TrustAnchorsEnabled {
		t.Fatal("proxy retry lost TLS settings")
	}
}
func TestTLSTrustAnchorsAuthenticationFlowsOnWire(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, flow := range []string{"login", "refresh", "session"} {
			t.Run(fmt.Sprintf("%t/%s", enabled, flow), func(t *testing.T) {
				t.Parallel()
				observed := make(chan []uint16, 8)
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("untrusted certificate accepted") }))
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.TLS = &stdtls.Config{GetConfigForClient: func(hello *stdtls.ClientHelloInfo) (*stdtls.Config, error) {
					observed <- slices.Clone(hello.Extensions)
					return nil, nil
				}}
				server.StartTLS()
				defer server.Close()
				service := NewService(Options{AuthBaseURL: server.URL, SessionBaseURL: server.URL, AcquisitionTimeout: time.Second})
				ctx := WithTLSOptions(t.Context(), TLSOptions{TrustAnchorsEnabled: enabled})
				credential := Credential{Email: "tls-test@example.com", Password: "test-password", TOTPSecret: "JBSWY3DPEHPK3PXP", RefreshToken: "test-refresh", Persona: DefaultPersona(), Cookies: []Cookie{{Name: "__Secure-next-auth.session-token", Value: "test-session", Path: "/", Secure: true}}}
				if flow != "session" {
					credential.Cookies = nil
				}
				var err error
				switch flow {
				case "login":
					_, err = service.Login(ctx, LoginInput{Credential: &credential})
				case "refresh":
					_, err = service.Refresh(ctx, credential, "")
				case "session":
					_, err = service.RefreshSession(ctx, credential, "")
				}
				if err == nil {
					t.Fatal("untrusted certificate accepted")
				}
				select {
				case extensions := <-observed:
					if slices.Contains(extensions, 0xca34) != enabled {
						t.Fatal("authentication TLS setting lost")
					}
				default:
					t.Fatalf("no authentication ClientHello captured: %+v", err)
				}
			})
		}
	}
}
