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
	"strings"
	"testing"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
)

func TestChatGPTWebTLSProfilePreservesChrome146ExceptTrustAnchors(t *testing.T) {
	t.Parallel()
	profile, ok := findTLSProfile(" CHROME_146 ")
	if !ok {
		t.Fatal("Chrome 146 profile unavailable")
	}
	assertChatGPTWebTLSProfileSpec(t, profile)

	base := profiles.Chrome_146
	gotID, wantID := profile.GetClientHelloId(), base.GetClientHelloId()
	gotID.SpecFactory, wantID.SpecFactory = nil, nil
	for _, check := range []struct {
		name      string
		got, want any
	}{
		{"identity", gotID, wantID},
		{"settings", profile.GetSettings(), base.GetSettings()},
		{"settings order", profile.GetSettingsOrder(), base.GetSettingsOrder()},
		{"pseudo headers", profile.GetPseudoHeaderOrder(), base.GetPseudoHeaderOrder()},
		{"connection flow", profile.GetConnectionFlow(), base.GetConnectionFlow()},
		{"priorities", profile.GetPriorities(), base.GetPriorities()},
		{"header priority", profile.GetHeaderPriority(), base.GetHeaderPriority()},
		{"stream ID", profile.GetStreamID(), base.GetStreamID()},
		{"HTTP allowed", profile.GetAllowHTTP(), base.GetAllowHTTP()},
		{"HTTP3 settings", profile.GetHttp3Settings(), base.GetHttp3Settings()},
		{"HTTP3 settings order", profile.GetHttp3SettingsOrder(), base.GetHttp3SettingsOrder()},
		{"HTTP3 priority", profile.GetHttp3PriorityParam(), base.GetHttp3PriorityParam()},
		{"HTTP3 pseudo headers", profile.GetHttp3PseudoHeaderOrder(), base.GetHttp3PseudoHeaderOrder()},
		{"HTTP3 GREASE", profile.GetHttp3SendGreaseFrames(), base.GetHttp3SendGreaseFrames()},
	} {
		if !reflect.DeepEqual(check.got, check.want) {
			t.Errorf("%s changed: got %v, want %v", check.name, check.got, check.want)
		}
	}
}

func TestChatGPTWebTLSProfileSpecsAreIndependent(t *testing.T) {
	profile, ok := findTLSProfile("chrome_146")
	if !ok {
		t.Fatal("Chrome 146 profile unavailable")
	}
	for i := range 16 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			first, err := profile.GetClientHelloSpec()
			if err != nil {
				t.Fatal(err)
			}
			first.CipherSuites[0] = 0
			first.Extensions[0] = nil
			for _, extension := range first.Extensions {
				if versions, ok := extension.(*tls.SupportedVersionsExtension); ok {
					versions.Versions[0] = 0
				}
			}
			// A handshake may mutate its spec. Neither the next handshake nor
			// the dependency's shared profile may inherit that state.
			assertChatGPTWebTLSProfileSpec(t, profile)
		})
	}
}

func TestChatGPTWebTLSProfileOnWireAndCertificateVerification(t *testing.T) {
	for _, kind := range []string{"session", "runtime", "acquisition"} {
		for _, path := range []string{"follow", "no_redirect", "poll", "bootstrap"} {
			t.Run(kind+"/"+path, func(t *testing.T) {
				t.Parallel()
				observed := make(chan []uint16, 1)
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
					t.Error("untrusted certificate was accepted")
				}))
				server.Config.ErrorLog = log.New(io.Discard, "", 0)
				server.TLS = &stdtls.Config{GetConfigForClient: func(hello *stdtls.ClientHelloInfo) (*stdtls.Config, error) {
					select {
					case observed <- slices.Clone(hello.Extensions):
					default:
					}
					if !slices.Contains(hello.SupportedProtos, "h2") || !slices.Contains(hello.SupportedVersions, stdtls.VersionTLS13) {
						t.Error("HTTP/2 or TLS 1.3 advertisement was lost")
					}
					return nil, nil
				}}
				server.EnableHTTP2 = true
				server.StartTLS()
				defer server.Close()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var client *Client
				var err error
				switch kind {
				case "session":
					client, err = NewClient(DefaultPersona(), "", nil)
				case "runtime":
					client, err = NewRequestClient(ctx, DefaultPersona(), "", nil, false)
				case "acquisition":
					client, err = NewAccessTokenAcquisitionClient(DefaultPersona(), "", nil, 5*time.Second)
				}
				if err != nil {
					t.Fatal(err)
				}
				defer client.CloseIdleConnections()
				request := client.DoNoRedirectStream
				switch path {
				case "follow":
					request = client.DoFollowStream
				case "poll":
					request = client.DoPollNoRedirectStream
				case "bootstrap":
					attempt, errAttempt := client.NewBootstrapAttempt(ctx)
					if errAttempt != nil {
						t.Fatal(errAttempt)
					}
					defer attempt.CloseIdleConnections()
					request = attempt.DoNoRedirectStream
				}
				response, err := request(ctx, http.MethodGet, server.URL, nil, nil)
				if response != nil && response.Body != nil {
					_ = response.Body.Close()
				}
				if err == nil || !strings.Contains(strings.ToLower(err.Error()), "certificate") {
					t.Fatalf("expected certificate validation failure, got %v", err)
				}
				select {
				case extensions := <-observed:
					if slices.Contains(extensions, 0xca34) {
						t.Error("wire ClientHello still advertises trust_anchors")
					}
				case <-ctx.Done():
					t.Fatal("ClientHello was not observed")
				}
			})
		}
	}
}

func assertChatGPTWebTLSProfileSpec(t *testing.T, profile profiles.ClientProfile) {
	t.Helper()
	got, err := profile.GetClientHelloSpec()
	if err != nil {
		t.Fatal(err)
	}
	want, err := profiles.Chrome_146.GetClientHelloSpec()
	if err != nil {
		t.Fatal(err)
	}
	var extensions []tls.TLSExtension
	removed := 0
	for _, extension := range want.Extensions {
		if generic, ok := extension.(*tls.GenericExtension); ok && generic.Id == 0xca34 {
			removed++
			continue
		}
		extensions = append(extensions, extension)
	}
	if removed != 1 {
		t.Fatalf("dependency profile changed or was mutated: trust-anchor count = %d", removed)
	}
	want.Extensions = extensions
	if !reflect.DeepEqual(got, want) {
		t.Fatal("ClientHello must omit only trust_anchors and preserve all other Chrome 146 parameters")
	}
}
