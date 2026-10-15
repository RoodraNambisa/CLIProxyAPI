package chatgptweb

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The subprocess isolates its test certificate trust from all other tests.
func TestChatGPTWebHTTPSConnectTrustedChild(t *testing.T) {
	proxyURL := os.Getenv("CPA_TEST_CONNECT_PROXY")
	if proxyURL == "" {
		return
	}
	der, err := base64.StdEncoding.DecodeString(os.Getenv("CPA_TEST_CONNECT_CERT"))
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	x509.SetFallbackRoots(roots)
	assertZeroTimeoutConnect(t, proxyURL)
}

func TestChatGPTWebConnectZeroTimeout(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "http"
		if secure {
			name = "https"
		}
		t.Run(name, func(t *testing.T) {
			proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodConnect || r.Host != "[2001:db8::1]:80" {
					t.Errorf("unexpected CONNECT: %s %s", r.Method, r.Host)
				}
				if r.Header.Get("Proxy-Authorization") != "Basic "+base64.StdEncoding.EncodeToString([]byte("fixture:password")) {
					t.Error("missing proxy authentication")
					w.WriteHeader(http.StatusProxyAuthRequired)
					return
				}
				connection, buffered, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Error(err)
					return
				}
				defer func() { _ = connection.Close() }()
				// A zero timeout must not expire while the proxy is negotiating.
				time.Sleep(80 * time.Millisecond)
				_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
				_ = buffered.Flush()
				request, errRead := http.ReadRequest(buffered.Reader)
				if errRead != nil {
					return
				}
				_ = request.Body.Close()
				if request.Header.Get("Proxy-Authorization") != "" {
					t.Error("proxy credentials leaked into the tunnel")
				}
				_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
				_ = buffered.Flush()
			}))
			if secure {
				proxy.StartTLS()
			} else {
				proxy.Start()
			}
			defer proxy.Close()
			parsed, _ := url.Parse(proxy.URL)
			parsed.User = url.UserPassword("fixture", "password")
			if !secure {
				assertZeroTimeoutConnect(t, parsed.String())
				return
			}
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			command := exec.CommandContext(t.Context(), executable, "-test.run=^TestChatGPTWebHTTPSConnectTrustedChild$", "-test.timeout=10s")
			command.Env = append(os.Environ(), "CPA_TEST_CONNECT_PROXY="+parsed.String(), "CPA_TEST_CONNECT_CERT="+base64.StdEncoding.EncodeToString(proxy.Certificate().Raw), "GODEBUG=x509usefallbackroots=1")
			if output, errRun := command.CombinedOutput(); errRun != nil {
				t.Fatalf("HTTPS CONNECT failed: %v\n%s", errRun, output)
			}
		})
	}
}

func assertZeroTimeoutConnect(t *testing.T, proxyURL string) {
	t.Helper()
	client, err := NewAccessTokenClient(DefaultPersona(), proxyURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if client.acquisitionTimeout != 0 || client.acquisitionTracker != nil {
		t.Fatal("regression must use the unbudgeted runtime client")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, body, err := client.DoNoRedirect(ctx, http.MethodGet, "http://[2001:db8::1]/", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("unexpected tunnel response: %d", response.StatusCode)
	}
}

func TestChatGPTWebConnectCancellationAndErrors(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		entered, released := make(chan struct{}), make(chan struct{})
		proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer func() { _ = connection.Close(); close(released) }()
			close(entered)
			_, _ = io.Copy(io.Discard, connection)
		}))
		defer proxy.Close()
		client, err := NewClient(DefaultPersona(), proxy.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, _, errRequest := client.DoNoRedirect(ctx, http.MethodGet, "http://127.0.0.1/", nil, nil)
			done <- errRequest
		}()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("CONNECT was not attempted")
		}
		cancel()
		select {
		case errRequest := <-done:
			if !errors.Is(errRequest, context.Canceled) {
				t.Fatalf("expected cancellation, got %v", errRequest)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("request did not cancel")
		}
		select {
		case <-released:
		case <-time.After(3 * time.Second):
			t.Fatal("cancellation leaked proxy connection")
		}
	})
	for _, kind := range []string{"407", "certificate"} {
		t.Run(kind, func(t *testing.T) {
			proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusProxyAuthRequired)
			}))
			if kind == "certificate" {
				proxy.StartTLS()
			} else {
				proxy.Start()
			}
			defer proxy.Close()
			client, err := NewClient(DefaultPersona(), proxy.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			_, _, err = client.DoNoRedirect(ctx, http.MethodGet, "http://127.0.0.1/", nil, nil)
			if err == nil || !strings.Contains(err.Error(), kind) {
				t.Fatalf("expected %s, got %v", kind, err)
			}
			var infrastructure interface{ ProxyInfrastructureError() bool }
			if !errors.As(err, &infrastructure) || !infrastructure.ProxyInfrastructureError() {
				t.Fatalf("lost proxy infrastructure classification: %v", err)
			}
		})
	}
}
