package chatgptweb

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tls_client "github.com/bogdanfinn/tls-client"
)

func TestBootstrapAttemptHTTP2CancellationIsIsolated(t *testing.T) {
	stopped := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("not HTTP/2: %s", r.Proto)
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		stopped <- struct{}{}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	parent, err := NewRequestClient(t.Context(), DefaultPersona(), "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer parent.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	client, err := parent.NewBootstrapAttempt(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { client.CloseActiveAcquisitionConnections(); client.CloseIdleConnections() }()
	profile, _ := findTLSProfile(client.persona.Profile)
	// Only the local test fixture bypasses certificate validation.
	transport, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), tls_client.WithClientProfile(profile), tls_client.WithCookieJar(client.jar), tls_client.WithTimeoutMilliseconds(0), tls_client.WithInsecureSkipVerify(), tls_client.WithProxyDialerFactory(client.acquisitionTracker.dialerFactory("")))
	if err != nil {
		t.Fatal(err)
	}
	client.noRedirect.CloseIdleConnections()
	client.noRedirect = transport
	response, err := client.DoNoRedirectStream(ctx, "GET", server.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err == nil {
		t.Fatal("blocked read succeeded")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("HTTP/2 socket remained active")
	}
	parent.acquisitionTracker.mu.Lock()
	closed := parent.acquisitionTracker.closed
	parent.acquisitionTracker.mu.Unlock()
	if closed || parent.jar != client.jar {
		t.Fatal("parent transport closed or cookies not shared")
	}
}

type bootstrapBlockedDialer struct{}

func (bootstrapBlockedDialer) Dial(string, string) (net.Conn, error) {
	return nil, errors.New("context lost")
}
func (bootstrapBlockedDialer) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestBootstrapAttemptCancellationReachesProxyDial(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	tracker := newConnectionTracker()
	tracker.lifetime = ctx
	dialer := &trackedConnectionDialer{base: bootstrapBlockedDialer{}, tracker: tracker}
	done := make(chan error, 1)
	go func() { _, err := dialer.DialContext(context.Background(), "tcp", "test:443"); done <- err }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("proxy dial ignored attempt cancellation")
	}
}
