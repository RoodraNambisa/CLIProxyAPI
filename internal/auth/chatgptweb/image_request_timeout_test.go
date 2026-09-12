package chatgptweb

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	tls_client "github.com/bogdanfinn/tls-client"
)

type stuckCloseBody struct{ released <-chan struct{} }

func (b stuckCloseBody) Close() error { <-b.released; return nil }

func TestImageRequestTimeoutPollRetiresDuringBlockedClose(t *testing.T) {
	client, err := NewClient(DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	poll, err := client.currentPollTransport()
	if err != nil {
		t.Fatal(err)
	}
	conn, peer := net.Pipe()
	defer peer.Close()
	if _, err = poll.tracker.track(conn); err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, peer); close(released) }()
	control := &pollRequestControl{client: client, generation: poll.generation, grace: time.Millisecond, done: make(chan struct{}), body: stuckCloseBody{released: released}}
	returned := make(chan struct{})
	go func() { control.cancel(); close(returned) }()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Close prevented transport retirement")
	}
}

func TestImageRequestTimeoutWebHTTP2Cancellation(t *testing.T) {
	stopped := make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("want HTTP/2, got %s", r.Proto)
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		stopped <- struct{}{}
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	client, err := NewRequestClient(ctx, DefaultPersona(), "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	profile, _ := findTLSProfile(client.persona.Profile)
	// Trust only the fixture's self-signed server in this test client.
	transport, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), tls_client.WithClientProfile(profile), tls_client.WithCookieJar(client.jar), tls_client.WithTimeoutMilliseconds(0), tls_client.WithInsecureSkipVerify(), tls_client.WithProxyDialerFactory(client.acquisitionTracker.dialerFactory("")))
	if err != nil {
		t.Fatal(err)
	}
	client.follow = transport
	client.noRedirect = transport
	resp, err := client.DoNoRedirectStream(ctx, "GET", server.URL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { _, err := io.ReadAll(resp.Body); _ = resp.Body.Close(); finished <- err }()
	cancel()
	select {
	case err := <-finished:
		if err == nil {
			t.Fatal("blocked read returned success")
		}
	case <-time.After(time.Second):
		t.Fatal("read stayed blocked")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("socket stayed active")
	}
}
