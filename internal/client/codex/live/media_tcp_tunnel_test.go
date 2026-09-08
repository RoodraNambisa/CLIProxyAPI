package live

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"
)

type mediaTunnelDialer struct {
	dial func(context.Context, string, string) (net.Conn, error)
}

func (d mediaTunnelDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return d.dial(ctx, network, address)
}

func mediaTunnelFixture(t *testing.T, dialer mediaTunnelDialer, onFailure func(error)) *tcpCandidateTunnel {
	t.Helper()
	tunnel, err := newTCPCandidateTunnel(t.Context(), netip.MustParseAddrPort("20.42.0.20:443"), dialer, "remote:local", "fixture-password", onFailure)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tunnel.Close(); err != nil {
			t.Error(err)
		}
	})
	if address, ok := tunnel.listener.Addr().(*net.TCPAddr); !ok || !address.IP.IsLoopback() {
		t.Fatal("candidate listener was not restricted to loopback")
	}
	return tunnel
}

func mediaTunnelClient(t *testing.T, tunnel *tcpCandidateTunnel, authenticated bool) net.Conn {
	t.Helper()
	client, err := net.Dial("tcp", tunnel.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if authenticated {
		if err := writeMediaTCP(client, mediaTestICEFrame(t, "remote:local", "fixture-password", true)); err != nil {
			t.Fatal(err)
		}
	}
	return client
}

func TestMediaTCPTunnelAuthenticatesThenForwardsFixedTarget(t *testing.T) {
	upstreams := make(chan net.Conn, 1)
	var dials atomic.Int32
	tunnel := mediaTunnelFixture(t, mediaTunnelDialer{dial: func(_ context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		if network != "tcp" || address != "20.42.0.20:443" {
			t.Error("media proxy target was not fixed")
		}
		local, remote := net.Pipe()
		upstreams <- remote
		return local, nil
	}}, nil)
	unauthenticated := mediaTunnelClient(t, tunnel, false)
	if err := writeMediaTCP(unauthenticated, mediaTestICEFrame(t, "wrong", "fixture-password", true)); err != nil {
		t.Fatal(err)
	}
	if _, err := unauthenticated.Read(make([]byte, 1)); err == nil {
		t.Fatal("invalid ICE handshake was not closed")
	}
	if dials.Load() != 0 {
		t.Fatal("invalid ICE handshake dialed the proxy")
	}
	client := mediaTunnelClient(t, tunnel, true)
	var upstream net.Conn
	select {
	case upstream = <-upstreams:
	case <-time.After(time.Second):
		t.Fatal("authenticated connection did not dial")
	}
	t.Cleanup(func() { _ = upstream.Close() })
	frame, err := readValidatedICEBindingFrame(upstream, "remote:local", "fixture-password")
	if err != nil || len(frame) == 0 {
		t.Fatal("first frame did not reach upstream unchanged")
	}
	for _, direction := range []struct{ writer, reader net.Conn }{{client, upstream}, {upstream, client}} {
		done := make(chan error, 1)
		go func() { done <- writeMediaTCP(direction.writer, []byte("media-fixture")) }()
		got := make([]byte, len("media-fixture"))
		if _, err := io.ReadFull(direction.reader, got); err != nil || string(got) != "media-fixture" {
			t.Fatal("native media bytes did not cross the tunnel")
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if dials.Load() != 1 {
		t.Fatal("one candidate made more than one proxy dial")
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMediaTCPTunnelCancellationJoinsDialAndPendingHandshakes(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	tunnel := mediaTunnelFixture(t, mediaTunnelDialer{dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
		close(started)
		<-ctx.Done()
		close(cancelled)
		return nil, context.Cause(ctx)
	}}, nil)
	pending := mediaTunnelClient(t, tunnel, false)
	_ = mediaTunnelClient(t, tunnel, true)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("proxy dial did not start")
	}
	// The successful candidate claim closes other pending local handshakes.
	if _, err := pending.Read(make([]byte, 1)); err == nil {
		t.Fatal("unused candidate handshake survived a claim")
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	default:
		t.Fatal("Close returned before dial cancellation completed")
	}
	tunnel.mu.Lock()
	count := len(tunnel.connections)
	tunnel.mu.Unlock()
	if count != 0 {
		t.Fatal("closed tunnel retained sockets")
	}
}

func TestMediaTCPTunnelFailureCanCloseWithoutFallbackOrLostCause(t *testing.T) {
	cause := errors.New("fixture-proxy-failure")
	var dials atomic.Int32
	finished := make(chan struct{})
	var tunnel *tcpCandidateTunnel
	tunnel = mediaTunnelFixture(t, mediaTunnelDialer{dial: func(context.Context, string, string) (net.Conn, error) { dials.Add(1); return nil, cause }}, func(err error) {
		if !errors.Is(err, cause) || err.Error() == cause.Error() {
			t.Error("media proxy failure lost its safe error chain")
		}
		_ = tunnel.Close()
		close(finished)
	})
	_ = mediaTunnelClient(t, tunnel, true)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("failure cleanup blocked on its own worker")
	}
	if dials.Load() != 1 {
		t.Fatal("proxy failure retried or fell back")
	}
}

func TestMediaTCPTunnelCloseReleasesPartialSTUNReaders(t *testing.T) {
	var dials atomic.Int32
	tunnel := mediaTunnelFixture(t, mediaTunnelDialer{dial: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("unexpected")
	}}, nil)
	for range maxUnauthenticatedTCPConns {
		client := mediaTunnelClient(t, tunnel, false)
		if _, err := client.Write([]byte{0}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tunnel.Close(); err != nil {
		t.Fatal(err)
	}
	if dials.Load() != 0 {
		t.Fatal("partial STUN frame caused a dial")
	}
}

func TestMediaTCPTunnelParentCancellationClosesPendingSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dialer := mediaTunnelDialer{dial: func(context.Context, string, string) (net.Conn, error) {
		t.Error("unexpected dial")
		return nil, errors.New("unexpected")
	}}
	tunnel, err := newTCPCandidateTunnel(ctx, netip.MustParseAddrPort("20.42.0.20:443"), dialer, "remote:local", "fixture-password", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tunnel.Close() })
	client := mediaTunnelClient(t, tunnel, false)
	closed := make(chan error, 1)
	go func() { _, err := client.Read(make([]byte, 1)); closed <- err }()
	cancel()
	select {
	case err := <-closed:
		if err == nil {
			t.Fatal("parent cancellation left socket open")
		}
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not close pending socket")
	}
	if _, err := newTCPCandidateTunnel(ctx, netip.MustParseAddrPort("20.42.0.20:443"), dialer, "remote:local", "fixture-password", nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled constructor created a listener")
	}
}

type mediaShortWriter struct {
	buffer bytes.Buffer
	zero   bool
}

func (w *mediaShortWriter) Write(data []byte) (int, error) {
	if w.zero {
		return 0, nil
	}
	return w.buffer.Write(data[:min(len(data), 2)])
}

func TestMediaTCPWriterHandlesPartialAndZeroWrites(t *testing.T) {
	writer := &mediaShortWriter{}
	if err := writeMediaTCP(writer, []byte("fixture")); err != nil || writer.buffer.String() != "fixture" {
		t.Fatal("partial writes changed bytes")
	}
	if err := writeMediaTCP(&mediaShortWriter{zero: true}, []byte("fixture")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatal("zero write did not terminate")
	}
}
