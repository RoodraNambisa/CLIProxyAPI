package live

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"strings"
	"sync"

	"golang.org/x/net/proxy"
)

const maxUnauthenticatedTCPConns = 4

type mediaProxyError struct{ cause error }

func (*mediaProxyError) Error() string   { return "realtime media TCP proxy failed" }
func (e *mediaProxyError) Unwrap() error { return e.cause }

type tcpCandidateTunnel struct {
	listener                     net.Listener
	target                       netip.AddrPort
	dialer                       proxy.ContextDialer
	expectedUser, remotePassword string
	ctx                          context.Context
	cancel                       context.CancelFunc
	mu                           sync.Mutex
	closed, claimed              bool
	connections                  map[net.Conn]struct{}
	validationSlots              chan struct{}
	stop                         func() bool
	workers                      sync.WaitGroup
	closeOnce                    sync.Once
	closeErr                     error
	failureOnce                  sync.Once
	onFailure                    func(error)
}

// Only the owning media session can cancel this listener. No request body or
// Gin context belongs in parent; the fixed target comes from validated SDP.
func newTCPCandidateTunnel(parent context.Context, target netip.AddrPort, dialer proxy.ContextDialer, expectedUser, remotePassword string, onFailure func(error)) (*tcpCandidateTunnel, error) {
	target = netip.AddrPortFrom(target.Addr().Unmap(), target.Port())
	if !isPublicProxyTarget(target.Addr()) || target.Port() != 443 {
		return nil, errors.New("realtime TCP proxy target is not allowed")
	}
	if dialer == nil || strings.TrimSpace(expectedUser) == "" || strings.TrimSpace(remotePassword) == "" {
		return nil, errors.New("realtime TCP proxy requires a dialer and ICE credentials")
	}
	if parent == nil {
		return nil, errors.New("realtime TCP proxy requires a session context")
	}
	if err := context.Cause(parent); err != nil {
		return nil, err
	}
	network, address := "tcp4", "127.0.0.1:0"
	if target.Addr().Is6() {
		network, address = "tcp6", "[::1]:0"
	}
	listener, errListen := net.Listen(network, address)
	if errListen != nil {
		return nil, &mediaProxyError{cause: errListen}
	}
	ctx, cancel := context.WithCancel(parent)
	tunnel := &tcpCandidateTunnel{listener: listener, target: target, dialer: dialer, expectedUser: expectedUser, remotePassword: remotePassword,
		ctx: ctx, cancel: cancel, connections: make(map[net.Conn]struct{}), validationSlots: make(chan struct{}, maxUnauthenticatedTCPConns), onFailure: onFailure}
	tunnel.workers.Add(1)
	go tunnel.accept()
	tunnel.mu.Lock()
	tunnel.stop = context.AfterFunc(ctx, func() { _ = tunnel.Close() })
	tunnel.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		_ = tunnel.Close()
		return nil, err
	}
	return tunnel, nil
}

func (t *tcpCandidateTunnel) accept() {
	defer t.workers.Done()
	for {
		client, errAccept := t.listener.Accept()
		if errAccept != nil {
			if !errors.Is(errAccept, net.ErrClosed) {
				t.fail(errAccept)
			}
			return
		}
		if !t.track(client, true) {
			_ = client.Close()
			continue
		}
		select {
		case t.validationSlots <- struct{}{}:
			t.workers.Go(func() { defer func() { <-t.validationSlots }(); t.forward(client) })
		default:
			t.untrack(client)
		}
	}
}

func (t *tcpCandidateTunnel) forward(client net.Conn) {
	defer t.untrack(client)
	frame, errFrame := readValidatedICEBindingFrame(client, t.expectedUser, t.remotePassword)
	if errFrame != nil || !t.claim(client) {
		return
	}
	upstream, errDial := t.dialer.DialContext(t.ctx, "tcp", t.target.String())
	if errDial != nil {
		if upstream != nil {
			_ = upstream.Close()
		}
		t.fail(errDial)
		return
	}
	if upstream == nil {
		t.fail(errors.New("media proxy returned no connection"))
		return
	}
	if !t.track(upstream, false) {
		_ = upstream.Close()
		return
	}
	defer t.untrack(upstream)
	if errWrite := writeMediaTCP(upstream, frame); errWrite != nil {
		t.fail(errWrite)
		return
	}
	finished := make(chan error, 2)
	copyStream := func(destination, source net.Conn) { _, errCopy := io.Copy(destination, source); finished <- errCopy }
	go copyStream(upstream, client)
	go copyStream(client, upstream)
	errCopy := <-finished
	_ = client.Close()
	_ = upstream.Close()
	<-finished
	if errCopy == nil {
		errCopy = io.EOF
	}
	t.fail(errCopy)
}

func (t *tcpCandidateTunnel) claim(client net.Conn) bool {
	t.mu.Lock()
	if t.closed || t.claimed {
		t.mu.Unlock()
		return false
	}
	t.claimed = true
	others := make([]net.Conn, 0, len(t.connections))
	for connection := range t.connections {
		if connection != client {
			others = append(others, connection)
		}
	}
	t.mu.Unlock()
	_ = t.listener.Close()
	// A successful claim makes every other pending handshake unnecessary.
	for _, connection := range others {
		t.untrack(connection)
	}
	return true
}

func (t *tcpCandidateTunnel) track(connection net.Conn, inbound bool) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || (inbound && t.claimed) {
		return false
	}
	t.connections[connection] = struct{}{}
	return true
}

func (t *tcpCandidateTunnel) untrack(connection net.Conn) {
	t.mu.Lock()
	delete(t.connections, connection)
	t.mu.Unlock()
	_ = connection.Close()
}

func (t *tcpCandidateTunnel) fail(cause error) {
	if context.Cause(t.ctx) != nil {
		return
	}
	t.failureOnce.Do(func() {
		if t.onFailure != nil {
			go t.onFailure(&mediaProxyError{cause: cause})
		}
	})
}

func (t *tcpCandidateTunnel) Close() error {
	if t == nil {
		return nil
	}
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		stop := t.stop
		connections := make([]net.Conn, 0, len(t.connections))
		for connection := range t.connections {
			connections = append(connections, connection)
		}
		t.connections = make(map[net.Conn]struct{})
		t.mu.Unlock()
		if stop != nil {
			stop()
		}
		t.cancel()
		var closeErrors []error
		if err := t.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
		for _, connection := range connections {
			if err := connection.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				closeErrors = append(closeErrors, err)
			}
		}
		t.workers.Wait()
		t.closeErr = errors.Join(closeErrors...)
	})
	return t.closeErr
}

func writeMediaTCP(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
