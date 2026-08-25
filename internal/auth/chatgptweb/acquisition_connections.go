package chatgptweb

import (
	"context"
	"net"
	"strings"
	"sync"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/proxyutil"
	"golang.org/x/net/proxy"
)

type connectionTracker struct {
	mu          sync.Mutex
	connections map[*trackedConnection]struct{}
	closed      bool
}

type trackedConnection struct {
	net.Conn
	tracker *connectionTracker
	once    sync.Once
}

type trackedConnectionDialer struct {
	base    proxyutil.ContextDialer
	tracker *connectionTracker
}

func newConnectionTracker() *connectionTracker {
	return &connectionTracker{
		connections: make(map[*trackedConnection]struct{}),
	}
}

func (tracker *connectionTracker) dialerFactory(configuredProxyURL string) tls_client.ProxyDialerFactory {
	configuredProxyURL = strings.TrimSpace(configuredProxyURL)
	return func(
		_ string,
		_ time.Duration,
		_ *net.TCPAddr,
		_ fhttp.Header,
		_ tls_client.Logger,
	) (proxy.ContextDialer, error) {
		raw := configuredProxyURL
		if raw == "" {
			raw = "direct"
		}
		base, _, errBuild := proxyutil.BuildContextDialer(raw)
		if errBuild != nil {
			return nil, errBuild
		}
		return &trackedConnectionDialer{base: base, tracker: tracker}, nil
	}
}

func (tracker *connectionTracker) track(connection net.Conn) (net.Conn, error) {
	if tracker == nil || connection == nil {
		return connection, nil
	}
	tracked := &trackedConnection{Conn: connection, tracker: tracker}
	tracker.mu.Lock()
	if tracker.closed {
		tracker.mu.Unlock()
		_ = connection.Close()
		return nil, net.ErrClosed
	}
	tracker.connections[tracked] = struct{}{}
	tracker.mu.Unlock()
	return tracked, nil
}

func (tracker *connectionTracker) closeAll() {
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	tracker.closed = true
	connections := make([]*trackedConnection, 0, len(tracker.connections))
	for connection := range tracker.connections {
		connections = append(connections, connection)
	}
	clear(tracker.connections)
	tracker.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (connection *trackedConnection) Close() error {
	if connection == nil {
		return nil
	}
	var errClose error
	connection.once.Do(func() {
		if connection.tracker != nil {
			connection.tracker.mu.Lock()
			delete(connection.tracker.connections, connection)
			connection.tracker.mu.Unlock()
		}
		if connection.Conn != nil {
			errClose = connection.Conn.Close()
		}
	})
	return errClose
}

func (dialer *trackedConnectionDialer) Dial(network, address string) (net.Conn, error) {
	return dialer.DialContext(context.Background(), network, address)
}

func (dialer *trackedConnectionDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if dialer == nil || dialer.base == nil {
		return nil, net.ErrClosed
	}
	connection, errDial := dialer.base.DialContext(ctx, network, address)
	if errDial != nil {
		return nil, errDial
	}
	return dialer.tracker.track(connection)
}
