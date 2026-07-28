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

type acquisitionConnectionTracker struct {
	mu          sync.Mutex
	connections map[*trackedAcquisitionConnection]struct{}
	closed      bool
}

type trackedAcquisitionConnection struct {
	net.Conn
	tracker *acquisitionConnectionTracker
	once    sync.Once
}

type trackedAcquisitionDialer struct {
	base    proxyutil.ContextDialer
	tracker *acquisitionConnectionTracker
}

func newAcquisitionConnectionTracker() *acquisitionConnectionTracker {
	return &acquisitionConnectionTracker{
		connections: make(map[*trackedAcquisitionConnection]struct{}),
	}
}

func (tracker *acquisitionConnectionTracker) dialerFactory(configuredProxyURL string) tls_client.ProxyDialerFactory {
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
		return &trackedAcquisitionDialer{base: base, tracker: tracker}, nil
	}
}

func (tracker *acquisitionConnectionTracker) track(connection net.Conn) (net.Conn, error) {
	if tracker == nil || connection == nil {
		return connection, nil
	}
	tracked := &trackedAcquisitionConnection{Conn: connection, tracker: tracker}
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

func (tracker *acquisitionConnectionTracker) closeAll() {
	if tracker == nil {
		return
	}
	tracker.mu.Lock()
	tracker.closed = true
	connections := make([]*trackedAcquisitionConnection, 0, len(tracker.connections))
	for connection := range tracker.connections {
		connections = append(connections, connection)
	}
	clear(tracker.connections)
	tracker.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (connection *trackedAcquisitionConnection) Close() error {
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

func (dialer *trackedAcquisitionDialer) Dial(network, address string) (net.Conn, error) {
	return dialer.DialContext(context.Background(), network, address)
}

func (dialer *trackedAcquisitionDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if dialer == nil || dialer.base == nil {
		return nil, net.ErrClosed
	}
	connection, errDial := dialer.base.DialContext(ctx, network, address)
	if errDial != nil {
		return nil, errDial
	}
	return dialer.tracker.track(connection)
}
