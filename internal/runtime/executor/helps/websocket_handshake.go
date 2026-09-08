package helps

import (
	"context"
	"errors"
	"net"
	"net/http"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// DialWebsocketHandshake closes an acquired socket if caller cancellation
// interrupts the HTTP upgrade. Gorilla otherwise uses only its handshake
// deadline for this phase. The original timeouts and dial functions remain.
// Successful connections are detached from this temporary cancellation link.
func DialWebsocketHandshake(ctx context.Context, dialer *websocket.Dialer, target string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if dialer == nil {
		return nil, nil, errors.New("websocket dialer is nil")
	}
	if errCtx := context.Cause(ctx); errCtx != nil {
		return nil, nil, errCtx
	}
	copy := *dialer
	var stops []func() bool
	track := func(conn net.Conn) {
		if conn == nil {
			return
		}
		// Use the caller context, not Gorilla's internally cancelled timeout context.
		stops = append(stops, context.AfterFunc(ctx, func() {
			if errClose := conn.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
				log.WithError(errClose).Debug("websocket handshake: close cancelled connection")
			}
		}))
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	dialContext := copy.NetDialContext
	if dialContext == nil {
		if dial := copy.NetDial; dial != nil {
			dialContext = func(_ context.Context, network, address string) (net.Conn, error) { return dial(network, address) }
		} else {
			dialContext = (&net.Dialer{}).DialContext
		}
	}
	copy.NetDialContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
		conn, errDial := dialContext(dialCtx, network, address)
		track(conn)
		return conn, errDial
	}
	if dialTLS := copy.NetDialTLSContext; dialTLS != nil {
		copy.NetDialTLSContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
			conn, errDial := dialTLS(dialCtx, network, address)
			track(conn)
			return conn, errDial
		}
	}
	conn, response, errDial := copy.DialContext(ctx, target, headers)
	if errCtx := context.Cause(ctx); errCtx != nil {
		if conn != nil {
			errCtx = errors.Join(errCtx, conn.Close())
		}
		return nil, response, errCtx
	}
	return conn, response, errDial
}
