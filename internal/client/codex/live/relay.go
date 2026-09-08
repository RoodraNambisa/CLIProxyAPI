package live

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	log "github.com/sirupsen/logrus"
)

// relayWebsockets streams message bodies with one data writer per connection.
// Control frames use gorilla's serialized WriteControl path, so they can pass
// between fragments without blocking a reader behind the opposite data stream.
// Cancellation closes the transport directly instead of waiting for a peer's
// close handshake; no post-connect network deadline is installed.
func relayWebsockets(ctx context.Context, downstream, upstream *websocket.Conn, keepalive time.Duration) error {
	if downstream == nil || upstream == nil {
		return errors.New("realtime relay requires both connections")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancelCause(ctx)
	var closeOnce sync.Once
	closeConnections := func() {
		closeOnce.Do(func() {
			for _, conn := range []*websocket.Conn{downstream, upstream} {
				if errClose := conn.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
					log.WithError(errClose).Debug("codex realtime: close relay connection")
				}
			}
		})
	}
	stop := context.AfterFunc(ctx, closeConnections)
	defer func() { cancel(nil); stop(); closeConnections() }()
	for _, conn := range []*websocket.Conn{downstream, upstream} {
		conn.SetPingHandler(func(data string) error {
			return conn.WriteControl(websocket.PongMessage, []byte(data), time.Time{})
		})
		conn.SetCloseHandler(func(code int, text string) error {
			return &websocket.CloseError{Code: code, Text: text}
		})
	}
	var activity chan struct{}
	if keepalive > 0 {
		activity = make(chan struct{}, 1)
	}
	var workers sync.WaitGroup
	workers.Go(func() { cancel(copyWebsocket(upstream, downstream, nil, cancel)) })
	workers.Go(func() { cancel(copyWebsocket(downstream, upstream, activity, cancel)) })
	if keepalive > 0 {
		workers.Go(func() {
			timer := time.NewTimer(keepalive)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-activity:
					timer.Reset(keepalive)
				case <-timer.C:
					select {
					case <-activity:
						timer.Reset(keepalive)
						continue
					default:
					}
					if errPing := downstream.WriteControl(websocket.PingMessage, nil, time.Time{}); errPing != nil {
						cancel(errPing)
						return
					}
					timer.Reset(keepalive)
				}
			}
		})
	}
	workers.Wait()
	return context.Cause(ctx)
}

func copyWebsocket(destination, source *websocket.Conn, activity chan<- struct{}, cancel context.CancelCauseFunc) error {
	for {
		kind, reader, errRead := source.NextReader()
		if errRead != nil {
			return errRead
		}
		writer, errWriter := destination.NextWriter(kind)
		if errWriter != nil {
			return errWriter
		}
		var output io.Writer = writer
		if activity != nil {
			output = websocketActivityWriter{Writer: writer, activity: activity}
		}
		_, errCopy := io.Copy(output, reader)
		if errCopy != nil {
			cancel(errCopy)
		}
		errClose := writer.Close()
		if errCopy != nil || errClose != nil {
			return errors.Join(errCopy, errClose)
		}
	}
}

type websocketActivityWriter struct {
	io.Writer
	activity chan<- struct{}
}

func (w websocketActivityWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	if n > 0 {
		select {
		case w.activity <- struct{}{}:
		default:
		}
	}
	return n, err
}
