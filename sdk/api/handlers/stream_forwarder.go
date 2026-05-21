package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

type StreamForwardOptions struct {
	// KeepAliveInterval overrides the configured streaming keep-alive interval.
	// If nil, the configured default is used. If set to <= 0, keep-alives are disabled.
	KeepAliveInterval *time.Duration

	// FlushInterval batches response flushes for up to this duration.
	// If nil or <= 0, every chunk is flushed immediately.
	FlushInterval *time.Duration

	// FlushMinBytes flushes once at least this many bytes have been written
	// since the previous flush. <= 0 disables the byte threshold.
	FlushMinBytes int

	// WriteChunk writes a single data chunk to the response body. It should not flush.
	WriteChunk func(chunk []byte)

	// WriteTerminalError writes an error payload to the response body when streaming fails
	// after headers have already been committed. It should not flush.
	WriteTerminalError func(errMsg *interfaces.ErrorMessage)

	// WriteDone optionally writes a terminal marker when the upstream data channel closes
	// without an error (e.g. OpenAI's `[DONE]`). It should not flush.
	WriteDone func()

	// WriteKeepAlive optionally writes a keep-alive heartbeat. It should not flush.
	// When nil, a standard SSE comment heartbeat is used.
	WriteKeepAlive func()
}

func (h *BaseAPIHandler) ForwardStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage, opts StreamForwardOptions) {
	if c == nil {
		return
	}
	if cancel == nil {
		return
	}

	writeChunk := opts.WriteChunk
	if writeChunk == nil {
		writeChunk = func([]byte) {}
	}

	writeKeepAlive := opts.WriteKeepAlive
	if writeKeepAlive == nil {
		writeKeepAlive = func() {
			_, _ = c.Writer.Write([]byte(": keep-alive\n\n"))
		}
	}

	keepAliveInterval := StreamingKeepAliveInterval(h.Cfg)
	if opts.KeepAliveInterval != nil {
		keepAliveInterval = *opts.KeepAliveInterval
	}
	var keepAlive *time.Ticker
	var keepAliveC <-chan time.Time
	if keepAliveInterval > 0 {
		keepAlive = time.NewTicker(keepAliveInterval)
		defer keepAlive.Stop()
		keepAliveC = keepAlive.C
	}

	var terminalErr *interfaces.ErrorMessage
	var flushTimer *time.Timer
	var flushC <-chan time.Time
	unflushedBytes := 0

	stopFlushTimer := func() {
		if flushTimer == nil {
			return
		}
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
		flushTimer = nil
		flushC = nil
	}
	defer stopFlushTimer()

	flushInterval := time.Duration(0)
	if opts.FlushInterval != nil {
		flushInterval = *opts.FlushInterval
	}
	batchedFlush := flushInterval > 0 || opts.FlushMinBytes > 0

	flushPending := func() {
		if unflushedBytes <= 0 {
			stopFlushTimer()
			return
		}
		flusher.Flush()
		unflushedBytes = 0
		stopFlushTimer()
	}

	noteWrite := func(size int) {
		if size <= 0 {
			return
		}
		if !batchedFlush {
			flusher.Flush()
			return
		}
		unflushedBytes += size
		if opts.FlushMinBytes > 0 && unflushedBytes >= opts.FlushMinBytes {
			flushPending()
			return
		}
		if flushInterval <= 0 || flushTimer != nil {
			return
		}
		flushTimer = time.NewTimer(flushInterval)
		flushC = flushTimer.C
	}

	for {
		select {
		case <-c.Request.Context().Done():
			cancel(c.Request.Context().Err())
			return
		case chunk, ok := <-data:
			if !ok {
				// Prefer surfacing a terminal error if one is pending.
				if terminalErr == nil {
					select {
					case errMsg, ok := <-errs:
						if ok && errMsg != nil {
							terminalErr = errMsg
						}
					default:
					}
				}
				if terminalErr != nil {
					flushPending()
					if opts.WriteTerminalError != nil {
						opts.WriteTerminalError(terminalErr)
					}
					flusher.Flush()
					cancel(terminalErr.Error)
					return
				}
				if opts.WriteDone != nil {
					opts.WriteDone()
				}
				flushPending()
				flusher.Flush()
				cancel(nil)
				return
			}
			writeChunk(chunk)
			noteWrite(len(chunk))
		case errMsg, ok := <-errs:
			if !ok {
				continue
			}
			if errMsg != nil {
				terminalErr = errMsg
				flushPending()
				if opts.WriteTerminalError != nil {
					opts.WriteTerminalError(errMsg)
					flusher.Flush()
				}
			}
			var execErr error
			if errMsg != nil {
				execErr = errMsg.Error
			}
			cancel(execErr)
			return
		case <-keepAliveC:
			flushPending()
			writeKeepAlive()
			flusher.Flush()
		case <-flushC:
			flushPending()
		}
	}
}
