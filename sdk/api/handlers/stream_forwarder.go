package handlers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

const sseDataJSONMaxPendingBytes = 50 << 20

type sseDataJSONFramer struct {
	pending         []byte
	maxPendingBytes int
}

func (framer *sseDataJSONFramer) Feed(chunk []byte) ([][]byte, error) {
	if framer == nil || len(chunk) == 0 {
		return nil, nil
	}
	var frames [][]byte
	appendData := func(data []byte) error {
		limit := framer.maxPendingBytes
		if limit <= 0 {
			limit = sseDataJSONMaxPendingBytes
		}
		consumeComplete := func() error {
			consumed := 0
			for consumed < len(framer.pending) {
				frameLen := SSEFrameLen(framer.pending[consumed:])
				if frameLen == 0 {
					break
				}
				frame := framer.pending[consumed : consumed+frameLen]
				if len(bytes.TrimSpace(frame)) == 0 {
					consumed += frameLen
					continue
				}
				if err := validateSSEDataJSONFrame(frame); err != nil {
					return err
				}
				frames = append(frames, append([]byte(nil), frame...))
				consumed += frameLen
			}
			if consumed > 0 {
				copy(framer.pending, framer.pending[consumed:])
				framer.pending = framer.pending[:len(framer.pending)-consumed]
			}
			return nil
		}
		if err := consumeComplete(); err != nil {
			return err
		}
		for len(data) > 0 {
			room := limit - len(framer.pending)
			if room <= 0 {
				return fmt.Errorf("upstream Responses SSE event exceeds %d bytes", limit)
			}
			take := len(data)
			if take > room {
				take = room
			}
			framer.pending = append(framer.pending, data[:take]...)
			data = data[take:]
			if err := consumeComplete(); err != nil {
				return err
			}
		}
		return nil
	}
	if sseDataJSONNeedsLineBreak(framer.pending, chunk) {
		if err := appendData([]byte{'\n'}); err != nil {
			return nil, err
		}
	}
	if err := appendData(chunk); err != nil {
		return nil, err
	}
	if sseDataJSONCanEmitWithoutDelimiter(framer.pending) {
		frames = append(frames, append([]byte(nil), framer.pending...))
		framer.pending = framer.pending[:0]
	}
	return frames, nil
}

func (framer *sseDataJSONFramer) Finish() ([][]byte, error) {
	if framer == nil || len(framer.pending) == 0 {
		return nil, nil
	}
	if len(bytes.TrimSpace(framer.pending)) == 0 {
		framer.pending = framer.pending[:0]
		return nil, nil
	}
	if err := validateSSEDataJSONFrame(framer.pending); err != nil {
		return nil, err
	}
	frame := append([]byte(nil), framer.pending...)
	framer.pending = framer.pending[:0]
	return [][]byte{frame}, nil
}

// SSEFrameLen returns the first complete SSE frame length. SSE permits CR,
// LF, and CRLF line endings.
func SSEFrameLen(data []byte) int {
	lineStart := 0
	for index := 0; index < len(data); {
		if data[index] != '\r' && data[index] != '\n' {
			index++
			continue
		}
		if data[index] == '\r' && index+1 == len(data) {
			return 0
		}
		lineEnd := index + 1
		if data[index] == '\r' && lineEnd < len(data) && data[lineEnd] == '\n' {
			lineEnd++
		}
		if index == lineStart {
			return lineEnd
		}
		lineStart = lineEnd
		index = lineEnd
	}
	return 0
}

// SplitSSELines splits SSE data on CR, LF, or CRLF without retaining endings.
func SplitSSELines(data []byte) [][]byte {
	lines := make([][]byte, 0, bytes.Count(data, []byte{'\n'})+bytes.Count(data, []byte{'\r'})+1)
	lineStart := 0
	for index := 0; index < len(data); {
		if data[index] != '\r' && data[index] != '\n' {
			index++
			continue
		}
		lines = append(lines, data[lineStart:index])
		index++
		if data[index-1] == '\r' && index < len(data) && data[index] == '\n' {
			index++
		}
		lineStart = index
	}
	lines = append(lines, data[lineStart:])
	return lines
}

func sseDataJSONPayload(frame []byte) ([]byte, bool) {
	var payload []byte
	found := false
	for _, line := range SplitSSELines(frame) {
		line = bytes.TrimSpace(line)
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		if found {
			payload = append(payload, '\n')
		}
		payload = append(payload, bytes.TrimSpace(line[len("data:"):])...)
		found = true
	}
	return payload, found
}

func validateSSEDataJSONFrame(frame []byte) error {
	payload, found := sseDataJSONPayload(frame)
	if !found || len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || json.Valid(payload) {
		return nil
	}
	const maxPreviewBytes = 512
	preview := payload
	if len(preview) > maxPreviewBytes {
		preview = preview[:maxPreviewBytes]
	}
	return fmt.Errorf("invalid upstream Responses SSE data JSON (len=%d): %q", len(payload), preview)
}

func sseDataJSONCanEmitWithoutDelimiter(frame []byte) bool {
	if len(frame) > 0 && frame[len(frame)-1] == '\r' {
		return false
	}
	payload, found := sseDataJSONPayload(frame)
	return found && (len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) || json.Valid(payload))
}

func sseDataJSONFrameHasSemanticData(frame []byte) bool {
	payload, found := sseDataJSONPayload(frame)
	return found && len(bytes.TrimSpace(payload)) > 0
}

func sseDataJSONNeedsLineBreak(pending, chunk []byte) bool {
	if len(pending) == 0 || len(chunk) == 0 ||
		bytes.HasSuffix(pending, []byte("\n")) || bytes.HasSuffix(pending, []byte("\r")) ||
		chunk[0] == '\n' || chunk[0] == '\r' {
		return false
	}
	trimmed := bytes.TrimLeft(chunk, " \t")
	if payload, found := sseDataJSONPayload(pending); found &&
		len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) && !json.Valid(payload) {
		return false
	}
	for _, prefix := range [][]byte{[]byte("data:"), []byte("event:"), []byte("id:"), []byte("retry:"), []byte(":")} {
		if !bytes.HasPrefix(trimmed, prefix) {
			continue
		}
		return true
	}
	return false
}

// SSECommentsOnly reports whether a chunk contains only SSE comment lines.
func SSECommentsOnly(chunk []byte) bool {
	if SSEFrameLen(chunk) != len(chunk) {
		return false
	}
	found := false
	for _, line := range SplitSSELines(chunk) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if line[0] != ':' {
			return false
		}
		found = true
	}
	return found
}

// SSEBootstrapDetector reports whether an arbitrary stream chunk contains
// non-comment SSE data while preserving line state across chunk boundaries.
type SSEBootstrapDetector struct {
	lineKind     uint8
	linePrefix   []byte
	lineHasValue bool
	frameHasData bool
	previousCR   bool
	committed    bool
}

// Feed reports whether the stream has emitted non-comment data.
func (detector *SSEBootstrapDetector) Feed(chunk []byte) bool {
	if detector == nil || detector.committed {
		return detector != nil && detector.committed
	}
	for _, value := range chunk {
		if detector.previousCR {
			detector.previousCR = false
			if value == '\n' {
				continue
			}
		}
		switch value {
		case '\r':
			if detector.finishLine() {
				return true
			}
			detector.previousCR = true
		case '\n':
			if detector.finishLine() {
				return true
			}
		default:
			if detector.consumeLineByte(value) {
				return true
			}
		}
	}
	return false
}

const (
	sseBootstrapLineUnknown uint8 = iota
	sseBootstrapLineComment
	sseBootstrapLineData
	sseBootstrapLineControl
	sseBootstrapLineGeneric
)

func (detector *SSEBootstrapDetector) consumeLineByte(value byte) bool {
	switch detector.lineKind {
	case sseBootstrapLineComment, sseBootstrapLineControl:
		return false
	case sseBootstrapLineData:
		if value != ' ' && value != '\t' {
			detector.lineHasValue = true
		}
		return false
	case sseBootstrapLineGeneric:
		return true
	}
	if len(detector.linePrefix) == 0 && (value == ' ' || value == '\t') {
		return false
	}
	detector.linePrefix = append(detector.linePrefix, value)
	switch {
	case bytes.Equal(detector.linePrefix, []byte(":")):
		detector.lineKind = sseBootstrapLineComment
		detector.linePrefix = detector.linePrefix[:0]
	case bytes.Equal(detector.linePrefix, []byte("data:")):
		detector.lineKind = sseBootstrapLineData
		detector.linePrefix = detector.linePrefix[:0]
	case bytes.Equal(detector.linePrefix, []byte("event:")),
		bytes.Equal(detector.linePrefix, []byte("id:")),
		bytes.Equal(detector.linePrefix, []byte("retry:")):
		detector.lineKind = sseBootstrapLineControl
		detector.linePrefix = detector.linePrefix[:0]
	case sseBootstrapKnownPrefix(detector.linePrefix):
		return false
	default:
		detector.lineKind = sseBootstrapLineGeneric
		detector.committed = true
		return true
	}
	return false
}

func (detector *SSEBootstrapDetector) finishLine() bool {
	lineEmpty := detector.lineKind == sseBootstrapLineUnknown && len(detector.linePrefix) == 0
	if detector.lineKind == sseBootstrapLineData && detector.lineHasValue {
		detector.frameHasData = true
	}
	detector.lineKind = sseBootstrapLineUnknown
	detector.linePrefix = detector.linePrefix[:0]
	detector.lineHasValue = false
	if !lineEmpty {
		return false
	}
	if detector.frameHasData {
		detector.committed = true
		return true
	}
	detector.frameHasData = false
	return false
}

func sseBootstrapKnownPrefix(prefix []byte) bool {
	for _, candidate := range [][]byte{
		[]byte("data:"),
		[]byte("event:"),
		[]byte("id:"),
		[]byte("retry:"),
	} {
		if bytes.HasPrefix(candidate, prefix) {
			return true
		}
	}
	return false
}

// SSECommentDetector tracks SSE frame boundaries across arbitrary stream chunks.
type SSECommentDetector struct {
	frameBytes      int
	lineBytes       int
	lineKind        uint8
	frameHasComment bool
	frameHasOther   bool
	previousCR      bool
	oversized       bool
}

// Feed reports whether chunk completes a comment-only SSE frame or exceeds the
// bounded frame-tracking buffer and should be flushed defensively.
func (detector *SSECommentDetector) Feed(chunk []byte) bool {
	if detector == nil || len(chunk) == 0 {
		return false
	}
	forceFlush := false
	for _, value := range chunk {
		if detector.previousCR {
			detector.previousCR = false
			if value == '\n' {
				continue
			}
		}
		detector.frameBytes++
		switch value {
		case '\r':
			if detector.finishLine() {
				forceFlush = true
			}
			detector.previousCR = true
		case '\n':
			if detector.finishLine() {
				forceFlush = true
			}
		default:
			detector.lineBytes++
			if detector.lineKind == 0 && value != ' ' && value != '\t' {
				if value == ':' {
					detector.lineKind = 1
				} else {
					detector.lineKind = 2
				}
			}
		}
		if detector.frameBytes > 64<<10 && !detector.oversized {
			detector.oversized = true
			forceFlush = true
		}
	}
	return forceFlush
}

func (detector *SSECommentDetector) finishLine() bool {
	if detector.lineBytes == 0 {
		foundComment := !detector.oversized && detector.frameHasComment && !detector.frameHasOther
		detector.frameBytes = 0
		detector.frameHasComment = false
		detector.frameHasOther = false
		detector.oversized = false
		return foundComment
	}
	switch detector.lineKind {
	case 1:
		detector.frameHasComment = true
	case 2:
		detector.frameHasOther = true
	}
	detector.lineBytes = 0
	detector.lineKind = 0
	return false
}

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

	// ResolveFlushPolicy optionally supplies a dynamic flush policy. It is
	// evaluated before each data chunk and overrides FlushInterval/FlushMinBytes.
	ResolveFlushPolicy func() (*time.Duration, int)

	// WriteChunk writes a single data chunk to the response body. It should not flush.
	WriteChunk func(chunk []byte)

	// ChunkError reports a fatal transformation error produced by WriteChunk.
	// When non-nil, forwarding stops and the execution context is canceled.
	ChunkError func() error

	// FlushChunk reports whether a data chunk must bypass configured flush batching.
	FlushChunk func(chunk []byte) bool

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

	resolveFlushPolicy := func() (time.Duration, int) {
		interval := time.Duration(0)
		minBytes := opts.FlushMinBytes
		intervalPtr := opts.FlushInterval
		if opts.ResolveFlushPolicy != nil {
			intervalPtr, minBytes = opts.ResolveFlushPolicy()
		}
		if intervalPtr != nil {
			interval = *intervalPtr
		}
		return interval, minBytes
	}
	flushInterval, flushMinBytes := resolveFlushPolicy()
	batchedFlush := flushInterval > 0 || flushMinBytes > 0

	flushPending := func() {
		if unflushedBytes <= 0 {
			stopFlushTimer()
			return
		}
		flusher.Flush()
		unflushedBytes = 0
		stopFlushTimer()
	}

	writeTerminalError := func(errMsg *interfaces.ErrorMessage) {
		flushPending()
		if opts.WriteTerminalError != nil {
			opts.WriteTerminalError(errMsg)
		}
		flusher.Flush()
	}

	refreshFlushPolicy := func() {
		nextInterval, nextMinBytes := resolveFlushPolicy()
		if nextInterval == flushInterval && nextMinBytes == flushMinBytes {
			return
		}
		stopFlushTimer()
		flushInterval = nextInterval
		flushMinBytes = nextMinBytes
		batchedFlush = flushInterval > 0 || flushMinBytes > 0
		if unflushedBytes <= 0 {
			return
		}
		if !batchedFlush || (flushMinBytes > 0 && unflushedBytes >= flushMinBytes) {
			flusher.Flush()
			unflushedBytes = 0
			return
		}
		if flushInterval > 0 {
			flushTimer = time.NewTimer(flushInterval)
			flushC = flushTimer.C
		}
	}

	noteWrite := func(size int) {
		if size <= 0 {
			return
		}
		refreshFlushPolicy()
		if !batchedFlush {
			flusher.Flush()
			return
		}
		unflushedBytes += size
		if flushMinBytes > 0 && unflushedBytes >= flushMinBytes {
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
					writeTerminalError(terminalErr)
					cancel(terminalErr.Error)
					return
				}
				if opts.WriteDone != nil {
					opts.WriteDone()
				}
				if opts.ChunkError != nil {
					if errChunk := opts.ChunkError(); errChunk != nil {
						writeTerminalError(&interfaces.ErrorMessage{
							StatusCode: http.StatusBadGateway,
							Error:      errChunk,
						})
						cancel(errChunk)
						return
					}
				}
				flushPending()
				flusher.Flush()
				cancel(nil)
				return
			}
			beforeWrite := c.Writer.Size()
			writeChunk(chunk)
			afterWrite := c.Writer.Size()
			writtenBytes := 0
			if afterWrite >= 0 {
				writtenBytes = afterWrite
				if beforeWrite >= 0 {
					writtenBytes -= beforeWrite
				}
			}
			noteWrite(writtenBytes)
			if opts.ChunkError != nil {
				if errChunk := opts.ChunkError(); errChunk != nil {
					writeTerminalError(&interfaces.ErrorMessage{
						StatusCode: http.StatusBadGateway,
						Error:      errChunk,
					})
					cancel(errChunk)
					return
				}
			}
			if opts.FlushChunk != nil && opts.FlushChunk(chunk) {
				flushPending()
			}
		case errMsg, ok := <-errs:
			if !ok {
				continue
			}
			if errMsg != nil {
				terminalErr = errMsg
				writeTerminalError(errMsg)
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
