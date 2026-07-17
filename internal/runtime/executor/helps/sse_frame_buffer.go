package helps

import (
	"bytes"
	"fmt"
)

// SSEFrameLimitError reports an upstream SSE frame that exceeds its bound.
type SSEFrameLimitError struct {
	Limit int
}

func (err *SSEFrameLimitError) Error() string {
	if err == nil || err.Limit <= 0 {
		return "upstream SSE frame exceeds the configured limit"
	}
	return fmt.Sprintf("upstream SSE frame exceeds %d bytes", err.Limit)
}

func (*SSEFrameLimitError) StatusCode() int      { return 502 }
func (*SSEFrameLimitError) SkipAuthResult() bool { return true }

// SSEFrameBuffer reconstructs bounded raw SSE frames across arbitrary chunks.
type SSEFrameBuffer struct {
	pending  []byte
	maxBytes int
}

// NewSSEFrameBuffer creates a raw SSE frame buffer with a per-frame limit.
func NewSSEFrameBuffer(maxBytes int) *SSEFrameBuffer {
	return &SSEFrameBuffer{maxBytes: maxBytes}
}

// Feed consumes bytes and returns complete frames. A trailing CR is retained
// until the next chunk unless flush is true.
func (buffer *SSEFrameBuffer) Feed(chunk []byte, flush bool) ([][]byte, error) {
	if buffer == nil || buffer.maxBytes <= 0 {
		return nil, fmt.Errorf("SSE frame buffer limit must be positive")
	}
	var frames [][]byte
	consume := func(atEOF bool) {
		for len(buffer.pending) > 0 {
			frameLen := rawSSEFrameLen(buffer.pending, atEOF)
			if frameLen == 0 {
				return
			}
			frames = append(frames, bytes.Clone(buffer.pending[:frameLen]))
			copy(buffer.pending, buffer.pending[frameLen:])
			buffer.pending = buffer.pending[:len(buffer.pending)-frameLen]
		}
	}
	consume(false)
	for len(chunk) > 0 {
		room := buffer.maxBytes - len(buffer.pending)
		if room <= 0 {
			buffer.pending = nil
			return nil, &SSEFrameLimitError{Limit: buffer.maxBytes}
		}
		take := len(chunk)
		if take > room {
			take = room
		}
		buffer.pending = append(buffer.pending, chunk[:take]...)
		chunk = chunk[take:]
		consume(false)
	}
	if flush {
		consume(true)
		buffer.pending = nil
	}
	return frames, nil
}

func rawSSEFrameLen(data []byte, atEOF bool) int {
	lineStart := 0
	for index := 0; index < len(data); {
		if data[index] != '\r' && data[index] != '\n' {
			index++
			continue
		}
		if data[index] == '\r' && index+1 == len(data) && !atEOF {
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
	if atEOF {
		return len(data)
	}
	return 0
}

// SplitSSEFrameLines splits raw SSE data on CR, LF, or CRLF.
func SplitSSEFrameLines(data []byte) [][]byte {
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
