package helps

import (
	"bytes"
	"fmt"
)

const retainedSSELineBufferBytes = 256 << 10

// ObserveSSELines emits complete SSE lines while retaining one bounded partial line.
func ObserveSSELines(pending *[]byte, chunk []byte, flush bool, maxLineBytes int, observe func([]byte)) error {
	if pending == nil {
		return fmt.Errorf("SSE line buffer is nil")
	}
	if maxLineBytes <= 0 {
		return fmt.Errorf("SSE line limit must be positive")
	}
	buffer := *pending
	for len(chunk) > 0 {
		newline := bytes.IndexByte(chunk, '\n')
		if newline < 0 {
			if len(buffer)+len(chunk) > maxLineBytes {
				*pending = nil
				return fmt.Errorf("SSE line exceeds %d bytes", maxLineBytes)
			}
			buffer = append(buffer, chunk...)
			break
		}
		if len(buffer)+newline > maxLineBytes {
			*pending = nil
			return fmt.Errorf("SSE line exceeds %d bytes", maxLineBytes)
		}
		buffer = append(buffer, chunk[:newline]...)
		if observe != nil {
			observe(buffer)
		}
		buffer = buffer[:0]
		chunk = chunk[newline+1:]
	}
	if flush {
		if len(buffer) > 0 && observe != nil {
			observe(buffer)
		}
		buffer = nil
	}
	*pending = buffer
	return nil
}

// SSELineBuffer incrementally splits LF, CRLF, and CR terminated lines. Large
// line buffers are released after delivery so partial image payloads do not
// remain live until the upstream response finishes.
type SSELineBuffer struct {
	pending []byte
	scanAt  int
}

// Feed visits complete lines and, when atEOF is true, the final unterminated
// line. It returns false when visit asks parsing to stop.
func (buffer *SSELineBuffer) Feed(chunk []byte, atEOF bool, visit func([]byte) bool) bool {
	if buffer == nil || visit == nil {
		return false
	}
	buffer.pending = append(buffer.pending, chunk...)
	lineStart := 0
	index := buffer.scanAt
	for index < len(buffer.pending) {
		value := buffer.pending[index]
		if value != '\r' && value != '\n' {
			index++
			continue
		}
		if value == '\r' && index+1 == len(buffer.pending) && !atEOF {
			break
		}
		lineEnd := index
		index++
		if value == '\r' && index < len(buffer.pending) && buffer.pending[index] == '\n' {
			index++
		}
		if !visit(buffer.pending[lineStart:lineEnd]) {
			buffer.pending = nil
			buffer.scanAt = 0
			return false
		}
		lineStart = index
	}
	if atEOF && lineStart < len(buffer.pending) {
		if !visit(buffer.pending[lineStart:]) {
			buffer.pending = nil
			buffer.scanAt = 0
			return false
		}
		lineStart = len(buffer.pending)
		index = lineStart
	}
	buffer.compact(lineStart, index)
	return true
}

func (buffer *SSELineBuffer) compact(consumed, scanned int) {
	if consumed <= 0 {
		buffer.scanAt = scanned
		return
	}
	remaining := len(buffer.pending) - consumed
	if remaining == 0 {
		if cap(buffer.pending) > retainedSSELineBufferBytes {
			buffer.pending = nil
		} else {
			buffer.pending = buffer.pending[:0]
		}
		buffer.scanAt = 0
		return
	}
	if cap(buffer.pending) > retainedSSELineBufferBytes && remaining < cap(buffer.pending)/2 {
		next := make([]byte, remaining)
		copy(next, buffer.pending[consumed:])
		buffer.pending = next
	} else {
		copy(buffer.pending, buffer.pending[consumed:])
		buffer.pending = buffer.pending[:remaining]
	}
	buffer.scanAt = scanned - consumed
}
