package helps

import (
	"bytes"
	"fmt"
)

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
