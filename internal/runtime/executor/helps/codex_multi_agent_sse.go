package helps

import (
	"bytes"
	"encoding/json"
)

type codexSSEDataSpan struct {
	start, dataStart, contentEnd, end int
}

// RewriteSSEChunk handles complete translator output events, including multiple
// SSE frames in one chunk. An incomplete trailing frame remains untouched.
func (policy CodexMultiAgentResponsePolicy) RewriteSSEChunk(chunk []byte) []byte {
	if !policy.NamespaceOptimized && !policy.PlaintextCalls {
		return chunk
	}
	return rewriteCodexSSEChunk(chunk, policy.Rewrite)
}

func rewriteCodexSSEChunk(chunk []byte, rewrite func([]byte) []byte) []byte {
	if json.Valid(chunk) {
		return rewrite(chunk)
	}
	var out []byte
	frameStart := 0
	appendFrame := func(end int) {
		frame := chunk[frameStart:end]
		rewritten := rewriteCodexSSEFrame(frame, rewrite)
		if out != nil || !bytes.Equal(frame, rewritten) {
			if out == nil {
				out = make([]byte, 0, len(chunk)+64)
				out = append(out, chunk[:frameStart]...)
			}
			out = append(out, rewritten...)
		}
		frameStart = end
	}
	for lineStart := 0; lineStart < len(chunk); {
		offset := bytes.IndexAny(chunk[lineStart:], "\r\n")
		if offset < 0 {
			break
		}
		contentEnd := lineStart + offset
		end := contentEnd + 1
		if chunk[contentEnd] == '\r' && end < len(chunk) && chunk[end] == '\n' {
			end++
		}
		if contentEnd == lineStart {
			appendFrame(end)
		}
		lineStart = end
	}
	if frameStart < len(chunk) {
		appendFrame(len(chunk))
	}
	if out == nil {
		return chunk
	}
	return out
}

// RewriteSSEFrame accepts one framed SSE event, a complete data line, or a raw
// Responses event. Unchanged frames retain their original bytes and ownership.
func (policy CodexMultiAgentResponsePolicy) RewriteSSEFrame(frame []byte) []byte {
	if !policy.NamespaceOptimized && !policy.PlaintextCalls {
		return frame
	}
	return rewriteCodexSSEFrame(frame, policy.Rewrite)
}

func rewriteCodexSSEFrame(frame []byte, rewrite func([]byte) []byte) []byte {
	if json.Valid(frame) {
		return rewrite(frame)
	}
	var spans []codexSSEDataSpan
	var data, joined []byte
	for start := 0; start < len(frame); {
		contentEnd := len(frame)
		if offset := bytes.IndexAny(frame[start:], "\r\n"); offset >= 0 {
			contentEnd = start + offset
		}
		end := contentEnd
		if end < len(frame) {
			end++
			if frame[contentEnd] == '\r' && end < len(frame) && frame[end] == '\n' {
				end++
			}
		}
		line := frame[start:contentEnd]
		if len(line) == 0 && len(spans) > 0 && len(bytes.TrimSpace(frame[end:])) > 0 {
			return frame
		}
		trimmed := bytes.TrimLeft(line, " \t")
		if bytes.HasPrefix(trimmed, []byte("data:")) {
			dataStart := start + len(line) - len(trimmed) + len("data:")
			if dataStart < contentEnd && frame[dataStart] == ' ' {
				dataStart++
			}
			part := frame[dataStart:contentEnd]
			if len(spans) == 0 {
				data = part
			} else {
				if len(spans) == 1 {
					joined = bytes.Clone(data)
				}
				joined = append(joined, '\n')
				joined = append(joined, part...)
			}
			spans = append(spans, codexSSEDataSpan{start: start, dataStart: dataStart, contentEnd: contentEnd, end: end})
		}
		start = end
	}
	if len(spans) == 0 {
		return frame
	}
	if len(spans) > 1 {
		data = joined
	}
	rewritten := rewrite(data)
	if bytes.Equal(rewritten, data) {
		return frame
	}
	var compact bytes.Buffer
	if errCompact := json.Compact(&compact, rewritten); errCompact != nil {
		return frame
	}
	out := make([]byte, 0, len(frame)+64)
	last := 0
	for index, span := range spans {
		out = append(out, frame[last:span.start]...)
		if index == 0 {
			out = append(out, frame[span.start:span.dataStart]...)
			out = append(out, compact.Bytes()...)
			out = append(out, frame[span.contentEnd:span.end]...)
		}
		last = span.end
	}
	return append(out, frame[last:]...)
}
