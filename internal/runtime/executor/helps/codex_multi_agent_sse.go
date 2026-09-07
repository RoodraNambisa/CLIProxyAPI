package helps

import (
	"bytes"
	"encoding/json"
)

type codexSSEDataSpan struct {
	start, dataStart, contentEnd, end int
}

// RewriteSSEFrame accepts one framed SSE event, a complete data line, or a raw
// Responses event. Unchanged frames retain their original bytes and ownership.
func (policy CodexMultiAgentResponsePolicy) RewriteSSEFrame(frame []byte) []byte {
	if !policy.NamespaceOptimized && !policy.PlaintextCalls {
		return frame
	}
	if json.Valid(frame) {
		return policy.Rewrite(frame)
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
	rewritten := policy.Rewrite(data)
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
