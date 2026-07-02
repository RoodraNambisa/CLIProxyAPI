package auth

import (
	"bytes"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var modelFieldPaths = []string{"model", "modelVersion", "response.model", "response.modelVersion", "message.model"}

const maxPendingBufSize = 1 << 20

func rewriteSSEPayloadLines(payload []byte, targetModel string) []byte {
	if targetModel == "" || len(payload) == 0 {
		return payload
	}
	lines := bytes.Split(payload, []byte("\n"))
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		prefix, jsonData, ok := extractSSEDataLine(line)
		if ok && len(jsonData) > 0 && jsonData[0] == '{' && gjson.ValidBytes(jsonData) {
			rewritten := rewriteModelInResponse(jsonData, targetModel)
			line = append(append([]byte{}, prefix...), rewritten...)
		}
		out = append(out, line)
	}
	joined := bytes.Join(out, []byte("\n"))
	if len(payload) > 0 && payload[len(payload)-1] == '\n' && (len(joined) == 0 || joined[len(joined)-1] != '\n') {
		joined = append(joined, '\n')
	}
	return joined
}

func rewriteModelInResponse(data []byte, targetModel string) []byte {
	if targetModel == "" || len(data) == 0 {
		return data
	}
	for _, path := range modelFieldPaths {
		if gjson.GetBytes(data, path).Exists() {
			data, _ = sjson.SetBytes(data, path, targetModel)
			log.Debugf("response rewriter: rewrote model at path %s to %s", path, targetModel)
		}
	}
	return data
}

type StreamRewriteOptions struct {
	RewriteModel string
}

type StreamRewriter struct {
	options    StreamRewriteOptions
	pendingBuf []byte
}

func NewStreamRewriter(options StreamRewriteOptions) *StreamRewriter {
	return &StreamRewriter{options: options}
}

func (r *StreamRewriter) RewriteChunk(chunk []byte) []byte {
	if r.options.RewriteModel == "" {
		return chunk
	}
	if len(r.pendingBuf) > 0 {
		combined := make([]byte, 0, len(r.pendingBuf)+len(chunk))
		combined = append(combined, r.pendingBuf...)
		combined = append(combined, chunk...)
		chunk = combined
		r.pendingBuf = nil
	}
	chunk = normalizeGluedSSEEvents(chunk)
	if len(chunk) > maxPendingBufSize {
		return chunk
	}
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) > 0 && trimmed[0] == '{' && gjson.ValidBytes(trimmed) {
		return rewriteModelInResponse(trimmed, r.options.RewriteModel)
	}
	lastDoubleNewline := bytes.LastIndex(chunk, []byte("\n\n"))
	var processChunk []byte
	if lastDoubleNewline >= 0 {
		afterComplete := chunk[lastDoubleNewline+2:]
		if len(afterComplete) > 0 && !bytes.Equal(afterComplete, []byte("\n")) {
			if gjson.ValidBytes(extractLastDataPayload(afterComplete)) {
				processChunk = chunk
			} else {
				processChunk = chunk[:lastDoubleNewline+2]
				r.pendingBuf = append(r.pendingBuf[:0], afterComplete...)
			}
		} else {
			processChunk = chunk
		}
	} else if gjson.ValidBytes(extractLastDataPayload(chunk)) {
		processChunk = chunk
	} else if len(bytes.TrimSpace(chunk)) == 0 {
		return chunk
	} else if len(chunk) > 0 {
		r.pendingBuf = append(r.pendingBuf[:0], chunk...)
		return nil
	} else {
		return chunk
	}

	lines := bytes.Split(processChunk, []byte("\n"))
	result := make([][]byte, 0, len(lines))
	var pendingEvent []byte
	skipBlanks := false
	for _, line := range lines {
		if len(line) == 0 && skipBlanks {
			continue
		}
		if len(line) != 0 && skipBlanks {
			skipBlanks = false
		}
		if bytes.HasPrefix(line, []byte("event:")) {
			pendingEvent = line
			continue
		}
		dataPrefix, jsonData, found := extractSSEDataLine(line)
		if found && len(jsonData) > 0 && jsonData[0] == '{' {
			if !gjson.ValidBytes(jsonData) {
				if pendingEvent != nil {
					r.pendingBuf = append(pendingEvent, '\n')
					r.pendingBuf = append(r.pendingBuf, line...)
					pendingEvent = nil
				} else {
					r.pendingBuf = append(r.pendingBuf, line...)
				}
				continue
			}
			if pendingEvent != nil {
				result = append(result, pendingEvent)
				pendingEvent = nil
			}
			rewritten := rewriteModelInResponse(jsonData, r.options.RewriteModel)
			result = append(result, append(dataPrefix, rewritten...))
			continue
		}
		if pendingEvent != nil {
			result = append(result, pendingEvent)
			pendingEvent = nil
		}
		result = append(result, line)
	}
	if pendingEvent != nil {
		result = append(result, pendingEvent)
	}
	joined := bytes.Join(result, []byte("\n"))
	if len(joined) == 0 && len(chunk) > 0 {
		return rewriteSSEPayloadLines(chunk, r.options.RewriteModel)
	}
	return joined
}

func extractLastDataPayload(chunk []byte) []byte {
	lines := bytes.Split(chunk, []byte("\n"))
	for i := len(lines) - 1; i >= 0; i-- {
		if _, jsonData, found := extractSSEDataLine(lines[i]); found && len(jsonData) > 0 {
			return jsonData
		}
	}
	return nil
}

func extractSSEDataLine(line []byte) (prefix []byte, jsonData []byte, ok bool) {
	if jsonData, found := bytes.CutPrefix(line, []byte("data: ")); found {
		return []byte("data: "), jsonData, true
	}
	if jsonData, found := bytes.CutPrefix(line, []byte("data:")); found {
		return []byte("data:"), jsonData, true
	}
	return nil, nil, false
}

func normalizeGluedSSEEvents(chunk []byte) []byte {
	if len(chunk) == 0 {
		return chunk
	}
	chunk = safeReplaceGlued(chunk, []byte("}event:"), []byte("}\n\nevent:"))
	chunk = safeReplaceGlued(chunk, []byte("}\r\nevent:"), []byte("}\r\n\r\nevent:"))
	chunk = safeReplaceGlued(chunk, []byte("}data:"), []byte("}\n\ndata:"))
	chunk = safeReplaceGlued(chunk, []byte("}\r\ndata:"), []byte("}\r\n\r\ndata:"))
	return chunk
}

func safeReplaceGlued(chunk []byte, old, replacement []byte) []byte {
	if len(old) == 0 || len(chunk) == 0 || !bytes.Contains(chunk, old) {
		return chunk
	}
	var result []byte
	remaining := chunk
	for {
		idx := bytes.Index(remaining, old)
		if idx == -1 {
			result = append(result, remaining...)
			break
		}
		lineStart := bytes.LastIndexByte(remaining[:idx], '\n')
		var part []byte
		if lineStart == -1 {
			part = remaining[:idx+1]
		} else {
			part = remaining[lineStart+1 : idx+1]
		}
		_, jsonData, ok := extractSSEDataLine(part)
		if ok && len(jsonData) > 0 && gjson.ValidBytes(jsonData) {
			result = append(result, remaining[:idx]...)
			result = append(result, replacement...)
			remaining = remaining[idx+len(old):]
			continue
		}
		result = append(result, remaining[:idx+len(old)]...)
		remaining = remaining[idx+len(old):]
	}
	return result
}

func (r *StreamRewriter) Finish() []byte {
	if r == nil || len(r.pendingBuf) == 0 {
		return nil
	}
	buf := make([]byte, len(r.pendingBuf)+2)
	copy(buf, r.pendingBuf)
	buf[len(r.pendingBuf)] = '\n'
	buf[len(r.pendingBuf)+1] = '\n'
	buf = normalizeGluedSSEEvents(buf)
	r.pendingBuf = nil
	out := r.RewriteChunk(buf)
	if len(r.pendingBuf) > 0 {
		tail := rewriteSSEPayloadLines(r.pendingBuf, r.options.RewriteModel)
		r.pendingBuf = nil
		if len(tail) > 0 {
			out = append(out, tail...)
		}
	}
	return out
}
