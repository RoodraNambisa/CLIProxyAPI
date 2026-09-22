package auth

import (
	"bytes"
	"encoding/json"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

var modelFieldPaths = []string{"model", "modelVersion", "response.model", "response.modelVersion", "message.model"}

const maxPendingBufSize = 1 << 20

func rewriteSSEPayloadLines(payload []byte, targetModel string) []byte {
	return rewriteSSEPayloadLinesWithOptions(payload, StreamRewriteOptions{RewriteModel: targetModel})
}

func rewriteSSEPayloadLinesWithOptions(payload []byte, options StreamRewriteOptions) []byte {
	if options.RewriteModel == "" && options.OnModel == nil || len(payload) == 0 {
		return payload
	}
	lines := bytes.Split(payload, []byte("\n"))
	out := make([][]byte, 0, len(lines))
	for _, line := range lines {
		prefix, jsonData, ok := extractSSEDataLine(line)
		if ok && len(jsonData) > 0 && jsonData[0] == '{' && gjson.ValidBytes(jsonData) {
			rewritten := rewriteModelWithOptions(jsonData, options)
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
	return rewriteModelWithOptions(data, StreamRewriteOptions{RewriteModel: targetModel})
}

func rewriteModelWithOptions(data []byte, options StreamRewriteOptions) []byte {
	targetModel := options.RewriteModel
	if targetModel == "" && options.OnModel == nil || len(data) == 0 {
		return data
	}
	if options.StrictModelFields {
		if !gjson.ValidBytes(data) {
			return data
		}
		root := gjson.ParseBytes(data)
		kind := root.Get("type").String()
		errorField := root.Get("error")
		if (errorField.Exists() && errorField.Type != gjson.Null) || root.Get("status").String() == "failed" || root.Get("response.status").String() == "failed" || kind == "error" || kind == "response.failed" {
			return data
		}
	}
	for _, path := range modelFieldPaths {
		value := gjson.GetBytes(data, path)
		if options.StrictModelFields && (value.Type != gjson.String || value.String() == "" || value.String() == targetModel) {
			continue
		}
		if targetModel == "" {
			if options.OnModel != nil && value.Type == gjson.String && value.String() != "" {
				options.OnModel(value.String())
			}
			continue
		}
		if value.Exists() {
			updated, err := sjson.SetBytes(data, path, targetModel)
			if err != nil {
				continue
			}
			data = updated
			if options.OnRewrite != nil && value.String() != targetModel {
				options.OnRewrite(value.String())
			}
			log.Debugf("response rewriter: rewrote model at path %s to %s", path, targetModel)
		}
	}
	return data
}

type StreamRewriteOptions struct {
	RewriteModel      string
	StrictModelFields bool
	OnRewrite         func(string)
	// OnModel observes forwarded model fields without changing payloads when RewriteModel is empty.
	OnModel func(string)
}

type StreamRewriter struct {
	options    StreamRewriteOptions
	pendingBuf []byte
}

func NewStreamRewriter(options StreamRewriteOptions) *StreamRewriter {
	return &StreamRewriter{options: options}
}

func (r *StreamRewriter) RewriteChunk(chunk []byte) []byte {
	if r.options.RewriteModel == "" && r.options.OnModel == nil {
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
	chunk = joinMultilineSSEJSON(chunk)
	trimmed := bytes.TrimSpace(chunk)
	if len(trimmed) > 0 && trimmed[0] == '{' && gjson.ValidBytes(trimmed) {
		return rewriteModelWithOptions(trimmed, r.options)
	}
	if len(chunk) > maxPendingBufSize {
		return rewriteSSEPayloadLinesWithOptions(chunk, r.options)
	}
	lastDoubleNewline := bytes.LastIndex(chunk, []byte("\n\n"))
	completeEnd := 0
	if lastDoubleNewline >= 0 {
		completeEnd = lastDoubleNewline + 2
	}
	if crlf := bytes.LastIndex(chunk, []byte("\r\n\r\n")); crlf >= 0 && crlf+4 > completeEnd {
		completeEnd = crlf + 4
	}
	var processChunk []byte
	if completeEnd > 0 {
		afterComplete := chunk[completeEnd:]
		if len(afterComplete) > 0 && !bytes.Equal(afterComplete, []byte("\n")) {
			if completeSingleSSEDataJSON(afterComplete) {
				processChunk = chunk
			} else {
				processChunk = chunk[:completeEnd]
				r.pendingBuf = append(r.pendingBuf[:0], afterComplete...)
			}
		} else {
			processChunk = chunk
		}
	} else if completeSingleSSEDataJSON(chunk) {
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
			rewritten := rewriteModelWithOptions(jsonData, r.options)
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
		return rewriteSSEPayloadLinesWithOptions(chunk, r.options)
	}
	return joined
}

// An individually valid JSON string or array in a later data line can still be
// a fragment of a multiline event. Only emit a single complete object early.
func completeSingleSSEDataJSON(chunk []byte) bool {
	var data []byte
	count := 0
	for _, line := range bytes.Split(chunk, []byte("\n")) {
		if _, value, ok := extractSSEDataLine(line); ok {
			count++
			data = bytes.TrimSpace(value)
		}
	}
	return count == 1 && len(data) > 0 && data[0] == '{' && gjson.ValidBytes(data)
}

// Join complete multiline JSON events before the line-oriented compatibility
// rewriter. SSE joins data fields with newlines, not as separate JSON documents.
func joinMultilineSSEJSON(chunk []byte) []byte {
	if !bytes.Contains(chunk, []byte("\ndata:")) {
		return chunk
	}
	var out []byte
	frameStart, lineStart := 0, 0
	for lineStart < len(chunk) {
		end := bytes.IndexByte(chunk[lineStart:], '\n')
		if end < 0 {
			break
		}
		end += lineStart
		line := bytes.TrimSuffix(chunk[lineStart:end], []byte("\r"))
		if len(line) == 0 {
			out = append(out, joinSSEJSONFrame(chunk[frameStart:end+1])...)
			frameStart = end + 1
		}
		lineStart = end + 1
	}
	return append(out, chunk[frameStart:]...)
}

func joinSSEJSONFrame(frame []byte) []byte {
	lines := bytes.Split(frame, []byte("\n"))
	var fields [][]byte
	for _, line := range lines {
		if _, data, ok := extractSSEDataLine(bytes.TrimSuffix(line, []byte("\r"))); ok {
			fields = append(fields, data)
		}
	}
	if len(fields) < 2 {
		return frame
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, bytes.Join(fields, []byte("\n"))); err != nil {
		return frame
	}
	result := make([][]byte, 0, len(lines))
	written := false
	for _, line := range lines {
		prefix, _, isData := extractSSEDataLine(bytes.TrimSuffix(line, []byte("\r")))
		if !isData {
			result = append(result, line)
			continue
		}
		if !written {
			joined := append(bytes.Clone(prefix), compact.Bytes()...)
			if bytes.HasSuffix(line, []byte("\r")) {
				joined = append(joined, '\r')
			}
			result = append(result, joined)
			written = true
		}
	}
	return bytes.Join(result, []byte("\n"))
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
		tail := rewriteSSEPayloadLinesWithOptions(r.pendingBuf, r.options)
		r.pendingBuf = nil
		if len(tail) > 0 {
			out = append(out, tail...)
		}
	}
	return out
}
