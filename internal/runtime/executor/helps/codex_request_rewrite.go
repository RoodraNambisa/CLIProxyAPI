package helps

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// CodexStreamRewrite controls how the top-level stream field is emitted.
type CodexStreamRewrite uint8

const (
	CodexStreamPreserve CodexStreamRewrite = iota
	CodexStreamForceEnabled
	CodexStreamRemove
)

// CodexRequestRewriteOptions describes the top-level fields changed before a
// request is sent to Codex. Nested JSON values are copied without reparsing.
type CodexRequestRewriteOptions struct {
	Model              string
	Stream             CodexStreamRewrite
	StripResponseState bool
	EnsureInstructions bool
}

// RewriteCodexRequestEnvelope applies common Codex top-level mutations in one
// scan, avoiding a full payload copy for every individual field update. Its
// callers provide translator-produced or previously validated JSON objects.
func RewriteCodexRequestEnvelope(payload []byte, opts CodexRequestRewriteOptions) ([]byte, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) < 2 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return nil, fmt.Errorf("codex request payload must be a JSON object")
	}

	model := strings.TrimSpace(opts.Model)
	out := make([]byte, 0, len(payload)+64)
	out = append(out, '{')
	fields := 0
	instructionsFound := false
	err := visitCodexTopLevelFields(trimmed, func(rawKey, rawValue []byte) {
		switch codexRequestField(rawKey) {
		case codexRequestFieldModel:
			if model != "" {
				return
			}
		case codexRequestFieldStream:
			if opts.Stream != CodexStreamPreserve {
				return
			}
		case codexRequestFieldResponseState:
			if opts.StripResponseState {
				return
			}
		case codexRequestFieldInstructions:
			instructionsFound = !bytes.Equal(bytes.TrimSpace(rawValue), []byte("null"))
			if opts.EnsureInstructions && !instructionsFound {
				return
			}
		}
		out = appendCodexRawJSONField(out, &fields, rawKey, rawValue)
	})
	if err != nil {
		return nil, err
	}

	if model != "" {
		out = appendCodexNamedJSONField(out, &fields, "model", strconv.Quote(model))
	}
	if opts.Stream == CodexStreamForceEnabled {
		out = appendCodexNamedJSONField(out, &fields, "stream", "true")
	}
	if opts.EnsureInstructions && !instructionsFound {
		out = appendCodexNamedJSONField(out, &fields, "instructions", `""`)
	}
	out = append(out, '}')
	return out, nil
}

type codexRequestFieldKind uint8

const (
	codexRequestFieldOther codexRequestFieldKind = iota
	codexRequestFieldModel
	codexRequestFieldStream
	codexRequestFieldResponseState
	codexRequestFieldInstructions
)

func codexRequestField(rawKey []byte) codexRequestFieldKind {
	switch {
	case bytes.Equal(rawKey, []byte(`"model"`)):
		return codexRequestFieldModel
	case bytes.Equal(rawKey, []byte(`"stream"`)):
		return codexRequestFieldStream
	case bytes.Equal(rawKey, []byte(`"instructions"`)):
		return codexRequestFieldInstructions
	case bytes.Equal(rawKey, []byte(`"previous_response_id"`)),
		bytes.Equal(rawKey, []byte(`"prompt_cache_retention"`)),
		bytes.Equal(rawKey, []byte(`"safety_identifier"`)),
		bytes.Equal(rawKey, []byte(`"stream_options"`)):
		return codexRequestFieldResponseState
	}
	if !bytes.Contains(rawKey, []byte{'\\'}) {
		return codexRequestFieldOther
	}
	name, err := strconv.Unquote(string(rawKey))
	if err != nil {
		return codexRequestFieldOther
	}
	switch name {
	case "model":
		return codexRequestFieldModel
	case "stream":
		return codexRequestFieldStream
	case "instructions":
		return codexRequestFieldInstructions
	case "previous_response_id", "prompt_cache_retention", "safety_identifier", "stream_options":
		return codexRequestFieldResponseState
	default:
		return codexRequestFieldOther
	}
}

func visitCodexTopLevelFields(payload []byte, visit func(rawKey, rawValue []byte)) error {
	index := 1
	for {
		index = skipCodexJSONSpace(payload, index)
		if index >= len(payload)-1 {
			return nil
		}
		keyStart := index
		keyEnd, ok := scanCodexJSONString(payload, index)
		if !ok {
			return fmt.Errorf("codex request payload contains an invalid object key")
		}
		index = skipCodexJSONSpace(payload, keyEnd)
		if index >= len(payload) || payload[index] != ':' {
			return fmt.Errorf("codex request payload contains an invalid object field")
		}
		index = skipCodexJSONSpace(payload, index+1)
		valueStart := index
		valueEnd, ok := scanCodexJSONValue(payload, index)
		if !ok {
			return fmt.Errorf("codex request payload contains an invalid object value")
		}
		visit(payload[keyStart:keyEnd], payload[valueStart:valueEnd])
		index = skipCodexJSONSpace(payload, valueEnd)
		if index >= len(payload) {
			return fmt.Errorf("codex request payload ended before the object closed")
		}
		switch payload[index] {
		case ',':
			index++
		case '}':
			if index != len(payload)-1 {
				return fmt.Errorf("codex request payload contains data after the object closed")
			}
			return nil
		default:
			return fmt.Errorf("codex request payload contains an invalid object delimiter")
		}
	}
}

func scanCodexJSONString(payload []byte, start int) (int, bool) {
	if start >= len(payload) || payload[start] != '"' {
		return start, false
	}
	for index := start + 1; index < len(payload); index++ {
		switch payload[index] {
		case '\\':
			index++
			if index >= len(payload) {
				return index, false
			}
		case '"':
			return index + 1, true
		}
	}
	return len(payload), false
}

func scanCodexJSONValue(payload []byte, start int) (int, bool) {
	if start >= len(payload) {
		return start, false
	}
	switch payload[start] {
	case '"':
		return scanCodexJSONString(payload, start)
	case '{', '[':
		depth := 0
		for index := start; index < len(payload); index++ {
			switch payload[index] {
			case '"':
				next, ok := scanCodexJSONString(payload, index)
				if !ok {
					return next, false
				}
				index = next - 1
			case '{', '[':
				depth++
			case '}', ']':
				depth--
				if depth == 0 {
					return index + 1, true
				}
			}
		}
		return len(payload), false
	default:
		index := start
		for index < len(payload) && payload[index] != ',' && payload[index] != '}' {
			index++
		}
		end := index
		for end > start && isCodexJSONSpace(payload[end-1]) {
			end--
		}
		return end, end > start
	}
}

func skipCodexJSONSpace(payload []byte, index int) int {
	for index < len(payload) && isCodexJSONSpace(payload[index]) {
		index++
	}
	return index
}

func isCodexJSONSpace(value byte) bool {
	return value == ' ' || value == '\t' || value == '\r' || value == '\n'
}

func appendCodexRawJSONField(dst []byte, fields *int, rawKey, rawValue []byte) []byte {
	if *fields > 0 {
		dst = append(dst, ',')
	}
	dst = append(dst, rawKey...)
	dst = append(dst, ':')
	dst = append(dst, rawValue...)
	(*fields)++
	return dst
}

func appendCodexNamedJSONField(dst []byte, fields *int, key, rawValue string) []byte {
	if *fields > 0 {
		dst = append(dst, ',')
	}
	dst = strconv.AppendQuote(dst, key)
	dst = append(dst, ':')
	dst = append(dst, rawValue...)
	(*fields)++
	return dst
}
