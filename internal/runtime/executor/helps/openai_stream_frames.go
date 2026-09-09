package helps

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// ScanSSEFrames is a bufio.Scanner split function with CR, LF and CRLF support.
// The scanner's maximum token size bounds one complete upstream frame.
func ScanSSEFrames(data []byte, atEOF bool) (int, []byte, error) {
	if size := rawSSEFrameLen(data, atEOF); size > 0 {
		return size, data[:size], nil
	}
	return 0, nil, nil
}

type OpenAIStreamEvent struct {
	Data []byte
	Err  error
}

func openAIErrorEvent(event string) bool {
	switch strings.ToLower(strings.TrimSpace(event)) {
	case "error", "response.error", "response.failed":
		return true
	}
	return false
}

func openAIFramingError(message string, payload []byte) error {
	return streamProtocolError{provider: "openai-compatible", message: message, body: string(payload)}
}

// ParseOpenAIStreamFrame joins multiline SSE data. Complete JSON records on
// adjacent data lines retain compatibility with providers that omit blank lines.
// Returned data and error bodies own their bytes independently of the scanner.
func ParseOpenAIStreamFrame(frame []byte) []OpenAIStreamEvent {
	var parts [][]byte
	errorEvent := false
	for _, line := range SplitSSEFrameLines(frame) {
		line = bytes.TrimSpace(line)
		switch {
		case bytes.HasPrefix(line, []byte("event:")):
			errorEvent = openAIErrorEvent(string(line[len("event:"):]))
		case bytes.HasPrefix(line, []byte("data:")):
			parts = append(parts, bytes.TrimSpace(line[len("data:"):]))
		case bytes.HasPrefix(line, []byte("{")), bytes.HasPrefix(line, []byte("[")):
			streamErr := openAIFramingError("upstream returned JSON instead of SSE data", line)
			root := gjson.ParseBytes(line)
			flatError := root.Get("code").Exists() && root.Get("message").Exists()
			if gjson.ValidBytes(line) && (IsJSONStreamProtocolError(line) || flatError || openAIErrorEvent(root.Get("type").String())) {
				streamErr = jsonStreamProtocolError("openai-compatible", line, true)
			}
			return []OpenAIStreamEvent{{Err: streamErr}}
		}
	}
	if len(parts) == 0 {
		if errorEvent {
			return []OpenAIStreamEvent{{Err: openAIFramingError("upstream error event ended without data", nil)}}
		}
		return nil
	}
	if len(parts) > 1 {
		completeRecords := true
		for _, part := range parts {
			if !IsOpenAIStreamTerminal(part) && !gjson.ValidBytes(part) {
				completeRecords = false
				break
			}
		}
		if !completeRecords {
			parts = [][]byte{bytes.Join(parts, []byte{'\n'})}
		}
	}
	events := make([]OpenAIStreamEvent, 0, len(parts))
	for _, part := range parts {
		if len(bytes.TrimSpace(part)) == 0 && !errorEvent {
			continue
		}
		if IsOpenAIStreamTerminal(part) {
			if errorEvent {
				return append(events, OpenAIStreamEvent{Err: openAIFramingError("upstream error event ended before [DONE]", part)})
			}
			return append(events, OpenAIStreamEvent{Data: []byte("[DONE]")})
		}
		if !gjson.ValidBytes(part) {
			return append(events, OpenAIStreamEvent{Err: openAIFramingError("upstream stream returned incomplete SSE data", part)})
		}
		root := gjson.ParseBytes(part)
		flatError := root.Get("code").Exists() && root.Get("message").Exists()
		if errorEvent || flatError || openAIErrorEvent(root.Get("type").String()) || IsJSONStreamProtocolError(part) {
			return append(events, OpenAIStreamEvent{Err: jsonStreamProtocolError("openai-compatible", part, errorEvent || flatError || openAIErrorEvent(root.Get("type").String()))})
		}
		if bytes.IndexAny(part, "\r\n") >= 0 {
			var compact bytes.Buffer
			if errCompact := json.Compact(&compact, part); errCompact != nil {
				return append(events, OpenAIStreamEvent{Err: openAIFramingError("upstream stream returned invalid SSE data", part)})
			}
			events = append(events, OpenAIStreamEvent{Data: compact.Bytes()})
		} else {
			events = append(events, OpenAIStreamEvent{Data: bytes.Clone(part)})
		}
	}
	return events
}
