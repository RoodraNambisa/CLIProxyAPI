package common

import (
	"bytes"
	"encoding/json"
)

type responsesUsageSSELine struct {
	start, contentEnd, end, dataStart int
}

// ensureResponsesSSEUsageDetails preserves frame boundaries, metadata and line
// endings. Multiple data fields are joined before JSON validation and updates.
func ensureResponsesSSEUsageDetails(payload []byte) []byte {
	var lines []responsesUsageSSELine
	var output []byte
	copied := 0
	flush := func(end int) {
		dataCount := 0
		var data []byte
		for _, line := range lines {
			if line.dataStart < 0 {
				continue
			}
			part := payload[line.dataStart:line.contentEnd]
			if dataCount == 0 {
				data = part
			} else {
				if dataCount == 1 {
					data = bytes.Clone(data)
				}
				data = append(data, '\n')
				data = append(data, part...)
			}
			dataCount++
		}
		updated := ensureResponsesJSONUsageDetails(data)
		if dataCount > 0 && !bytes.Equal(updated, data) {
			if dataCount > 1 {
				var compact bytes.Buffer
				if err := json.Compact(&compact, updated); err != nil {
					lines = lines[:0]
					return
				}
				updated = compact.Bytes()
			}
			output = append(output, payload[copied:lines[0].start]...)
			wroteData := false
			for _, line := range lines {
				if line.dataStart < 0 {
					output = append(output, payload[line.start:line.end]...)
					continue
				}
				if !wroteData {
					output = append(output, payload[line.start:line.dataStart]...)
					output = append(output, updated...)
					output = append(output, payload[line.contentEnd:line.end]...)
					wroteData = true
				}
			}
			copied = end
		}
		lines = lines[:0]
	}
	for start := 0; start < len(payload); {
		contentEnd := start
		for contentEnd < len(payload) && payload[contentEnd] != '\r' && payload[contentEnd] != '\n' {
			contentEnd++
		}
		end := contentEnd
		if end < len(payload) {
			end++
			if payload[contentEnd] == '\r' && end < len(payload) && payload[end] == '\n' {
				end++
			}
		}
		line := responsesUsageSSELine{start: start, contentEnd: contentEnd, end: end, dataStart: -1}
		content := payload[start:contentEnd]
		field := bytes.TrimLeft(content, " \t")
		if bytes.HasPrefix(field, []byte("data:")) {
			line.dataStart = start + len(content) - len(field) + len("data:")
			if line.dataStart < contentEnd && payload[line.dataStart] == ' ' {
				line.dataStart++
			}
		}
		lines = append(lines, line)
		if len(bytes.TrimSpace(content)) == 0 {
			flush(end)
		}
		start = end
	}
	if len(lines) > 0 {
		flush(len(payload))
	}
	if output == nil {
		return payload
	}
	return append(output, payload[copied:]...)
}
