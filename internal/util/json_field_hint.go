package util

import "bytes"

// JSONMayContainAnyField is a conservative hint for unescaped ASCII identifiers
// used as protocol field names. It neither validates JSON nor identifies the
// protocol scope of a key. Matches and escaped keys still require JSON parsing.
func JSONMayContainAnyField(body []byte, fields ...string) bool {
	remaining := body
	for {
		quote := bytes.IndexByte(remaining, '"')
		if quote < 0 {
			return false
		}
		remaining = remaining[quote+1:]
		end := 0
		for {
			closing := bytes.IndexByte(remaining[end:], '"')
			if closing < 0 {
				// An incomplete string cannot safely exclude later fields.
				return true
			}
			end += closing
			slashes := 0
			for i := end - 1; i >= 0 && remaining[i] == '\\'; i-- {
				slashes++
			}
			if slashes%2 == 0 {
				break
			}
			end++
		}
		key := remaining[:end]
		remaining = remaining[end+1:]
		for len(remaining) > 0 && (remaining[0] == ' ' || remaining[0] == '\t' || remaining[0] == '\r' || remaining[0] == '\n') {
			remaining = remaining[1:]
		}
		if len(remaining) == 0 || remaining[0] != ':' {
			continue
		}
		if bytes.IndexByte(key, '\\') >= 0 {
			return true
		}
		for _, field := range fields {
			if string(key) == field {
				return true
			}
		}
	}
}
