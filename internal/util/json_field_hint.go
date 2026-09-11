package util

import "bytes"

// JSONMayContainAnyField is a conservative hint for unescaped ASCII identifiers
// used as protocol field names. It neither validates JSON nor identifies the
// protocol scope of a key. Matches and escaped keys still require JSON parsing.
func JSONMayContainAnyField(body []byte, fields ...string) bool {
	// Non-container tails may be incomplete, so keep them on the parser path.
	// This is only a cheap boundary check, not JSON validation.
	tail := bytes.TrimSpace(body)
	if len(tail) > 0 && tail[len(tail)-1] != '}' && tail[len(tail)-1] != ']' {
		return true
	}
	for offset := 0; offset < len(body); {
		colon := bytes.IndexByte(body[offset:], ':')
		if colon < 0 {
			break
		}
		colon += offset
		offset = colon + 1
		end := colon - 1
		for end >= 0 && (body[end] == ' ' || body[end] == '\t' || body[end] == '\r' || body[end] == '\n') {
			end--
		}
		if end < 0 || body[end] != '"' {
			continue
		}
		// A real key ends at an unescaped quote. Quoted code inside a JSON
		// string has escaped quotes, even when it contains a colon afterwards.
		slashes := 0
		for i := end - 1; i >= 0 && body[i] == '\\'; i-- {
			slashes++
		}
		if slashes%2 != 0 {
			continue
		}
		start := bytes.LastIndexByte(body[:end], '"')
		if start < 0 || (start > 0 && body[start-1] == '\\') {
			return true
		}
		key := body[start+1 : end]
		if bytes.IndexByte(key, '\\') >= 0 {
			return true
		}
		for _, field := range fields {
			if string(key) == field {
				return true
			}
		}
	}
	return false
}
