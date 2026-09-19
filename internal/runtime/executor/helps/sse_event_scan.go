package helps

import (
	"bytes"
	"encoding/json"
)

// SplitSSEDataEvents retains ordinary lines and joins a multi-line JSON data
// field before identity mapping. Scanner bounds incomplete event buffering.
func SplitSSEDataEvents(data []byte, atEOF bool) (advance int, token []byte, err error) {
	end, next := codexSSEScanLine(data, atEOF)
	if next == 0 {
		return 0, nil, nil
	}
	line := data[:end]
	trimmed := bytes.TrimLeft(line, " \t")
	if !bytes.HasPrefix(trimmed, []byte("data:")) {
		return next, line, nil
	}
	part := bytes.TrimSpace(trimmed[len("data:"):])
	if len(part) == 0 || json.Valid(part) || bytes.Equal(part, []byte("[DONE]")) {
		return next, line, nil
	}
	if part[0] != '{' && part[0] != '[' {
		return next, line, nil
	}
	joined := bytes.Clone(part)
	for offset := next; ; {
		if offset == len(data) {
			if atEOF {
				return next, line, nil
			}
			return 0, nil, nil
		}
		partEnd, partNext := codexSSEScanLine(data[offset:], atEOF)
		if partNext == 0 {
			return 0, nil, nil
		}
		following := bytes.TrimLeft(data[offset:offset+partEnd], " \t")
		if !bytes.HasPrefix(following, []byte("data:")) {
			return next, line, nil
		}
		joined = append(joined, '\n')
		joined = append(joined, bytes.TrimSpace(following[len("data:"):])...)
		offset += partNext
		if json.Valid(joined) {
			var compact bytes.Buffer
			if json.Compact(&compact, joined) == nil {
				return offset, append([]byte("data: "), compact.Bytes()...), nil
			}
		}
	}
}

func codexSSEScanLine(data []byte, atEOF bool) (end, next int) {
	if offset := bytes.IndexAny(data, "\r\n"); offset >= 0 {
		if data[offset] == '\r' && offset+1 == len(data) && !atEOF {
			return 0, 0
		}
		next = offset + 1
		if data[offset] == '\r' && next < len(data) && data[next] == '\n' {
			next++
		}
		return offset, next
	}
	if atEOF && len(data) > 0 {
		return len(data), len(data)
	}
	return 0, 0
}
