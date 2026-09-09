package helps

import (
	"bytes"

	"github.com/tidwall/gjson"
)

// HasResponsesStreamTerminal recognizes completed or incomplete response
// envelopes, never similarly named fields inside tool arguments or text.
func HasResponsesStreamTerminal(payload []byte) bool {
	for len(payload) > 0 {
		line, rest, _ := bytes.Cut(payload, []byte("\n"))
		payload = rest
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data:")) {
			line = bytes.TrimSpace(line[5:])
		}
		kind := gjson.GetBytes(line, "type").String()
		if (kind == "response.completed" || kind == "response.incomplete") && gjson.ValidBytes(line) && gjson.GetBytes(line, "response").IsObject() {
			return true
		}
	}
	return false
}
