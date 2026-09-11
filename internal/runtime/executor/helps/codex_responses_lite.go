package helps

import (
	"bytes"
	"net/http"
	"strconv"
	"strings"
	"unsafe"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const CodexResponsesLiteHeader = "X-OpenAI-Internal-Codex-Responses-Lite"
const CodexResponsesLiteMetadata = "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite"

// CodexResponsesLiteSnapshot is immutable across credential and transport retries.
type CodexResponsesLiteSnapshot struct {
	Enabled   bool
	Specified bool
}

// SnapshotCodexResponsesLite gives an explicit frame value priority. A downstream
// websocket turn without metadata is ordinary Responses, independent of earlier turns.
func SnapshotCodexResponsesLite(body []byte, headers http.Header, downstreamWebsocket bool) CodexResponsesLiteSnapshot {
	if value := gjson.GetBytes(body, CodexResponsesLiteMetadata); value.Exists() {
		return CodexResponsesLiteSnapshot{Specified: true, Enabled: value.Type == gjson.True || value.Type == gjson.String && strings.EqualFold(strings.TrimSpace(value.Str), "true")}
	}
	if downstreamWebsocket {
		return CodexResponsesLiteSnapshot{Specified: true}
	}
	for name, values := range headers {
		if strings.EqualFold(name, CodexResponsesLiteHeader) && len(values) > 0 {
			return CodexResponsesLiteSnapshot{Specified: true, Enabled: strings.EqualFold(strings.TrimSpace(values[0]), "true")}
		}
	}
	return CodexResponsesLiteSnapshot{}
}

// ApplyHeaders aligns HTTP fallback with the current frame instead of a stale handshake flag.
func (snapshot CodexResponsesLiteSnapshot) ApplyHeaders(headers http.Header) {
	if !snapshot.Specified || headers == nil {
		return
	}
	for name := range headers {
		if strings.EqualFold(name, CodexResponsesLiteHeader) {
			delete(headers, name)
		}
	}
	if snapshot.Enabled {
		headers.Set(CodexResponsesLiteHeader, "true")
	}
}

// ApplyBody carries the current mode in websocket frames and disables parallel
// tool calls only for explicitly selected Lite requests.
func (snapshot CodexResponsesLiteSnapshot) ApplyBody(body []byte, websocket bool) []byte {
	if snapshot.Enabled {
		body, _ = SetBoolIfDifferent(body, "parallel_tool_calls", false)
	}
	if websocket && snapshot.Specified && !gjson.GetBytes(body, CodexResponsesLiteMetadata).Exists() {
		body, _ = sjson.SetBytes(body, CodexResponsesLiteMetadata, strconv.FormatBool(snapshot.Enabled))
	}
	return body
}

// HasCodexToolDeclarations includes the in-history declaration channel used by Lite.
func HasCodexToolDeclarations(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	// Both root and historical declarations require a tools field. Escaped
	// field names still use the full parser below.
	if !bytes.Contains(body, []byte(`"tools"`)) && !bytes.Contains(body, []byte(`\u`)) {
		return false
	}
	// The read-only view and all parsed results stay within this call. Only the
	// boolean escapes, so releasing the caller's body cannot retain an alias.
	bodyJSON := unsafe.String(unsafe.SliceData(body), len(body))
	if tools := gjson.Get(bodyJSON, "tools"); tools.IsArray() && len(tools.Array()) > 0 {
		return true
	}
	for _, item := range gjson.Get(bodyJSON, "input").Array() {
		if item.Get("type").String() == "additional_tools" && item.Get("tools").IsArray() && len(item.Get("tools").Array()) > 0 {
			return true
		}
	}
	return false
}
