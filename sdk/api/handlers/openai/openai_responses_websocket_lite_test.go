package openai

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketNormalizationDoesNotInheritLiteMetadata(t *testing.T) {
	previous := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"first"}],"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`)
	for _, incremental := range []bool{false, true} {
		for _, kind := range []string{"response.create", "response.append"} {
			for _, metadata := range []string{"", `,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"false","other":"keep"}`} {
				frame := []byte(`{"type":"` + kind + `","input":[{"role":"user","content":"next"}]` + metadata + `}`)
				normalized, _, failure := normalizeResponsesWebsocketRequestWithMode(frame, previous, []byte(`[]`), incremental)
				if failure != nil {
					t.Fatal("valid follow-up frame failed normalization")
				}
				headers := http.Header{}
				headers.Set(helps.CodexResponsesLiteHeader, "true")
				snapshot := helps.SnapshotCodexResponsesLite(normalized, headers, true)
				if snapshot.Enabled || !snapshot.Specified {
					t.Fatal("normalized frame inherited Lite mode from history or handshake")
				}
				if metadata != "" && gjson.GetBytes(normalized, "client_metadata.other").String() != "keep" {
					t.Fatal("normalization dropped current frame metadata")
				}
			}
		}
	}
}
