package openai

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketContextRejectsIncompleteReplayAndUnknownReferences(t *testing.T) {
	full := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"full history"}]}`)
	partial := []byte(`{"model":"gpt-5.4","previous_response_id":"older","input":[{"role":"user","content":"only last delta"}]}`)
	for _, incremental := range []bool{false, true} {
		for _, previous := range [][]byte{nil, full, partial} {
			for _, ref := range []string{"last", "unknown"} {
				raw := []byte(`{"type":"response.create","previous_response_id":"` + ref + `","input":[{"role":"user","content":"next"}]}`)
				got, _, errMsg := normalizeResponsesWebsocketRequestWithContext(raw, previous, []byte(`[]`), "last", incremental)
				allowed := len(previous) > 0 && (incremental || (string(previous) == string(full) && ref == "last"))
				if (errMsg == nil) != allowed {
					t.Fatal("context availability check disagrees with transport/history")
				}
				if errMsg == nil && !incremental && (gjson.GetBytes(got, "previous_response_id").Exists() || !strings.Contains(string(got), "full history")) {
					t.Fatal("HTTP replay lost full context or retained state pointer")
				}
			}
		}
	}
	create := []byte(`{"type":"response.create","input":[{"role":"user","content":"complete replacement"}]}`)
	got, _, errMsg := normalizeResponsesWebsocketRequestWithContext(create, partial, []byte(`[]`), "last", false)
	if errMsg != nil || strings.Contains(string(got), "only last delta") || !strings.Contains(string(got), "complete replacement") {
		t.Fatal("full replacement appended a partial transcript")
	}
	appendRequest := []byte(`{"type":"response.append","input":[{"role":"user","content":"next"}]}`)
	if _, _, errMsg = normalizeResponsesWebsocketRequestWithContext(appendRequest, partial, []byte(`[]`), "last", false); errMsg == nil {
		t.Fatal("implicit append crossed to HTTP without full context")
	}
	got, _, errMsg = normalizeResponsesWebsocketRequestWithContext(appendRequest, partial, []byte(`[]`), "last", true)
	if errMsg != nil || gjson.GetBytes(got, "previous_response_id").String() != "last" || len(gjson.GetBytes(got, "input").Array()) != 1 {
		t.Fatal("same-connection append lost the last response reference")
	}
	if _, _, errMsg = normalizeResponsesWebsocketRequestWithContext([]byte(`{"type":"response.create","input":123}`), partial, nil, "last", true); errMsg == nil {
		t.Fatal("invalid replacement bypassed array validation")
	}
	spacedCreate := []byte(`{"type":" response.create ","input":[{"role":"user","content":"new full input"}]}`)
	got, _, errMsg = normalizeResponsesWebsocketRequestWithContext(spacedCreate, partial, []byte(`[]`), "last", false)
	if errMsg != nil || strings.Contains(string(got), "only last delta") {
		t.Fatal("accepted whitespace around request type bypassed full-history replacement")
	}
}
