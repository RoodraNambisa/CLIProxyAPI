package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestHydrateCodexOutputItemIDsPreservesIdentity(t *testing.T) {
	tests := []struct{ name, item, completed, want string }{
		{"missing", `{"type":"message"}`, `{"type":"message","id":"msg_a"}`, `"msg_a"`},
		{"empty", `{"type":"message","id":""}`, `{"type":"message","id":"msg_a"}`, `"msg_a"`},
		{"whitespace", `{"type":"message","id":" \t"}`, `{"type":"message","id":"msg_a"}`, `"msg_a"`},
		{"null", `{"type":"message","id":null}`, `{"type":"message","id":"msg_a"}`, `"msg_a"`},
		{"existing", `{"type":"message","id":"kept"}`, `{"type":"message","id":"msg_a"}`, `"kept"`},
		{"existing number", `{"type":"message","id":123}`, `{"type":"message","id":"msg_a"}`, `123`},
		{"wrong role", `{"type":"message"}`, `{"type":"function_call","id":"fc_a"}`, ""},
		{"wrong call", `{"type":"function_call","call_id":"a"}`, `{"type":"function_call","call_id":"b","id":"fc_b"}`, ""},
		{"paired call", `{"type":"function_call","call_id":"a"}`, `{"type":"function_call","call_id":"a","id":"fc_a"}`, `"fc_a"`},
		{"invalid source ID", `{"type":"message"}`, `{"type":"message","id":123}`, ""},
		{"blank source ID", `{"type":"message"}`, `{"type":"message","id":" "}`, ""},
		{"nonobject", `[]`, `{"type":"message","id":"msg_a"}`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := []byte(`{"type":"response.completed","response":{"output":[` + tt.item + `],"metadata":{"id":"keep"}}}`)
			original := bytes.Clone(source)
			got := HydrateCodexOutputItemIDs(source, map[int64][]byte{0: []byte(tt.completed)})
			if gjson.GetBytes(got, "response.output.0.id").Raw != tt.want || !bytes.Equal(source, original) || gjson.GetBytes(got, "response.metadata.id").String() != "keep" {
				t.Fatal("hydration changed a conflicting identity, source bytes, or unrelated field")
			}
			if repeated := HydrateCodexOutputItemIDs(got, map[int64][]byte{0: []byte(tt.completed)}); !bytes.Equal(repeated, got) {
				t.Fatal("hydration was not idempotent")
			}
		})
	}
	for _, source := range []string{
		`{"response":{"output":[{"type":"message"},{"type":"message","id":"msg_a"}]}}`,
		`{"response":{"output":[{"type":"message"},{"type":"message"}]}}`,
	} {
		got := HydrateCodexOutputItemIDs([]byte(source), map[int64][]byte{0: []byte(`{"type":"message","id":"msg_a"}`), 1: []byte(`{"type":"message","id":"msg_a"}`)})
		if bytes.Count(got, []byte(`"id":"msg_a"`)) != 1 {
			t.Fatal("hydration introduced a duplicate output ID")
		}
	}
}

func TestCollectCodexOutputItemDoneRejectsInvalidIndicesAndOwnsBytes(t *testing.T) {
	for _, index := range []string{"-1", "0.5", `"0"`, "null", "9223372036854775808"} {
		indexed := make(map[int64][]byte)
		var fallback [][]byte
		CollectCodexOutputItemDone([]byte(`{"output_index":`+index+`,"item":{"type":"message","id":"bad"}}`), indexed, &fallback)
		if len(indexed) != 0 || len(fallback) != 0 {
			t.Fatal("invalid index was coerced into a valid position")
		}
	}
	indexed := make(map[int64][]byte)
	var fallback [][]byte
	for _, raw := range []string{`{"output_index":7,"item":{"type":"message","id":"indexed"}}`, `{"item":{"type":"message","id":"fallback"}}`, `{"output_index":0,"item":[]}`} {
		source := []byte(raw)
		CollectCodexOutputItemDone(source, indexed, &fallback)
		clear(source)
	}
	if len(indexed) != 1 || len(fallback) != 1 || gjson.GetBytes(indexed[7], "id").String() != "indexed" || gjson.GetBytes(fallback[0], "id").String() != "fallback" {
		t.Fatal("collected output aliased the input buffer or accepted an array")
	}
}
