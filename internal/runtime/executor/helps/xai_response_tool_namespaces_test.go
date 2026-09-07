package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestXAIResponseNamespaceMapIsDetachedAndRoleScoped(t *testing.T) {
	original := []byte(`{"tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"description":"discarded schema"}}]}]}`)
	translated := []byte(`{"tools":[{"type":"function","name":"spawn_agent"}]}`)
	namespaces := NewXAIResponseToolNamespaces(original, translated)
	clear(original)
	clear(translated)
	call := `{"type":"function_call","name":"spawn_agent","call_id":"pair","arguments":"{\"name\":\"spawn_agent\"}","metadata":{"type":"function_call","name":"spawn_agent"}}`
	for _, tc := range []struct{ raw, prefix string }{
		{call, ""},
		{`{"type":"response.output_item.added","item":` + call + `}`, "item."},
		{`{"type":"response.output_item.done","item":` + call + `}`, "item."},
		{`{"output":[` + call + `]}`, "output.0."},
		{`{"type":"response.completed","response":{"output":[` + call + `]}}`, "response.output.0."},
	} {
		got := namespaces.Restore([]byte(tc.raw))
		if gjson.GetBytes(got, tc.prefix+"namespace").String() != "collaboration" || gjson.GetBytes(got, tc.prefix+"name").String() != "spawn_agent" || gjson.GetBytes(got, tc.prefix+"call_id").String() != "pair" {
			t.Fatal("namespace restoration changed a wire name or call pairing")
		}
		for _, field := range []string{"arguments", "metadata"} {
			if gjson.GetBytes(got, tc.prefix+field).Raw != gjson.Get(tc.raw, tc.prefix+field).Raw {
				t.Fatal("business data was traversed")
			}
		}
	}
	for _, raw := range []string{`{"type":"function_call","namespace":"other","name":"spawn_agent"}`, `{"type":"custom_tool_call","name":"spawn_agent"}`, `{"type":"function_call","name":"unknown"}`, `{"type":"message","output":[` + call + `]}`} {
		if got := namespaces.Restore([]byte(raw)); string(got) != raw {
			t.Fatal("unrelated response changed")
		}
	}
}

func TestXAIResponseNamespaceMapRejectsAmbiguousAndFilteredNames(t *testing.T) {
	declaration := `{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent"}]}`
	for _, tc := range []struct{ original, translated string }{
		{`{"tools":[` + declaration + `,{"type":"function","name":"spawn_agent"}]}`, `{"tools":[{"type":"function","name":"spawn_agent"}]}`},
		{`{"tools":[` + declaration + `,{"type":"custom","name":"spawn_agent"}]}`, `{"tools":[{"type":"function","name":"spawn_agent"}]}`},
		{`{"tools":[` + declaration + `]}`, `{"tools":[{"type":"function","name":"spawn_agent"},{"type":"function","name":"spawn_agent"}]}`},
		{`{"tools":[` + declaration + `]}`, `{"tools":[]}`},
	} {
		namespaces := NewXAIResponseToolNamespaces([]byte(tc.original), []byte(tc.translated))
		raw := []byte(`{"type":"function_call","name":"spawn_agent"}`)
		if got := namespaces.Restore(raw); !bytes.Equal(got, raw) {
			t.Fatal("ambiguous or undeclared wire name was assigned a namespace")
		}
	}
}
