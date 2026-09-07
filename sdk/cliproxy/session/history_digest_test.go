package session

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestHistoryDigestTextMediaAndToolRoles(t *testing.T) {
	text := historyDigest("user", gjson.Parse(`"hello"`))
	wrapped := historyDigest("user", gjson.Parse(`[{"type":"input_text","text":"hello"}]`))
	if !text.valid || !text.userContent || text.sum != wrapped.sum || text.parts != 1 {
		t.Fatal("equivalent protocol text wrappers diverged")
	}
	for _, role := range []string{"system", "assistant", "tool"} {
		if historyDigest(role, gjson.Parse(`"hello"`)).userContent {
			t.Fatal("a non-user role established a user anchor")
		}
	}
	for _, raw := range []string{`{"type":"reasoning","text":"private"}`, `{"type":"thinking","thinking":"private"}`, `{"thought":true,"text":"private"}`} {
		got := historyDigest("assistant", gjson.Parse(raw))
		if !got.valid || got.parts != 0 || got.userContent {
			t.Fatal("reasoning content became conversation evidence")
		}
	}
	for _, raw := range []string{`{"type":"tool_result","tool_use_id":"one","content":"hello"}`, `{"type":"custom_tool_call_output","call_id":"one","output":"hello"}`, `{"functionResponse":{"name":"run","response":{"text":"hello"}}}`} {
		got := historyDigest("user", gjson.Parse(raw))
		if !got.valid || got.parts != 1 || got.userContent || got.sum == text.sum {
			t.Fatal("tool data was flattened into a user anchor")
		}
	}
	media := historyDigest("user", gjson.Parse(`{"type":"input_image","image_url":"fixture://one"}`))
	if !media.valid || !media.userContent {
		t.Fatal("user media lost its conversation anchor")
	}
	if historyDigest("user", gjson.Parse(`"hello "`)).sum == text.sum || historyDigest("assistant", gjson.Parse(`"hello"`)).sum == text.sum {
		t.Fatal("text bytes or role were erased")
	}
}

func TestHistoryDigestPreservesJSONPrecisionOrderAndLargeContent(t *testing.T) {
	digest := func(raw string) [32]byte { return historyDigest("assistant", gjson.Parse(raw)).sum }
	if digest(`{"value":9007199254740992}`) == digest(`{"value":9007199254740993}`) {
		t.Fatal("large integers lost precision")
	}
	if digest(`{"a":1,"b":2}`) != digest(`{"b":2,"a":1}`) {
		t.Fatal("JSON object order changed the digest")
	}
	if digest(`[{"type":"tool_use","name":"first"},{"type":"tool_use","name":"second"}]`) == digest(`[{"type":"tool_use","name":"second"},{"type":"tool_use","name":"first"}]`) {
		t.Fatal("tool sequence was reordered")
	}
	prefix := strings.Repeat("x", historyCanonicalJSONLimit/2)
	tail := strings.Repeat("x", historyCanonicalJSONLimit*3)
	first := `{"data":"` + prefix + `A` + tail + `"}`
	second := `{"data":"` + prefix + `B` + tail + `"}`
	if digest(first) == digest(second) {
		t.Fatal("large content differences were sampled away")
	}
}

func TestHistoryDigestRejectsTruncatedOrDeepEvidence(t *testing.T) {
	parts := "[" + strings.Repeat(`"item",`, historyMaxParts) + `"last"]`
	if historyDigest("user", gjson.Parse(parts)).valid {
		t.Fatal("excess parts produced a partial matching fingerprint")
	}
	deep := strings.Repeat("[", historyMaxDepth+2) + `"item"` + strings.Repeat("]", historyMaxDepth+2)
	if historyDigest("user", gjson.Parse(deep)).valid {
		t.Fatal("nested content bypassed the depth limit")
	}
	payload := []byte(`{"type":"input_text","text":"hello"}`)
	got := historyDigest("user", gjson.ParseBytes(payload))
	clear(payload)
	if got.sum != historyDigest("user", gjson.Parse(`"hello"`)).sum {
		t.Fatal("digest retained the request buffer")
	}
}
