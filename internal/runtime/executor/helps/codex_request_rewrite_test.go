package helps

import (
	"bytes"
	"testing"

	"github.com/tidwall/gjson"
)

func TestRewriteCodexRequestEnvelope(t *testing.T) {
	payload := []byte(`{"model":"old","stream":false,"previous_response_id":"resp-1","prompt_cache_retention":"24h","safety_identifier":"safe","stream_options":{"include_usage":true},"instructions":null,"input":[{"type":"input_image","image_url":"data:image/png;base64,AAAA"}],"unknown":{"nested":[1,{"keep":true}]}}`)
	wantInput := gjson.GetBytes(payload, "input").Raw
	wantUnknown := gjson.GetBytes(payload, "unknown").Raw

	got, err := RewriteCodexRequestEnvelope(payload, CodexRequestRewriteOptions{
		Model:              "gpt-5.5",
		Stream:             CodexStreamForceEnabled,
		StripResponseState: true,
		EnsureInstructions: true,
	})
	if err != nil {
		t.Fatalf("RewriteCodexRequestEnvelope() error = %v", err)
	}
	if model := gjson.GetBytes(got, "model").String(); model != "gpt-5.5" {
		t.Fatalf("model = %q", model)
	}
	if !gjson.GetBytes(got, "stream").Bool() {
		t.Fatalf("stream was not enabled: %s", got)
	}
	if instructions := gjson.GetBytes(got, "instructions"); instructions.Type != gjson.String || instructions.String() != "" {
		t.Fatalf("instructions = %s, want empty string", instructions.Raw)
	}
	for _, field := range []string{"previous_response_id", "prompt_cache_retention", "safety_identifier", "stream_options"} {
		if value := gjson.GetBytes(got, field); value.Exists() {
			t.Fatalf("%s was retained: %s", field, got)
		}
	}
	if input := gjson.GetBytes(got, "input").Raw; input != wantInput {
		t.Fatalf("nested input changed:\ngot  %s\nwant %s", input, wantInput)
	}
	if unknown := gjson.GetBytes(got, "unknown").Raw; unknown != wantUnknown {
		t.Fatalf("unknown nested value changed:\ngot  %s\nwant %s", unknown, wantUnknown)
	}
}

func TestRewriteCodexRequestEnvelopePreservesOrRemovesStream(t *testing.T) {
	payload := []byte(`{"model":"old","stream":false,"instructions":"keep","input":"hello"}`)
	preserved, err := RewriteCodexRequestEnvelope(payload, CodexRequestRewriteOptions{
		Model:              "new",
		Stream:             CodexStreamPreserve,
		EnsureInstructions: true,
	})
	if err != nil {
		t.Fatalf("preserve rewrite error = %v", err)
	}
	if gjson.GetBytes(preserved, "stream").Bool() || gjson.GetBytes(preserved, "instructions").String() != "keep" {
		t.Fatalf("preserved payload changed unexpectedly: %s", preserved)
	}

	removed, err := RewriteCodexRequestEnvelope(payload, CodexRequestRewriteOptions{
		Model:  "new",
		Stream: CodexStreamRemove,
	})
	if err != nil {
		t.Fatalf("remove rewrite error = %v", err)
	}
	if gjson.GetBytes(removed, "stream").Exists() {
		t.Fatalf("stream was retained: %s", removed)
	}
}

func TestRewriteCodexRequestEnvelopeRecognizesEscapedKeys(t *testing.T) {
	payload := []byte(`{"\u006dodel":"old","\u0073tream":false,"\u0069nstructions":null,"input":"hello"}`)
	got, err := RewriteCodexRequestEnvelope(payload, CodexRequestRewriteOptions{
		Model:              "new",
		Stream:             CodexStreamForceEnabled,
		EnsureInstructions: true,
	})
	if err != nil {
		t.Fatalf("RewriteCodexRequestEnvelope() error = %v", err)
	}
	if gjson.GetBytes(got, "model").String() != "new" || !gjson.GetBytes(got, "stream").Bool() {
		t.Fatalf("escaped model or stream was not replaced: %s", got)
	}
	if instructions := gjson.GetBytes(got, "instructions"); instructions.Type != gjson.String || instructions.String() != "" {
		t.Fatalf("escaped instructions were not normalized: %s", got)
	}
}

func TestRewriteCodexRequestEnvelopeRejectsInvalidPayload(t *testing.T) {
	for _, payload := range [][]byte{[]byte(`{"model":`), []byte(`[]`), []byte(`{"model":"one"} {"model":"two"}`), nil} {
		if _, err := RewriteCodexRequestEnvelope(payload, CodexRequestRewriteOptions{}); err == nil {
			t.Fatalf("payload %q accepted", payload)
		}
	}
}

func BenchmarkRewriteCodexRequestEnvelopeLargePayload(b *testing.B) {
	payload := make([]byte, 0, 1<<20)
	payload = append(payload, `{"model":"old","stream":false,"input":[{"type":"input_image","image_url":"data:image/png;base64,`...)
	payload = append(payload, bytes.Repeat([]byte("A"), 1<<20)...)
	payload = append(payload, `"}],"previous_response_id":"resp-1"}`...)
	opts := CodexRequestRewriteOptions{
		Model:              "gpt-5.5",
		Stream:             CodexStreamForceEnabled,
		StripResponseState: true,
		EnsureInstructions: true,
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		if _, err := RewriteCodexRequestEnvelope(payload, opts); err != nil {
			b.Fatal(err)
		}
	}
}
