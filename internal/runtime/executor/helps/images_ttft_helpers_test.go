package helps

import "testing"

func TestImageTTFTRequiresActualImageData(t *testing.T) {
	for _, payload := range []string{
		`{"data":[{"b64_json":"fixture"}]}`,
		`{"data":[{}, {"url":"https://fixture.invalid/image"}]}`,
		`{"type":"image_generation.partial_image","b64_json":"fixture"}`,
		`{"type":"image_generation.completed","b64_json":"fixture"}`,
		`{"type":"image_edit.partial_image","b64_json":"fixture"}`,
		`{"type":"image_edit.completed","url":"https://fixture.invalid/image"}`,
		`{"type":"response.image_generation_call.partial_image","partial_image_b64":"fixture"}`,
	} {
		if !IsOpenAIImageTokenEvent([]byte(payload)) {
			t.Fatalf("image content not recognized: %s", payload)
		}
	}
	for _, payload := range []string{
		``, `{"data":[]}`, `{"data":{"b64_json":"fixture"}}`,
		`{"data":[{"b64_json":"","url":null}]}`, `{"data":[{"b64_json":123}]}`,
		`{"type":"image_generation.completed","usage":{"output_tokens":2}}`,
		`{"type":"image_generation.in_progress","b64_json":"metadata"}`,
		`{"type":"error","b64_json":"metadata"}`, `{"data":[{"b64_json":"fixture"}]`,
		`{"type":"response.created","response":{"output":[]}}`,
	} {
		if IsOpenAIImageTokenEvent([]byte(payload)) {
			t.Fatalf("metadata counted as image content: %s", payload)
		}
	}
}
