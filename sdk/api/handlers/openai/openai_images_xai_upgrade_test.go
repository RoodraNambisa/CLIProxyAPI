package openai

import (
	"github.com/tidwall/gjson"
	"testing"
)

func TestXAIImageV2QualityAndPrivateRefs(t *testing.T) {
	for _, ratio := range []string{"2:1", "1:2", "19.5:9", "9:19.5", "20:9", "9:20", "21:9", "5:2", "auto"} {
		if xaiImagesAspectRatio(ratio, "") != ratio {
			t.Fatal(ratio)
		}
	}
	raw := []byte(`{"model":"grok-imagine-image-2.0","prompt":"fixture","quality":"medium","aspect_ratio":"20:9","storage_options":{"store":true}}`)
	request := buildXAIImagesGenerationsRequest(raw, "grok-imagine-image-2.0", "url")
	if !isXAIImagesModel("grok-imagine-image-2.0") || gjson.GetBytes(request, "model").String() != "grok-imagine-image-2.0" || gjson.GetBytes(request, "quality").String() != "medium" || gjson.GetBytes(request, "aspect_ratio").String() != "20:9" {
		t.Fatal(string(request))
	}
	refs := collectXAIImageRefs([]byte(`{"images":[{"file_id":"file-fixture"},{"url":"https://example.com/image.png"}]}`))
	if len(refs) != 2 || gjson.GetBytes(refs[0], "file_id").String() != "file-fixture" || gjson.GetBytes(refs[1], "url").String() != "https://example.com/image.png" {
		t.Fatalf("refs: %s", refs)
	}
	if gjson.GetBytes(applyXAIImagesOptions([]byte(`{"model":"grok-imagine-image"}`), raw), "quality").Exists() {
		t.Fatal("v2 quality sent to legacy model")
	}
}
