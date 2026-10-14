package openai

import (
	"bytes"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"mime/multipart"
	"net/http/httptest"
	"testing"
)

func TestXAIMultipartMixedImageReferences(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	_ = w.WriteField("model", "grok-imagine-image-2.0")
	_ = w.WriteField("images", `[{"url":"https://example.test/a"},{"file_id":"file-1"},"data:image/png;base64,AA"]`)
	_ = w.WriteField("image[]", `{"image_url":{"url":"https://example.test/b"}}`)
	file, err := w.CreateFormFile("image", "fixture.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a})
	_ = w.Close()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/images/edits", &body)
	c.Request.Header.Set("Content-Type", w.FormDataContentType())
	req, err := parseMultipartImageEditRequest(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(req.Images) != 5 || req.Images[3].FileID != "file-1" {
		t.Fatalf("mixed inputs: %+v", req.Images)
	}
	if _, err := parseXAIMultipartReferences(`[{"unrecognized":"input"}]`); err == nil {
		t.Fatal("invalid reference silently discarded")
	}
}

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
	if gjson.GetBytes(applyXAIImagesOptions([]byte(`{"model":"grok-imagine-image"}`), raw), "quality").String() != "medium" {
		t.Fatal("explicit quality must survive until credential capability resolution")
	}
}
