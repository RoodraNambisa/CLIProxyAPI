package openai

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	helpers "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestOpenAIImagesGIFWebRoundTripAndControls(t *testing.T) {
	var data bytes.Buffer
	p := color.Palette{color.Black, color.White}
	a := &gif.GIF{Image: []*image.Paletted{image.NewPaletted(image.Rect(0, 0, 4, 4), p), image.NewPaletted(image.Rect(0, 0, 4, 4), p)}, Delay: []int{18, 18}, LoopCount: 0}
	if err := gif.EncodeAll(&data, a); err != nil {
		t.Fatal(err)
	}
	e := &imageCaptureExecutor{provider: "chatgpt-web", response: []byte(fmt.Sprintf(`{"output":[{"type":"image_generation_call","result":%q,"output_format":"gif"}]}`, base64.StdEncoding.EncodeToString(data.Bytes())))}
	h := newImagesTestHandler(t, e, "gpt-image-2.6")
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2.6","prompt":"bounce","output_format":"gif","frame_count":2,"frame_duration_ms":180}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	parsed, err := helpers.ParseChatGPTWebRequest(e.payload)
	if err != nil || parsed.Image == nil || parsed.Image.FrameCount != 2 || parsed.Image.FrameDurationMS != 180 || parsed.Image.OutputFormat != "gif" {
		t.Fatalf("parameters lost: %+v %v", parsed.Image, err)
	}
	if gjson.Get(w.Body.String(), "output_format").String() != "gif" {
		t.Fatal("wrong response format")
	}
	output, err := base64.StdEncoding.DecodeString(gjson.Get(w.Body.String(), "data.0.b64_json").String())
	if err != nil {
		t.Fatal(err)
	}
	actual, err := gif.DecodeAll(bytes.NewReader(output))
	if err != nil || len(actual.Image) != 2 || actual.Delay[0] != 18 {
		t.Fatalf("animation lost: %v", err)
	}
}

func TestOpenAIImagesGIFMultipartControls(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	for k, v := range map[string]string{"prompt": "bounce", "output_format": "gif", "frame_count": "9", "frame_duration_ms": "180"} {
		if err := w.WriteField(k, v); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	r.Header.Set("Content-Type", w.FormDataContentType())
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		t.Fatal(err)
	}
	defer r.MultipartForm.RemoveAll()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = r
	parsed, err := parseMultipartImageEditRequest(c)
	if err != nil || parsed.FrameCount == nil || *parsed.FrameCount != 9 || parsed.FrameDurationMS == nil || *parsed.FrameDurationMS != 180 {
		t.Fatalf("multipart: %+v %v", parsed, err)
	}
}

func TestOpenAIImagesGIFInvalidControlsCannotBeIgnored(t *testing.T) {
	for _, tc := range []struct {
		format string
		frames int
		delay  int
	}{{"gif", 0, 180}, {"gif", 1, 180}, {"gif", 65, 180}, {"gif", 9, 0}, {"gif", 9, 19}, {"gif", 9, 2001}, {"png", 9, 180}} {
		request := openAIImageRequest{Model: "gpt-image-2.6", OutputFormat: tc.format, FrameCount: &tc.frames, FrameDurationMS: &tc.delay}
		if err := chatGPTWebImageRequestCompatibilityError(request, true, core.ChatGPTWebImageConfigSnapshot{}); err == nil || !err.providerFiltered {
			t.Fatalf("invalid animation controls accepted: %+v", tc)
		}
	}
}
