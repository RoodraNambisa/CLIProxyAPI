package openai

import (
	"bytes"
	"image"
	"image/png"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/tidwall/gjson"
)

func TestWebImageAliasFormatMultipartRejectedBeforeExecution(t *testing.T) {
	e := &imageCaptureExecutor{provider: constant.ChatGPTWeb}
	h := newImagesTestHandler(t, e, "gpt-image-2.5")
	h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
	r := gin.New()
	r.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
	r.POST("/v1/images/edits", h.Edits)
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	for k, v := range map[string]string{"model": "gpt-image-2.5", "prompt": "animate", "output_format": "gif", "n": "2"} {
		if err := w.WriteField(k, v); err != nil {
			t.Fatal(err)
		}
	}
	f, err := w.CreateFormFile("image", "reference.png")
	if err != nil {
		t.Fatal(err)
	}
	if err = png.Encode(f, image.NewRGBA(image.Rect(0, 0, 2, 2))); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	q := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
	q.Header.Set("Content-Type", w.FormDataContentType())
	response := httptest.NewRecorder()
	r.ServeHTTP(response, q)
	if response.Code != 400 || gjson.GetBytes(response.Body.Bytes(), "error.code").String() != "unsupported_parameter" || e.calls+e.streamCalls != 0 {
		t.Fatalf("status=%d calls=%d body=%s", response.Code, e.calls+e.streamCalls, response.Body.String())
	}
}

func TestWebImageAliasFormatRejectedBeforeExecution(t *testing.T) {
	for _, stream := range []bool{false, true} {
		e := &imageCaptureExecutor{provider: constant.ChatGPTWeb}
		h := newImagesTestHandler(t, e, "gpt-image-2.5")
		h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
		r := gin.New()
		r.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
		r.POST("/v1/images/generations", h.Generations)
		body := `{"model":"gpt-image-2.5","prompt":"bounce","output_format":"gif"}`
		if stream {
			body = strings.TrimSuffix(body, "}") + `,"stream":true}`
		}
		q := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(body))
		q.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, q)
		if w.Code != 400 || gjson.GetBytes(w.Body.Bytes(), "error.code").String() != "unsupported_parameter" || e.calls+e.streamCalls != 0 {
			t.Fatalf("status=%d calls=%d body=%s", w.Code, e.calls+e.streamCalls, w.Body.String())
		}
	}
}

func TestWebImageAliasFormatOverridesAndMixedRouting(t *testing.T) {
	h := newImagesTestHandler(t, &imageCaptureExecutor{provider: constant.ChatGPTWeb})
	h.Cfg.Images.ChatGPTWeb.ModelOutputFormats = map[string][]string{"gpt-image-2": {"gif"}}
	snapshot := h.chatGPTWebImageConfigSnapshot()
	if err := chatGPTWebImageRequestCompatibilityError(openAIImageRequest{Model: "gpt-image-2", OutputFormat: "gif"}, true, snapshot); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"", "png", "svg"} {
		if err := chatGPTWebImageRequestCompatibilityError(openAIImageRequest{Model: "gpt-image-2", OutputFormat: format}, true, snapshot); err == nil {
			t.Fatalf("format=%q bypassed policy", format)
		}
	}
	providers := imageResponsesProviders(openAIImageRequest{Model: "gpt-image-2.5", OutputFormat: "gif"}, true, snapshot)
	if len(providers) != 1 || providers[0] != constant.Codex {
		t.Fatalf("mixed route=%v", providers)
	}
	h.Cfg.Images.ChatGPTWeb.ModelOutputFormats["gpt-image-2"][0] = "png"
	if snapshot.ModelOutputFormats["gpt-image-2"][0] != "gif" {
		t.Fatal("snapshot mutated")
	}
}
