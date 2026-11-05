package openai

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

func TestChatGPTWebImageNewParametersHonorIgnorePolicy(t *testing.T) {
	for _, field := range []string{"complexity", "prompt_rewriter", "markup", "svg"} {
		for _, ignore := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/ignore=%t", field, ignore), func(t *testing.T) {
				e := &imageCaptureExecutor{provider: constant.ChatGPTWeb}
				h := newImagesTestHandler(t, e)
				h.Cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = ignore
				router := gin.New()
				router.Use(allowedImageProvidersMiddleware(constant.ChatGPTWeb))
				router.POST("/v1/images/generations", h.Generations)
				body, err := json.Marshal(map[string]any{"model": "gpt-image-2", "prompt": "draw", field: map[string]any{"requested": true}})
				if err != nil {
					t.Fatal(err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if ignore {
					if w.Code != http.StatusOK || e.calls != 1 {
						t.Fatalf("ignored field status=%d calls=%d body=%s", w.Code, e.calls, w.Body.String())
					}
					if gjson.GetBytes(e.payload, "tools.0."+field).Exists() {
						t.Fatal("unsupported field was sent to Web")
					}
				} else if w.Code == http.StatusOK || e.calls != 0 || gjson.GetBytes(w.Body.Bytes(), "error.param").String() != field || gjson.GetBytes(w.Body.Bytes(), "error.code").String() != "unsupported_parameter" {
					t.Fatalf("unacknowledged field was dropped: status=%d calls=%d body=%s", w.Code, e.calls, w.Body.String())
				}
			})
		}
	}
}

func TestChatGPTWebImageNewParametersNullAndMultipart(t *testing.T) {
	req, err := parseImageGenerationRequestBytes([]byte(`{"prompt":"draw","complexity":null,"prompt_rewriter":null,"markup":null,"svg":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := chatGPTWebImageRequestCompatibilityError(req, false, core.ChatGPTWebImageConfigSnapshot{}); err != nil {
		t.Fatalf("null should be omitted: %v", err)
	}
	for _, field := range []string{"complexity", "prompt_rewriter", "markup", "svg"} {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		if field == "markup" {
			part, err := writer.CreateFormFile(field, "fixture.png")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := part.Write([]byte("fixture")); err != nil {
				t.Fatal(err)
			}
		} else if err := writer.WriteField(field, "requested"); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		r := httptest.NewRequest(http.MethodPost, "/v1/images/edits", &body)
		r.Header.Set("Content-Type", writer.FormDataContentType())
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		parsed, err := parseMultipartImageEditRequest(c)
		if err != nil {
			t.Fatal(err)
		}
		compat := chatGPTWebImageRequestCompatibilityError(parsed, false, core.ChatGPTWebImageConfigSnapshot{})
		if compat == nil || compat.parameter != field {
			t.Fatalf("multipart %s was dropped: %v", field, compat)
		}
		if err := chatGPTWebImageRequestCompatibilityError(parsed, true, core.ChatGPTWebImageConfigSnapshot{}); err != nil {
			t.Fatalf("multipart ignore policy: %v", err)
		}
		if r.MultipartForm != nil {
			if err := r.MultipartForm.RemoveAll(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
