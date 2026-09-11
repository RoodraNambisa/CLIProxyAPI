package openai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
)

func serveImageModelRequest(h *OpenAIImagesAPIHandler, operation, body string) *httptest.ResponseRecorder {
	router := gin.New()
	router.POST("/v1/images/generations", h.Generations)
	router.POST("/v1/images/edits", h.Edits)
	req := httptest.NewRequest(http.MethodPost, "/v1/images/"+operation, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	return response
}

func TestImage25ModelsSelectCodexToolOrNative(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, model := range []string{"gpt-image-2.5", "gpt-image-2.5-flare", "gpt-image-2.5-sunburst"} {
		for _, native := range []bool{false, true} {
			for _, operation := range []string{"generations", "edits"} {
				t.Run(fmt.Sprintf("%s/native=%t/%s", model, native, operation), func(t *testing.T) {
					exec := &imageCaptureExecutor{}
					if native {
						exec.response = []byte(`{"created":1,"data":[{"b64_json":"bmF0aXZl"}]}`)
					}
					h := newImagesTestHandler(t, exec, model, "gpt-5.4-mini")
					h.Cfg.Images.Native.Generations.Enabled = native
					h.Cfg.Images.Native.Edits.Enabled = native
					input := ""
					if operation == "edits" {
						input = `,"images":[{"image_url":"data:image/png;base64,aGVsbG8="}]`
					}
					response := serveImageModelRequest(h, operation, fmt.Sprintf(`{"model":%q,"prompt":"draw"%s}`, model, input))
					if response.Code != http.StatusOK {
						t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
					}
					if native {
						if exec.sourceFormat != nativeImagesHandlerType || exec.alt != "images/"+operation || gjson.GetBytes(exec.payload, "model").String() != model || response.Body.String() != string(exec.response) {
							t.Fatalf("native variant changed: format=%s alt=%s payload=%s response=%s", exec.sourceFormat, exec.alt, exec.payload, response.Body.String())
						}
					} else if gjson.GetBytes(exec.payload, "tools.0.model").String() != model || exec.model != "gpt-5.4-mini" || gjson.GetBytes(exec.payload, "model").String() != "gpt-5.4-mini" {
						t.Fatalf("tool model was not preserved: model=%s payload=%s", exec.model, exec.payload)
					}
				})
			}
		}
	}
}

func TestNativeImageSwitchDoesNotChangeWeb(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, operation := range []string{"generations", "edits"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", operation, stream), func(t *testing.T) {
				web := &imageCaptureExecutor{provider: "chatgpt-web", streamChunks: []coreexecutor.StreamChunk{{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"aGVsbG8=\"}]}}\n\n")}}}
				codex := &imageCaptureExecutor{}
				h := newMixedImagesTestHandler(t, codex, web)
				native := sdkconfig.NativeImageEndpointConfig{Enabled: true, ParamRules: []string{"quality=high", "background=transparent", "prompt=codex-only"}}
				h.Cfg.Images.Native.Generations = native
				h.Cfg.Images.Native.Edits = native
				input := ""
				if operation == "edits" {
					input = `,"images":[{"image_url":"data:image/png;base64,aGVsbG8="}]`
				}
				response := serveImageModelRequest(h, operation, fmt.Sprintf(`{"model":"gpt-image-2","prompt":"draw","stream":%t%s}`, stream, input))
				if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "aGVsbG8=") {
					t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
				}
				if web.calls+web.streamCalls != 1 || codex.calls+codex.streamCalls != 0 {
					t.Fatalf("Web was excluded: Web=%d Codex=%d", web.calls+web.streamCalls, codex.calls+codex.streamCalls)
				}
				if web.sourceFormat != "openai-response" || web.alt != "" || gjson.GetBytes(web.payload, "tools.0.background").String() == "transparent" || gjson.GetBytes(web.payload, "tools.0.quality").String() == "high" || strings.Contains(string(web.payload), "codex-only") {
					t.Fatalf("Codex native rules leaked into Web: format=%s alt=%s body=%s", web.sourceFormat, web.alt, web.payload)
				}
			})
		}
	}
}

func TestNativeUnsupportedModelStillRoutesToWeb(t *testing.T) {
	gin.SetMode(gin.TestMode)
	web := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newImagesTestHandler(t, web, "web-only")
	h.Cfg.Images.ChatGPTWeb.ImageModels = []string{"web-only"}
	h.Cfg.Images.Native.Generations = sdkconfig.NativeImageEndpointConfig{Enabled: true, Models: []string{"gpt-image-2.5"}}
	response := serveImageModelRequest(h, "generations", `{"model":"web-only","prompt":"draw"}`)
	if response.Code != http.StatusOK || web.calls != 1 {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	if gjson.GetBytes(web.payload, "tools.0.model").String() != "web-only" {
		t.Fatalf("alias lost: %s", web.payload)
	}
}

func TestMixedNativeImageRetryKeepsProviderFormats(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, first := range []string{"codex", "chatgpt-web"} {
		t.Run(first, func(t *testing.T) {
			codex := &imageCaptureExecutor{response: []byte(`{"created":1,"data":[{"b64_json":"bmF0aXZl"}]}`)}
			web := &imageCaptureExecutor{provider: "chatgpt-web"}
			h := newMixedImagesTestHandler(t, codex, web)
			h.Cfg.Images.Native.Generations.Enabled = true
			if first == "codex" {
				auth, _ := h.AuthManager.GetByID("codex-mixed-auth")
				auth.Attributes["priority"] = "2"
				if _, err := h.AuthManager.Update(context.Background(), auth); err != nil {
					t.Fatal(err)
				}
				codex.executeErr = genericImageProviderTestError{message: "temporarily unavailable", status: 503}
			} else {
				web.executeErr = genericImageProviderTestError{message: "temporarily unavailable", status: 503}
			}
			response := serveImageModelRequest(h, "generations", `{"model":"gpt-image-2","prompt":"draw"}`)
			if response.Code != http.StatusOK || codex.calls != 1 || web.calls != 1 {
				t.Fatalf("status=%d Code=%d Web=%d body=%s", response.Code, codex.calls, web.calls, response.Body.String())
			}
			if codex.sourceFormat != nativeImagesHandlerType || web.sourceFormat != "openai-response" {
				t.Fatalf("formats: Code=%s Web=%s", codex.sourceFormat, web.sourceFormat)
			}
			want := "bmF0aXZl"
			if first == "codex" {
				want = "aGVsbG8="
			}
			if gjson.Get(response.Body.String(), "data.0.b64_json").String() != want {
				t.Fatalf("wrong response decoder: %s", response.Body.String())
			}
		})
	}
}

func TestNativeImageNDoesNotDuplicateBatches(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, aggregation := range []bool{false, true} {
		t.Run(fmt.Sprint(aggregation), func(t *testing.T) {
			codex := &imageCaptureExecutor{response: []byte(`{"created":1,"data":[{"b64_json":"bmF0aXZl"},{"b64_json":"bmF0aXZl"}]}`)}
			web := &imageCaptureExecutor{provider: "chatgpt-web"}
			h := newMixedImagesTestHandler(t, codex, web)
			h.Cfg.Images.Native.Generations.Enabled = true
			h.Cfg.Images.EnableNAggregation = &aggregation
			maxN := 2
			h.Cfg.Images.ChatGPTWeb.MaxN = &maxN
			response := serveImageModelRequest(h, "generations", `{"model":"gpt-image-2","prompt":"draw","n":2}`)
			if response.Code != http.StatusOK || len(gjson.Get(response.Body.String(), "data").Array()) != 2 {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if aggregation {
				if web.calls != 2 || codex.calls != 0 {
					t.Fatalf("Web aggregation changed: Web=%d Code=%d", web.calls, codex.calls)
				}
			} else if web.calls != 0 || codex.calls != 1 || gjson.GetBytes(codex.payload, "n").Int() != 2 {
				t.Fatalf("native batch duplicated or n lost: Web=%d Code=%d payload=%s", web.calls, codex.calls, codex.payload)
			}
		})
	}
}

func TestNativeImageStreamSurvivesWebBootstrapFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	wire := "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"bmF0aXZl\"}\n\n"
	codex := &imageCaptureExecutor{streamChunks: []coreexecutor.StreamChunk{{Payload: []byte(wire)}}}
	web := &imageCaptureExecutor{provider: "chatgpt-web", streamChunks: []coreexecutor.StreamChunk{{Err: genericImageProviderTestError{message: "unavailable", status: 503}}}}
	h := newMixedImagesTestHandler(t, codex, web)
	h.Cfg.Images.Native.Generations.Enabled = true
	response := serveImageModelRequest(h, "generations", `{"model":"gpt-image-2","prompt":"draw","stream":true}`)
	if response.Code != http.StatusOK || response.Body.String() != wire {
		t.Fatalf("native stream was converted as Responses: status=%d body=%s", response.Code, response.Body.String())
	}
	if codex.streamCalls != 1 || web.streamCalls != 1 || codex.sourceFormat != nativeImagesHandlerType || web.sourceFormat != "openai-response" {
		t.Fatalf("stream fallback formats: Web=%d/%s Code=%d/%s", web.streamCalls, web.sourceFormat, codex.streamCalls, codex.sourceFormat)
	}
}

func TestNativeImagePassthroughDoesNotUseWebInputNormalization(t *testing.T) {
	gin.SetMode(gin.TestMode)
	codex := &imageCaptureExecutor{response: []byte(`{"created":1,"data":[{"b64_json":"bmF0aXZl"}]}`)}
	web := &imageCaptureExecutor{provider: "chatgpt-web"}
	h := newMixedImagesTestHandler(t, codex, web)
	h.Cfg.Images.Native.Edits.Enabled = true
	h.Cfg.Images.ChatGPTWeb.NormalizeMismatchedImageMIME = true
	response := serveImageModelRequest(h, "edits", `{"model":"gpt-image-2","prompt":"draw","images":[{"image_url":"data:image/png;base64,aGVsbG8="}]}`)
	if response.Code != http.StatusOK || codex.calls != 1 || web.calls != 0 {
		t.Fatalf("Web-only normalization blocked native: status=%d Code=%d Web=%d body=%s", response.Code, codex.calls, web.calls, response.Body.String())
	}
	if gjson.GetBytes(codex.payload, "images.0.image_url").String() != "data:image/png;base64,aGVsbG8=" {
		t.Fatalf("native input was rewritten: %s", codex.payload)
	}
}
