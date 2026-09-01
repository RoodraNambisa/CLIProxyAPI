package handlers

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type sanitizerTestImageError struct {
	message string
	code    string
	stage   string
}

func (e sanitizerTestImageError) Error() string { return e.message }
func (e sanitizerTestImageError) ExecutionResultErrorCode() string {
	return e.code
}
func (e sanitizerTestImageError) ChatGPTWebFailureStage() string { return e.stage }

func TestChatGPTWebImageErrorSanitizationDefaultsOff(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	handler.BeginChatGPTWebImageErrorSanitization(c, true)
	original := `{"error":{"message":"chatgpt web image task failed","type":"server_error","code":"chatgpt_web_image_poll_stalled","failure_stage":"poll"}}`
	handler.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusServiceUnavailable, Error: errors.New(original)})
	if recorder.Code != http.StatusServiceUnavailable || recorder.Body.String() != original {
		t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebImageErrorSanitizationReturnsSafeActionableInputErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	maxEdge := 2048
	maxN := 4
	tests := []struct {
		name        string
		err         error
		wantParam   string
		wantMessage []string
	}{
		{
			name:        "size",
			err:         errors.New(`{"error":{"message":"Invalid value for 'size': \"512x512\" cannot be handled by chatgpt web image generation.","type":"invalid_request_error","param":"size","code":"invalid_value"}}`),
			wantParam:   "size",
			wantMessage: []string{"512x512", "multiples of 16", "between 1:3 and 3:1", "2048 pixels", "655360", "8294400"},
		},
		{
			name:        "count",
			err:         errors.New(`{"error":{"message":"Invalid value for 'n': 8 exceeds the chatgpt web image generation limit of 4.","type":"invalid_request_error","param":"n","code":"invalid_value"}}`),
			wantParam:   "n",
			wantMessage: []string{"between 1 and 4"},
		},
		{
			name:        "mime mismatch",
			err:         &executorhelps.ChatGPTWebImageMIMEMismatchError{Declared: "image/jpg", Detected: "image/png"},
			wantParam:   "image",
			wantMessage: []string{"declared MIME type \"image/jpeg\"", "detected format \"image/png\""},
		},
		{
			name:        "base64",
			err:         errors.New("invalid images[0]: invalid base64 image data: corrupt input byte 8"),
			wantParam:   "image",
			wantMessage: []string{"image data is not valid base64"},
		},
		{
			name:        "decode",
			err:         errors.New("invalid images[0]: decode image: image: unknown format"),
			wantParam:   "image",
			wantMessage: []string{"could not be decoded as JPEG, PNG, GIF, or WebP"},
		},
		{
			name:        "mask",
			err:         errors.New(`{"error":{"message":"chatgpt web does not support WebP masks","type":"invalid_request_error","param":"mask","code":"unsupported_parameter"}}`),
			wantParam:   "mask",
			wantMessage: []string{"WebP masks are not supported"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{
				SanitizeErrorResponses: true,
				MaxResizeEdgePixels:    &maxEdge,
				MaxN:                   &maxN,
			}}}, nil)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			handler.BeginChatGPTWebImageErrorSanitization(c, true)
			handler.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: tt.err})

			var response struct {
				Error struct {
					Message string `json:"message"`
					Param   string `json:"param"`
					Code    string `json:"code"`
				} `json:"error"`
			}
			if errUnmarshal := json.Unmarshal(recorder.Body.Bytes(), &response); errUnmarshal != nil {
				t.Fatalf("decode response: %v", errUnmarshal)
			}
			if response.Error.Param != tt.wantParam || response.Error.Code != "invalid_value" {
				t.Fatalf("error = %#v", response.Error)
			}
			for _, fragment := range tt.wantMessage {
				if !strings.Contains(response.Error.Message, fragment) {
					t.Fatalf("message %q missing %q", response.Error.Message, fragment)
				}
			}
			assertSanitizedImageBody(t, recorder.Body.String())
		})
	}
}

func TestChatGPTWebImageErrorSanitizationPreservesSafeRewriteAndBlocksUnsafeRewrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name     string
		body     map[string]any
		wantBody string
	}{
		{
			name: "safe rewrite",
			body: map[string]any{"error": map[string]any{
				"message": "Rate limit reached for image generation. Please try again later.",
				"type":    "rate_limit_error",
				"code":    "rate_limit_exceeded",
			}},
			wantBody: "Rate limit reached for image generation",
		},
		{
			name: "unsafe rewrite",
			body: map[string]any{"error": map[string]any{
				"message":   "request failed at https://internal.example/tasks/123",
				"pollState": "stalled",
				"taskId":    "task_123",
			}},
			wantBody: "Rate limit reached for image generation",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
				Images: sdkconfig.ImagesConfig{ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{SanitizeErrorResponses: true}},
				ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{{
					StatusCode:         http.StatusBadRequest,
					MessageContains:    "chatgpt web",
					ResponseStatusCode: http.StatusTooManyRequests,
					ResponseBody:       &tt.body,
				}},
			}, nil)
			original := &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New("chatgpt web image request failed")}
			projected := handler.RewriteExecutionErrorResponse(original)
			if projected == original {
				t.Fatal("rewrite did not match the original error")
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			handler.BeginChatGPTWebImageErrorSanitization(c, true)
			handler.WriteErrorResponse(c, projected)
			if recorder.Code != http.StatusTooManyRequests || !strings.Contains(recorder.Body.String(), tt.wantBody) {
				t.Fatalf("response = %d %s", recorder.Code, recorder.Body.String())
			}
			assertSanitizedImageBody(t, recorder.Body.String())
		})
	}
}

func TestChatGPTWebImageErrorSanitizationFiltersHeadersAndPreservesInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Images: sdkconfig.ImagesConfig{ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{SanitizeErrorResponses: true}},
	}, nil)
	originalError := sanitizerTestImageError{
		message: "chatgpt web image poll task task_123 failed at https://internal.example",
		code:    "chatgpt_web_image_poll_stalled",
		stage:   "poll",
	}
	original := &interfaces.ErrorMessage{
		StatusCode: http.StatusServiceUnavailable,
		Error:      originalError,
		Addon: http.Header{
			"Retry-After":     {"30"},
			"X-Upstream-Task": {"task_123"},
			"Server":          {"internal"},
		},
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	handler.BeginChatGPTWebImageErrorSanitization(c, true)
	handler.WriteErrorResponse(c, original)

	if recorder.Code != http.StatusServiceUnavailable || recorder.Header().Get("Retry-After") != "30" {
		t.Fatalf("status/Retry-After = %d/%q", recorder.Code, recorder.Header().Get("Retry-After"))
	}
	if recorder.Header().Get("X-Upstream-Task") != "" || recorder.Header().Get("Server") != "" {
		t.Fatalf("unsafe headers leaked: %v", recorder.Header())
	}
	if OriginalErrorText(original) != originalError.Error() || OriginalErrorStatusCode(original) != http.StatusServiceUnavailable {
		t.Fatal("sanitization mutated the internal error")
	}
	assertSanitizedImageBody(t, recorder.Body.String())
	if !strings.Contains(recorder.Body.String(), "temporarily unavailable") {
		t.Fatalf("body = %s", recorder.Body.String())
	}
}

func TestChatGPTWebImageErrorSanitizationProjectsOperationalCategories(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name        string
		status      int
		err         sanitizerTestImageError
		wantMessage string
		wantType    string
		wantCode    string
	}{
		{
			name:        "content safety",
			status:      http.StatusBadRequest,
			err:         sanitizerTestImageError{message: "chatgpt web moderation result", code: "moderation_blocked", stage: "settle"},
			wantMessage: "rejected by the safety system",
			wantType:    "image_generation_user_error",
			wantCode:    "moderation_blocked",
		},
		{
			name:        "quota",
			status:      http.StatusTooManyRequests,
			err:         sanitizerTestImageError{message: "chatgpt web image quota exhausted", code: "chatgpt_web_image_quota", stage: "settle"},
			wantMessage: "Rate limit reached for image generation",
			wantType:    "rate_limit_error",
			wantCode:    "rate_limit_exceeded",
		},
		{
			name:        "capacity",
			status:      http.StatusServiceUnavailable,
			err:         sanitizerTestImageError{message: "chatgpt web image capacity exhausted", code: "image_generation_capacity", stage: "admission"},
			wantMessage: "temporarily unavailable",
			wantType:    "server_error",
			wantCode:    "internal_server_error",
		},
		{
			name:        "network protocol",
			status:      http.StatusBadGateway,
			err:         sanitizerTestImageError{message: "chatgpt web upstream protocol failed", code: "chatgpt_web_image_upstream_failed", stage: "upload"},
			wantMessage: "An error occurred while processing the image",
			wantType:    "server_error",
			wantCode:    "internal_server_error",
		},
		{
			name:        "no output",
			status:      http.StatusBadGateway,
			err:         sanitizerTestImageError{message: "chatgpt web task returned no image", code: "chatgpt_web_image_no_output", stage: "settle"},
			wantMessage: "An error occurred while processing the image",
			wantType:    "server_error",
			wantCode:    "internal_server_error",
		},
		{
			name:        "authentication",
			status:      http.StatusUnauthorized,
			err:         sanitizerTestImageError{message: "chatgpt web credential rejected", code: "http_401", stage: "upload"},
			wantMessage: "Authentication failed for image generation",
			wantType:    "authentication_error",
			wantCode:    "invalid_api_key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
				ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{SanitizeErrorResponses: true},
			}}, nil)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			handler.BeginChatGPTWebImageErrorSanitization(c, true)
			handler.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: tt.status, Error: tt.err})

			var response sanitizedImageErrorResponse
			if errUnmarshal := json.Unmarshal(recorder.Body.Bytes(), &response); errUnmarshal != nil {
				t.Fatalf("decode response: %v", errUnmarshal)
			}
			if recorder.Code != tt.status || response.Error.Type != tt.wantType || response.Error.Code != tt.wantCode ||
				!strings.Contains(response.Error.Message, tt.wantMessage) {
				t.Fatalf("response = %d %#v", recorder.Code, response.Error)
			}
			assertSanitizedImageBody(t, recorder.Body.String())
		})
	}
}

func TestChatGPTWebImageErrorSanitizationDoesNotAffectUnidentifiedOrNonImageErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{
		Images: sdkconfig.ImagesConfig{ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{SanitizeErrorResponses: true}},
	}, nil)
	for _, imageRequest := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		handler.BeginChatGPTWebImageErrorSanitization(c, imageRequest)
		handler.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New("n > 1 is not supported")})
		var response sanitizedImageErrorResponse
		if errUnmarshal := json.Unmarshal(recorder.Body.Bytes(), &response); errUnmarshal != nil || response.Error.Message != "n > 1 is not supported" {
			t.Fatalf("image=%t body=%s", imageRequest, recorder.Body.String())
		}
	}
}

func assertSanitizedImageBody(t *testing.T, body string) {
	t.Helper()
	lower := strings.ToLower(body)
	for _, forbidden := range []string{"chatgpt web", "chatgpt-web", "chatgpt_web", "failure_stage", "task_", "conversation_", "internal.example"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("sanitized body leaked %q: %s", forbidden, body)
		}
	}
}
