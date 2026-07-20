package openai

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type trackedOpenAIRequestBody struct {
	io.Reader
	closed bool
}

func (body *trackedOpenAIRequestBody) Close() error {
	body.closed = true
	return nil
}

func TestReadOpenAIJSONRequestBodyWithLimitRejectsChunkedOverflow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("12345"))
	request.ContentLength = -1
	c.Request = request

	_, err := readOpenAIJSONRequestBodyWithLimit(c, 4)
	var maxBytesErr *http.MaxBytesError
	if !errors.As(err, &maxBytesErr) || maxBytesErr.Limit != 4 {
		t.Fatalf("read error = %#v, want MaxBytesError limit 4", err)
	}
}

func TestResponsesRejectsOversizedContentLengthBeforeReading(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	router := gin.New()
	router.POST("/v1/responses", handler.Responses)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	request.ContentLength = int64(executorhelps.ChatGPTWebMaxRequestBytes) + 1
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", response.Code, response.Body.String())
	}
}

func TestReadOpenAIImageJSONRequestBodyUsesPublicImageLimit(t *testing.T) {
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	trackedBody := &trackedOpenAIRequestBody{Reader: strings.NewReader(`{}`)}
	request := httptest.NewRequest(http.MethodPost, "/v1/images/edits", trackedBody)
	request.ContentLength = int64(executorhelps.ChatGPTWebMaxRequestBytes) + 1
	c.Request = request

	body, err := readOpenAIImageJSONRequestBody(c)
	if err != nil {
		t.Fatalf("readOpenAIImageJSONRequestBody() error: %v", err)
	}
	if string(body) != `{}` {
		t.Fatalf("body = %q", body)
	}
	if !trackedBody.closed {
		t.Fatal("parsed request body was not closed")
	}
	if request.Body != http.NoBody || request.ContentLength != 0 {
		t.Fatalf("request body retained after parsing: body=%T contentLength=%d", request.Body, request.ContentLength)
	}
}

func TestReadOpenAIJSONRequestBodyReleasesBodyOnOverflow(t *testing.T) {
	response := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(response)
	trackedBody := &trackedOpenAIRequestBody{Reader: strings.NewReader("12345")}
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", trackedBody)
	request.ContentLength = -1
	c.Request = request

	_, err := readOpenAIJSONRequestBodyWithLimit(c, 4)
	var maxBytesErr *http.MaxBytesError
	if !errors.As(err, &maxBytesErr) {
		t.Fatalf("read error = %#v, want MaxBytesError", err)
	}
	if !trackedBody.closed || request.Body != http.NoBody {
		t.Fatalf("overflow request body was retained: closed=%t body=%T", trackedBody.closed, request.Body)
	}
}

func TestImagesRejectOversizedContentLengthBeforeReading(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewOpenAIImagesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
	router := gin.New()
	router.POST("/v1/images/generations", handler.Generations)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{}`))
	request.ContentLength = int64(maxImageMultipartBytes) + 1
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", response.Code, response.Body.String())
	}
}
