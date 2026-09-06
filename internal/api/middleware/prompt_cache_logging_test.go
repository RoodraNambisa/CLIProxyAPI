package middleware

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	"github.com/tidwall/gjson"
)

type cacheLogCapture struct {
	testRequestLogger
	requestHeaders, responseHeaders map[string][]string
	request, response               []byte
	responseErrors                  []*interfaces.ErrorMessage
	stream                          *cacheStreamCapture
}

func (l *cacheLogCapture) LogRequest(_ string, _ string, requestHeaders map[string][]string, request []byte, _ int, responseHeaders map[string][]string, response, _, _, _, _ []byte, responseErrors []*interfaces.ErrorMessage, _ string, _, _ time.Time) error {
	l.requestHeaders, l.responseHeaders = requestHeaders, responseHeaders
	l.request, l.response = bytes.Clone(request), bytes.Clone(response)
	l.responseErrors = responseErrors
	return nil
}

func (l *cacheLogCapture) LogStreamingRequest(_ string, _ string, headers map[string][]string, body []byte, _ string) (logging.StreamingLogWriter, error) {
	l.requestHeaders = headers
	l.request = bytes.Clone(body)
	l.stream = &cacheStreamCapture{}
	return l.stream, nil
}

type cacheStreamCapture struct {
	testStreamingLogWriter
	chunks  []byte
	headers map[string][]string
}

func (s *cacheStreamCapture) WriteChunkAsync(chunk []byte) { s.chunks = append(s.chunks, chunk...) }
func (s *cacheStreamCapture) WriteStatus(_ int, headers map[string][]string) error {
	s.headers = headers
	return nil
}

func TestRequestLogsRedactCacheKeysWithoutChangingWire(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, key := range []string{"a", "cache-key-value", `quoted"cache\key`} {
		for _, stream := range []bool{false, true} {
			logger := &cacheLogCapture{testRequestLogger: testRequestLogger{enabled: true}}
			body, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "session_id": key, "stream": stream, "model": "astra"})
			response, _ := json.Marshal(map[string]any{"prompt_cache_key": key, "session_id": key, "enabled": true, "model": "astra"})
			originalError := &interfaces.ErrorMessage{StatusCode: 400, Error: errors.New(key), Addon: http.Header{"Session-Id": {key}}}
			router := gin.New()
			router.Use(RequestLoggingMiddleware(logger))
			router.POST("/v1/responses", func(c *gin.Context) {
				wire, err := io.ReadAll(c.Request.Body)
				if err != nil || !bytes.Equal(wire, body) || c.Request.Header.Get("Session-Id") != key {
					t.Error("logging changed incoming wire data")
				}
				c.Set("API_RESPONSE_ERROR", []*interfaces.ErrorMessage{originalError})
				c.Header("Session-Id", key)
				if stream {
					c.Header("Content-Type", "text/event-stream")
				} else {
					c.Header("Content-Type", "application/json")
				}
				c.Status(http.StatusOK)
				split := gjson.GetBytes(response, "prompt_cache_key").Index + 2
				_, _ = c.Writer.Write(response[:split])
				_, _ = c.Writer.WriteString(string(response[split:]))
			})
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			req.Header.Set("Session-Id", key)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			if !bytes.Equal(recorder.Body.Bytes(), response) || recorder.Header().Get("Session-Id") != key {
				t.Fatal("logging changed outgoing wire data")
			}
			if logger.requestHeaders["Session-Id"][0] != util.PromptCacheLogMarker {
				t.Fatal("request header leaked cache identity alias")
			}
			var requestLog map[string]any
			if err := json.Unmarshal(logger.request, &requestLog); err != nil || requestLog["prompt_cache_key"] != util.PromptCacheLogMarker || requestLog["model"] != "astra" {
				t.Fatal("request log did not redact the key safely")
			}
			logged := logger.response
			if stream {
				logged = logger.stream.chunks
				if logger.stream.headers["Session-Id"][0] != util.PromptCacheLogMarker {
					t.Fatal("stream header leaked cache identity alias")
				}
			} else {
				if logger.responseHeaders["Session-Id"][0] != util.PromptCacheLogMarker || logger.responseErrors[0].Error.Error() != util.PromptCacheLogMarker {
					t.Fatal("response header/error leaked cache key")
				}
			}
			var responseLog map[string]any
			if err := json.Unmarshal(logged, &responseLog); err != nil || responseLog["prompt_cache_key"] != util.PromptCacheLogMarker || responseLog["enabled"] != true {
				t.Fatal("split chunks leaked key or corrupted JSON")
			}
			if originalError.Error.Error() != key || originalError.Addon.Get("Session-Id") != key {
				t.Fatal("log projection changed the original error")
			}
		}
	}
}
