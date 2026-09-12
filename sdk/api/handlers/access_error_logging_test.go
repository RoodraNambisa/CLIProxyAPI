package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestHandlerFailuresAppearInAccessLogsWithRequestLoggingDisabled(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			hook := logtest.NewGlobal()
			defer hook.Reset()
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{RequestLog: false}, nil)
			router := gin.New()
			router.Use(logging.GinLogrusLogger())
			router.POST("/v1/chat/completions", func(c *gin.Context) {
				failure := &interfaces.ErrorMessage{StatusCode: 503, Error: errors.New("auth_not_found: no auth available")}
				if !stream {
					handler.WriteErrorResponse(c, failure)
					return
				}
				c.Header("Content-Type", "text/event-stream")
				c.Writer.Flush()
				errorsChannel := make(chan *interfaces.ErrorMessage, 1)
				errorsChannel <- failure
				close(errorsChannel)
				handler.ForwardStream(c, c.Writer, func(error) {}, nil, errorsChannel, StreamForwardOptions{
					WriteTerminalError: func(*interfaces.ErrorMessage) { _, _ = c.Writer.WriteString("event: error\ndata: {}\n\n") },
				})
			})
			writer := httptest.NewRecorder()
			router.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))
			entry := hook.LastEntry()
			if entry == nil || !strings.Contains(entry.Message, "auth_not_found: no auth available") {
				t.Fatal("pre-executor failure was lost in access logging")
			}
			wantStatus := 503
			if stream {
				wantStatus = 200
			}
			if writer.Code != wantStatus || entry.Data["status"] != wantStatus {
				t.Fatal("logging changed the response status")
			}
		})
	}
}

func TestAccessErrorLoggingPreservesCancellationAndPublicRewrite(t *testing.T) {
	publicBody := map[string]any{"error": map[string]any{"code": "public_failure", "message": "public explanation"}}
	for _, testCase := range []struct {
		name    string
		failure *interfaces.ErrorMessage
		rewrite bool
	}{
		{name: "cancel without status", failure: &interfaces.ErrorMessage{Error: context.Canceled}},
		{name: "client closed", failure: &interfaces.ErrorMessage{StatusCode: 499, Error: context.Canceled}},
		{name: "rewritten error", failure: &interfaces.ErrorMessage{StatusCode: 503, Error: errors.New("original explanation")}, rewrite: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			hook := logtest.NewGlobal()
			defer hook.Reset()
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{ErrorResponseRewrites: []sdkconfig.ErrorResponseRewriteRule{{
				StatusCode: 503, ResponseStatusCode: 429, ResponseBody: &publicBody,
			}}}, nil)
			router := gin.New()
			router.Use(logging.GinLogrusLogger())
			router.GET("/stream", func(c *gin.Context) {
				c.Writer.Flush()
				failure := testCase.failure
				if testCase.rewrite {
					failure = handler.RewriteExecutionErrorResponse(failure)
				}
				handler.LoggingAPIResponseError(context.WithValue(c.Request.Context(), "gin", c), failure)
			})
			writer := httptest.NewRecorder()
			router.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/stream", nil))
			entry := hook.LastEntry()
			if writer.Code != 200 || entry == nil {
				t.Fatal("logging changed stream behavior")
			}
			if testCase.rewrite {
				if entry.Level != log.WarnLevel || !strings.Contains(entry.Message, "error_status=429") || !strings.Contains(entry.Message, "public explanation") || strings.Contains(entry.Message, "original explanation") {
					t.Fatal("access log did not use the public rewritten error")
				}
			} else if entry.Level != log.InfoLevel || strings.Contains(entry.Message, "error_status=") {
				t.Fatal("client cancellation was logged as an upstream failure")
			}
		})
	}
}
