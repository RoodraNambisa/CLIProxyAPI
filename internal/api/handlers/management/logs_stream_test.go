package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	internallogging "github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	log "github.com/sirupsen/logrus"
)

func TestStreamLogsDisabledAndInvalidFilter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	internallogging.ConfigureLiveLogs(false)
	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)

	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/logs/stream", nil)
	handler.StreamLogs(context)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled status = %d, body=%s", recorder.Code, recorder.Body.String())
	}

	recorder = httptest.NewRecorder()
	context, _ = gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodGet, "/logs/stream?status=secret", nil)
	handler.StreamLogs(context)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("invalid filter status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestStreamLogsReplaysFilteredSafeEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	internallogging.SetupBaseLogger()
	internallogging.ConfigureLiveLogs(true)
	t.Cleanup(func() { internallogging.ConfigureLiveLogs(false) })

	const requestID = "live-stream-test-request"
	log.WithFields(log.Fields{
		"request_id":   requestID,
		"provider":     "chatgpt-web",
		"stage":        "file_sign",
		"status":       403,
		"access_token": "must-not-appear",
	}).Warn("failed for user@example.com with Bearer secret-token")

	handler := NewHandlerWithoutConfigFilePath(&config.Config{}, nil)
	router := gin.New()
	router.GET("/logs/stream", handler.StreamLogs)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/logs/stream?request_id="+requestID, nil)
	requestContext, cancel := context.WithCancel(request.Context())
	request = request.WithContext(requestContext)
	done := make(chan struct{})
	go func() {
		defer close(done)
		router.ServeHTTP(recorder, request)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stream did not stop after cancellation")
	}

	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" ||
		recorder.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatalf("stream response status=%d headers=%v", recorder.Code, recorder.Header())
	}
	if !strings.Contains(body, `"request_id":"`+requestID+`"`) || !strings.Contains(body, `"stage":"file_sign"`) {
		t.Fatalf("filtered event missing: %s", body)
	}
	for _, secret := range []string{"user@example.com", "secret-token", "must-not-appear"} {
		if strings.Contains(body, secret) {
			t.Fatalf("stream leaked %q: %s", secret, body)
		}
	}
}
