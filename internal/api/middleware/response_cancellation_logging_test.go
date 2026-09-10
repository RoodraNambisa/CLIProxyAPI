package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

func TestCanceledRequestsDoNotForceErrorLogs(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		failures []*interfaces.ErrorMessage
		want     bool
	}{
		{"499", 499, nil, false},
		{"wrapped cancellation", 200, []*interfaces.ErrorMessage{{Error: fmt.Errorf("request: %w", context.Canceled)}}, false},
		{"wrapped 499", 200, []*interfaces.ErrorMessage{{Error: fmt.Errorf("request: %w", &cancellationLoggingStatusError{error: context.Canceled, status: 499})}}, false},
		{"wrapped 429", 200, []*interfaces.ErrorMessage{{Error: &cancellationLoggingStatusError{error: context.Canceled, status: 429}}}, true},
		{"explicit canceled entry", 200, []*interfaces.ErrorMessage{{StatusCode: 499, Error: errors.New("closed")}}, false},
		{"nil entry", 200, []*interfaces.ErrorMessage{nil}, false},
		{"real quota", http.StatusTooManyRequests, []*interfaces.ErrorMessage{{StatusCode: http.StatusTooManyRequests, Error: fmt.Errorf("quota with canceled body: %w", context.Canceled)}}, true},
		{"real server failure", http.StatusInternalServerError, []*interfaces.ErrorMessage{{Error: context.Canceled}}, true},
		{"mixed errors", 499, []*interfaces.ErrorMessage{{Error: context.Canceled}, {StatusCode: 502, Error: errors.New("upstream failed")}}, true},
		{"deadline", 200, []*interfaces.ErrorMessage{{Error: context.DeadlineExceeded}}, true},
		{"business text", 200, []*interfaces.ErrorMessage{{Error: errors.New("business message mentions context canceled")}}, true},
	} {
		for _, enabled := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/full=%t", tc.name, enabled), func(t *testing.T) {
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				c.Set("API_RESPONSE_ERROR", tc.failures)
				logger := &testRequestLogger{enabled: enabled}
				wrapper := &ResponseWriterWrapper{ResponseWriter: c.Writer, body: &bytes.Buffer{}, logger: logger, requestInfo: &RequestInfo{}, statusCode: tc.status, logOnErrorOnly: true}
				if err := wrapper.Finalize(c); err != nil {
					t.Fatal(err)
				}
				want := 0
				if tc.want || enabled {
					want = 1
				}
				if logger.calls != want {
					t.Fatalf("got %d log calls, want %d", logger.calls, want)
				}
				if tc.status == 499 && wrapper.shouldBufferResponseBody() != enabled {
					t.Fatal("499 body capture ignored logging mode")
				}
			})
		}
	}
}

type cancellationLoggingStatusError struct {
	error
	status int
}

func (e *cancellationLoggingStatusError) StatusCode() int { return e.status }
func (e *cancellationLoggingStatusError) Unwrap() error   { return e.error }
