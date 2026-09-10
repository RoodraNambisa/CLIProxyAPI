package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
)

func TestFinalizeClosesStartedStreamAfterLoggingDisabled(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "canceled"}[canceled], func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			logger := &testRequestLogger{enabled: false}
			wrapper := NewResponseWriterWrapper(c.Writer, logger, &RequestInfo{})
			wrapper.logOnErrorOnly = true
			// Logging may be enabled after middleware entry and disabled again before finalization.
			logger.enabled = true
			wrapper.Header().Set("Content-Type", "text/event-stream")
			wrapper.WriteHeader(http.StatusOK)
			writer, ok := wrapper.streamWriter.(*testStreamingLogWriter)
			if !ok || writer == nil {
				t.Fatal("stream log never started")
			}
			done := wrapper.streamDone
			t.Cleanup(func() {
				if wrapper.chunkChannel != nil {
					close(wrapper.chunkChannel)
				}
				if wrapper.streamDone != nil {
					<-wrapper.streamDone
				}
				if wrapper.streamWriter != nil {
					_ = wrapper.streamWriter.Close()
				}
			})
			if _, err := wrapper.Write([]byte("data: {}\n\n")); err != nil {
				t.Fatal(err)
			}
			logger.enabled = false
			if canceled {
				c.Set("API_RESPONSE_ERROR", []*interfaces.ErrorMessage{{Error: context.Canceled}})
			}
			if err := wrapper.Finalize(c); err != nil {
				t.Fatal(err)
			}
			if !writer.closed || wrapper.streamWriter != nil || wrapper.chunkChannel != nil || wrapper.streamDone != nil {
				t.Fatal("disabled logging leaked the active stream")
			}
			select {
			case <-done:
			default:
				t.Fatal("stream processor still running")
			}
			if logger.calls != 0 {
				t.Fatal("closing a stream created a second request log")
			}
		})
	}
}
