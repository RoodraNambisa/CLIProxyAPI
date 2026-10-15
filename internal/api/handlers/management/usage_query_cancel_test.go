package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
)

func TestUsageQueryHandlersPropagateCancellation(t *testing.T) {
	h := &Handler{usageStats: usage.NewRequestStatistics()}
	for _, test := range []struct {
		path   string
		handle func(*gin.Context)
	}{{"/usage/details", h.GetUsageDetails}, {"/usage/failures/summary", h.GetUsageFailureSummary}} {
		t.Run(test.path, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest(http.MethodGet, test.path, nil).WithContext(ctx)
			test.handle(c)
			c.Writer.WriteHeaderNow()
			if w.Code != http.StatusRequestTimeout || w.Body.Len() != 0 {
				t.Fatalf("canceled query returned %d: %s", w.Code, w.Body.String())
			}
		})
	}
}
