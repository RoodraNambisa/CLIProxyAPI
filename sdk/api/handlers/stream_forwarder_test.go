package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type countingFlusher struct {
	count int
}

func (f *countingFlusher) Flush() {
	f.count++
}

func newForwardStreamTestContext(t *testing.T) (*BaseAPIHandler, *gin.Context, *countingFlusher) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/stream", nil)
	return NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil), c, &countingFlusher{}
}

func TestForwardStreamBatchesFlushesByMinBytes(t *testing.T) {
	h, c, flusher := newForwardStreamTestContext(t)
	data := make(chan []byte, 4)
	errs := make(chan *interfaces.ErrorMessage)
	data <- []byte("ab")
	data <- []byte("cd")
	data <- []byte("ef")
	data <- []byte("g")
	close(data)
	close(errs)

	interval := time.Hour
	h.ForwardStream(c, flusher, func(error) {}, data, errs, StreamForwardOptions{
		FlushInterval: &interval,
		FlushMinBytes: 5,
		WriteChunk: func(chunk []byte) {
			_, _ = c.Writer.Write(chunk)
		},
	})

	if flusher.count >= 5 {
		t.Fatalf("flush count = %d, want fewer than per-chunk flushing", flusher.count)
	}
}
