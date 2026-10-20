package middleware

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestDecodedRequestBodyIsAuditedAndBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	for _, tc := range []struct {
		name, encoding string
		body           []byte
		status         int
	}{
		{"allowed", "zstd", encoder.EncodeAll([]byte(`{"input":"hello"}`), nil), 200},
		{"audited", "zstd", encoder.EncodeAll([]byte(`{"input":"blocked"}`), nil), 403},
		{"already-decoded", "zstd", []byte(`{"input":"hello"}`), 200},
		{"plain-audited", "", []byte(`{"input":"blocked"}`), 403},
		{"oversized", "zstd", encoder.EncodeAll(bytes.Repeat([]byte("x"), 4096), nil), 413},
		{"corrupt", "zstd", []byte("invalid compressed body"), 400},
		{"unsupported", "compress", []byte(`{}`), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.Use(RequestBodyDecodingMiddleware(1024))
			router.Use(RequestBodyAuditMiddleware(func() config.RequestBodyAuditConfig {
				return config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
					Enable: true, Keywords: []string{"blocked"},
					Error: config.RequestBodyAuditErrorConfig{StatusCode: 403, Message: "blocked"},
				})
			}))
			router.POST("/v1/responses", func(c *gin.Context) {
				got, errRead := io.ReadAll(c.Request.Body)
				if errRead != nil || string(got) != `{"input":"hello"}` {
					t.Error("decoded body changed")
				}
				if c.Request.Header.Get("Content-Encoding") != "" {
					t.Error("stale encoding reached handler")
				}
				c.Status(200)
			})
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(tc.body))
			req.Header.Set("Content-Encoding", tc.encoding)
			router.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("status=%d, want %d: %s", rec.Code, tc.status, rec.Body.String())
			}
		})
	}
}
