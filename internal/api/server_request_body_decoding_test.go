package api

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestServerZstdCannotBypassBodyAudit(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *config.Config) {
		cfg.RequestBodyAudit = config.NormalizeRequestBodyAudit(config.RequestBodyAuditConfig{
			Enable: true, Keywords: []string{"blocked-marker"},
			Error: config.RequestBodyAuditErrorConfig{StatusCode: 403, Message: "audit blocked"},
		})
	})
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	for _, path := range []string{"/v1/responses", "/v1/responses/compact", "/v1/chat/completions"} {
		for _, authenticated := range []bool{false, true} {
			req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoder.EncodeAll([]byte(`{"model":"fixture","input":"blocked-marker"}`), nil)))
			req.Header.Set("Content-Encoding", "zstd")
			req.Header.Set("Content-Type", "application/json")
			if authenticated {
				req.Header.Set("Authorization", "Bearer test-key")
			}
			rec := httptest.NewRecorder()
			server.engine.ServeHTTP(rec, req)
			want := 401
			if authenticated {
				want = 403
			}
			if rec.Code != want {
				t.Fatalf("%s auth=%t status=%d want=%d: %s", path, authenticated, rec.Code, want, rec.Body.String())
			}
		}
	}
}
