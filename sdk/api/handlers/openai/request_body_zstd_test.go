package openai

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/zstd"
)

func TestOpenAIRequestBodyDecodesZstd(t *testing.T) {
	gin.SetMode(gin.TestMode)
	plain := []byte(`{"model":"fixture","input":[],"stream":false}`)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	for _, encoding := range []string{"zstd", " ZSTD ", "identity, zstd"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(encoder.EncodeAll(plain, nil)))
		c.Request.Header.Set("Content-Encoding", encoding)
		got, errRead := readOpenAIJSONRequestBodyWithLimit(c, 1024)
		if errRead != nil || !bytes.Equal(got, plain) {
			t.Fatalf("encoding %q: decoded=%t, err=%v", encoding, bytes.Equal(got, plain), errRead)
		}
		if c.Request.Header.Get("Content-Encoding") != "" || c.Request.Body != http.NoBody {
			t.Fatal("encoding header or consumed body retained")
		}
	}
}

func TestOpenAIRequestBodyZstdLimitsAndCorruption(t *testing.T) {
	gin.SetMode(gin.TestMode)
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer encoder.Close()
	wire := encoder.EncodeAll(bytes.Repeat([]byte("x"), 4096), nil)
	for _, tc := range []struct {
		name     string
		wire     []byte
		limit    int64
		overflow bool
	}{
		{"decoded-overflow", wire, 1024, true},
		{"decoder-memory-limit", encoder.EncodeAll(bytes.Repeat([]byte("x"), 2<<20), nil), 1024, true},
		{"encoded-overflow", wire, 4, true},
		{"truncated", wire[:len(wire)-1], 8192, false},
		{"invalid", []byte("not zstd"), 8192, false},
		{"json-marked-zstd", []byte(`{"model":"fixture","input":[]}`), 8192, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", bytes.NewReader(tc.wire))
			c.Request.Header.Set("Content-Encoding", "zstd")
			_, errRead := readOpenAIJSONRequestBodyWithLimit(c, tc.limit)
			if errRead == nil {
				t.Fatal("invalid or oversized body accepted")
			}
			var maxErr *http.MaxBytesError
			if tc.overflow != errors.As(errRead, &maxErr) {
				t.Fatalf("incorrect overflow classification: %v", errRead)
			}
			if c.Request.Body != http.NoBody {
				t.Fatal("failed request body retained")
			}
		})
	}
}
