package config

import (
	"encoding/base64"
	"net/http"
	"testing"
)

func TestNormalizeRequestBodyAudit(t *testing.T) {
	rawBinary := []byte{0x00, 'S', 'E', 'C', 'R', 'E', 'T'}
	cfg := NormalizeRequestBodyAudit(RequestBodyAuditConfig{
		Enable:         true,
		Keywords:       []string{" Blocked ", "", "Blocked"},
		KeywordsBase64: []string{base64.StdEncoding.EncodeToString(rawBinary), "not-base64"},
		MaxBodyBytes:   -1,
		Error: RequestBodyAuditErrorConfig{
			StatusCode: 999,
			Message:    " custom ",
			Type:       " custom_error ",
			Code:       " custom_code ",
		},
	})

	if !cfg.Enable {
		t.Fatal("enable = false, want true")
	}
	if cfg.MaxBodyBytes != 0 {
		t.Fatalf("max body bytes = %d, want 0", cfg.MaxBodyBytes)
	}
	if len(cfg.Keywords) != 1 || cfg.Keywords[0] != "Blocked" {
		t.Fatalf("keywords = %#v, want trimmed deduplicated keyword", cfg.Keywords)
	}
	if len(cfg.KeywordsBase64) != 1 {
		t.Fatalf("keywords-base64 = %#v, want only valid base64 keyword", cfg.KeywordsBase64)
	}
	if cfg.Error.StatusCode != http.StatusBadRequest {
		t.Fatalf("status code = %d, want %d", cfg.Error.StatusCode, http.StatusBadRequest)
	}
	if cfg.Error.Message != "custom" || cfg.Error.Type != "custom_error" || cfg.Error.Code != "custom_code" {
		t.Fatalf("error = %#v, want trimmed custom error", cfg.Error)
	}

	compiled := CompiledRequestBodyAuditKeywords(cfg)
	if len(compiled) != 2 {
		t.Fatalf("compiled keywords = %d, want 2", len(compiled))
	}
}

func TestNormalizeRequestBodyAuditDefaultsError(t *testing.T) {
	cfg := NormalizeRequestBodyAudit(RequestBodyAuditConfig{})
	if cfg.Error.StatusCode != DefaultRequestBodyAuditStatusCode {
		t.Fatalf("status code = %d, want %d", cfg.Error.StatusCode, DefaultRequestBodyAuditStatusCode)
	}
	if cfg.Error.Message != DefaultRequestBodyAuditMessage {
		t.Fatalf("message = %q, want default", cfg.Error.Message)
	}
	if cfg.Error.Type != DefaultRequestBodyAuditType {
		t.Fatalf("type = %q, want default", cfg.Error.Type)
	}
	if cfg.Error.Code != DefaultRequestBodyAuditCode {
		t.Fatalf("code = %q, want default", cfg.Error.Code)
	}
}

func TestNormalizeRequestBodyRelease(t *testing.T) {
	cfg := NormalizeRequestBodyRelease(RequestBodyReleaseConfig{
		Enable:       true,
		LogOnly:      true,
		AfterSeconds: -30,
		MinBodyBytes: -1024,
	})
	if !cfg.Enable {
		t.Fatal("enable = false, want true")
	}
	if cfg.AfterSeconds != 0 {
		t.Fatalf("after-seconds = %d, want 0", cfg.AfterSeconds)
	}
	if !cfg.LogOnly {
		t.Fatal("log-only = false, want true")
	}
	if cfg.MinBodyBytes != 0 {
		t.Fatalf("min-body-bytes = %d, want 0", cfg.MinBodyBytes)
	}
}
