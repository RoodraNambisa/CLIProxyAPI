package helps

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type diagnosticStatusError struct {
	status int
	body   string
}

func (e diagnosticStatusError) Error() string   { return e.body }
func (e diagnosticStatusError) StatusCode() int { return e.status }
func (e diagnosticStatusError) Headers() http.Header {
	return http.Header{"X-Request-Id": {"req-upstream-fixture"}}
}

func TestUsageFailurePreservesPublicUpstreamCauseAndRemovesEchoedSecrets(t *testing.T) {
	ctx := executor.WithUpstreamAttempt(logging.WithRequestID(t.Context(), "req-local-fixture"))
	executor.MarkUpstreamAttempt(ctx)
	cause := fmt.Errorf("gateway: %w", diagnosticStatusError{429, `{"error":{"type":"quota_error","code":"usage_limit_reached","message":"user fixture@example.test has exhausted quota; token=secret-material sk-fixture-secret-123456", "resets_at":12345678,"access_token":"never-keep","input":"private prompt"},"request":{"input":"private request"}}`})
	record := usage.Record{Failed: true}
	populateUsageFailure(ctx, &record, cause)
	if record.StatusCode != 429 || record.ErrorCode != "usage_limit_reached" || record.ErrorType != "quota_error" || record.FailureStage != "upstream" {
		t.Fatalf("classification = %d %s %s %s", record.StatusCode, record.ErrorCode, record.ErrorType, record.FailureStage)
	}
	if record.RequestID != "req-local-fixture" || record.UpstreamRequestID != "req-upstream-fixture" || !strings.Contains(record.ErrorResponse, "resets_at") {
		t.Fatal("public recovery and tracing details were lost")
	}
	for _, secret := range []string{"fixture@example.test", "secret-material", "sk-fixture-secret-123456", "never-keep", "private prompt", "private request"} {
		if strings.Contains(record.ErrorMessage+record.ErrorResponse, secret) {
			t.Fatal("sensitive content retained")
		}
	}
}

func TestUsageFailureFallbacks(t *testing.T) {
	for _, tt := range []struct {
		name   string
		cause  error
		code   string
		status int
	}{
		{"missing", nil, "request_failed", 0},
		{"http", diagnosticStatusError{503, "service temporarily unavailable"}, "http_503", 503},
		{"cancel", fmt.Errorf("stream: %w", context.Canceled), "request_canceled", 0},
		{"deadline", context.DeadlineExceeded, "request_deadline_exceeded", 0},
		{"network", errors.New("dial https://name:password@example.test/path?token=secret failed"), "request_failed", 0},
		{"json without message", diagnosticStatusError{502, `{"input":"private full request"}`}, "http_502", 502},
		{"detail", diagnosticStatusError{503, `{"detail":"service unavailable"}`}, "http_503", 503},
	} {
		t.Run(tt.name, func(t *testing.T) {
			record := usage.Record{Failed: true}
			populateUsageFailure(t.Context(), &record, tt.cause)
			if record.ErrorCode != tt.code || record.StatusCode != tt.status {
				t.Fatalf("classification = %s %d", record.ErrorCode, record.StatusCode)
			}
			if strings.Contains(record.ErrorMessage, "password") || strings.Contains(record.ErrorMessage, "secret") || strings.Contains(record.ErrorMessage, "private full request") {
				t.Fatal("unsafe fallback message")
			}
		})
	}
	record := usage.Record{Failed: true}
	populateUsageFailure(t.Context(), &record, diagnosticStatusError{500, strings.Repeat("错", 40000)})
	if len(record.ErrorMessage) > 1024 {
		t.Fatal("unbounded error preview")
	}
}

func TestUsageFailureProtectsExplicitCacheKeysAndOpaqueCredentials(t *testing.T) {
	ctx := WithCodexPromptCacheLogRedaction(t.Context(), util.NewPromptCacheLogRedactor("cache-fixture-value"))
	record := usage.Record{APIKey: "opaque-client-key", Failed: true}
	populateUsageFailure(ctx, &record, diagnosticStatusError{500, `{"error":{"message":"cache-fixture-value opaque-client-key opaque-upstream-key"}}`}, "opaque-upstream-key")
	for _, private := range []string{"cache-fixture-value", "opaque-client-key", "opaque-upstream-key"} {
		if strings.Contains(record.ErrorMessage+record.ErrorResponse, private) {
			t.Fatal("private diagnostic value leaked")
		}
	}
}

func TestUsageReporterGetsDiagnosticsFromContextAndFreezesErrorBeforeRetry(t *testing.T) {
	const id = "usage-context-diagnostics-fixture"
	records := make(chan usage.Record, 1)
	usage.RegisterPlugin(&usageReporterTestPlugin{authID: id, records: records, done: t.Context().Done()})
	diagnostics := &executor.RequestExecutionDiagnostics{}
	ctx := executor.WithRequestExecutionDiagnostics(t.Context(), diagnostics)
	outcome := &executor.RequestUsageOutcome{}
	ctx = executor.WithRequestUsageOutcome(ctx, outcome)
	slot := &executor.AuthRequestSlot{}
	slot.SetDiagnostics(diagnostics)
	slot.Bind(&usageReporterTestReservation{consumed: true})
	if !slot.Commit() {
		t.Fatal("commit failed")
	}
	reporter := NewUsageReporter(ctx, "codex", "model", &cliproxyauth.Auth{ID: id})
	cause := error(diagnosticStatusError{500, `{"error":{"code":"resource_exhausted","message":"capacity exhausted"}}`})
	reporter.TrackFailure(ctx, &cause)
	diagnostics.ClearFailure()
	diagnostics.SetFailure("selection", "auth_unavailable")
	outcome.FinalizeFailure()
	select {
	case record := <-records:
		if !record.CredentialSelected || !record.UpstreamCommitted || !record.AuthRequestSlotConsumed || record.ErrorCode != "resource_exhausted" || record.StatusCode != 500 || record.ErrorMessage != "capacity exhausted" {
			t.Fatal("frozen attempt diagnostics were not retained")
		}
	case <-time.After(time.Second):
		t.Fatal("usage record missing")
	}
}
