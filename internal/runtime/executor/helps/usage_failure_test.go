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
	logtest "github.com/sirupsen/logrus/hooks/test"
)

type diagnosticStatusError struct {
	status int
	body   string
}

type diagnosticLocalPolicyError struct{ diagnosticStatusError }

func (diagnosticLocalPolicyError) LocalPolicyReason() string { return "disabled_image_generation_tool" }

func TestUsageFailureDistinguishesLocalPolicyFromIdenticalUpstreamError(t *testing.T) {
	const body = `{"error":{"type":"rate_limit_exceeded","code":"rate_limit_exceeded","message":"Rate limit exceeded for image_generation. Please try again later."}}`
	for _, local := range []bool{false, true} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()
			cause := error(diagnosticStatusError{429, body})
			if local {
				cause = fmt.Errorf("execution: %w", diagnosticLocalPolicyError{diagnosticStatusError{429, body}})
			}
			record := usage.Record{Provider: "codex", Failed: true, UpstreamCommitted: true, AuthRequestSlotConsumed: true}
			populateUsageFailure(t.Context(), &record, cause)
			wantStage, wantMessage := "upstream", "provider request attempt failed"
			if local {
				wantStage, wantMessage = "local_policy", "provider request rejected by local policy"
			}
			if record.FailureStage != wantStage || record.StatusCode != 429 || record.ErrorCode != "rate_limit_exceeded" || record.ErrorType != "rate_limit_exceeded" || !record.UpstreamCommitted || !record.AuthRequestSlotConsumed {
				t.Fatalf("incorrect classification or changed accounting: %+v", record)
			}
			logUsageAttemptFailure(t.Context(), record, "fixture.json", logging.LocalPolicyReason(cause))
			entry := hook.LastEntry()
			if entry == nil || !strings.HasPrefix(entry.Message, wantMessage+": Rate limit exceeded for image_generation.") || entry.Data["stage"] != wantStage {
				t.Fatalf("incorrect failure log: %+v", entry)
			}
			if local {
				if entry.Data["error_origin"] != "local" || entry.Data["policy"] != "disabled_image_generation_tool" {
					t.Fatal("local policy metadata missing")
				}
			} else if entry.Data["error_origin"] != nil || entry.Data["policy"] != nil {
				t.Fatal("upstream error incorrectly labeled as local")
			}
		})
	}
}

func TestUsageFailureFreezesCredentialNameAcrossAttempts(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()
	ctx := logging.WithRequestID(t.Context(), "a5c2bae8")
	auth := &cliproxyauth.Auth{ID: "first", Provider: "codex", FileName: "/auths/first@example.test.json"}
	first := NewUsageReporter(ctx, "codex", "model", auth)
	auth.FileName = "/auths/renamed.json"
	second := NewUsageReporter(ctx, "codex", "model", &cliproxyauth.Auth{ID: "second", FileName: "second.json"})
	cause := diagnosticStatusError{429, `{"error":{"message":"quota exceeded"}}`}
	first.PublishFailure(ctx, cause)
	second.PublishFailure(ctx, cause)
	entries := hook.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("entries = %d", len(entries))
	}
	if entries[0].Data["auth_name"] != "first@example.test.json" || entries[1].Data["auth_name"] != "second.json" || entries[0].Data["auth_index"] == entries[1].Data["auth_index"] {
		t.Fatal("attempt credential snapshot drifted")
	}
	for _, entry := range entries {
		if entry.Data["request_id"] != "a5c2bae8" {
			t.Fatal("lost request ID")
		}
	}
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
