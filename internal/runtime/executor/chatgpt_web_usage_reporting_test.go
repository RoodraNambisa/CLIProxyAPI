package executor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
)

type chatGPTWebUsageReportingPlugin struct {
	authID  string
	records chan coreusage.Record
}

type chatGPTWebUsageTestReservation struct {
	reserved  bool
	committed bool
}

func (reservation *chatGPTWebUsageTestReservation) Commit() bool {
	if reservation == nil || !reservation.reserved || reservation.committed {
		return false
	}
	reservation.reserved = false
	reservation.committed = true
	return true
}

func (reservation *chatGPTWebUsageTestReservation) Release() bool {
	if reservation == nil || !reservation.reserved {
		return false
	}
	reservation.reserved = false
	return true
}

func (reservation *chatGPTWebUsageTestReservation) Reserved() bool {
	return reservation != nil && reservation.reserved
}

func (reservation *chatGPTWebUsageTestReservation) Committed() bool {
	return reservation != nil && reservation.committed
}

func (reservation *chatGPTWebUsageTestReservation) Consumed() bool {
	return reservation != nil
}

func (plugin *chatGPTWebUsageReportingPlugin) HandleUsage(_ context.Context, record coreusage.Record) {
	if record.AuthID == plugin.authID {
		plugin.records <- record
	}
}

func TestPublishChatGPTWebTerminalUsageIncludesImageToolModel(t *testing.T) {
	const authID = "chatgpt-web-image-usage-reporting"
	records := make(chan coreusage.Record, 2)
	coreusage.RegisterPlugin(&chatGPTWebUsageReportingPlugin{authID: authID, records: records})
	reporter := helps.NewUsageReporter(context.Background(), "chatgpt-web", "gpt-5.4", &cliproxyauth.Auth{ID: authID})
	prepared := &chatGPTWebPreparedRequest{request: helps.ChatGPTWebRequest{Image: &helps.ChatGPTWebImageRequest{Model: "gpt-image-2"}}}
	completed := []byte(`{"response":{"usage":{"input_tokens":11,"output_tokens":2,"total_tokens":13},"tool_usage":{"image_gen":{"input_tokens":3,"output_tokens":7024,"total_tokens":7027}}}}`)

	publishChatGPTWebTerminalUsage(context.Background(), reporter, prepared, completed)

	got := make(map[string]coreusage.Record, 2)
	deadline := time.After(time.Second)
	for len(got) < 2 {
		select {
		case record := <-records:
			got[record.Model] = record
		case <-deadline:
			t.Fatalf("timed out waiting for usage records: %#v", got)
		}
	}
	if outer := got["gpt-5.4"]; outer.Detail.TotalTokens != 13 {
		t.Fatalf("outer usage = %#v", outer)
	} else if outer.Auxiliary {
		t.Fatalf("outer usage marked auxiliary: %#v", outer)
	}
	if image := got["gpt-image-2"]; image.Detail.OutputTokens != 7024 || image.Detail.TotalTokens != 7027 {
		t.Fatalf("image tool usage = %#v", image)
	} else if !image.Auxiliary {
		t.Fatalf("image tool usage not marked auxiliary: %#v", image)
	}
}

func TestChatGPTWebImageFailureStagePersistsInUsage(t *testing.T) {
	tests := []struct {
		name     string
		stage    string
		err      error
		wantCode string
	}{
		{name: "generic settle", stage: "settle", err: errors.New("image completion failed"), wantCode: "chatgpt_web_request_failed"},
		{name: "classified settle", stage: "settle", err: newChatGPTWebImageSettleStatusError(chatGPTWebImageErrorPollUnsettled, "polling did not converge"), wantCode: chatGPTWebImageErrorPollUnsettled},
		{name: "moderation settle", stage: "settle", err: newChatGPTWebImageModerationResultError(), wantCode: "moderation_blocked"},
		{name: "generic download", stage: "download", err: errors.New("image download failed"), wantCode: "chatgpt_web_request_failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authID := "chatgpt-web-image-failure-stage-" + strings.ReplaceAll(test.name, " ", "-")
			records := make(chan coreusage.Record, 1)
			coreusage.RegisterPlugin(&chatGPTWebUsageReportingPlugin{authID: authID, records: records})
			diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
			err := withChatGPTWebFailureStage(test.stage, test.err)
			recordChatGPTWebExecutionFailure(diagnostics, err)

			reporter := helps.NewUsageReporter(context.Background(), "chatgpt-web", "gpt-image-2", &cliproxyauth.Auth{ID: authID})
			reporter.SetExecutionDiagnostics(diagnostics)
			reporter.PublishFailure(context.Background(), err)

			select {
			case record := <-records:
				if record.FailureStage != test.stage {
					t.Fatalf("failure stage = %q, want %q", record.FailureStage, test.stage)
				}
				if record.ErrorCode != test.wantCode {
					t.Fatalf("error code = %q, want %q", record.ErrorCode, test.wantCode)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for image failure usage record")
			}
		})
	}
}

func TestChatGPTWebFailureStageContractNormalizesAuthLifecycleStages(t *testing.T) {
	newSelectedDiagnostics := func(committed bool) *cliproxyexecutor.RequestExecutionDiagnostics {
		diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
		slot := &cliproxyexecutor.AuthRequestSlot{}
		slot.SetDiagnostics(diagnostics)
		slot.Bind(&chatGPTWebUsageTestReservation{reserved: true})
		if committed {
			slot.Commit()
		}
		return diagnostics
	}
	authError := func() error {
		return &chatgptwebauth.AuthError{
			Code:           "authentication_failed",
			DiagnosticCode: "invalid_passkey_response",
			FailureStage:   "passkey_verify",
		}
	}

	tests := []struct {
		name        string
		diagnostics *cliproxyexecutor.RequestExecutionDiagnostics
		err         error
		wantStage   string
	}{
		{
			name:        "outer settle stage wins",
			diagnostics: newSelectedDiagnostics(true),
			err:         withChatGPTWebFailureStage("settle", authError()),
			wantStage:   "settle",
		},
		{
			name:        "pre commit auth error is selection",
			diagnostics: newSelectedDiagnostics(false),
			err:         authError(),
			wantStage:   "selection",
		},
		{
			name:        "committed auth error is upstream",
			diagnostics: newSelectedDiagnostics(true),
			err:         authError(),
			wantStage:   "upstream",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recordChatGPTWebExecutionFailure(test.diagnostics, test.err)
			snapshot := test.diagnostics.Snapshot()
			if snapshot.FailureStage != test.wantStage {
				t.Fatalf("failure stage = %q, want %q", snapshot.FailureStage, test.wantStage)
			}
			if snapshot.ErrorCode != "invalid_passkey_response" {
				t.Fatalf("error code = %q, want invalid_passkey_response", snapshot.ErrorCode)
			}
			for _, internalStage := range []string{"passkey_verify", "authorize_continue"} {
				if snapshot.FailureStage == internalStage {
					t.Fatalf("usage failure stage leaked internal lifecycle stage %q", internalStage)
				}
			}
		})
	}
}

func TestChatGPTWebStreamSetupFailurePublishesSingleUsageRecord(t *testing.T) {
	tests := []struct {
		name          string
		prepareAuth   func(*cliproxyauth.Auth)
		serverHandler http.Handler
		wantStage     string
		wantCommitted bool
		wantConsumed  bool
	}{
		{
			name: "pre request credential failure",
			prepareAuth: func(auth *cliproxyauth.Auth) {
				auth.Metadata["access_token"] = ""
			},
			wantStage: "selection",
		},
		{
			name: "upstream failure before stream result",
			serverHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "upstream unavailable", http.StatusServiceUnavailable)
			}),
			wantStage:     "upstream",
			wantCommitted: true,
			wantConsumed:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			auth := chatGPTWebRuntimeAuth()
			auth.ID = "stream-setup-usage-" + test.name
			if test.prepareAuth != nil {
				test.prepareAuth(auth)
			}
			records := make(chan coreusage.Record, 2)
			coreusage.RegisterPlugin(&chatGPTWebUsageReportingPlugin{authID: auth.ID, records: records})

			executor := NewChatGPTWebExecutor(nil, nil)
			var server *httptest.Server
			if test.serverHandler != nil {
				server = httptest.NewServer(test.serverHandler)
				defer server.Close()
				executor.runtimeBaseURL = server.URL
			}

			diagnostics := &cliproxyexecutor.RequestExecutionDiagnostics{}
			reservation := &chatGPTWebUsageTestReservation{reserved: true}
			slot := &cliproxyexecutor.AuthRequestSlot{}
			slot.SetDiagnostics(diagnostics)
			slot.Bind(reservation)
			outcome := &cliproxyexecutor.RequestUsageOutcome{}
			_, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{
				Model:   "gpt-5",
				Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
			}, cliproxyexecutor.Options{
				SourceFormat:         "codex",
				ResponseFormat:       "codex",
				AuthRequestSlot:      slot,
				ExecutionDiagnostics: diagnostics,
				UsageOutcome:         outcome,
			})
			if err == nil {
				t.Fatal("ExecuteStream() error = nil")
			}
			outcome.FinalizeFailure()
			slot.Release()

			select {
			case record := <-records:
				if !record.Failed || record.FailureStage != test.wantStage {
					t.Fatalf("usage failure = failed:%v stage:%q, want true/%q", record.Failed, record.FailureStage, test.wantStage)
				}
				if !record.CredentialSelected || record.UpstreamCommitted != test.wantCommitted || record.AuthRequestSlotConsumed != test.wantConsumed {
					t.Fatalf("usage ownership = selected:%v committed:%v consumed:%v", record.CredentialSelected, record.UpstreamCommitted, record.AuthRequestSlotConsumed)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for stream setup failure usage")
			}
			select {
			case duplicate := <-records:
				t.Fatalf("duplicate stream setup failure usage = %#v", duplicate)
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}
