package executor

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestCodexLocalPolicyLoggingAcrossTransports(t *testing.T) {
	const message = "Rate limit exceeded for image_generation. Please try again later."
	const body = `{"error":{"message":"` + message + `","type":"rate_limit_exceeded","code":"rate_limit_exceeded"}}`
	for _, transport := range []string{"http", "http-stream", "compact", "websocket", "websocket-stream"} {
		t.Run(transport, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()
			cfg := &config.Config{
				SDKConfig:                         sdkconfig.SDKConfig{ProxyURL: "direct"},
				DisabledImageGenerationToolAction: config.DisabledImageGenerationToolActionError,
				DisabledImageGenerationToolError: config.DisabledImageGenerationToolErrorConfig{
					StatusCode: 429, Message: message, Type: "rate_limit_exceeded", Code: "rate_limit_exceeded",
				},
				AuthModelExclusions: []config.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{3}}},
			}
			credential := &cliproxyauth.Auth{ID: "policy-" + transport, Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL, "priority": "3"}}
			diagnostics := &core.RequestExecutionDiagnostics{}
			slot := &core.AuthRequestSlot{}
			slot.SetDiagnostics(diagnostics)
			slot.Bind(&chatGPTWebUsageTestReservation{reserved: true})
			ctx := core.WithRequestExecutionDiagnostics(t.Context(), diagnostics)
			ctx = core.WithUpstreamAttemptSlot(ctx, slot)
			request := core.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[],"tools":[{"type":"image_generation"}]}`)}
			options := core.Options{SourceFormat: translator.FromString("codex")}
			var err error
			switch transport {
			case "http", "compact":
				if transport == "compact" {
					options.Alt = "responses/compact"
				}
				_, err = NewCodexExecutor(cfg).Execute(ctx, credential, request, options)
			case "http-stream":
				_, err = NewCodexExecutor(cfg).ExecuteStream(ctx, credential, request, options)
			case "websocket":
				_, err = NewCodexWebsocketsExecutor(cfg).Execute(ctx, credential, request, options)
			case "websocket-stream":
				_, err = NewCodexWebsocketsExecutor(cfg).ExecuteStream(ctx, credential, request, options)
			}
			var policyErr statusErr
			if !errors.As(err, &policyErr) || policyErr.StatusCode() != 429 || policyErr.Error() != body || string(policyErr.ResponseBody()) != body || !policyErr.SkipAuthResult() || policyErr.RetryOtherAuth() || policyErr.Headers() != nil {
				t.Fatalf("changed public error or auth policy: %v", err)
			}
			if calls.Load() != 0 || logging.LocalPolicyReason(err) != "disabled_image_generation_tool" {
				t.Fatal("local policy rejection reached upstream or lost its marker")
			}
			entry := hook.LastEntry()
			if entry == nil || entry.Data["stage"] != "local_policy" || entry.Data["error_origin"] != "local" || entry.Data["policy"] != "disabled_image_generation_tool" || !strings.HasPrefix(entry.Message, "provider request rejected by local policy: "+message) {
				t.Fatalf("misleading local rejection log: %+v", entry)
			}
			if snapshot := diagnostics.Snapshot(); snapshot.UpstreamCommitted || snapshot.AuthRequestSlotConsumed || !slot.Release() {
				t.Fatal("local policy consumed its request slot")
			}
		})
	}
}
