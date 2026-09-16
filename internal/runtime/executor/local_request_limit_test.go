package executor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestCodexManagerLocalRejectionDoesNotConsumeRequestLimit(t *testing.T) {
	for _, transport := range []string{"http", "http-stream", "compact", "websocket", "websocket-stream", "auto"} {
		t.Run(transport, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(transport, "websocket") {
					conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					if _, _, err := conn.ReadMessage(); err != nil {
						t.Error(err)
						return
					}
					calls.Add(1)
					_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":400,"error":{"code":"invalid_value","message":"fixture upstream validation"}}`))
					_, _, _ = conn.ReadMessage()
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(400)
				_, _ = w.Write([]byte(`{"error":{"code":"invalid_value","message":"fixture upstream validation"}}`))
			}))
			defer server.Close()
			cfg := &config.Config{
				SDKConfig:                         sdkconfig.SDKConfig{ProxyURL: "direct"},
				Routing:                           config.RoutingConfig{PerAuthRequestLimit: 1, PerAuthRequestWindowMinutes: 60},
				DisabledImageGenerationToolAction: config.DisabledImageGenerationToolActionError,
				DisabledImageGenerationToolError:  config.DisabledImageGenerationToolErrorConfig{StatusCode: 429, Code: "rate_limit_exceeded"},
				AuthModelExclusions:               []config.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{3}}},
			}
			manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
			manager.SetConfig(cfg)
			manager.SetRetryConfig(0, 0, 1)
			var exec coreauth.ProviderExecutor = NewCodexExecutor(cfg)
			if strings.HasPrefix(transport, "websocket") {
				exec = NewCodexWebsocketsExecutor(cfg)
			} else if transport == "auto" {
				exec = NewCodexAutoExecutor(cfg)
			}
			manager.RegisterExecutor(exec)
			credential := &coreauth.Auth{ID: "request-limit-" + t.Name(), Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL, "priority": "3"}}
			if _, err := manager.Register(coreauth.WithSkipPersist(t.Context()), credential); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: "gpt-5.4"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
			manager.RefreshSchedulerEntry(credential.ID)
			opts := core.Options{SourceFormat: translator.FormatCodex}
			if transport == "compact" {
				opts.Alt = "responses/compact"
			}
			invoke := func(req core.Request, diagnostics *core.RequestExecutionDiagnostics) error {
				options := opts
				options.ExecutionDiagnostics = diagnostics
				if strings.HasSuffix(transport, "stream") {
					result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, options)
					if result != nil {
						for chunk := range result.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
					return err
				}
				_, err := manager.Execute(t.Context(), []string{"codex"}, req, options)
				return err
			}
			request := core.Request{Model: "gpt-5.4", Payload: []byte(`{"input":[],"tools":[{"type":"image_generation"}]}`)}
			for range 3 {
				diagnostics := &core.RequestExecutionDiagnostics{}
				if err := invoke(request, diagnostics); logging.LocalPolicyReason(err) != "disabled_image_generation_tool" {
					t.Fatalf("local rejection exhausted request capacity: %v", err)
				}
				if snapshot := diagnostics.Snapshot(); snapshot.UpstreamCommitted || snapshot.AuthRequestSlotConsumed {
					t.Fatal("local rejection consumed capacity")
				}
				if _, err := manager.ExecuteCount(t.Context(), []string{"codex"}, request, core.Options{SourceFormat: translator.FormatCodex}); err != nil {
					t.Fatalf("local token count exhausted request capacity: %v", err)
				}
			}
			if calls.Load() != 0 {
				t.Fatal("local operations sent upstream traffic")
			}
			request.Payload = []byte(`{"input":[]}`)
			diagnostics := &core.RequestExecutionDiagnostics{}
			if err := invoke(request, diagnostics); err == nil || !strings.Contains(err.Error(), "fixture upstream validation") || calls.Load() != 1 || !diagnostics.Snapshot().AuthRequestSlotConsumed {
				t.Fatalf("allowed upstream request lost accounting: %v; calls=%d", err, calls.Load())
			}
			if err := invoke(request, &core.RequestExecutionDiagnostics{}); err == nil || !strings.Contains(err.Error(), "auth_request_limited") || calls.Load() != 1 {
				t.Fatalf("request window limit bypassed: %v; calls=%d", err, calls.Load())
			}
		})
	}
}

func TestBuiltInExecutorsDeferRequestLimitForLocalValidation(t *testing.T) {
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
	executors := []coreauth.ProviderExecutor{
		NewCodexExecutor(cfg), NewCodexWebsocketsExecutor(cfg), NewCodexAutoExecutor(cfg),
		NewClaudeExecutor(cfg), NewGeminiExecutor(cfg), NewGeminiVertexExecutor(cfg),
		NewAntigravityExecutor(cfg), NewKimiExecutor(cfg), NewOpenAICompatExecutor("compat", cfg),
		NewXAIExecutor(cfg), NewXAIAutoExecutor(cfg), NewXAIWebsocketsExecutor(cfg), NewAIStudioExecutor(cfg, "aistudio", nil),
	}
	for _, exec := range executors {
		if deferred, ok := exec.(coreauth.DeferredAuthRequestCommitter); !ok || !deferred.DeferAuthRequestCommitUntilUpstream() {
			t.Errorf("%T still commits before local validation", exec)
		}
	}
	for _, exec := range []coreauth.ProviderExecutor{NewClaudeExecutor(cfg), NewGeminiExecutor(cfg), NewAntigravityExecutor(cfg), NewOpenAICompatExecutor("compat", cfg), NewAIStudioExecutor(cfg, "aistudio", nil)} {
		for _, stream := range []bool{false, true} {
			slot := &core.AuthRequestSlot{}
			slot.Bind(&chatGPTWebUsageTestReservation{reserved: true})
			ctx := core.WithUpstreamAttemptSlot(t.Context(), slot)
			request := core.Request{Model: "fixture", Payload: []byte(`{}`)}
			opts := core.Options{Alt: "responses/compact", SourceFormat: translator.FormatCodex}
			credential := &coreauth.Auth{Attributes: map[string]string{"base_url": "http://[invalid"}}
			var err error
			if stream {
				_, err = exec.ExecuteStream(ctx, credential, request, opts)
			} else {
				_, err = exec.Execute(ctx, credential, request, opts)
			}
			if err == nil || slot.Committed() || !slot.Release() {
				t.Errorf("%T local validation consumed request capacity (stream=%t): %v", exec, stream, err)
			}
		}
	}
}
