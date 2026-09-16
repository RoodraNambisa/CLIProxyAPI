package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

const detailedChatFixture = `{"id":"chat-answer","model":"returned-model","choices":[{"message":{"role":"assistant","content":"The answer is 42."},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19,"prompt_tokens_details":{"cached_tokens":5},"completion_tokens_details":{"reasoning_tokens":2}}}`

type sharedProbeExecutor struct {
	coreauth.ProviderExecutor
	provider string
	calls    int
	payload  []byte
	fail     bool
}

func (e *sharedProbeExecutor) Identifier() string { return e.provider }
func (e *sharedProbeExecutor) Execute(ctx context.Context, a *coreauth.Auth, req core.Request, _ core.Options) (core.Response, error) {
	e.calls++
	e.payload = append([]byte(nil), req.Payload...)
	if !core.SingleAttempt(ctx) || a.ID != "chosen.json" {
		return core.Response{}, fmt.Errorf("probe lost credential or single-attempt policy")
	}
	helps.RecordAPIRequest(ctx, nil, helps.UpstreamRequestLog{URL: "https://provider.example/v1/chat/completions?key=secret", Body: req.Payload})
	helps.RecordAPIResponseMetadata(ctx, nil, 200, http.Header{"X-Request-Id": {"trace-id"}, "Set-Cookie": {"not-for-details"}})
	if e.fail {
		return core.Response{}, &coreauth.Error{HTTPStatus: 503, Message: "fixture unavailable"}
	}
	return core.Response{Payload: []byte(detailedChatFixture)}, nil
}
func (e *sharedProbeExecutor) ExecuteStream(ctx context.Context, a *coreauth.Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	_, err := e.Execute(ctx, a, req, opts)
	if err != nil {
		return nil, err
	}
	chunks := make(chan core.StreamChunk, 3)
	chunks <- core.StreamChunk{Payload: []byte(`{"id":"chat-answer","model":"returned-model","choices":[{"delta":{"content":"The answer is 42."},"finish_reason":null}]}`)}
	chunks <- core.StreamChunk{Payload: []byte(`{"id":"chat-answer","model":"returned-model","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":7,"total_tokens":19,"prompt_tokens_details":{"cached_tokens":5},"completion_tokens_details":{"reasoning_tokens":2}}}`)}
	close(chunks)
	return &core.StreamResult{Chunks: chunks}, nil
}

func TestAuthFileModelProbeReusesAllRegisteredProviders(t *testing.T) {
	for _, provider := range []string{"claude", "gemini", "gemini-interactions", "vertex", "antigravity", "aistudio", "kimi", "chatgpt-web", "custom-compat"} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", provider, stream), func(t *testing.T) {
				manager := coreauth.NewManager(nil, nil, nil)
				executor := &sharedProbeExecutor{provider: provider}
				manager.RegisterExecutor(executor)
				_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: provider})
				if err != nil {
					t.Fatal(err)
				}
				h := &Handler{cfg: &config.Config{}, authManager: manager}
				code, result := runModelProbe(t, h, t.Context(), modelProbeRequest{Name: "chosen.json", Model: "test-model", Protocol: "chat", Stream: stream, Prompt: "What is the answer?", MaxOutputTokens: 512, RequestBody: json.RawMessage(`{"temperature":0,"model":"wrong-model","stream":false}`)})
				if code != 200 || !result.Success || executor.calls != 1 {
					t.Fatalf("failed probe: code=%d %+v calls=%d", code, result, executor.calls)
				}
				if gjson.GetBytes(executor.payload, "messages.0.content").String() != "What is the answer?" || gjson.GetBytes(executor.payload, "model").String() != "test-model" || gjson.GetBytes(executor.payload, "stream").Bool() != stream || gjson.GetBytes(executor.payload, "max_tokens").Int() != 512 {
					t.Fatal("temporary request or selected controls lost")
				}
				if result.Response != "The answer is 42." || result.ReturnedModel != "returned-model" || result.ResponseID != "chat-answer" || result.RequestID != "trace-id" || result.UpstreamURL != "https://provider.example/v1/chat/completions" || !strings.Contains(result.UpstreamRequestBody, "What is the answer?") {
					t.Fatalf("missing details: %+v", result)
				}
				if result.Usage == nil || result.Usage.InputTokens == nil || *result.Usage.InputTokens != 12 || *result.Usage.OutputTokens != 7 || *result.Usage.CachedTokens != 5 || *result.Usage.ReasoningTokens != 2 {
					t.Fatalf("missing usage: %+v", result.Usage)
				}
				if result.ResponseBody == "" || strings.Contains(result.ResponseBody, "not-for-details") {
					t.Fatal("missing response or exposed private headers")
				}
			})
		}
	}
}

func TestAuthFileModelProbeNativeClaudeGeminiAndCompatibility(t *testing.T) {
	for _, provider := range []string{"claude", "gemini", "custom-compat"} {
		t.Run(provider, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				if strings.Contains(r.Header.Get("Authorization"), "management-secret") || !strings.Contains(string(body), "temporary question") {
					t.Fatal("missing prompt or forwarded management credential")
				}
				w.Header().Set("Content-Type", "application/json")
				switch provider {
				case "claude":
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"claude-answer\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4\",\"content\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"native answer\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
				case "gemini":
					_, _ = io.WriteString(w, `{"candidates":[{"content":{"role":"model","parts":[{"text":"native answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":7,"totalTokenCount":19}}`)
				default:
					_, _ = io.WriteString(w, detailedChatFixture)
				}
			}))
			defer server.Close()
			cfg := &config.Config{}
			cfg.ProxyURL = "direct"
			manager := coreauth.NewManager(nil, nil, nil)
			var executor coreauth.ProviderExecutor
			model := "test-model"
			switch provider {
			case "claude":
				executor = runtimeexecutor.NewClaudeExecutor(cfg)
				model = "claude-sonnet-4"
			case "gemini":
				executor = runtimeexecutor.NewGeminiExecutor(cfg)
				model = "gemini-2.5-flash"
			default:
				executor = runtimeexecutor.NewOpenAICompatExecutor(provider, cfg)
			}
			manager.RegisterExecutor(executor)
			_, err := manager.Register(t.Context(), &coreauth.Auth{ID: "chosen.json", FileName: "chosen.json", Provider: provider, Attributes: map[string]string{"api_key": "chosen-fixture-token", "base_url": server.URL}})
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{cfg: cfg, authManager: manager}
			_, result := runModelProbe(t, h, t.Context(), modelProbeRequest{Name: "chosen.json", Model: model, Protocol: "chat", Prompt: "temporary question"})
			if !result.Success || result.Response == "" || result.Usage == nil || result.Usage.InputTokens == nil || *result.Usage.InputTokens != 12 || !strings.HasPrefix(result.UpstreamURL, server.URL) {
				t.Fatalf("native probe: success=%t error=%s usage=%+v response=%s", result.Success, result.Error, result.Usage, result.ResponseBody)
			}
		})
	}
}

func TestSharedModelProbeDoesNotRetryOrSwitchAfterFailure(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	executor := &sharedProbeExecutor{provider: "claude", fail: true}
	manager.RegisterExecutor(executor)
	for _, name := range []string{"chosen.json", "another.json"} {
		if _, err := manager.Register(t.Context(), &coreauth.Auth{ID: name, FileName: name, Provider: "claude"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, stream := range []bool{false, true} {
		executor.calls = 0
		_, result := runModelProbe(t, &Handler{cfg: &config.Config{}, authManager: manager}, t.Context(), modelProbeRequest{Name: "chosen.json", Model: "claude-sonnet-4", Protocol: "chat", Stream: stream})
		if result.Success || executor.calls != 1 || !strings.Contains(result.Error, "fixture unavailable") {
			t.Fatalf("retried or suppressed failure: calls=%d result=%+v", executor.calls, result)
		}
	}
}

func TestModelProbeRedactsSecretsAndPreservesDirectRoute(t *testing.T) {
	secret := "fixture-\"token\\secret"
	encoded, _ := json.Marshal(map[string]string{"token": secret})
	auth := &coreauth.Auth{ProxyURL: "direct", Metadata: map[string]any{"access_token": secret}, Attributes: map[string]string{"api_key": "fixture-api-token"}}
	cfg := &config.Config{}
	cfg.ProxyURL, cfg.APIKeys = "direct", []string{"fixture-client-key"}
	redacted := probeSafeText("direct "+string(encoded)+" "+secret+" fixture-api-token fixture-client-key", auth, cfg, 4096)
	if !strings.HasPrefix(redacted, "direct ") || strings.Contains(redacted, "fixture-") || !strings.Contains(redacted, "[redacted]") {
		t.Fatalf("redaction corrupted route or leaked secret: %s", redacted)
	}
	if got := probeSafeURL("https://username:password@example.test/v1/responses?api_key=secret#private"); got != "https://example.test/v1/responses" {
		t.Fatalf("URL exposed secret: %s", got)
	}
}

func TestModelProbeDetailsPartialUsageAndBounds(t *testing.T) {
	observer := modelProbeObserver{}
	if err := observer.observe([]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"Partial answer\"}\n\n"), true); err != nil {
		t.Fatal(err)
	}
	err := observer.observe([]byte(`{"type":"response.incomplete","response":{"id":"partial-id","model":"actual","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[],"usage":{"input_tokens":4,"output_tokens":3,"input_tokens_details":{"cached_tokens":0}}}}`), true)
	if err == nil {
		t.Fatal("incomplete response accepted")
	}
	result := modelProbeResult{}
	observer.fill(&result)
	if result.Response != "Partial answer" || result.Usage == nil || *result.Usage.TotalTokens != 7 || result.Usage.CachedTokens == nil || *result.Usage.CachedTokens != 0 || result.Usage.ReasoningTokens != nil {
		t.Fatalf("partial usage lost or invented: %+v", result)
	}
	complete := modelProbeObserver{}
	for _, payload := range []string{`{"type":"response.output_text.delta","delta":"answer"}`, `{"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"answer"}]}]}}`} {
		if err := complete.observe([]byte(payload), true); err != nil {
			t.Fatal(err)
		}
	}
	complete.fill(&result)
	if result.Response != "answer" || result.Usage != nil {
		t.Fatal("duplicated completion or invented usage")
	}
	large, _ := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": strings.Repeat("x", modelProbeDetailLimit+100)}, "finish_reason": "stop"}}})
	bounded := modelProbeObserver{}
	if err := bounded.observe(large, false); err != nil {
		t.Fatal(err)
	}
	bounded.fill(&result)
	if !result.DetailsTruncated || len(result.ResponseBody) > modelProbeDetailLimit || len(result.Response) > modelProbeDetailLimit {
		t.Fatal("unbounded diagnostic output")
	}
}

func TestModelProbePayloadValidationAndExplicitParameters(t *testing.T) {
	for _, raw := range []string{`null`, `[]`, `{"tools":[{"type":"image_generation"}]}`} {
		if _, err := buildModelProbePayload(modelProbeRequest{RequestBody: json.RawMessage(raw)}, sdktranslator.FormatOpenAIResponse); err == nil {
			t.Fatal("invalid request accepted")
		}
	}
	payload, err := buildModelProbePayload(modelProbeRequest{Model: "chosen", Stream: true, RequestBody: json.RawMessage(`{"messages":[{"role":"user","content":"JSON question"}],"seed":9007199254740993,"max_completion_tokens":123,"stream_options":{"include_usage":false}}`)}, sdktranslator.FormatOpenAI)
	if err != nil || gjson.GetBytes(payload, "max_tokens").Exists() || gjson.GetBytes(payload, "stream_options.include_usage").Bool() || gjson.GetBytes(payload, "messages.0.content").String() != "JSON question" {
		t.Fatalf("explicit parameters changed: %s %v", payload, err)
	}
	if gjson.GetBytes(payload, "seed").Raw != "9007199254740993" {
		t.Fatal("custom JSON number lost precision")
	}
}
