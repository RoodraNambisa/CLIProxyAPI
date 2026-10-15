package executor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIReviewSpeechHonorsRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "75")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"Too many requests"}}`))
	}))
	defer server.Close()
	auth := &coreauth.Auth{ID: "speech-retry-after", Provider: "xai", Attributes: map[string]string{"base_url": server.URL, "api_key": "fixture"}}
	_, err := NewXAIExecutor(&config.Config{}).Execute(t.Context(), auth, core.Request{Model: "grok-tts", Payload: []byte(`{"text":"hello","voice_id":"eve"}`)}, core.Options{SourceFormat: sdktranslator.FromString("openai-speech")})
	status, ok := err.(statusErr)
	if !ok || status.RetryAfter() == nil || *status.RetryAfter() != 75*time.Second {
		t.Fatalf("TTS lost upstream Retry-After: %v", err)
	}
}

func TestXAIReviewClientSearchDoesNotSuppressHostedSearch(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{InjectWebSearch: true}})
	payload := []byte(`{"input":"search","tools":[{"type":"function","name":"web_search","parameters":{"type":"object"}}]}`)
	prepared, err := exec.prepareResponsesRequest(t.Context(), &coreauth.Auth{}, core.Request{Model: "grok-4.7", Payload: payload}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, true)
	if err != nil {
		t.Fatal(err)
	}
	var hosted, client int
	for _, tool := range gjson.GetBytes(prepared.body, "tools").Array() {
		if tool.Get("type").String() == "web_search" {
			hosted++
		}
		if tool.Get("type").String() == "function" && strings.HasPrefix(tool.Get("name").String(), "client_web_search") {
			client++
		}
	}
	if hosted != 1 || client != 1 {
		t.Fatalf("hosted search and client function must coexist: %s", prepared.body)
	}
}

func TestXAIReviewPreviewRetainsRequestedAliasForPayloadRules(t *testing.T) {
	cfg := &config.Config{}
	cfg.Payload.Override = []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "public-grok"}}, Params: map[string]any{"safety_identifier": strings.Repeat("p", 200)}}}
	auth := &coreauth.Auth{ID: "preview-alias", Provider: "xai", Attributes: map[string]string{"base_url": "https://api.x.ai/v1", "api_key": "fixture"}}
	payload := []byte(`{"input":"hello"}`)
	req := core.Request{Model: "grok-4.7", Payload: payload}
	opts := core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, OriginalRequest: payload, Metadata: map[string]any{
		core.RequestedModelMetadataKey:   "public-grok",
		core.RequestPathMetadataKey:      "/v1/responses",
		core.ExecutionSessionMetadataKey: "preview-session",
	}}
	exec := NewXAIExecutor(cfg)
	runner, scoped, err := exec.requestExecutor(t.Context(), req, opts)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := runner.prepareResponsesRequest(t.Context(), auth, req, scoped, true)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(prepared.body, "safety_identifier").String() == "" {
		t.Fatal("fixture did not apply the alias rule")
	}
	ctx := helps.WithXAIDiagnostics(t.Context(), true, nil)
	preview, err := exec.PreviewHeaders(ctx, auth, helps.XAIHeaderPreviewInput{Model: "public-grok", Purpose: "inference", Protocol: "responses", RequestBody: payload}, req)
	if err != nil {
		t.Fatal(err)
	}
	if preview.Compression.OriginalBytes == nil || *preview.Compression.OriginalBytes != int64(len(prepared.body)) {
		t.Fatalf("preview omits alias payload rules: got %+v, actual JSON bytes %d", preview.Compression, len(prepared.body))
	}
}

func TestXAIReviewPositiveReasoningDeclarationIsNotDenial(t *testing.T) {
	for _, model := range []string{"grok-4.7", "grok-build-0.1", "grok-custom-reasoner"} {
		t.Run(model, func(t *testing.T) {
			catalog, err := helps.ParseXAIModels([]byte(`{"data":[{"id":"`+model+`","supports_reasoning_effort":true}]}`), "https://api.x.ai/v1/models")
			if err != nil {
				t.Fatal(err)
			}
			auth := &coreauth.Auth{ID: "positive-reasoning", Provider: "xai", Attributes: map[string]string{"base_url": "https://api.x.ai/v1"}, Metadata: map[string]any{helps.XAIModelCatalogKey: catalog}}
			prepared, err := NewXAIExecutor(&config.Config{}).prepareResponsesRequest(t.Context(), auth, core.Request{Model: model, Payload: []byte(`{"input":"hi","reasoning":{"effort":"high"}}`)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, true)
			if err != nil {
				t.Fatal(err)
			}
			if gjson.GetBytes(prepared.body, "reasoning.effort").String() != "high" {
				t.Fatalf("positive support was interpreted as a denial: %s", prepared.body)
			}
		})
	}
}
