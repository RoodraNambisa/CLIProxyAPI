package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestOpenAICompatCustomToolSurvivesBodyReleaseWithoutOriginalOption(t *testing.T) {
	controller := cliproxyexecutor.NewRequestBodyReleaseController(1, []byte("<released>"))
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Error(err)
			return
		}
		controller.Release()
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: " + `{"id":"custom_reply","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_exec","function":{"name":"exec","arguments":"{\"input\":\"run\"}"}}]},"finish_reason":"tool_calls"}]}` + "\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	executor := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test-key"}}
	request := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec","description":"discarded schema text"}]},{"role":"user","content":"discarded prompt"}]}`)}
	result, err := executor.ExecuteStream(t.Context(), auth, request, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Metadata: map[string]any{cliproxyexecutor.BodyReleaseControllerMetadataKey: controller}})
	if err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		output.Write(chunk.Payload)
	}
	if !strings.Contains(output.String(), `"type":"custom_tool_call"`) || !strings.Contains(output.String(), `"input":"run"`) {
		t.Fatal("released request lost custom tool classification")
	}
	slim := string(helps.SlimRequestBodyForTranslation(request.Payload))
	if strings.Contains(slim, "discarded") || !strings.Contains(slim, "additional_tools") {
		t.Fatal("released metadata retained content or lost declarations")
	}
}

func TestOpenAICompatCustomToolNonStreamWithoutOriginalOption(t *testing.T) {
	for _, release := range []bool{false, true} {
		controller := cliproxyexecutor.NewRequestBodyReleaseController(1, []byte("<released>"))
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := io.Copy(io.Discard, r.Body); err != nil {
				t.Error(err)
				return
			}
			if release {
				controller.Release()
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"custom_reply","choices":[{"message":{"tool_calls":[{"id":"call_exec","function":{"name":"exec","arguments":"{\"input\":\"run\"}"}}]},"finish_reason":"tool_calls"}]}`))
		}))
		exec := NewOpenAICompatExecutor("openai-compatibility", &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}})
		auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": server.URL, "api_key": "test-key"}}
		req := cliproxyexecutor.Request{Model: "gpt-5.4", Payload: []byte(`{"model":"gpt-5.4","input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]},{"role":"user","content":"hello"}]}`)}
		opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}
		if release {
			opts.Metadata = map[string]any{cliproxyexecutor.BodyReleaseControllerMetadataKey: controller}
		}
		response, err := exec.Execute(t.Context(), auth, req, opts)
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(response.Payload), `"type":"custom_tool_call"`) || !strings.Contains(string(response.Payload), `"input":"run"`) || controller.Released() != release {
			t.Fatal("non-stream request lost its original declaration or release behavior")
		}
	}
}
