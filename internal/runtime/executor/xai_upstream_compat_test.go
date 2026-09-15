package executor

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestXAIImageToolPolicyAndForcedChoice(t *testing.T) {
	for _, policy := range []string{"", "remove", "error", "allow"} {
		for _, model := range []string{"grok-4.6", "grok-4.5", "grok-4.20-0309-reasoning"} {
			for _, choice := range []string{`{"type":"image_generation"}`, `{"type":"allowed_tools","mode":"auto","tools":[{"type":"image_generation"}]}`} {
				body := []byte(`{"input":"draw a cat","tools":[{"type":"image_generation"}],"tool_choice":` + choice + `}`)
				exec := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{ImageGenerationToolPolicy: policy, InjectXSearch: true}})
				prepared, err := exec.prepareResponsesRequest(t.Context(), nil, core.Request{Model: model, Payload: body}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, true)
				if policy == "error" || (policy == "allow" && model != "grok-4.6") || ((policy == "" || policy == "remove") && strings.Contains(choice, "allowed_tools")) {
					if err == nil {
						t.Fatalf("policy %s model %s should reject", policy, model)
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if policy == "allow" {
					tools := gjson.GetBytes(prepared.body, "tools").Array()
					if len(tools) != 1 || tools[0].Get("type").String() != "image_generation" {
						t.Fatalf("forced image call was broadened: %s", prepared.body)
					}
					wanted := "required"
					if strings.Contains(choice, "auto") {
						wanted = "auto"
					}
					if gjson.GetBytes(prepared.body, "tool_choice").String() != wanted {
						t.Fatalf("image choice changed: %s", prepared.body)
					}
				} else if gjson.GetBytes(prepared.body, `tools.#(type=="image_generation")`).Exists() {
					t.Fatal("disabled image tool leaked")
				}
			}
		}
	}
}

func TestXAIChatPreservesTopKAndExpandsSchema(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hello"}],"top_k":7,"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","$defs":{"x":{"type":"string","enum":["a","b"]}},"properties":{"key":{"$ref":"#/$defs/x"}}}}}]}`)
	prepared, err := NewXAIExecutor(&config.Config{}).prepareResponsesRequest(t.Context(), nil, core.Request{Model: "grok-4.6", Payload: body}, core.Options{SourceFormat: sdktranslator.FormatOpenAI}, true)
	if err != nil {
		t.Fatal(err)
	}
	if gjson.GetBytes(prepared.body, "top_k").Int() != 7 || gjson.GetBytes(prepared.body, "tools.0.parameters.properties.key.type").String() != "string" || strings.Contains(string(prepared.body), `"$ref"`) {
		t.Fatalf("compatibility parameters: %s", prepared.body)
	}
}

func TestXAIDirectChatOrphanChoicePreservesParameterIntent(t *testing.T) {
	exec := NewXAIExecutor(&config.Config{XAI: config.XAIConfig{ChatCompletionsMode: "direct"}})
	for _, choice := range []string{"none", "auto", "required"} {
		body := []byte(`{"messages":[{"role":"user","content":"hi"}],"top_k":7,"parallel_tool_calls":false,"tool_choice":"` + choice + `"}`)
		prepared, err := exec.prepareChatRequest(t.Context(), nil, core.Request{Model: "grok-4.6", Payload: body}, core.Options{SourceFormat: sdktranslator.FormatOpenAI}, false)
		if choice == "required" {
			if err == nil {
				t.Fatal("required choice was silently removed")
			}
			continue
		}
		if err != nil || gjson.GetBytes(prepared.body, "tool_choice").Exists() || !gjson.GetBytes(prepared.body, "parallel_tool_calls").Exists() || gjson.GetBytes(prepared.body, "parallel_tool_calls").Bool() || gjson.GetBytes(prepared.body, "top_k").Int() != 7 {
			t.Fatalf("orphan cleanup changed explicit parameters: %v", err)
		}
	}
}

func TestXAIWebsocketCompactionWithoutLocalTranscript(t *testing.T) {
	for _, payload := range []string{
		`{"input":[{"role":"user","content":"hello"},{"type":"compaction_trigger"}]}`,
		`{"input":[{"type":"compaction_trigger"}],"previous_response_id":"resp-existing"}`,
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if r.URL.Path != "/responses/compact" || gjson.GetBytes(body, `input.#(type=="compaction_trigger")`).Exists() || gjson.GetBytes(body, "tools").Exists() || gjson.GetBytes(body, "tool_choice").Exists() {
				t.Errorf("invalid compact request: %s %s", r.URL.Path, body)
			}
			if strings.Contains(payload, "resp-existing") && gjson.GetBytes(body, "previous_response_id").String() != "resp-existing" {
				t.Error("previous response ID lost")
			}
			_, _ = io.WriteString(w, `{"id":"resp_compact","object":"response.compaction","output":[{"type":"compaction","encrypted_content":"opaque"}]}`)
		}))
		exec := NewXAIWebsocketsExecutor(&config.Config{})
		store := &xaiWebsocketIDStateStore{sessions: make(map[string]*xaiWebsocketIDState)}
		mapper := newXAIWebsocketRequestIDMapper(store, "empty-history", []byte(payload))
		result, err := exec.executeCompactionTriggerFromWebsocketContext(t.Context(), &coreauth.Auth{Provider: "xai", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL}}, core.Request{Model: "grok-4.6", Payload: []byte(payload)}, core.Options{SourceFormat: sdktranslator.FormatOpenAIResponse}, mapper)
		if err != nil {
			server.Close()
			t.Fatal(err)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
		server.Close()
	}
}
