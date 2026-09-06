package executor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexResponsesLitePreparationSnapshotsEachTurn(t *testing.T) {
	executor := NewCodexExecutor(&config.Config{})
	headers := http.Header{helps.CodexResponsesLiteHeader: []string{"true"}}
	opts := cliproxyexecutor.Options{Headers: headers}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(t.Context())
	lite, err := executor.PrepareProviderRequest(ctx, cliproxyexecutor.Request{Payload: []byte(`{"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`)}, opts, cliproxyexecutor.RequestOperationStream)
	if err != nil {
		t.Fatal(err)
	}
	ordinary, err := executor.PrepareProviderRequest(ctx, cliproxyexecutor.Request{Payload: []byte(`{}`)}, opts, cliproxyexecutor.RequestOperationStream)
	if err != nil {
		t.Fatal(err)
	}
	if !lite.(codexPreparedSessionIdentity).ResponsesLite.Enabled || ordinary.(codexPreparedSessionIdentity).ResponsesLite.Enabled {
		t.Fatal("Lite mode was not isolated by turn")
	}
	headerSnapshot, err := executor.PrepareProviderRequest(t.Context(), cliproxyexecutor.Request{Payload: []byte(`{}`)}, opts, cliproxyexecutor.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	headers.Set(helps.CodexResponsesLiteHeader, "false")
	if !headerSnapshot.(codexPreparedSessionIdentity).ResponsesLite.Enabled {
		t.Fatal("header mutation changed an in-flight request")
	}
}

func TestCodexResponsesLitePreservesImageToolPolicy(t *testing.T) {
	for _, action := range []string{"remove", "error"} {
		cfg := &config.Config{DisabledImageGenerationToolAction: action, AuthModelExclusions: []config.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{-1}}}}
		executor := NewCodexExecutor(cfg)
		auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"priority": "-1"}}
		body := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"},{"type":"function","name":"inspect"}]},{"type":"custom","name":"exec"}]}],"tool_choice":"required"}`)
		if !cliproxyauth.PayloadHasImageGenerationTool(body) {
			t.Fatal("image declarations escaped scheduling checks")
		}
		got, err := executor.applyDisabledImageGenerationToolPolicy(auth, body)
		if action == "error" {
			if err == nil {
				t.Fatal("Lite image tool bypassed rejection")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if cliproxyauth.PayloadHasImageGenerationTool(got) {
			t.Fatal("Lite image tool bypassed removal")
		}
		if gjson.GetBytes(got, "input.0.tools.0.tools.0.name").String() != "inspect" || gjson.GetBytes(got, "input.0.tools.1.name").String() != "exec" || gjson.GetBytes(got, "tool_choice").String() != "required" {
			t.Fatal("removal changed unrelated declarations or tool choice")
		}
	}
}

func TestCodexResponsesLiteImageUsageUsesDeclaredModel(t *testing.T) {
	body := []byte(`{"input":[{"type":"additional_tools","tools":[{"type":"image_generation","model":"gpt-image-1.5"}]}]}`)
	if codexImageGenerationToolModel(body) != "gpt-image-1.5" {
		t.Fatal("additional image declaration lost its billing model")
	}
	body = []byte(`{"tools":[{"type":"image_generation"}],"input":[{"type":"additional_tools","tools":[{"type":"image_generation","model":"gpt-image-1.5"}]}]}`)
	if codexImageGenerationToolModel(body) != codexDefaultImageToolModel {
		t.Fatal("additional declaration overrode the top-level default model")
	}
	for _, invalid := range []string{
		`{"tools":{"type":"image_generation","model":"gpt-image-1.5"}}`,
		`{"input":{"type":"additional_tools","tools":[{"type":"image_generation","model":"gpt-image-1.5"}]}}`,
		`{"input":[{"type":"additional_tools","tools":{"type":"image_generation","model":"gpt-image-1.5"}}]}`,
	} {
		if codexImageGenerationToolModel([]byte(invalid)) != codexDefaultImageToolModel {
			t.Fatal("non-array declaration changed the default billing model")
		}
	}
}

func TestCodexResponsesLiteWireTurnsAndHTTPHeaders(t *testing.T) {
	for _, useWebsocket := range []bool{false, true} {
		t.Run(fmt.Sprintf("websocket=%t", useWebsocket), func(t *testing.T) {
			payloads := make(chan []byte, 2)
			requestHeaders := make(chan http.Header, 2)
			terminal := []byte(`{"type":"response.completed","response":{"id":"resp_lite","output":[]}}`)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestHeaders <- r.Header.Clone()
				if useWebsocket {
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						t.Error(err)
						return
					}
					defer func() { _ = conn.Close() }()
					for range 2 {
						_, body, errRead := conn.ReadMessage()
						if errRead != nil {
							t.Error(errRead)
							return
						}
						payloads <- body
						if errWrite := conn.WriteMessage(websocket.TextMessage, terminal); errWrite != nil {
							t.Error(errWrite)
							return
						}
					}
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				payloads <- body
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write(append(append([]byte("data: "), terminal...), []byte("\n\n")...))
			}))
			defer server.Close()
			cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}}
			auth := &cliproxyauth.Auth{ID: "lite-auth", Provider: "codex", Attributes: map[string]string{"base_url": server.URL, "api_key": "test-key"}}
			var executor cliproxyauth.ProviderExecutor = NewCodexExecutor(cfg)
			ctx := t.Context()
			opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
			if useWebsocket {
				ws := NewCodexWebsocketsExecutor(cfg)
				session := uuid.NewString()
				defer ws.CloseExecutionSession(session)
				opts.Metadata = map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: session}
				executor = ws
				ctx = cliproxyexecutor.WithDownstreamWebsocket(ctx)
			}
			for index := range 2 {
				opts.Headers = http.Header{helps.CodexResponsesLiteHeader: []string{"false"}}
				metadata := ""
				if index == 0 {
					opts.Headers.Set(helps.CodexResponsesLiteHeader, "true")
					if useWebsocket {
						metadata = `,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}`
					}
				}
				body := []byte(`{"model":"gpt-6-astra","input":[{"type":"additional_tools","tools":[{"type":"custom","name":"exec"}]},{"role":"user","content":"hello"}],"parallel_tool_calls":true,"tool_choice":"required"` + metadata + `}`)
				result, err := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gpt-6-astra", Payload: body}, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
				got := <-payloads
				if gjson.GetBytes(got, "parallel_tool_calls").Bool() != (index == 1) {
					t.Fatal("wire mode was stale or inferred from the model name")
				}
				if gjson.GetBytes(got, "input.0.type").String() != "additional_tools" || gjson.GetBytes(got, "tool_choice").String() != "required" {
					t.Fatal("Lite declarations or choice were lost")
				}
				if !useWebsocket {
					headers := <-requestHeaders
					if (headers.Get(helps.CodexResponsesLiteHeader) == "true") != (index == 0) {
						t.Fatal("HTTP Lite header disagrees with body normalization")
					}
				}
			}
		})
	}
}
