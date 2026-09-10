package openai

import (
	"bytes"
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"

	"github.com/tidwall/gjson"
)

func TestResponsesWebsocketCompletionRepairCommitsOnlySuccessfulTurn(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if outcome == "canceled" {
				cancel()
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("GET", "/v1/responses", nil).WithContext(ctx)
			data := make(chan []byte, 2)
			data <- []byte(`data: {"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"exec","input":"pwd","status":"completed"}}`)
			if outcome == "failed" {
				data <- []byte(`data: {"type":"response.failed","response":{"error":{"code":"cyber_policy","message":"test refusal"}}}`)
			} else {
				data <- []byte(`data: {"type":"response.completed","response":{"id":"resp-tool","output":[{"type":"function_call","call_id":"call-1","name":"exec"}]}}`)
			}
			close(data)
			state := newWebsocketToolPairState()
			output := &recordedResponsesWebsocketOutput{}
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil))
			completed, failure, err := h.forwardResponsesWebsocket(c, output, func(...interface{}) {}, data, nil, nil, "test-tool-repair", state)
			if outcome == "completed" {
				if failure != nil || err != nil || gjson.GetBytes(completed, "0.type").String() != "custom_tool_call" {
					t.Fatal("successful forwarding retained placeholder")
				}
				if gjson.GetBytes(output.frames[len(output.frames)-1], "response.output.0.input").String() != "pwd" || gjson.GetBytes(state.calls["call-1"], "input").String() != "pwd" {
					t.Fatal("forwarded result and committed tool cache disagree")
				}
			} else if len(state.calls) != 0 {
				t.Fatal("failed or canceled turn committed a tool call")
			}
		})
	}
}

func TestResponsesWebsocketCompletionRejectsAmbiguousToolRepair(t *testing.T) {
	placeholder := `{"type":"function_call","call_id":"call-1","name":"exec"}`
	complete := `{"type":"custom_tool_call","call_id":"call-1","name":"exec","input":"pwd"}`
	for _, tc := range []struct {
		name, terminal string
		sources        []string
	}{
		{"different calls claim same ID", placeholder, []string{complete, strings.Replace(complete, "pwd", "ls", 1)}},
		{"duplicate terminal ID", placeholder + "," + placeholder, []string{complete}},
		{"different name", strings.Replace(placeholder, "exec", "other", 1), []string{complete}},
		{"different namespace", `{"type":"function_call","call_id":"call-1","name":"exec","namespace":"wrong"}`, []string{complete}},
		{"incomplete source", placeholder, []string{`{"type":"custom_tool_call","call_id":"call-1","name":"exec"}`}},
		{"numeric source call ID", placeholder, []string{`{"type":"custom_tool_call","call_id":1,"name":"exec","input":"pwd"}`}},
		{"unfinished source", placeholder, []string{`{"type":"custom_tool_call","call_id":"call-1","name":"exec","input":"pwd","status":"in_progress"}`}},
		{"complete final remains authoritative", `{"type":"function_call","call_id":"call-1","name":"exec","arguments":"{}"}`, []string{complete}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminal := []byte(`{"type":"response.completed","response":{"output":[` + tc.terminal + `]}}`)
			indexed := make(map[int64][]byte)
			for i, source := range tc.sources {
				indexed[int64(i)] = []byte(source)
			}
			for range 20 {
				if repaired := restoreResponsesWebsocketCompletionOutput(terminal, indexed, nil); !bytes.Equal(repaired, terminal) {
					t.Fatalf("unsafe repair: %s", repaired)
				}
			}
		})
	}
	for _, kind := range []string{"function_call", "custom_tool_call"} {
		field := "arguments"
		if kind == "custom_tool_call" {
			field = "input"
		}
		item := gjson.Parse(`{"type":"` + kind + `","call_id":"valid","name":"exec","` + field + `":""}`)
		if !isCompleteResponsesWebsocketToolCall(item) {
			t.Fatal("empty valid tool payload treated as missing")
		}
	}
}

func TestResponsesWebsocketCompletionRepairsUniqueToolPlaceholder(t *testing.T) {
	terminal := []byte(`{"type":"response.completed","response":{"id":"resp-tool","output":[{"type":"message","id":"msg-keep"},{"type":"function_call","call_id":"call-1","name":"exec","local_extension":true}]}}`)
	collected := []byte(`{"type":"custom_tool_call","id":"ctc-1","call_id":"call-1","name":"exec","input":"pwd","status":"completed"}`)
	before := bytes.Clone(terminal)
	repaired := restoreResponsesWebsocketCompletionOutput(terminal, map[int64][]byte{1: collected}, nil)
	if gjson.GetBytes(repaired, "response.output.1.type").String() != "custom_tool_call" || gjson.GetBytes(repaired, "response.output.1.input").String() != "pwd" {
		t.Fatalf("terminal retained invalid placeholder: %s", repaired)
	}
	if !gjson.GetBytes(repaired, "response.output.1.local_extension").Bool() || gjson.GetBytes(repaired, "response.output.0.id").String() != "msg-keep" {
		t.Fatal("unrelated terminal fields changed")
	}
	if !bytes.Equal(terminal, before) {
		t.Fatal("source terminal mutated")
	}
	lastRequest := []byte(`{"model":"tool-model","input":[{"role":"user","content":"test"}]}`)
	nextRequest := []byte(`{"type":"response.create","previous_response_id":"resp-tool","input":[{"type":"custom_tool_call_output","call_id":"call-1","output":"ok"}]}`)
	normalized, _, errMsg := normalizeResponsesWebsocketRequestWithContext(nextRequest, lastRequest, []byte(gjson.GetBytes(repaired, "response.output").Raw), "resp-tool", false)
	if errMsg != nil {
		t.Fatal(errMsg.Error)
	}
	input := gjson.GetBytes(normalized, "input").Array()
	if len(input) != 4 || input[2].Get("type").String() != "custom_tool_call" || input[2].Get("input").String() != "pwd" || input[3].Get("call_id").String() != "call-1" {
		t.Fatal("next request did not replay the repaired call with its result")
	}
}

func TestResponsesWebsocketCompletionDropsIncompleteCollectedCalls(t *testing.T) {
	terminal := []byte(`{"type":"response.done","response":{"output":[]}}`)
	collected := map[int64][]byte{
		0: []byte(`{"type":"message","id":"keep"}`),
		1: []byte(`{"type":"function_call","call_id":"bad","name":"exec"}`),
		2: []byte(`{"type":"custom_tool_call","call_id":"good","name":"exec","input":""}`),
	}
	output := responseCompletedOutputFromPayloadWithFallback(terminal, collected, nil)
	items := gjson.ParseBytes(output).Array()
	if len(items) != 2 || items[0].Get("id").String() != "keep" || items[1].Get("call_id").String() != "good" {
		t.Fatalf("incomplete tool leaked into recovered output: %s", output)
	}
}
