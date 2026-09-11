package executor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
)

func TestChatGPTWebPrimaryTaskIDCountDoesNotRequirePolling(t *testing.T) {
	before := ChatGPTWebImageProtocolSnapshot()
	body := "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"metadata\":{\"image_gen_task_id\":\"imagegen_primary\"}}}\n\ndata: [DONE]\n\n"
	accumulator, err := (&ChatGPTWebExecutor{}).consumeChatGPTWebImageStreamWithTaskPolling(t.Context(), nil, nil,
		&fhttp.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))})
	if err != nil || !reflect.DeepEqual(accumulator.TaskIDs, []string{"imagegen_primary"}) {
		t.Fatalf("primary stream IDs = %v, err = %v", accumulator.TaskIDs, err)
	}
	after := ChatGPTWebImageProtocolSnapshot()
	if after.TaskIDsObserved != before.TaskIDsObserved+1 || after.TaskPagesFetched != before.TaskPagesFetched ||
		after.ExactStreamsStarted != before.ExactStreamsStarted {
		t.Fatalf("primary ID metrics: before=%+v after=%+v", before, after)
	}
}

func TestChatGPTWebCanceledExactStreamCanResumeInNextPollPhase(t *testing.T) {
	entered := make(chan struct{})
	var streams atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_, _ = io.WriteString(w, `{"tasks":[{"task_id":"imagegen_resume","conversation_id":"current","response_message_id":"response","status":"running"}]}`)
		case "/backend-api/tasks/imagegen_resume/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			if streams.Add(1) == 1 {
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				close(entered)
				<-request.Context().Done()
				return
			}
			_, _ = io.WriteString(w, "data: {\"task_status\":\"completed\"}\n\ndata: [DONE]\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	defer func() { _ = executor.Close() }()
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	registry := &chatGPTWebImageExactStreamRegistry{}
	seed := &helps.ChatGPTWebImageAccumulator{ConversationID: "current"}
	finished := make(chan chatGPTWebImageTaskPollResult, 1)
	go func() {
		finished <- executor.fetchChatGPTWebImageTaskPages(ctx, client, credential, seed,
			newChatGPTWebPollResponseBudget(chatGPTWebPollResponseMaxBytes), registry)
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("exact stream did not start")
	}
	cancel()
	select {
	case result := <-finished:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled result = %v", result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("exact stream did not cancel")
	}
	result := executor.fetchChatGPTWebImageTaskPages(t.Context(), client, credential, seed,
		newChatGPTWebPollResponseBudget(chatGPTWebPollResponseMaxBytes), registry)
	if result.err != nil || streams.Load() != 2 {
		t.Fatalf("resumed exact stream: err=%v attempts=%d", result.err, streams.Load())
	}
}

func TestChatGPTWebTaskPageDiagnosticCounters(t *testing.T) {
	before := ChatGPTWebImageProtocolSnapshot().TaskDiagnostics
	for _, payload := range []string{`{"tasks":[],"cursor":null}`, `{"unexpected":[]}`, `{"tasks":`,
		`{"tasks":[null,{"task_id":"imagegen_task","conversation_id":"current"}]}`} {
		state, err := helps.CaptureChatGPTWebImageTasks([]byte(payload), "current", &helps.ChatGPTWebImageAccumulator{})
		observeChatGPTWebImageTaskPage(state, err)
	}
	after := ChatGPTWebImageProtocolSnapshot().TaskDiagnostics
	if after.EmptyPages != before.EmptyPages+1 || after.UnrecognizedPages != before.UnrecognizedPages+1 ||
		after.ParseErrors != before.ParseErrors+1 || after.Records != before.Records+2 ||
		after.InvalidRecords != before.InvalidRecords+1 || after.ImageRecords != before.ImageRecords+1 ||
		after.MatchedRecords != before.MatchedRecords+1 || after.MissingResponseIDRecords != before.MissingResponseIDRecords+1 {
		t.Fatalf("diagnostic delta: before=%+v after=%+v", before, after)
	}
	encoded, err := json.Marshal(ChatGPTWebImageProtocolSnapshot())
	if err != nil || strings.Contains(string(encoded), "imagegen_task") || strings.Contains(string(encoded), "current") {
		t.Fatalf("diagnostics leaked identities: %s, err=%v", encoded, err)
	}
}

func TestChatGPTWebPrimaryTaskIDsReachConcurrentWatcher(t *testing.T) {
	for _, source := range []string{"primary", "conversation"} {
		t.Run(source, func(t *testing.T) { testChatGPTWebTaskIDsReachConcurrentWatcher(t, source) })
	}
}

func testChatGPTWebTaskIDsReachConcurrentWatcher(t *testing.T, source string) {
	before := ChatGPTWebImageProtocolSnapshot()
	var exactRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			// The timestamp alone would exclude the exact current task. The ID
			// delivered by the SSE or conversation must take precedence without accepting
			// another conversation's task.
			_, _ = io.WriteString(w, `{"tasks":[
				{"task_id":"imagegen_exact","conversation_id":"current","created_at":50,"response_message_id":"response","status":"running"},
				{"task_id":"imagegen_other","conversation_id":"other","created_at":101,"response_message_id":"other","status":"running"}
			]}`)
		case "/backend-api/tasks/imagegen_exact/stream":
			exactRequests.Add(1)
			if request.URL.Query().Get("message_id") != "response" || request.URL.Query().Get("parent_conversation_id") != "current" {
				t.Error("exact stream query does not match the selected task")
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"task_id\":\"imagegen_exact\",\"task_status\":\"completed\",\"final_message\":{\"author\":{\"role\":\"tool\"},\"status\":\"finished_successfully\",\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://exact-output\"}]}}}\n\ndata: [DONE]\n\n")
		case "/backend-api/conversation/current":
			if source == "conversation" {
				_, _ = io.WriteString(w, `{"current_node":"response","mapping":{
					"request":{"id":"request","children":["response"],"message":{"id":"request","author":{"role":"user"},"create_time":100,"content":{"parts":["draw"]}}},
					"response":{"id":"response","parent":"request","message":{"id":"response","author":{"role":"assistant"},"create_time":101,"status":"in_progress","metadata":{"image_gen_task_id":"imagegen_exact","image_gen_async":true},"content":{"parts":["Creating image"]}}}
				}}`)
			} else {
				_, _ = io.WriteString(w, `{"mapping":{}}`)
			}
		default:
			t.Errorf("unexpected request: %s", request.URL.Path)
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	defer func() { _ = executor.Close() }()
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 8
	disableChatGPTWebImagePollWaits(executor)
	executor.imagePollInterval = 5 * time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() {
		metadata := map[string]any{"image_gen_async": true}
		if source == "primary" {
			metadata["image_gen_task_id"] = "imagegen_exact"
		}
		payload, _ := json.Marshal(map[string]any{"conversation_id": "current", "message": map[string]any{
			"author": map[string]any{"role": "assistant"}, "metadata": metadata,
		}})
		_, _ = io.WriteString(writer, "data: "+string(payload)+"\n\n")
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx, registry := withChatGPTWebImageExactStreamRegistry(ctx)
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPollingForTurn(ctx, client, credential,
		&fhttp.Response{StatusCode: http.StatusOK, Body: reader}, helps.ChatGPTWebImageTurn{MessageID: "request", CreatedAt: 100})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"exact-output"}) || exactRequests.Load() != 1 {
		t.Fatalf("files=%v exact requests=%d", accumulator.FileIDs, exactRequests.Load())
	}
	_, sameRegistry := withChatGPTWebImageExactStreamRegistry(newChatGPTWebImageTaskPollContext(ctx))
	if registry != sameRegistry {
		t.Fatal("request registry was lost across poll context isolation")
	}
	if after := ChatGPTWebImageProtocolSnapshot(); after.TaskIDsObserved != before.TaskIDsObserved+1 ||
		after.ExactStreamsStarted != before.ExactStreamsStarted+1 || after.FinalMessagesCaptured != before.FinalMessagesCaptured+1 {
		t.Fatalf("primary and polling observations were double counted: before=%+v after=%+v", before, after)
	}
}
