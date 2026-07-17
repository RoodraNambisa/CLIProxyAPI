package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"image/color"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestChatGPTWebExecutorExecuteTextConversation(t *testing.T) {
	server := newChatGPTWebRuntimeFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	response, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gjson.GetBytes(response.Payload, "response.output.0.content.0.text").String(); got != "Hello world" {
		t.Fatalf("assistant text = %q, payload=%s", got, response.Payload)
	}
}

func TestChatGPTWebRuntimeClientRestoresStableLegacyIdentity(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	auth := chatGPTWebRuntimeAuth()
	delete(auth.Metadata, "device_id")
	delete(auth.Metadata, "session_id")
	auth.Metadata["cookies"] = []chatgptwebauth.Cookie{{
		Name: "oai-did", Value: "cookie-device", Domain: "chatgpt.com", Path: "/",
	}}

	firstClient, firstCredential, err := executor.newRuntimeClient(auth)
	if err != nil {
		t.Fatal(err)
	}
	firstClient.CloseIdleConnections()
	secondClient, secondCredential, err := executor.newRuntimeClient(auth)
	if err != nil {
		t.Fatal(err)
	}
	secondClient.CloseIdleConnections()

	if firstCredential.DeviceID != "cookie-device" || secondCredential.DeviceID != "cookie-device" {
		t.Fatalf("device IDs = %q/%q", firstCredential.DeviceID, secondCredential.DeviceID)
	}
	if firstCredential.SessionID == "" || firstCredential.SessionID != secondCredential.SessionID {
		t.Fatalf("session IDs = %q/%q, want stable non-empty values", firstCredential.SessionID, secondCredential.SessionID)
	}
}

func TestChatGPTWebRuntimeClientPersistsRotatedCookiesAndIdentity(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebRuntimeAuth()
	delete(auth.Metadata, "device_id")
	delete(auth.Metadata, "session_id")
	registered, err := manager.Register(context.Background(), auth)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	executor := NewChatGPTWebExecutor(nil, manager)
	client, credential, err := executor.newRuntimeClient(registered)
	if err != nil {
		t.Fatal(err)
	}
	if err = client.SetCookie("https://chatgpt.com", "rotated-session", "rotated-value"); err != nil {
		t.Fatal(err)
	}
	executor.finishChatGPTWebRuntimeClient(context.Background(), registered, credential, client)

	current, ok := manager.GetByID(registered.ID)
	if !ok || current == nil {
		t.Fatal("updated auth missing")
	}
	persisted, err := chatgptwebauth.ParseCredential(current.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if persisted.DeviceID == "" || persisted.SessionID == "" {
		t.Fatalf("runtime IDs = %q/%q, want non-empty", persisted.DeviceID, persisted.SessionID)
	}
	found := false
	for _, cookie := range persisted.Cookies {
		if cookie.Name == "rotated-session" && cookie.Value == "rotated-value" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("persisted cookies = %#v, want rotated-session", persisted.Cookies)
	}
}

func TestMergeChatGPTWebCookieDeltaPreservesConcurrentCookies(t *testing.T) {
	baseline := []chatgptwebauth.Cookie{
		{Name: "session", Value: "old", Domain: "chatgpt.com", Path: "/"},
		{Name: "removed", Value: "old", Domain: "chatgpt.com", Path: "/"},
	}
	current := append(append([]chatgptwebauth.Cookie(nil), baseline...),
		chatgptwebauth.Cookie{Name: "concurrent", Value: "kept", Domain: "chatgpt.com", Path: "/"})
	next := []chatgptwebauth.Cookie{
		{Name: "session", Value: "rotated", Domain: "chatgpt.com", Path: "/"},
	}

	merged := mergeChatGPTWebCookieDelta(current, baseline, next)
	values := make(map[string]string, len(merged))
	for _, cookie := range merged {
		values[cookie.Name] = cookie.Value
	}
	if values["session"] != "rotated" || values["concurrent"] != "kept" {
		t.Fatalf("merged cookies = %#v", merged)
	}
	if _, exists := values["removed"]; exists {
		t.Fatalf("deleted cookie remained in %#v", merged)
	}
}

func TestChatGPTWebExecutorExecuteStreamTextConversation(t *testing.T) {
	server := newChatGPTWebRuntimeFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var output strings.Builder
	var events []gjson.Result
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		output.Write(chunk.Payload)
		output.WriteByte('\n')
		payload := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(chunk.Payload)), "data:"))
		if gjson.Valid(payload) {
			events = append(events, gjson.Parse(payload))
		}
	}
	if !strings.Contains(output.String(), `"type":"response.output_text.delta"`) ||
		!strings.Contains(output.String(), `"type":"response.in_progress"`) ||
		!strings.Contains(output.String(), `"type":"response.completed"`) ||
		!strings.Contains(output.String(), `"delta":" world"`) {
		t.Fatalf("stream output = %s", output.String())
	}
	assertChatGPTWebEventSequence(t, events)
}

func TestChatGPTWebExecutorTextStreamCommitsBeforeLateProtocolFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"metadata\":{\"finish_details\":{\"type\":\"finished_partial_completion\"}},\"content\":{\"parts\":[\"partial\"]}}}\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	chunks := make([]cliproxyexecutor.StreamChunk, 0, 1)
	for chunk := range result.Chunks {
		chunks = append(chunks, chunk)
	}
	if len(chunks) != 2 ||
		!cliproxyexecutor.IsBootstrapCommitStreamChunk(chunks[0]) ||
		chunks[1].Err == nil ||
		len(chunks[1].Payload) != 0 {
		t.Fatalf("stream chunks = %#v", chunks)
	}
}

func TestChatGPTWebExecutorTextStreamHeartbeatsBeforeDelayedAssistantData(t *testing.T) {
	started := make(chan struct{})
	releaseContext, release := context.WithCancel(context.Background())
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			close(started)
			<-releaseContext.Done()
			_, _ = io.WriteString(w, "data: {\"message\":{\"id\":\"answer\",\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"done\"]}}}\n\n")
			_, _ = io.WriteString(w, "data: {\"message\":{\"id\":\"answer\",\"author\":{\"role\":\"assistant\"},\"metadata\":{\"finish_details\":{\"type\":\"finished_successfully\"}},\"content\":{\"parts\":[\"done\"]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.streamInitialWait = 10 * time.Millisecond
	executor.streamHeartbeat = 0
	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("conversation request did not start")
	}
	select {
	case chunk := <-result.Chunks:
		if !cliproxyexecutor.IsBootstrapCommitStreamChunk(chunk) {
			t.Fatalf("bootstrap marker = %#v", chunk)
		}
	case <-time.After(time.Second):
		t.Fatal("text stream did not commit immediately")
	}
	select {
	case chunk := <-result.Chunks:
		if string(chunk.Payload) != ": chatgpt-web upstream pending\n\n" {
			t.Fatalf("heartbeat = %#v", chunk)
		}
	case <-time.After(time.Second):
		t.Fatal("text stream did not emit a heartbeat")
	}
	release()
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
	}
}

func TestChatGPTWebExecutorTrustedSSEFramesTranslatedEvents(t *testing.T) {
	server := newChatGPTWebRuntimeFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatCodex,
		ResponseFormat: sdktranslator.FormatOpenAIResponse,
		Metadata: map[string]any{
			cliproxyexecutor.TrustUpstreamSSEMetadataKey: true,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	eventCount := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		if cliproxyexecutor.IsBootstrapCommitStreamChunk(chunk) {
			continue
		}
		if !bytes.HasSuffix(chunk.Payload, []byte("\n\n")) {
			t.Fatalf("trusted SSE chunk is not a complete frame: %q", chunk.Payload)
		}
		eventCount++
	}
	if eventCount < 2 {
		t.Fatalf("trusted SSE event count = %d, want multiple events", eventCount)
	}
}

func TestChatGPTWebExecutorTextStreamSurfacesLateFailureWithoutCoolingAuth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"message\":{\"id\":\"answer\",\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"partial\"]}}}\n\n")
			_, _ = io.WriteString(w, "data: {\"message\":{\"id\":\"answer\",\"author\":{\"role\":\"assistant\"},\"metadata\":{\"finish_details\":{\"type\":\"finished_partial_completion\"}},\"content\":{\"parts\":[\"partial\"]}}}\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var sawDelta bool
	var terminalErr error
	for chunk := range result.Chunks {
		if strings.Contains(string(chunk.Payload), `"type":"response.output_text.delta"`) {
			sawDelta = true
		}
		if chunk.Err != nil {
			terminalErr = chunk.Err
		}
	}
	if !sawDelta {
		t.Fatal("late failure stream did not preserve the already emitted delta")
	}
	if terminalErr == nil || !strings.Contains(terminalErr.Error(), "finished_partial_completion") {
		t.Fatalf("terminal error = %v", terminalErr)
	}
	var status interface{ StatusCode() int }
	var skip interface{ SkipAuthResult() bool }
	if !errors.As(terminalErr, &status) || status.StatusCode() != http.StatusBadGateway ||
		!errors.As(terminalErr, &skip) || !skip.SkipAuthResult() {
		t.Fatalf("late failure error = %#v", terminalErr)
	}
}

func TestChatGPTWebExecutorExecuteSearch(t *testing.T) {
	server := newChatGPTWebSearchFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0

	response, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-5",
		Payload: []byte(`{
			"model":"gpt-5",
			"input":"latest answer",
			"tools":[{"type":"web_search_preview"}],
			"tool_choice":{"type":"web_search_preview"}
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gjson.GetBytes(response.Payload, "response.output.0.type").String(); got != "web_search_call" {
		t.Fatalf("first output type = %q, payload=%s", got, response.Payload)
	}
	inputTokens := gjson.GetBytes(response.Payload, "response.usage.input_tokens").Int()
	outputTokens := gjson.GetBytes(response.Payload, "response.usage.output_tokens").Int()
	if inputTokens <= 0 || outputTokens <= 0 ||
		gjson.GetBytes(response.Payload, "response.usage.total_tokens").Int() != inputTokens+outputTokens {
		t.Fatalf("search usage = %s, payload=%s", gjson.GetBytes(response.Payload, "response.usage").Raw, response.Payload)
	}
	searchAction := gjson.GetBytes(response.Payload, "response.output.0.action")
	if got := searchAction.Get("query").String(); got != "latest answer" {
		t.Fatalf("search query = %q, payload=%s", got, response.Payload)
	}
	if got := searchAction.Get("queries.0").String(); got != "latest answer" {
		t.Fatalf("search queries = %s, payload=%s", searchAction.Get("queries").Raw, response.Payload)
	}
	if got := searchAction.Get("sources.0.type").String(); got != "url" {
		t.Fatalf("search source type = %q, payload=%s", got, response.Payload)
	}
	if got := searchAction.Get("sources.0.url").String(); got != "https://example.com" {
		t.Fatalf("search source URL = %q, payload=%s", got, response.Payload)
	}
	if gjson.GetBytes(response.Payload, "response.output.0.results").Exists() {
		t.Fatalf("non-standard search results field exists: %s", response.Payload)
	}
	if got := gjson.GetBytes(response.Payload, "response.output.1.content.0.text").String(); !strings.Contains(got, "Sources:") {
		t.Fatalf("search answer = %q", got)
	}
	text := gjson.GetBytes(response.Payload, "response.output.1.content.0.text").String()
	annotation := gjson.GetBytes(response.Payload, "response.output.1.content.0.annotations.0")
	start := int(annotation.Get("start_index").Int())
	end := int(annotation.Get("end_index").Int())
	runes := []rune(text)
	if start < 0 || end <= start || end > len(runes) || string(runes[start:end]) != "https://example.com" {
		t.Fatalf("citation range = %d:%d in %q, annotation=%s", start, end, text, annotation.Raw)
	}
}

func TestChatGPTWebExecutorExecuteStreamSearchResponsesLifecycle(t *testing.T) {
	server := newChatGPTWebSearchFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0

	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-5",
		Payload: []byte(`{
			"model":"gpt-5",
			"input":"latest answer",
			"tools":[{"type":"web_search_preview"}],
			"tool_choice":{"type":"web_search_preview"},
			"stream":true
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var events []gjson.Result
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		payload := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(string(chunk.Payload)), "data:"))
		if gjson.Valid(payload) {
			events = append(events, gjson.Parse(payload))
		}
	}
	if got := result.Headers.Get("X-Request-ID"); got != "search-request" {
		t.Fatalf("stream response header = %q", got)
	}

	var searchDone, searchSchema, messageAdded, partAdded, textDelta, textDone, partDone, messageDone, completedUsage bool
	for _, event := range events {
		eventType := event.Get("type").String()
		switch eventType {
		case "response.output_item.done":
			switch event.Get("item.type").String() {
			case "web_search_call":
				searchDone = event.Get("output_index").Int() == 0
				searchSchema = event.Get("item.action.queries.0").String() == "latest answer" &&
					event.Get("item.action.sources.0.type").String() == "url" &&
					event.Get("item.action.sources.0.url").String() == "https://example.com" &&
					!event.Get("item.results").Exists()
			case "message":
				messageDone = event.Get("output_index").Int() == 1
			}
		case "response.output_item.added":
			if event.Get("item.type").String() == "message" {
				messageAdded = event.Get("output_index").Int() == 1 && event.Get("item.status").String() == "in_progress"
			}
		case "response.content_part.added":
			partAdded = event.Get("output_index").Int() == 1
		case "response.output_text.delta":
			textDelta = event.Get("output_index").Int() == 1 && strings.Contains(event.Get("delta").String(), "The answer.")
		case "response.output_text.done":
			textDone = event.Get("output_index").Int() == 1
		case "response.content_part.done":
			partDone = event.Get("output_index").Int() == 1
		case "response.completed":
			usage := event.Get("response.usage")
			completedUsage = usage.Get("input_tokens").Int() > 0 &&
				usage.Get("output_tokens").Int() > 0 &&
				usage.Get("total_tokens").Int() == usage.Get("input_tokens").Int()+usage.Get("output_tokens").Int()
		}
	}
	if !searchDone || !searchSchema || !messageAdded || !partAdded || !textDelta || !textDone || !partDone || !messageDone || !completedUsage {
		t.Fatalf("incomplete search event lifecycle: searchDone=%t searchSchema=%t messageAdded=%t partAdded=%t textDelta=%t textDone=%t partDone=%t messageDone=%t completedUsage=%t events=%v",
			searchDone, searchSchema, messageAdded, partAdded, textDelta, textDone, partDone, messageDone, completedUsage, events)
	}
	assertChatGPTWebEventSequence(t, events)
}

func assertChatGPTWebEventSequence(t *testing.T, events []gjson.Result) {
	t.Helper()
	if len(events) < 2 {
		t.Fatalf("event count = %d", len(events))
	}
	if events[0].Get("type").String() != "response.created" ||
		events[1].Get("type").String() != "response.in_progress" {
		t.Fatalf("initial events = %q, %q", events[0].Get("type").String(), events[1].Get("type").String())
	}
	for index, event := range events {
		if got := event.Get("sequence_number"); !got.Exists() || got.Int() != int64(index) {
			t.Fatalf("event %d sequence_number = %s", index, got.Raw)
		}
	}
}

func TestEmitChatGPTWebImageEventsKeepsResultOutOfAddedEvent(t *testing.T) {
	response := gjson.Parse(`{
		"id":"resp_image",
		"model":"gpt-image-2",
		"output":[{
			"type":"image_generation_call",
			"id":"img_1",
			"status":"completed",
			"result":"aGVsbG8="
		}]
	}`)
	var events []gjson.Result

	if ok := emitChatGPTWebEventsFromCompleted("resp_image", "gpt-image-2", response, func(event []byte) bool {
		events = append(events, gjson.ParseBytes(event))
		return true
	}); !ok {
		t.Fatal("event emission stopped unexpectedly")
	}

	var added, done, completed gjson.Result
	for _, event := range events {
		switch event.Get("type").String() {
		case "response.output_item.added":
			added = event
		case "response.output_item.done":
			done = event
		case "response.completed":
			completed = event
		}
	}
	if !added.Exists() || added.Get("item.result").Exists() {
		t.Fatalf("added image event contains terminal result: %s", added.Raw)
	}
	if got := added.Get("item.status").String(); got != "in_progress" {
		t.Fatalf("added image status = %q", got)
	}
	if got := done.Get("item.result").String(); got != "aGVsbG8=" {
		t.Fatalf("done image result = %q", got)
	}
	if got := completed.Get("response.output.0.result").String(); got != "aGVsbG8=" {
		t.Fatalf("completed image result = %q", got)
	}
	assertChatGPTWebEventSequence(t, events)
}

func TestEmitChatGPTWebEventsStopsWhenConsumerStops(t *testing.T) {
	response := gjson.Parse(`{
		"id":"resp_image",
		"model":"gpt-image-2",
		"output":[{"type":"image_generation_call","id":"img_1","status":"completed","result":"aGVsbG8="}]
	}`)
	emitted := 0

	ok := emitChatGPTWebEventsFromCompleted("resp_image", "gpt-image-2", response, func([]byte) bool {
		emitted++
		return emitted < 3
	})

	if ok {
		t.Fatal("event emission succeeded after consumer stopped")
	}
	if emitted != 3 {
		t.Fatalf("emitted event count = %d, want 3", emitted)
	}
}

func TestChatGPTWebExecutorRejectsChatCompletionsToolHistory(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-5",
		Payload: []byte(`{
			"model":"gpt-5",
			"messages":[
				{"role":"user","content":"look it up"},
				{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
				{"role":"tool","tool_call_id":"call_1","content":"result"}
			]
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI, ResponseFormat: sdktranslator.FormatOpenAI})
	if err == nil || !strings.Contains(err.Error(), "function_call") {
		t.Fatalf("Execute() error = %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
}

func TestChatGPTWebExecutorExecuteImageGeneration(t *testing.T) {
	server := newChatGPTWebImageFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)

	response, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"input":[{"role":"user","content":[{"type":"input_text","text":"draw a square"}]}],
			"tools":[{"type":"image_generation","model":"gpt-image-2","output_format":"png"}]
		}`),
	}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex,
		Metadata: map[string]any{cliproxyexecutor.RequestedModelMetadataKey: "gpt-image-2"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gjson.GetBytes(response.Payload, "response.output.0.type").String(); got != "image_generation_call" {
		t.Fatalf("output type = %q, payload=%s", got, response.Payload)
	}
	expected := base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255}))
	if got := gjson.GetBytes(response.Payload, "response.output.0.result").String(); got != expected {
		t.Fatalf("image result = %q", got)
	}
	if got := gjson.GetBytes(response.Payload, "response.output.0.output_format").String(); got != "png" {
		t.Fatalf("output format = %q", got)
	}
	if revised := gjson.GetBytes(response.Payload, "response.output.0.revised_prompt"); revised.Exists() {
		t.Fatalf("revised_prompt = %s, want absent", revised.Raw)
	}
}

func TestChatGPTWebExecutorFetchModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/models":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{
				map[string]any{"slug": "gpt-5", "title": "GPT-5"},
				map[string]any{"slug": "gpt-image-2"},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	models, err := executor.FetchModels(context.Background(), chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("FetchModels() error = %v", err)
	}
	if len(models) != 2 || models[0].Slug != "gpt-5" {
		t.Fatalf("models = %#v", models)
	}
}

func TestChatGPTWebExecutorFetchModelsFollowsSameOriginBootstrapRedirect(t *testing.T) {
	var bootstrapHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			bootstrapHits++
			http.Redirect(w, request, "/home", http.StatusTemporaryRedirect)
		case "/home":
			bootstrapHits++
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/models":
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{
				map[string]any{"slug": "gpt-5", "title": "GPT-5"},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	models, err := executor.FetchModels(context.Background(), chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("FetchModels() error = %v", err)
	}
	if bootstrapHits != 2 {
		t.Fatalf("bootstrap hits = %d, want 2", bootstrapHits)
	}
	if len(models) != 1 || models[0].Slug != "gpt-5" {
		t.Fatalf("models = %#v", models)
	}
}

func TestChatGPTWebExecutorFetchModelsFollowsSameOriginCatalogRedirect(t *testing.T) {
	var catalogHits int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/models":
			catalogHits++
			http.Redirect(w, request, "/backend-api/models/final", http.StatusFound)
		case "/backend-api/models/final":
			catalogHits++
			_ = json.NewEncoder(w).Encode(map[string]any{"models": []any{
				map[string]any{"slug": "gpt-5", "title": "GPT-5"},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	models, err := executor.FetchModels(context.Background(), chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatalf("FetchModels() error = %v", err)
	}
	if catalogHits != 2 {
		t.Fatalf("catalog hits = %d, want 2", catalogHits)
	}
	if len(models) != 1 || models[0].Slug != "gpt-5" {
		t.Fatalf("models = %#v", models)
	}
}

func newChatGPTWebRuntimeFixture(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"Hello\"]}}}\n\n")
			_, _ = io.WriteString(w, "data: {\"o\":\"patch\",\"v\":[{\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\" world\"}]}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
}

func newChatGPTWebSearchFixture(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/f/conversation/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"conduit_token": "conduit"})
		case "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("X-Request-ID", "search-request")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"search-conversation\"}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/search-conversation":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"answer": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "assistant"},
					"create_time": 2,
					"content":     map[string]any{"parts": []any{"The answer."}},
					"metadata": map[string]any{
						"finish_details": map[string]any{"type": "finished_successfully"},
						"citations":      []any{map[string]any{"title": "Example", "url": "https://example.com"}},
					},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
}

func newChatGPTWebImageFixture(t *testing.T) *httptest.Server {
	t.Helper()
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/f/conversation/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"conduit_token": "conduit"})
		case "/backend-api/f/conversation":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"image-conversation\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated-image\"}]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/image-conversation":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"generated": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata": map[string]any{
						"async_task_type": "image_gen",
						"finish_details":  map[string]any{"type": "finished_successfully"},
						"is_complete":     true,
					},
					"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated-image"}}},
				}},
			}})
		case "/backend-api/files/generated-image/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/signed-image"})
		case "/signed-image":
			if accept := request.Header.Get("Accept"); strings.Contains(accept, "avif") || strings.Contains(accept, "svg") {
				t.Errorf("unsupported image formats advertised: %q", accept)
			}
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	return server
}

func chatGPTWebRuntimeAuth() *cliproxyauth.Auth {
	credential := &chatgptwebauth.Credential{
		Type:           chatgptwebauth.Provider,
		Email:          "user@example.com",
		AccessToken:    "access-token",
		Persona:        chatgptwebauth.DefaultPersona(),
		DeviceID:       "device-id",
		SessionID:      "session-id",
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	return &cliproxyauth.Auth{
		ID: "chatgpt-web-test.json", Provider: chatgptwebauth.Provider, Status: cliproxyauth.StatusActive, Metadata: metadata,
	}
}

func disableChatGPTWebImagePollWaits(executor *ChatGPTWebExecutor) {
	if executor == nil {
		return
	}
	executor.imageInitialWait = 0
	executor.imagePollInterval = 0
	executor.imageSettleWait = 0
}
