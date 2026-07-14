package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	internalcache "github.com/router-for-me/CLIProxyAPI/v6/internal/cache"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestAntigravityReasoningReplayAccumulatorMultiToolSSEChunks(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	requestPayload := []byte(`{"sessionId":"sess-1","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)
	scope := antigravityReasoningReplayScope{modelName: "gemini-3-flash-agent", sessionKey: "session:sess-1"}
	acc := newAntigravityReasoningReplayAccumulator(scope, requestPayload)
	if acc == nil {
		t.Fatal("accumulator is nil")
	}
	if acc.contentIndex != 1 || acc.nextPartIndex != 0 {
		t.Fatalf("pending model slot = %d/%d, want 1/0", acc.contentIndex, acc.nextPartIndex)
	}

	line1 := []byte(`data: {"response":{"candidates":[{"content":{"parts":[{"thoughtSignature":"sig-first","functionCall":{"name":"Read","args":{"file_path":"/a"},"id":"id1"}}]}}]}}`)
	line2 := []byte(`data: {"response":{"candidates":[{"content":{"parts":[{"functionCall":{"name":"Read","args":{"file_path":"/b"},"id":"id2"}}]}}]}}`)
	acc.ObserveSSELine(line1)
	acc.ObserveSSELine(line2)
	acc.Flush(context.Background())

	items, ok := internalcache.GetAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-1")
	if !ok || len(items) != 2 {
		t.Fatalf("cached items = %v ok=%v, want 2 items", len(items), ok)
	}
	pi0 := int(gjson.GetBytes(items[0], "partIndex").Int())
	pi1 := int(gjson.GetBytes(items[1], "partIndex").Int())
	if pi0 != 0 || pi1 != 1 {
		t.Fatalf("partIndex = %d,%d, want 0,1", pi0, pi1)
	}
	if got := gjson.GetBytes(items[0], "thoughtSignature").String(); got != "sig-first" {
		t.Fatalf("first sig = %q", got)
	}
}

func TestAntigravityReasoningReplaySuccessfulResponseWithoutItemsClearsPreviousTurn(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	model := "gemini-3-flash-agent"
	sessionKey := "session:sess-no-replay"
	previous := []byte(`{"type":"thought_signature","thoughtSignature":"0123456789abcdef","contentIndex":1,"partIndex":0}`)
	if !internalcache.CacheAntigravityReasoningReplayItems(model, sessionKey, [][]byte{previous}) {
		t.Fatal("failed to seed previous replay state")
	}

	acc := newAntigravityReasoningReplayAccumulator(
		antigravityReasoningReplayScope{modelName: model, sessionKey: sessionKey},
		[]byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"next"}]}]}}`),
	)
	acc.observeResponsePayload([]byte(`{"response":{"candidates":[{"content":{"role":"model","parts":[{"text":"plain response"}]},"finishReason":"STOP"}]}}`))
	acc.Flush(context.Background())

	if _, ok := internalcache.GetAntigravityReasoningReplayItems(model, sessionKey); ok {
		t.Fatal("successful response without replay items retained the previous turn")
	}
}

func TestPrepareAntigravityGeminiReasoningReplayPayloadInjectsCachedToolPart(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"function_call_part","contentIndex":1,"partIndex":0,"name":"Read","call_id":"id1","args":{"file_path":"/a"},"thoughtSignature":"sig-first"}`)
	if !internalcache.CacheAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-2", [][]byte{item}) {
		t.Fatal("cache write failed")
	}

	req := cliproxyexecutor.Request{}
	opts := cliproxyexecutor.Options{}
	payload := []byte(`{"sessionId":"sess-2","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"user","parts":[{"functionResponse":{"id":"id1","name":"Read","response":{"result":"ok"}}}]}]}}`)
	out, scope, err := prepareAntigravityGeminiReasoningReplayPayload(context.Background(), "gemini-3-flash-agent", req, opts, payload)
	if err != nil {
		t.Fatalf("prepare error: %v", err)
	}
	if !scope.valid() {
		t.Fatal("scope invalid")
	}
	if gjson.GetBytes(out, "request.contents.1.role").String() != "model" {
		t.Fatalf("functionCall replay must be model role at [1], got %s", string(out))
	}
	if got := gjson.GetBytes(out, "request.contents.1.parts.0.thoughtSignature").String(); got != "sig-first" {
		t.Fatalf("thoughtSignature = %q, want sig-first", got)
	}
	if !gjson.GetBytes(out, "request.contents.1.parts.0.functionCall").Exists() {
		t.Fatalf("functionCall not injected: %s", string(out))
	}
	if !gjson.GetBytes(out, "request.contents.2.parts.0.functionResponse").Exists() {
		t.Fatalf("functionResponse should follow model functionCall at [2]: %s", string(out))
	}
}

func TestPrepareAntigravityGeminiReasoningReplayInsertsBeforeModelFunctionResponse(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"function_call_part","contentIndex":1,"partIndex":0,"name":"Read","call_id":"id1","args":{"file_path":"/a"},"thoughtSignature":"sig-first"}`)
	internalcache.CacheAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-3", [][]byte{item})

	payload := []byte(`{"sessionId":"sess-3","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"model","parts":[{"functionResponse":{"id":"id1","name":"Read","response":{"result":"ok"}}}]}]}}`)
	out, _, err := prepareAntigravityGeminiReasoningReplayPayload(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if !gjson.GetBytes(out, "request.contents.1.parts.0.functionCall").Exists() || gjson.GetBytes(out, "request.contents.1.role").String() != "model" {
		t.Fatalf("want model functionCall at [1]: %s", string(out))
	}
	if !gjson.GetBytes(out, "request.contents.2.parts.0.functionResponse").Exists() {
		t.Fatalf("functionResponse should be at [2]: %s", string(out))
	}
}

func TestMergeAntigravityFunctionCallPartReplayMergesSignatureIntoExistingFunctionCall(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"function_call_part","contentIndex":1,"partIndex":0,"name":"Read","call_id":"id1","args":{"file_path":"/a"},"thoughtSignature":"sig-first"}`)
	internalcache.CacheAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-merge", [][]byte{item})

	payload := []byte(`{"sessionId":"sess-merge","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"model","parts":[{"functionCall":{"id":"id1","name":"Read","args":{"file_path":"/a"}}}]},{"role":"user","parts":[{"functionResponse":{"id":"id1","name":"Read","response":{"result":"ok"}}}]}]}}`)
	out, _, err := prepareAntigravityGeminiReasoningReplayPayload(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if got := gjson.GetBytes(out, "request.contents.1.parts.0.thoughtSignature").String(); got != "sig-first" {
		t.Fatalf("thoughtSignature = %q, want sig-first; body=%s", got, out)
	}
}

func TestPrepareAntigravityGeminiReasoningReplayPayloadAppendsStaleThoughtSignatureWithoutNullParts(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"thought_signature","contentIndex":8,"partIndex":3,"thoughtSignature":"0123456789abcdef"}`)
	if !internalcache.CacheAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-stale-text", [][]byte{item}) {
		t.Fatal("cache write failed")
	}

	payload := []byte(`{"sessionId":"sess-stale-text","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"model","parts":[{"text":"visible answer"}]},{"role":"user","parts":[{"text":"next"}]}]}}`)
	out, _, err := prepareAntigravityGeminiReasoningReplayPayload(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, payload)
	if err != nil {
		t.Fatal(err)
	}

	parts := gjson.GetBytes(out, "request.contents.1.parts").Array()
	if len(parts) != 2 {
		t.Fatalf("parts length = %d, want 2; body=%s", len(parts), out)
	}
	for i, part := range parts {
		if part.Type == gjson.Null {
			t.Fatalf("parts.%d is null; body=%s", i, out)
		}
	}
	if got := parts[0].Get("text").String(); got != "visible answer" {
		t.Fatalf("text part = %q, want visible answer; body=%s", got, out)
	}
	if got := parts[1].Get("thoughtSignature").String(); got != "0123456789abcdef" {
		t.Fatalf("thoughtSignature = %q, want 0123456789abcdef; body=%s", got, out)
	}
}

func TestPrepareAntigravityGeminiReasoningReplayDoesNotAttachSignatureToUserContent(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	item := []byte(`{"type":"thought_signature","contentIndex":1,"partIndex":0,"thoughtSignature":"0123456789abcdef"}`)
	if !internalcache.CacheAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:sess-user-only", [][]byte{item}) {
		t.Fatal("cache write failed")
	}

	payload := []byte(`{"sessionId":"sess-user-only","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]},{"role":"user","parts":[{"functionResponse":{"id":"id1","name":"Read","response":{"result":"ok"}}}]}]}}`)
	out, _, err := prepareAntigravityGeminiReasoningReplayPayload(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{}, payload)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != string(payload) {
		t.Fatalf("signature replay modified a request without model content: %s", out)
	}
}

func TestAntigravityReasoningReplayScopeRequiresExplicitSessionBoundary(t *testing.T) {
	payload := []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"stable-user-text"}]}]}}`)
	scope := antigravityReasoningReplayScopeFromPayload("gemini-3-flash-agent", payload)
	if scope.valid() {
		t.Fatalf("content-derived scope must be disabled, got %+v", scope)
	}
	scope = antigravityReasoningReplayScopeFromRequest(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "request-session-1"},
	}, payload)
	if !scope.valid() || scope.sessionKey != "execution:request-session-1" {
		t.Fatalf("metadata scope = %+v", scope)
	}
	payload = []byte(`{"sessionId":"client-controlled","request":{"contents":[]}}`)
	scope = antigravityReasoningReplayScopeFromRequest(context.Background(), "gemini-3-flash-agent", cliproxyexecutor.Request{}, cliproxyexecutor.Options{
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "trusted-execution"},
	}, payload)
	if scope.sessionKey != "execution:trusted-execution" {
		t.Fatalf("execution metadata must take precedence over payload session, got %+v", scope)
	}
}

func TestInsertAntigravityModelFunctionCallBeforeContentPreservesLargeIntegers(t *testing.T) {
	payload := []byte(`{"request":{"contents":[
		{"role":"user","parts":[{"functionResponse":{"id":"call-1","name":"lookup","response":{"account_id":9007199254740995}}}]}
	]}}`)
	args := gjson.Parse(`{"account_id":9007199254740993}`)

	out, ok := insertAntigravityModelFunctionCallBeforeContent(payload, 0, "lookup", "call-1", "0123456789abcdef", args)
	if !ok {
		t.Fatal("insert failed")
	}
	if got := gjson.GetBytes(out, "request.contents.0.parts.0.functionCall.args.account_id").Raw; got != "9007199254740993" {
		t.Fatalf("functionCall account_id = %s, want 9007199254740993. Output: %s", got, out)
	}
	if got := gjson.GetBytes(out, "request.contents.1.parts.0.functionResponse.response.account_id").Raw; got != "9007199254740995" {
		t.Fatalf("existing response account_id = %s, want 9007199254740995. Output: %s", got, out)
	}
}

func TestAntigravityReplayToolCallKeysUsesNativeFunctionCallID(t *testing.T) {
	fc := gjson.Parse(`{"name":"Read","args":{"file_path":"/a"},"id":"id-native"}`)
	keys := antigravityReplayToolCallKeysFromPart(fc)
	if len(keys) != 1 {
		t.Fatalf("keys = %v", keys)
	}
	fc2 := gjson.Parse(`{"name":"Read","args":{"file_path":"/a"},"id":"id-native-2"}`)
	keys2 := antigravityReplayToolCallKeysFromPart(fc2)
	if keys[0] == keys2[0] {
		t.Fatalf("parallel tool calls should not share replay key: %v vs %v", keys, keys2)
	}
}

func TestAntigravityRequestHasMatchingFunctionResponseWhitespaceCallID(t *testing.T) {
	item := gjson.Parse(`{"call_id":" "}`)
	if !antigravityRequestHasMatchingFunctionResponse(nil, item) {
		t.Fatal("whitespace-only call_id should be treated as empty => true")
	}
}

func TestAntigravityExecuteStreamCachesReplayOnlyAfterTerminal(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"reasoning\",\"thought\":true,\"thoughtSignature\":\"0123456789abcdef\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1,\"totalTokenCount\":2}}}\n\n"
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	}))
	auth := antigravityStreamTestAuth()
	req := antigravityStreamTestRequest("gemini-3-flash-agent")
	req.Payload = []byte(`{"sessionId":"stream-success","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)

	result, err := NewAntigravityExecutor(nil).ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
	items, ok := internalcache.GetAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:stream-success")
	if !ok || len(items) != 1 {
		t.Fatalf("cached items = %d ok=%v, want one", len(items), ok)
	}
	if got := gjson.GetBytes(items[0], "thoughtSignature").String(); got != "0123456789abcdef" {
		t.Fatalf("thoughtSignature = %q", got)
	}
}

func TestAntigravityExecuteStreamDoesNotCacheIncompleteReplay(t *testing.T) {
	internalcache.ClearAntigravityReasoningReplayCache()
	t.Cleanup(internalcache.ClearAntigravityReasoningReplayCache)

	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\",\"thought\":true,\"thoughtSignature\":\"0123456789abcdef\"}]}}]}}\n\n"
	ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", antigravityStreamRoundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	}))
	auth := antigravityStreamTestAuth()
	req := antigravityStreamTestRequest("gemini-3-flash-agent")
	req.Payload = []byte(`{"sessionId":"stream-incomplete","request":{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}}`)

	result, err := NewAntigravityExecutor(nil).ExecuteStream(ctx, auth, req, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FormatAntigravity,
		Stream:       true,
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	var streamErr error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			streamErr = chunk.Err
		}
	}
	if streamErr == nil {
		t.Fatal("incomplete stream returned no terminal error")
	}
	if _, ok := internalcache.GetAntigravityReasoningReplayItems("gemini-3-flash-agent", "session:stream-incomplete"); ok {
		t.Fatal("incomplete stream cached replay state")
	}
}
