package executor

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

const chatGPTWebSentinelRuntimeTestSDK = `var SentinelSDK=function(t){
const P={};
async function _n(){return "sdk-turnstile-token"}
async function Nt(){return "sdk-snapshot-token"}
function Et(){globalThis.__collector_started=(globalThis.__collector_started||0)+1;return Promise.resolve()}
function D(t,n){globalThis.__bound_challenge=t;globalThis.__bound_requirements=n}
async function we(){}
async function ye(){}
return t.init=we,t.sessionObserverToken=async function(t){return null},t.token=ye,t
}({});`

type fakeChatGPTWebSentinelRuntime struct {
	mu            sync.Mutex
	config        chatgptwebauth.SentinelRuntimeConfig
	beginErr      error
	snapshotErr   error
	fallbackCount atomic.Int64
	initialized   atomic.Bool
}

type fakeChatGPTWebSentinelObserver struct {
	request     chatgptwebauth.SentinelSDKRequest
	snapshotErr error
}

func (runtime *fakeChatGPTWebSentinelRuntime) Close() {}

func (runtime *fakeChatGPTWebSentinelRuntime) UpdateConfig(config chatgptwebauth.SentinelRuntimeConfig) {
	runtime.mu.Lock()
	runtime.config = config
	runtime.mu.Unlock()
}

func (runtime *fakeChatGPTWebSentinelRuntime) Snapshot() chatgptwebauth.SentinelRuntimeSnapshot {
	runtime.mu.Lock()
	config := runtime.config
	runtime.mu.Unlock()
	return chatgptwebauth.SentinelRuntimeSnapshot{
		Initialized:   runtime.initialized.Load(),
		Available:     config.Enabled,
		WorkerLimit:   config.Workers,
		FallbackCount: uint64(runtime.fallbackCount.Load()),
	}
}

func (runtime *fakeChatGPTWebSentinelRuntime) BeginObserver(_ context.Context, request chatgptwebauth.SentinelSDKRequest) (helps.ChatGPTWebSentinelObserver, error) {
	runtime.mu.Lock()
	config := runtime.config
	beginErr := runtime.beginErr
	snapshotErr := runtime.snapshotErr
	runtime.mu.Unlock()
	if !config.Enabled || !requiredJSONFlag(request.Challenge, "so", "required") {
		return nil, nil
	}
	if beginErr != nil {
		return nil, beginErr
	}
	if strings.TrimSpace(request.SDKURL) == "" {
		return nil, &chatgptwebauth.SentinelRuntimeError{Code: "sentinel_sdk_unavailable", Err: errors.New("Sentinel SDK URL is missing")}
	}
	runtime.initialized.Store(true)
	return &fakeChatGPTWebSentinelObserver{request: request, snapshotErr: snapshotErr}, nil
}

func (runtime *fakeChatGPTWebSentinelRuntime) SolveTurnstile(
	ctx context.Context,
	_ chatgptwebauth.ConversationTurnstileSolveRequest,
	request chatgptwebauth.SentinelSDKRequest,
	_ helps.ChatGPTWebSentinelObserver,
) (string, error) {
	runtime.mu.Lock()
	config := runtime.config
	runtime.mu.Unlock()
	if !config.Enabled {
		return "", errors.New("fake Sentinel runtime is disabled")
	}
	if request.Fetcher == nil {
		return "", errors.New("Sentinel SDK fetcher is missing")
	}
	if _, _, _, err := request.Fetcher(ctx, request.SDKURL, 4<<20); err != nil {
		return "", err
	}
	runtime.initialized.Store(true)
	runtime.fallbackCount.Add(1)
	return "sdk-turnstile-token", nil
}

func (observer *fakeChatGPTWebSentinelObserver) Snapshot(_ context.Context) (string, error) {
	if observer.snapshotErr != nil {
		return "", observer.snapshotErr
	}
	payload, err := json.Marshal(map[string]any{
		"so":   "sdk-snapshot-token",
		"c":    chatGPTWebAnyString(observer.request.Challenge["token"]),
		"id":   observer.request.DeviceID,
		"flow": observer.request.Flow,
	})
	return string(payload), err
}

func (*fakeChatGPTWebSentinelObserver) Close() {}

func TestChatGPTWebRequirementsUsesSentinelSDKCompatibilityFallback(t *testing.T) {
	var finalized map[string]any
	server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(pToken string) map[string]any {
		return map[string]any{
			"prepare_token": "prepare",
			"turnstile": map[string]any{
				"required": true,
				"dx":       chatGPTWebTurnstileTestDX(t, pToken, []any{[]any{36, "new-opcode"}}),
			},
		}
	}, func(body map[string]any) map[string]any {
		finalized = body
		return map[string]any{"token": "requirements"}
	})
	defer server.Close()

	executor, fetches := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	requirements := runChatGPTWebSentinelRequirements(t, executor)
	if requirements.TurnstileToken != "sdk-turnstile-token" {
		t.Fatalf("Turnstile token = %q", requirements.TurnstileToken)
	}
	if got := chatGPTWebAnyString(finalized["turnstile_token"]); got != "sdk-turnstile-token" {
		t.Fatalf("finalize Turnstile token = %q", got)
	}
	if fetches.Load() != 1 || executor.SentinelSnapshot().FallbackCount != 1 {
		t.Fatalf("fetches = %d, snapshot = %+v", fetches.Load(), executor.SentinelSnapshot())
	}
}

func TestChatGPTWebRequirementsPrefersFinalizeSessionObserverToken(t *testing.T) {
	brokenSnapshotSDK := strings.Replace(chatGPTWebSentinelRuntimeTestSDK, `async function Nt(){return "sdk-snapshot-token"}`, `async function Nt(){throw new Error("snapshot must not run")}`, 1)
	server := newChatGPTWebSentinelRequirementsServer(t, brokenSnapshotSDK, func(string) map[string]any {
		return chatGPTWebSentinelObserverPrepare()
	}, func(map[string]any) map[string]any {
		return map[string]any{"token": "requirements", "so_token": "server-so-token"}
	})
	defer server.Close()

	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, brokenSnapshotSDK)
	defer func() { _ = executor.Close() }()
	requirements := runChatGPTWebSentinelRequirements(t, executor)
	if requirements.SOToken != "server-so-token" {
		t.Fatalf("SO token = %q", requirements.SOToken)
	}
}

func TestChatGPTWebRequirementsIgnoresNonStringFinalizeSessionObserverToken(t *testing.T) {
	server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(string) map[string]any {
		return chatGPTWebSentinelObserverPrepare()
	}, func(map[string]any) map[string]any {
		return map[string]any{"token": "requirements", "so_token": false}
	})
	defer server.Close()

	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	requirements := runChatGPTWebSentinelRequirements(t, executor)
	var soToken map[string]any
	if err := json.Unmarshal([]byte(requirements.SOToken), &soToken); err != nil {
		t.Fatalf("decode local SO token: %v", err)
	}
	if soToken["so"] != "sdk-snapshot-token" {
		t.Fatalf("SO token = %#v", soToken)
	}
}

func TestChatGPTWebRequirementsUsesFinalizeTokenWhenObserverPoolIsBusy(t *testing.T) {
	server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(string) map[string]any {
		return chatGPTWebSentinelObserverPrepare()
	}, func(map[string]any) map[string]any {
		return map[string]any{"token": "requirements", "so_token": "server-so-token"}
	})
	defer server.Close()
	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	runtime := executor.sentinelRuntime.(*fakeChatGPTWebSentinelRuntime)
	runtime.mu.Lock()
	runtime.beginErr = &chatgptwebauth.SentinelRuntimeError{
		Code:       "sentinel_sdk_busy",
		RetryAfter: time.Second,
		Err:        errors.New("queue full"),
	}
	runtime.mu.Unlock()
	requirements := runChatGPTWebSentinelRequirements(t, executor)
	if requirements.SOToken != "server-so-token" {
		t.Fatalf("SO token = %q", requirements.SOToken)
	}
}

func TestChatGPTWebRequirementsUsesLocalSessionObserverSnapshot(t *testing.T) {
	server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(string) map[string]any {
		return chatGPTWebSentinelObserverPrepare()
	}, func(map[string]any) map[string]any {
		return map[string]any{"token": "requirements"}
	})
	defer server.Close()

	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	requirements := runChatGPTWebSentinelRequirements(t, executor)
	var soToken map[string]any
	if err := json.Unmarshal([]byte(requirements.SOToken), &soToken); err != nil {
		t.Fatalf("decode SO token: %v", err)
	}
	if soToken["so"] != "sdk-snapshot-token" || soToken["c"] != "challenge-token" || soToken["flow"] != "conversation" {
		t.Fatalf("SO token = %#v", soToken)
	}
}

func TestChatGPTWebExecutorExecuteSendsSessionObserverToken(t *testing.T) {
	server, receivedToken := newChatGPTWebSentinelExecutionServer(t)
	defer server.Close()
	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()

	_, err := executor.Execute(t.Context(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	assertChatGPTWebSentinelExecution(t, executor, receivedToken)
}

func TestChatGPTWebExecutorExecuteStreamSendsSessionObserverToken(t *testing.T) {
	server, receivedToken := newChatGPTWebSentinelExecutionServer(t)
	defer server.Close()
	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()

	result, err := executor.ExecuteStream(t.Context(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","stream":true}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex, Stream: true})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}
	assertChatGPTWebSentinelExecution(t, executor, receivedToken)
}

func TestChatGPTWebImageEntrySendsSessionObserverToken(t *testing.T) {
	server, receivedToken := newChatGPTWebSentinelExecutionServer(t)
	defer server.Close()
	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	execution, err := executor.beginChatGPTWebImage(t.Context(), client, credential, &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw a square"},
		},
	})
	if err != nil {
		t.Fatalf("beginChatGPTWebImage() error = %v", err)
	}
	if execution == nil || execution.response == nil {
		t.Fatal("beginChatGPTWebImage() returned no response")
	}
	_ = execution.response.Body.Close()
	assertChatGPTWebSentinelExecution(t, executor, receivedToken)
}

func TestChatGPTWebRequirementsRejectsRequiredObserverWhenSDKDisabled(t *testing.T) {
	server := newChatGPTWebSentinelRequirementsServer(t, chatGPTWebSentinelRuntimeTestSDK, func(string) map[string]any {
		return chatGPTWebSentinelObserverPrepare()
	}, func(map[string]any) map[string]any {
		return map[string]any{"token": "requirements"}
	})
	defer server.Close()

	enabled := false
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{
		Sentinel: config.ChatGPTWebSentinelConfig{SDKRuntimeEnabled: &enabled},
	}}, nil)
	executor.runtimeBaseURL = server.URL
	defer func() { _ = executor.Close() }()
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(t.Context(), client, credential)
	var protocolErr statusErr
	if !errors.As(err, &protocolErr) || protocolErr.code != http.StatusBadGateway || !strings.Contains(protocolErr.msg, "sentinel_session_observer_unavailable") {
		t.Fatalf("chatGPTWebRequirements() error = %#v", err)
	}
	if executor.SentinelSnapshot().Initialized {
		t.Fatal("disabled Sentinel runtime was initialized")
	}
}

func TestChatGPTWebRequirementsClassifiesRequiredObserverInitializationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, "<html></html>")
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(chatGPTWebSentinelObserverPrepare())
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor, _ := newChatGPTWebSentinelTestExecutor(server.URL, chatGPTWebSentinelRuntimeTestSDK)
	defer func() { _ = executor.Close() }()
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(t.Context(), client, credential)
	var protocolErr statusErr
	if !errors.As(err, &protocolErr) || protocolErr.code != http.StatusBadGateway || !strings.Contains(protocolErr.msg, "sentinel_session_observer_unavailable") {
		t.Fatalf("chatGPTWebRequirements() error = %#v", err)
	}
}

func TestChatGPTWebSentinelSDKFetcherDoesNotSendCredentialCookies(t *testing.T) {
	var cookieHeader string
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		cookieHeader = request.Header.Get("Cookie")
		response.Header().Set("Content-Type", "application/javascript")
		_, _ = io.WriteString(response, "sdk-source")
	}))
	defer server.Close()

	client, err := chatgptwebauth.NewClient(chatgptwebauth.DefaultPersona(), "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	if err = client.SetCookie(server.URL, "session", "credential-secret"); err != nil {
		t.Fatal(err)
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	defer func() { _ = executor.Close() }()
	fetch := executor.chatGPTWebSentinelSDKFetcher(client, &chatgptwebauth.Credential{Email: "person@example.com"})
	payload, contentType, finalURL, err := fetch(t.Context(), server.URL, 1024)
	if err != nil {
		t.Fatalf("fetch() error = %v", err)
	}
	if string(payload) != "sdk-source" || contentType != "application/javascript" || finalURL != server.URL {
		t.Fatalf("fetch() = %q, %q, %q", payload, contentType, finalURL)
	}
	if cookieHeader != "" {
		t.Fatalf("Sentinel SDK request leaked cookies: %q", cookieHeader)
	}
}

func TestChatGPTWebSentinelBusyErrorDoesNotMutateAuthState(t *testing.T) {
	err := chatGPTWebSentinelRuntimeProtocolError(&chatgptwebauth.SentinelRuntimeError{
		Code:       "sentinel_sdk_busy",
		RetryAfter: time.Second,
		Err:        errors.New("queue full"),
	})
	if err.StatusCode() != http.StatusServiceUnavailable || !err.SkipAuthResult() || err.RetryOtherAuth() {
		t.Fatalf("classified error = %#v", err)
	}
	if err.RetryAfter() == nil || *err.RetryAfter() != time.Second {
		t.Fatalf("RetryAfter = %v", err.RetryAfter())
	}
	if got := err.Headers().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After header = %q", got)
	}
	observerErr := chatGPTWebSentinelObserverProtocolError(&chatgptwebauth.SentinelRuntimeError{
		Code:       "sentinel_sdk_busy",
		RetryAfter: time.Second,
		Err:        errors.New("queue full"),
	})
	if observerErr.StatusCode() != http.StatusServiceUnavailable || !strings.Contains(observerErr.msg, "sentinel_sdk_busy") {
		t.Fatalf("Observer busy error = %#v", observerErr)
	}
}

func TestChatGPTWebSentinelObserverUnavailablePreservesRetryAfter(t *testing.T) {
	err := chatGPTWebSentinelObserverProtocolError(&chatgptwebauth.SentinelRuntimeError{
		Code:       "sentinel_sdk_unavailable",
		RetryAfter: 37 * time.Second,
		Err:        errors.New("source circuit is open"),
	})
	if err.StatusCode() != http.StatusBadGateway || !err.SkipAuthResult() || err.RetryOtherAuth() {
		t.Fatalf("classified error = %#v", err)
	}
	if !strings.Contains(err.msg, "sentinel_session_observer_unavailable") {
		t.Fatalf("classified error body = %q", err.msg)
	}
	if err.RetryAfter() == nil || *err.RetryAfter() != 37*time.Second {
		t.Fatalf("RetryAfter = %v", err.RetryAfter())
	}
	if got := err.Headers().Get("Retry-After"); got != "37" {
		t.Fatalf("Retry-After header = %q", got)
	}
}

func TestChatGPTWebSentinelTransportKeyIsStableAndSecretFree(t *testing.T) {
	first, err := chatgptwebauth.NewClient(chatgptwebauth.DefaultPersona(), "http://user:secret@proxy-a.example:8080", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	second, err := chatgptwebauth.NewClient(chatgptwebauth.DefaultPersona(), "http://user:secret@proxy-b.example:8080", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	firstKey := chatGPTWebSentinelTransportKey(first)
	if firstKey == "" || firstKey != chatGPTWebSentinelTransportKey(first) {
		t.Fatalf("unstable transport key = %q", firstKey)
	}
	if firstKey == chatGPTWebSentinelTransportKey(second) {
		t.Fatal("different proxies received the same transport key")
	}
	if strings.Contains(firstKey, "secret") || strings.Contains(firstKey, "proxy-a") {
		t.Fatalf("transport key leaked proxy details: %q", firstKey)
	}
}

func newChatGPTWebSentinelTestExecutor(baseURL, sdkSource string) (*ChatGPTWebExecutor, *atomic.Int64) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.sentinelRuntime.Close()
	runtime := &fakeChatGPTWebSentinelRuntime{
		config: chatgptwebauth.SentinelRuntimeConfig{Enabled: true, Workers: 1, QueueSize: 2, CacheVersions: 3},
	}
	if strings.Contains(sdkSource, "snapshot must not run") {
		runtime.snapshotErr = errors.New("snapshot must not run")
	}
	executor.sentinelRuntime = runtime
	executor.runtimeBaseURL = baseURL
	var fetches atomic.Int64
	executor.sentinelSDKFetcherFactory = func(*chatgptwebauth.Client, *chatgptwebauth.Credential) chatgptwebauth.SentinelSDKFetcher {
		return func(_ context.Context, targetURL string, maxBytes int64) ([]byte, string, string, error) {
			fetches.Add(1)
			if targetURL != "https://sentinel.openai.com/sentinel/20260721/sdk.js" {
				return nil, "", "", fmt.Errorf("unexpected SDK URL %q", targetURL)
			}
			if int64(len(sdkSource)) > maxBytes {
				return nil, "", "", errors.New("SDK source exceeds limit")
			}
			return []byte(sdkSource), "application/javascript", targetURL, nil
		}
	}
	return executor, &fetches
}

func newChatGPTWebSentinelRequirementsServer(
	t *testing.T,
	sdkSource string,
	prepare func(string) map[string]any,
	finalize func(map[string]any) map[string]any,
) *httptest.Server {
	t.Helper()
	digest := sha256.Sum256([]byte(sdkSource))
	integrity := "sha256-" + base64.StdEncoding.EncodeToString(digest[:])
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="https://sentinel.openai.com/sentinel/20260721/sdk.js" integrity="`+integrity+`"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(w, "invalid prepare", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(prepare(chatGPTWebAnyString(body["p"])))
		case "/backend-api/sentinel/chat-requirements/finalize":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				http.Error(w, "invalid finalize", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(finalize(body))
		default:
			http.NotFound(w, request)
		}
	}))
}

func newChatGPTWebSentinelExecutionServer(t *testing.T) (*httptest.Server, <-chan string) {
	t.Helper()
	digest := sha256.Sum256([]byte(chatGPTWebSentinelRuntimeTestSDK))
	integrity := "sha256-" + base64.StdEncoding.EncodeToString(digest[:])
	receivedToken := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="https://sentinel.openai.com/sentinel/20260721/sdk.js" integrity="`+integrity+`"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(chatGPTWebSentinelObserverPrepare())
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements", "so_token": "server-so-token"})
		case "/backend-api/conversation":
			receivedToken <- request.Header.Get("OpenAI-Sentinel-So-Token")
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"Hello\"]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/f/conversation/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"conduit_token": "conduit-token"})
		case "/backend-api/f/conversation":
			receivedToken <- request.Header.Get("OpenAI-Sentinel-So-Token")
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	return server, receivedToken
}

func assertChatGPTWebSentinelExecution(t *testing.T, executor *ChatGPTWebExecutor, receivedToken <-chan string) {
	t.Helper()
	select {
	case token := <-receivedToken:
		if token != "server-so-token" {
			t.Fatalf("OpenAI-Sentinel-So-Token = %q", token)
		}
	case <-time.After(time.Second):
		t.Fatal("conversation request did not arrive")
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := executor.SentinelSnapshot()
		if snapshot.Busy == 0 && snapshot.Queued == 0 && snapshot.ObserverSessions == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Sentinel resources were not released: %+v", snapshot)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func chatGPTWebSentinelObserverPrepare() map[string]any {
	return map[string]any{
		"prepare_token": "prepare",
		"token":         "challenge-token",
		"so": map[string]any{
			"required":     true,
			"collector_dx": "collector",
			"snapshot_dx":  "snapshot",
		},
	}
}

func runChatGPTWebSentinelRequirements(t *testing.T, executor *ChatGPTWebExecutor) chatGPTWebRequirements {
	t.Helper()
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	requirements, err := executor.chatGPTWebRequirements(t.Context(), client, credential)
	if err != nil {
		t.Fatalf("chatGPTWebRequirements() error = %v", err)
	}
	return requirements
}
