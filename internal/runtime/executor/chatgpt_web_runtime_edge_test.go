package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

type chatGPTWebBrokenErrorBody struct{}

type chatGPTWebTrackedBody struct {
	io.Reader
	closed atomic.Bool
}

type chatGPTWebWatcherProtocolErrorBody struct {
	reads     atomic.Int32
	closed    chan struct{}
	closeOnce sync.Once
}

type chatGPTWebBlockingBody struct {
	payload   []byte
	reads     atomic.Int32
	closed    chan struct{}
	closeOnce sync.Once
}

func newChatGPTWebWatcherProtocolErrorBody() *chatGPTWebWatcherProtocolErrorBody {
	return &chatGPTWebWatcherProtocolErrorBody{closed: make(chan struct{})}
}

func newChatGPTWebBlockingBody(payload string) *chatGPTWebBlockingBody {
	return &chatGPTWebBlockingBody{payload: []byte(payload), closed: make(chan struct{})}
}

func (body *chatGPTWebBlockingBody) Read(buffer []byte) (int, error) {
	if body.reads.Add(1) == 1 {
		return copy(buffer, body.payload), nil
	}
	<-body.closed
	return 0, io.EOF
}

func (body *chatGPTWebBlockingBody) Close() error {
	body.closeOnce.Do(func() { close(body.closed) })
	return nil
}

func (body *chatGPTWebWatcherProtocolErrorBody) Read(buffer []byte) (int, error) {
	switch body.reads.Add(1) {
	case 1:
		return copy(buffer, "data: {\"conversation_id\":\"watcher-error\"}\n\n"), nil
	case 2:
		<-body.closed
		payload := "data: {\"type\":\"error\",\"error\":{\"message\":\"image policy rejected\"}}\n\n"
		return copy(buffer, payload), io.EOF
	default:
		return 0, io.EOF
	}
}

func (body *chatGPTWebWatcherProtocolErrorBody) Close() error {
	body.closeOnce.Do(func() { close(body.closed) })
	return nil
}

func (body *chatGPTWebTrackedBody) Close() error {
	body.closed.Store(true)
	return nil
}

func (chatGPTWebBrokenErrorBody) Read(payload []byte) (int, error) {
	if len(payload) > 0 {
		payload[0] = 'x'
		return 1, io.ErrUnexpectedEOF
	}
	return 0, io.ErrUnexpectedEOF
}

func (chatGPTWebBrokenErrorBody) Close() error {
	return errors.New("close failed")
}

func TestConsumeChatGPTWebSearchBootstrapFlushesFinalFrame(t *testing.T) {
	conversationID, err := consumeChatGPTWebSearchBootstrap(context.Background(), strings.NewReader(
		"data: {\"conversation_id\":\"search-final\"}\n\ndata: [DONE]",
	))
	if err != nil {
		t.Fatalf("consumeChatGPTWebSearchBootstrap() error = %v", err)
	}
	if conversationID != "search-final" {
		t.Fatalf("conversation ID = %q", conversationID)
	}
}

func TestConsumeChatGPTWebSearchBootstrapRejectsErrorBeforeDone(t *testing.T) {
	_, err := consumeChatGPTWebSearchBootstrap(context.Background(), strings.NewReader(
		"data: {\"conversation_id\":\"search-failed\"}\n\n"+
			"data: {\"type\":\"error\",\"error\":{\"message\":\"search unavailable\"}}\n\n"+
			"data: [DONE]\n\n",
	))
	if err == nil || !strings.Contains(err.Error(), "search unavailable") {
		t.Fatalf("consumeChatGPTWebSearchBootstrap() error = %v", err)
	}
}

func TestConsumeChatGPTWebSearchBootstrapEnforcesAggregateLimits(t *testing.T) {
	if _, err := consumeChatGPTWebSearchBootstrapWithLimits(
		context.Background(),
		strings.NewReader(": comment\n\n: another\n\n"),
		8,
		100,
	); err == nil {
		t.Fatal("expected bootstrap byte limit error")
	}
	if _, err := consumeChatGPTWebSearchBootstrapWithLimits(
		context.Background(),
		strings.NewReader("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n"),
		1024,
		2,
	); err == nil {
		t.Fatal("expected bootstrap event limit error")
	}
}

func TestFinishChatGPTWebSearchWrapsBootstrapProtocolFailure(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.finishChatGPTWebSearch(context.Background(), nil, nil, &chatGPTWebSearchExecution{
		response: &fhttp.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("data: {\"conversation_id\":\"unfinished\"")),
		},
	})
	if err == nil {
		t.Fatal("expected bootstrap protocol error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("status error = %v", err)
	}
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
}

func TestFinishChatGPTWebSearchClosesBootstrapBeforePolling(t *testing.T) {
	body := &chatGPTWebTrackedBody{Reader: strings.NewReader(
		"data: {\"conversation_id\":\"search-closed\"}\n\ndata: [DONE]\n\n",
	)}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if !body.closed.Load() {
			t.Error("search bootstrap body was still open when polling started")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"current_node": "answer",
			"mapping": map[string]any{
				"answer": map[string]any{"message": map[string]any{
					"author":  map[string]any{"role": "assistant"},
					"content": map[string]any{"parts": []any{"answer"}},
					"metadata": map[string]any{
						"finish_details": map[string]any{"type": "finished_successfully"},
						"is_complete":    true,
					},
				}},
			},
		})
	}))
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	executor.searchMaxPolls = 1
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	result, err := executor.finishChatGPTWebSearch(context.Background(), client, credential, &chatGPTWebSearchExecution{
		response: &fhttp.Response{StatusCode: http.StatusOK, Body: body},
		query:    "question",
	})
	if err != nil {
		t.Fatalf("finishChatGPTWebSearch() error = %v", err)
	}
	if result.Text != "answer" || result.Query != "question" || !result.Search {
		t.Fatalf("search result = %#v", result)
	}
}

func TestExtractChatGPTWebSearchResultRejectsInvalidConversationPayload(t *testing.T) {
	for _, payload := range []string{
		`not-json`,
		`{"detail":"unexpected HTML challenge replacement"}`,
		`{"mapping":[]}`,
	} {
		_, _, _, err := extractChatGPTWebSearchResult([]byte(payload))
		if err == nil {
			t.Fatalf("extractChatGPTWebSearchResult(%q) accepted invalid conversation payload", payload)
		}
	}

	if _, _, complete, err := extractChatGPTWebSearchResult([]byte(`{"mapping":{}}`)); err != nil || complete {
		t.Fatalf("empty in-progress conversation = (complete=%t, err=%v)", complete, err)
	}
}

func TestCollectChatGPTWebSearchSourcesOnlyUsesCitationContainers(t *testing.T) {
	sources := collectChatGPTWebSearchSources(map[string]any{
		"metadata": map[string]any{
			"citations": []any{
				map[string]any{"title": "Direct", "url": "https://example.com/direct?q=public"},
				map[string]any{"metadata": map[string]any{
					"title": "Nested",
					"url":   "https://example.com/nested",
				}},
				map[string]any{"title": "Signed", "url": "https://storage.example/file?token=secret"},
				map[string]any{"title": "API key", "url": "https://storage.example/file?api_key=secret"},
				map[string]any{"title": "Fragment token", "url": "https://storage.example/file#access_token=secret"},
				map[string]any{"title": "Fragment query token", "url": "https://storage.example/file#?access_token=secret"},
				map[string]any{"title": "Bare fragment token", "url": "https://storage.example/file#token"},
				map[string]any{"title": "Bare fragment access token", "url": "https://storage.example/file#access_token"},
				map[string]any{"title": "Refresh token", "url": "https://storage.example/file?refresh_token=secret"},
				map[string]any{"title": "ID token", "url": "https://storage.example/file?id_token=secret"},
				map[string]any{"title": "Client secret", "url": "https://storage.example/file#client_secret=secret"},
				map[string]any{"title": "AWS access key", "url": "https://storage.example/file?AWSAccessKeyId=secret"},
				map[string]any{"title": "Bare refresh token", "url": "https://storage.example/file#refresh_token"},
				map[string]any{"title": "Semicolon token", "url": "https://storage.example/file?ok=1;access_token=secret"},
				map[string]any{"title": "Fragment signed query", "url": "https://storage.example/file#section?x-amz-signature=secret"},
				map[string]any{"title": "Fragment colon token", "url": "https://storage.example/file#access_token:secret"},
				map[string]any{"title": "Fragment path token", "url": "https://storage.example/file#token/secret"},
				map[string]any{"title": "Encoded fragment token", "url": "https://storage.example/file#%61ccess_token%3Asecret"},
			},
			"content_references": []any{
				map[string]any{"items": []any{
					map[string]any{"title": "Grouped", "url": "https://example.com/grouped"},
				}},
			},
			"attachment": map[string]any{
				"url": "https://storage.example/attachment?sig=secret",
			},
		},
		"content": map[string]any{
			"parts": []any{
				map[string]any{
					"text": "https://example.com/plain-text",
					"asset": map[string]any{
						"url": "https://storage.example/asset?x-amz-signature=secret",
					},
				},
			},
		},
	})
	wantURLs := []string{
		"https://example.com/direct?q=public",
		"https://example.com/grouped",
		"https://example.com/nested",
	}
	if len(sources) != len(wantURLs) {
		t.Fatalf("sources = %#v, want %d explicit citation sources", sources, len(wantURLs))
	}
	for index, wantURL := range wantURLs {
		if sources[index].URL != wantURL {
			t.Fatalf("sources[%d].URL = %q, want %q; sources=%#v", index, sources[index].URL, wantURL, sources)
		}
	}
}

func TestChatGPTWebUsageInputDoesNotIncludeImageData(t *testing.T) {
	input := chatGPTWebUsageInput(helps.ChatGPTWebRequest{
		Messages: []helps.ChatGPTWebMessage{{
			Role: "user",
			Parts: []helps.ChatGPTWebContentPart{{
				Text:     "describe this",
				ImageURL: "data:image/png;base64,SECRET_IMAGE_BYTES",
			}},
		}},
	})
	if strings.Contains(input, "SECRET_IMAGE_BYTES") {
		t.Fatalf("usage input included image bytes: %q", input)
	}
	if !strings.Contains(input, "describe this") || !strings.Contains(input, "[image]") {
		t.Fatalf("usage input = %q, want text and image marker", input)
	}
}

func TestExtractChatGPTWebSearchResultUsesAllStatusFields(t *testing.T) {
	result, status, complete, err := extractChatGPTWebSearchResult([]byte(`{
		"mapping":{
			"answer":{"message":{
				"author":{"role":"assistant"},
				"create_time":1,
				"content":{"parts":["answer"]},
				"metadata":{
					"finish_details":{"type":"stop"},
					"status":"finished_successfully"
				}
			}}
		}
	}`))
	if err != nil {
		t.Fatalf("extractChatGPTWebSearchResult() error = %v", err)
	}
	if result.Text != "answer" || status != "stop" || complete {
		t.Fatalf("result = %#v, status = %q, complete = %t", result, status, complete)
	}

	_, status, _, err = extractChatGPTWebSearchResult([]byte(`{
		"mapping":{
			"answer":{"message":{
				"author":{"role":"assistant"},
				"create_time":1,
				"content":{"parts":["failed"]},
				"status":"finished_with_error",
				"metadata":{"finish_details":{"type":"stop"}}
			}}
		}
	}`))
	if err != nil {
		t.Fatalf("extractChatGPTWebSearchResult() failure error = %v", err)
	}
	if status != "finished_with_error" {
		t.Fatalf("failure status = %q", status)
	}
}

func TestExtractChatGPTWebSearchResultUsesCurrentBranch(t *testing.T) {
	result, status, complete, err := extractChatGPTWebSearchResult([]byte(`{
		"current_node":"active-tool",
		"mapping":{
			"active-answer":{"parent":"root","message":{
				"author":{"role":"assistant"},
				"create_time":1,
				"content":{"parts":["active answer"]},
				"metadata":{"finish_details":{"type":"finished_successfully"},"is_complete":true}
			}},
			"active-tool":{"parent":"active-answer","message":{"author":{"role":"tool"},"create_time":2}},
			"other-answer":{"parent":"root","message":{
				"author":{"role":"assistant"},
				"create_time":99,
				"content":{"parts":["wrong branch"]},
				"metadata":{"finish_details":{"type":"finished_successfully"},"is_complete":true}
			}}
		}
	}`))
	if err != nil {
		t.Fatalf("extractChatGPTWebSearchResult() error = %v", err)
	}
	if result.Text != "active answer" || status != "finished_successfully" || !complete {
		t.Fatalf("result = %#v, status = %q, complete = %t", result, status, complete)
	}
}

func TestExtractChatGPTWebSearchResultDoesNotReusePreviousTurnAssistant(t *testing.T) {
	payload := []byte(`{
		"current_node":"current-user",
		"mapping":{
			"previous-user":{"parent":"root","message":{"author":{"role":"user"},"content":{"parts":["old question"]}}},
			"previous-answer":{"parent":"previous-user","message":{
				"author":{"role":"assistant"},
				"content":{"parts":["old answer"]},
				"metadata":{"finish_details":{"type":"finished_successfully"},"is_complete":true}
			}},
			"current-user":{"parent":"previous-answer","message":{"author":{"role":"user"},"content":{"parts":["new question"]}}}
		}
	}`)
	result, status, complete, err := extractChatGPTWebSearchResult(payload, "current-user")
	if err != nil {
		t.Fatalf("extractChatGPTWebSearchResult() error = %v", err)
	}
	if result.Text != "" || status != "" || complete {
		t.Fatalf("current turn result = %#v, status = %q, complete = %t", result, status, complete)
	}
}

func TestChatGPTWebRequestLogURLRemovesSecrets(t *testing.T) {
	got := chatGPTWebRequestLogURL("https://user:pass@files.example/image.png?sig=secret&token=value#fragment")
	if got != "https://files.example/image.png" {
		t.Fatalf("log URL = %q", got)
	}
}

func TestChatGPTWebResponseLogBodyRedactsSignedURLs(t *testing.T) {
	body := []byte(`{
		"file_id":"file-1",
		"upload_url":"https://storage.example/upload?sig=secret",
		"prepare_token":"prepare-secret",
		"nested":{
			"download_url":"https://storage.example/download?token=secret",
			"token":"final-secret",
			"turnstile_token":"turnstile-secret",
			"so_token":"so-secret"
		}
	}`)
	logged := chatGPTWebResponseLogBody("/backend-api/files", body)
	for _, secret := range []string{"sig=secret", "token=secret", "prepare-secret", "final-secret", "turnstile-secret", "so-secret"} {
		if strings.Contains(string(logged), secret) {
			t.Fatalf("sanitized response leaked %q: %s", secret, logged)
		}
	}
	if strings.Contains(string(logged), "token=secret") {
		t.Fatalf("sanitized response leaked signed URL: %s", logged)
	}
	if got := gjson.GetBytes(logged, "file_id").String(); got != "file-1" {
		t.Fatalf("file_id = %q", got)
	}

	download := chatGPTWebResponseLogBody("/backend-api/files/file-1/download", []byte(`{"url":"https://storage.example/image?sig=secret"}`))
	if got := gjson.GetBytes(download, "url").String(); got != "<redacted-signed-url>" {
		t.Fatalf("download url = %q", got)
	}

	generic := chatGPTWebResponseLogBody("/backend-api/files", []byte(`{"message":"open https://storage.example/image?sig=secret","urls":["https://storage.example/other?token=secret"]}`))
	if strings.Contains(string(generic), "sig=secret") || strings.Contains(string(generic), "token=secret") {
		t.Fatalf("generic signed URL leaked: %s", generic)
	}

	rootURL := chatGPTWebResponseLogBody("/backend-api/files", []byte(`"https://storage.example/root?sig=secret"`))
	var redactedRoot string
	if err := json.Unmarshal(rootURL, &redactedRoot); err != nil {
		t.Fatalf("decode root signed URL: %v", err)
	}
	if redactedRoot != "<redacted-signed-url>" {
		t.Fatalf("root signed URL = %q", redactedRoot)
	}
}

func TestChatGPTWebResponseLogBodyRedactsTurnstileDXOnlyForPrepare(t *testing.T) {
	payload := []byte(`{
		"turnstile":{"required":true,"dx":"turnstile-secret","challenge":{"dx":"nested-turnstile-secret"}},
		"so":{"required":true,"collector_dx":"collector-secret","snapshot_dx":"snapshot-secret"},
		"other":{"dx":"keep-nested"},
		"dx":"keep-root"
	}`)

	logged := chatGPTWebResponseLogBody("/backend-api/sentinel/chat-requirements/prepare", payload)
	if got := gjson.GetBytes(logged, "turnstile.dx").String(); got != "<redacted>" {
		t.Fatalf("turnstile.dx = %q", got)
	}
	if got := gjson.GetBytes(logged, "turnstile.challenge.dx").String(); got != "<redacted>" {
		t.Fatalf("turnstile.challenge.dx = %q", got)
	}
	if got := gjson.GetBytes(logged, "so.collector_dx").String(); got != "<redacted>" {
		t.Fatalf("so.collector_dx = %q", got)
	}
	if got := gjson.GetBytes(logged, "so.snapshot_dx").String(); got != "<redacted>" {
		t.Fatalf("so.snapshot_dx = %q", got)
	}
	if got := gjson.GetBytes(logged, "other.dx").String(); got != "keep-nested" {
		t.Fatalf("other.dx = %q", got)
	}
	if got := gjson.GetBytes(logged, "dx").String(); got != "keep-root" {
		t.Fatalf("root dx = %q", got)
	}

	nonPrepare := chatGPTWebResponseLogBody("/backend-api/test", payload)
	if got := gjson.GetBytes(nonPrepare, "turnstile.dx").String(); got != "turnstile-secret" {
		t.Fatalf("non-prepare turnstile.dx = %q", got)
	}
	if got := gjson.GetBytes(nonPrepare, "so.collector_dx").String(); got != "<redacted>" {
		t.Fatalf("non-prepare so.collector_dx = %q", got)
	}
	if got := gjson.GetBytes(nonPrepare, "so.snapshot_dx").String(); got != "<redacted>" {
		t.Fatalf("non-prepare so.snapshot_dx = %q", got)
	}
}

func TestChatGPTWebResponseLogBodyRedactsNonJSON(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte("access_token=secret"),
		[]byte(`{"token":"truncated"`),
	} {
		if got := string(chatGPTWebResponseLogBody("/backend-api/test", payload)); got != "<redacted-non-json-response-body>" {
			t.Fatalf("response log body = %q", got)
		}
		if got := string(chatGPTWebStatusErrorBody("/backend-api/test", payload)); got != "<redacted-non-json-response-body>" {
			t.Fatalf("status error body = %q", got)
		}
	}
}

func TestChatGPTWebResponseLogHeadersRedactsSecrets(t *testing.T) {
	headers := fhttp.Header{
		"Set-Cookie": {"__Secure-next-auth.session-token=secret"},
		"Location":   {"https://storage.example/file?sig=secret"},
		"X-Test":     {"kept"},
	}
	logged := chatGPTWebResponseLogHeaders(headers)
	if logged.Get("Set-Cookie") != "" {
		t.Fatalf("Set-Cookie leaked: %v", logged)
	}
	if got := logged.Get("Location"); got != "<redacted-location>" {
		t.Fatalf("Location = %q", got)
	}
	if got := logged.Get("X-Test"); got != "kept" {
		t.Fatalf("X-Test = %q", got)
	}
}

func TestCloneChatGPTWebHeadersDoesNotExposeUpstreamCookies(t *testing.T) {
	headers := fhttp.Header{
		"set-cookie":  {"__Secure-next-auth.session-token=secret"},
		"SET-COOKIE2": {"legacy-session=secret"},
		"X-Test":      {"kept"},
	}
	cloned := cloneChatGPTWebHeaders(headers)
	if cloned.Get("Set-Cookie") != "" || cloned.Get("Set-Cookie2") != "" {
		t.Fatalf("upstream cookies leaked: %v", cloned)
	}
	if got := cloned.Get("X-Test"); got != "kept" {
		t.Fatalf("X-Test = %q", got)
	}
}

func TestChatGPTWebRequestLogHeadersRedactsStableIdentity(t *testing.T) {
	logged := chatGPTWebRequestLogHeaders(map[string]string{
		"authorization":  "Bearer secret",
		"oai-device-id":  "stable-device",
		"oai-session-id": "stable-session",
		"OpenAI-Sentinel-Chat-Requirements-Token": "requirements-secret",
		"openai-sentinel-proof-token":             "proof-secret",
		"openai-sentinel-turnstile-token":         "turnstile-secret",
		"openai-sentinel-so-token":                "so-secret",
		"x-conduit-token":                         "conduit-secret",
		"x-test":                                  "kept",
	})
	for _, key := range []string{
		"Authorization",
		"Oai-Device-Id",
		"Oai-Session-Id",
		"Openai-Sentinel-Chat-Requirements-Token",
		"Openai-Sentinel-Proof-Token",
		"Openai-Sentinel-Turnstile-Token",
		"Openai-Sentinel-So-Token",
		"X-Conduit-Token",
	} {
		if got := logged.Get(key); got != "<redacted>" {
			t.Fatalf("%s = %q", key, got)
		}
	}
	if got := logged.Get("X-Test"); got != "kept" {
		t.Fatalf("X-Test = %q", got)
	}
}

func TestChatGPTWebRequestLogURLRedactsMalformedSignedURL(t *testing.T) {
	rawURL := "https://storage.example/%zz?sig=secret"
	if got := chatGPTWebRequestLogURL(rawURL); got != "<redacted-invalid-url>" {
		t.Fatalf("chatGPTWebRequestLogURL() = %q", got)
	}
}

func TestChatGPTWebRequirementsDoesNotFollowCrossOriginRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		targetCalls.Add(1)
		if request.Header.Get("Oai-Device-Id") != "" || request.Header.Get("Oai-Session-Id") != "" {
			t.Errorf("bootstrap leaked stable identity headers")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL+"/capture", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
	if err == nil {
		t.Fatal("expected redirect status error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTemporaryRedirect {
		t.Fatalf("redirect error = %v", err)
	}
	if calls := targetCalls.Load(); calls != 0 {
		t.Fatalf("target calls = %d", calls)
	}
}

func TestChatGPTWebRequirementsLocalFailuresDoNotRetryOrCoolCredential(t *testing.T) {
	tests := []struct {
		name     string
		prepare  string
		finalize string
	}{
		{name: "malformed prepare", prepare: `{"prepare_token":`},
		{name: "unsupported challenge", prepare: `{"prepare_token":"prepare","turnstile":{"required":true}}`},
		{name: "missing finalized token", prepare: `{"prepare_token":"prepare"}`, finalize: `{}`},
		{name: "non-string finalized token", prepare: `{"prepare_token":"prepare"}`, finalize: `{"token":false}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/":
					_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
				case "/backend-api/sentinel/chat-requirements/prepare":
					_, _ = io.WriteString(w, tt.prepare)
				case "/backend-api/sentinel/chat-requirements/finalize":
					_, _ = io.WriteString(w, tt.finalize)
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()

			executor := NewChatGPTWebExecutor(nil, nil)
			executor.runtimeBaseURL = server.URL
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
			if err == nil {
				t.Fatal("expected requirements error")
			}
			assertChatGPTWebNonAuthNonRetryError(t, err)
		})
	}
}

func TestChatGPTWebRequirementsSolvesTurnstileAndForwardsToken(t *testing.T) {
	for _, test := range []struct {
		name                  string
		finalizedToken        any
		wantRequirementsToken string
	}{
		{name: "string override", finalizedToken: "finalized-turnstile-token", wantRequirementsToken: "finalized-turnstile-token"},
		{name: "non-string falls back", finalizedToken: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			var finalized map[string]any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/":
					_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
				case "/backend-api/sentinel/chat-requirements/prepare":
					var body map[string]any
					if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
						t.Errorf("decode prepare body: %v", err)
						http.Error(w, "invalid prepare", http.StatusBadRequest)
						return
					}
					pToken := chatGPTWebAnyString(body["p"])
					dx := chatGPTWebTurnstileTestDX(t, pToken, []any{
						[]any{2, 40, "turnstile-token"},
						[]any{7, 3, 40},
					})
					_ = json.NewEncoder(w).Encode(map[string]any{
						"prepare_token": "prepare",
						"turnstile":     map[string]any{"required": true, "dx": dx},
					})
				case "/backend-api/sentinel/chat-requirements/finalize":
					if err := json.NewDecoder(request.Body).Decode(&finalized); err != nil {
						t.Errorf("decode finalize body: %v", err)
						http.Error(w, "invalid finalize", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{
						"token":           "requirements",
						"turnstile_token": test.finalizedToken,
					})
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()

			executor := NewChatGPTWebExecutor(nil, nil)
			executor.runtimeBaseURL = server.URL
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			requirements, err := executor.chatGPTWebRequirements(context.Background(), client, credential)
			if err != nil {
				t.Fatalf("chatGPTWebRequirements() error = %v", err)
			}
			generatedToken := base64.StdEncoding.EncodeToString([]byte("turnstile-token"))
			if got := chatGPTWebAnyString(finalized["turnstile_token"]); got != generatedToken {
				t.Fatalf("finalize turnstile token = %q, want %q", got, generatedToken)
			}
			wantRequirementsToken := test.wantRequirementsToken
			if wantRequirementsToken == "" {
				wantRequirementsToken = generatedToken
			}
			if requirements.TurnstileToken != wantRequirementsToken {
				t.Fatalf("requirements turnstile token = %q, want %q", requirements.TurnstileToken, wantRequirementsToken)
			}
			headers := chatGPTWebRequirementsHeaders(map[string]string{}, requirements)
			if got := headers["openai-sentinel-turnstile-token"]; got != wantRequirementsToken {
				t.Fatalf("conversation Turnstile header = %q, want %q", got, wantRequirementsToken)
			}
		})
	}
}

func TestChatGPTWebRequirementsTurnstileFailureIsExplicit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"prepare_token": "prepare",
				"turnstile":     map[string]any{"required": true, "dx": "invalid"},
			})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
	if err == nil {
		t.Fatal("chatGPTWebRequirements() error = nil")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("Turnstile error status = %v, want 502", err)
	}
	if !strings.Contains(err.Error(), `"code":"turnstile_required"`) || strings.Contains(err.Error(), "insufficient_quota") {
		t.Fatalf("Turnstile error = %v", err)
	}
}

func TestChatGPTWebRequirementsRejectedTurnstileDoesNotCoolCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/sentinel/20260219f9f6/sdk.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode prepare body: %v", err)
				return
			}
			pToken := chatGPTWebAnyString(body["p"])
			_ = json.NewEncoder(w).Encode(map[string]any{
				"prepare_token": "prepare",
				"turnstile": map[string]any{
					"required": true,
					"dx": chatGPTWebTurnstileTestDX(t, pToken, []any{
						[]any{2, 40, "turnstile-token"},
						[]any{7, 3, 40},
					}),
				},
			})
		case "/backend-api/sentinel/chat-requirements/finalize":
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"turnstile_verification","message":"challenge rejected"}}`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
	if err == nil {
		t.Fatal("chatGPTWebRequirements() error = nil")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("Turnstile rejection status = %v, want 502", err)
	}
	if !strings.Contains(err.Error(), `"code":"turnstile_required"`) || strings.Contains(err.Error(), "insufficient_quota") {
		t.Fatalf("Turnstile rejection error = %v", err)
	}
}

func TestChatGPTWebRequirementsRejectedSessionObserverDoesNotCoolCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/sentinel/20260219f9f6/sdk.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"prepare_token": "prepare",
				"token":         "challenge-token",
				"so": map[string]any{
					"required":     true,
					"collector_dx": "collector",
					"snapshot_dx":  "snapshot",
				},
			})
		case "/backend-api/sentinel/chat-requirements/finalize":
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_sentinel_so_token","message":"session observer token rejected"}}`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	defer executor.Close()
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
	if err == nil {
		t.Fatal("chatGPTWebRequirements() error = nil")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("Session Observer rejection status = %v, want 502", err)
	}
	if !strings.Contains(err.Error(), `"code":"sentinel_session_observer_unavailable"`) || strings.Contains(err.Error(), "insufficient_quota") {
		t.Fatalf("Session Observer rejection error = %v", err)
	}
}

func TestChatGPTWebRequirementsNonJSONTurnstileRejectionDoesNotCoolCredential(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/sentinel/20260219f9f6/sdk.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode prepare body: %v", err)
				return
			}
			pToken := chatGPTWebAnyString(body["p"])
			_ = json.NewEncoder(w).Encode(map[string]any{
				"prepare_token": "prepare",
				"turnstile": map[string]any{
					"required": true,
					"dx": chatGPTWebTurnstileTestDX(t, pToken, []any{
						[]any{2, 40, "turnstile-token"},
						[]any{7, 3, 40},
					}),
				},
			})
		case "/backend-api/sentinel/chat-requirements/finalize":
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `<html>Turnstile challenge rejected: sig=secret</html>`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.chatGPTWebRequirements(context.Background(), client, credential)
	if err == nil {
		t.Fatal("chatGPTWebRequirements() error = nil")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("Turnstile rejection status = %v, want 502", err)
	}
	if !strings.Contains(err.Error(), `"code":"turnstile_required"`) || strings.Contains(err.Error(), "sig=secret") {
		t.Fatalf("Turnstile rejection error = %v", err)
	}
}

func TestChatGPTWebTurnstileFinalizeRejectionPreservesUnrelatedForbidden(t *testing.T) {
	err := newChatGPTWebStatusError(
		http.StatusForbidden,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`{"error":{"code":"account_deactivated"}}`),
		nil,
	)
	if chatGPTWebTurnstileFinalizeRejection(err) {
		t.Fatal("unrelated 403 was classified as a Turnstile rejection")
	}
}

func TestChatGPTWebSessionObserverFinalizeRejectionSkipsCredentialState(t *testing.T) {
	err := newChatGPTWebStatusError(
		http.StatusForbidden,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`{"error":{"code":"invalid_sentinel_so_token","message":"session observer token rejected"}}`),
		nil,
	)
	if !chatGPTWebSentinelFinalizeRejection(err) || chatGPTWebTurnstileFinalizeRejection(err) {
		t.Fatalf("Session Observer finalize classification = %#v", err)
	}
	if !err.SkipAuthResult() || err.RetryOtherAuth() {
		t.Fatalf("Session Observer finalize credential behavior = %#v", err)
	}
}

func TestChatGPTWebTurnstileFinalizeRejectionRequiresMarkerForBadRequest(t *testing.T) {
	unrelated := newChatGPTWebStatusError(
		http.StatusBadRequest,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`{"error":{"code":"invalid_proof_token"}}`),
		nil,
	)
	if chatGPTWebTurnstileFinalizeRejection(unrelated) {
		t.Fatal("unrelated 400 was classified as a Turnstile rejection")
	}
	challenge := newChatGPTWebStatusError(
		http.StatusBadRequest,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`{"error":{"code":"turnstile_verification"}}`),
		nil,
	)
	if !chatGPTWebTurnstileFinalizeRejection(challenge) {
		t.Fatal("Turnstile 400 was not classified as a Turnstile rejection")
	}
	genericChallenge := newChatGPTWebStatusError(
		http.StatusBadRequest,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`{"error":{"code":"invalid_proof_token","message":"challenge rejected"}}`),
		nil,
	)
	if chatGPTWebTurnstileFinalizeRejection(genericChallenge) {
		t.Fatal("generic challenge rejection was classified as a Turnstile rejection")
	}
}

func TestChatGPTWebTurnstileFinalizeRejectionClassifiesRedactedNonJSONBody(t *testing.T) {
	marked := newChatGPTWebStatusError(
		http.StatusForbidden,
		"/backend-api/sentinel/chat-requirements/finalize",
		[]byte(`<html>turnstile challenge rejected</html>`),
		nil,
	)
	if !chatGPTWebTurnstileFinalizeRejection(marked) {
		t.Fatal("marked non-JSON finalize rejection was not classified as a Turnstile rejection")
	}
	if strings.Contains(marked.Error(), "turnstile challenge rejected") {
		t.Fatalf("non-JSON finalize body leaked through error: %v", marked)
	}
	for name, payload := range map[string][]byte{
		"unmarked_html": []byte(`<html>challenge rejected</html>`),
		"empty":         nil,
	} {
		t.Run(name, func(t *testing.T) {
			err := newChatGPTWebStatusError(
				http.StatusForbidden,
				"/backend-api/sentinel/chat-requirements/finalize",
				payload,
				nil,
			)
			if chatGPTWebTurnstileFinalizeRejection(err) {
				t.Fatalf("unmarked finalize body was classified as Turnstile: %v", err)
			}
		})
	}
}

func TestChatGPTWebConversationSentinelRejectionSkipsCredentialState(t *testing.T) {
	for _, path := range []string{"/backend-api/conversation", "/backend-api/f/conversation"} {
		err := newChatGPTWebStatusError(
			http.StatusForbidden,
			path,
			[]byte(`{"error":{"code":"invalid_sentinel_so_token","message":"session observer token rejected"}}`),
			nil,
		)
		if !err.SkipAuthResult() || err.RetryOtherAuth() {
			t.Fatalf("%s classified error = %#v", path, err)
		}
	}
}

func TestChatGPTWebConversationOrdinaryForbiddenStillMutatesCredentialState(t *testing.T) {
	err := newChatGPTWebStatusError(
		http.StatusForbidden,
		"/backend-api/conversation",
		[]byte(`{"error":{"code":"account_restricted","message":"access denied"}}`),
		nil,
	)
	if err.SkipAuthResult() || err.RetryOtherAuth() {
		t.Fatalf("ordinary forbidden classified error = %#v", err)
	}
}

func chatGPTWebTurnstileTestDX(t *testing.T, requirementsToken string, program []any) string {
	t.Helper()
	payload, err := json.Marshal(program)
	if err != nil {
		t.Fatalf("encode Turnstile program: %v", err)
	}
	key := []byte(requirementsToken)
	for index := range payload {
		payload[index] ^= key[index%len(key)]
	}
	return base64.StdEncoding.EncodeToString(payload)
}

func TestChatGPTWebBootstrapRedirectLogsEachHop(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" {
			http.Redirect(w, request, "/final", http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	response, err := executor.doChatGPTWebBootstrapRequest(ctx, client, credential, server.URL+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	rawRequest, ok := ginCtx.Get("API_REQUEST")
	if !ok {
		t.Fatal("request log missing")
	}
	requestLog, _ := rawRequest.([]byte)
	if got := strings.Count(string(requestLog), "HTTP Method: GET"); got != 2 {
		t.Fatalf("request attempts = %d, log=%s", got, requestLog)
	}
	rawResponse, ok := ginCtx.Get("API_RESPONSE")
	if !ok {
		t.Fatal("response log missing")
	}
	responseLog, _ := rawResponse.([]byte)
	if !bytes.Contains(responseLog, []byte("Status: 307")) || !bytes.Contains(responseLog, []byte("Status: 200")) {
		t.Fatalf("response log = %s", responseLog)
	}
}

func TestChatGPTWebRuntimeJSONDoesNotFollowRedirects(t *testing.T) {
	var targetHits int
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		targetHits++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		http.Redirect(w, request, target.URL+"/captured", http.StatusTemporaryRedirect)
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, _, err = executor.doChatGPTWebJSON(context.Background(), client, credential, "/backend-api/test", map[string]any{"secret": "value"})
	if err == nil {
		t.Fatal("expected redirect response error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTemporaryRedirect {
		t.Fatalf("redirect error = %v", err)
	}
	if targetHits != 0 {
		t.Fatalf("redirect target hits = %d", targetHits)
	}
}

func TestChatGPTWebStreamErrorBodyIsBounded(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, strings.Repeat("x", chatGPTWebMaxErrorBodyBytes+1))
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	headers := executor.chatGPTWebHeaders(credential, "/backend-api/test", map[string]string{
		"accept":       "text/event-stream",
		"content-type": "application/json",
	})
	_, err = executor.doChatGPTWebJSONStream(context.Background(), client, credential, "/backend-api/test", headers, map[string]any{"input": "hello"})
	if err == nil || err.Error() != "<upstream-error-body-truncated>" {
		t.Fatalf("stream error = %v", err)
	}
}

func TestChatGPTWebConversationTransportErrorAfterWriteIsNotReplayable(t *testing.T) {
	requestReceived := make(chan struct{})
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		close(requestReceived)
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("test server does not support hijacking")
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack connection: %v", err)
		}
		_ = connection.Close()
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	headers := executor.chatGPTWebHeaders(credential, "/backend-api/conversation", map[string]string{
		"accept":       "text/event-stream",
		"content-type": "application/json",
	})
	_, err = executor.doChatGPTWebJSONStream(context.Background(), client, credential, "/backend-api/conversation", headers, map[string]any{"input": "hello"})
	if err == nil {
		t.Fatal("expected transport error")
	}
	select {
	case <-requestReceived:
	default:
		t.Fatal("server did not receive committed request")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestChatGPTWebBufferedErrorBodiesAreBounded(t *testing.T) {
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, strings.Repeat("x", chatGPTWebMaxErrorBodyBytes+1))
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, _, err = executor.doChatGPTWebJSON(context.Background(), client, credential, "/backend-api/json", map[string]any{"input": "hello"})
	if err == nil || err.Error() != "<upstream-error-body-truncated>" {
		t.Fatalf("JSON error = %v", err)
	}
	_, _, err = executor.doChatGPTWebGET(context.Background(), client, credential, "/backend-api/get", nil)
	if err == nil || err.Error() != "<upstream-error-body-truncated>" {
		t.Fatalf("GET error = %v", err)
	}
}

func TestChatGPTWebGETWithoutPollBudget(t *testing.T) {
	const responseBody = `{"ok":true}`
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, responseBody)
	}))
	defer origin.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = origin.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, payload, err := executor.doChatGPTWebGET(context.Background(), client, credential, "/backend-api/get", nil)
	if err != nil {
		t.Fatalf("GET error = %v", err)
	}
	if string(payload) != responseBody {
		t.Fatalf("GET payload = %q, want %q", payload, responseBody)
	}
}

func TestChatGPTWebSuccessfulResponseBodyIsBounded(t *testing.T) {
	response := &fhttp.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("12345")),
	}
	_, err := readChatGPTWebResponseBody(response, 4)
	if err == nil {
		t.Fatal("expected successful response body limit error")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestChatGPTWebPollResponsesUsePerResponseBudgetAndOmitSnapshotsFromLogs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const responseBody = `{"marker":"secret-poll-snapshot"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = io.WriteString(w, responseBody)
	}))
	defer server.Close()

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	budget := newChatGPTWebPollResponseLimit(len(responseBody))
	_, payload, err := executor.doChatGPTWebPollGET(ctx, client, credential, "/backend-api/conversation/test", nil, budget)
	if err != nil {
		t.Fatalf("first poll error = %v", err)
	}
	if string(payload) != responseBody {
		t.Fatalf("first poll payload = %q", payload)
	}
	_, payload, err = executor.doChatGPTWebPollGET(ctx, client, credential, "/backend-api/conversation/test", nil, budget)
	if err != nil {
		t.Fatalf("second poll error = %v", err)
	}
	if string(payload) != responseBody {
		t.Fatalf("second poll payload = %q", payload)
	}

	_, _, err = executor.doChatGPTWebPollGET(
		ctx,
		client,
		credential,
		"/backend-api/conversation/test",
		nil,
		newChatGPTWebPollResponseLimit(len(responseBody)-1),
	)
	var limitErr *helps.ChatGPTWebResponseLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("oversized poll error = %v, want response limit error", err)
	}
	if _, retryable := chatGPTWebPollRetryDelay(err, 0); retryable {
		t.Fatal("response limit error remained poll-retryable")
	}

	rawResponse, ok := ginCtx.Get("API_RESPONSE")
	if !ok {
		t.Fatal("response log missing")
	}
	responseLog, _ := rawResponse.([]byte)
	if bytes.Contains(responseLog, []byte("secret-poll-snapshot")) {
		t.Fatalf("poll response log retained full snapshot: %s", responseLog)
	}
	if !bytes.Contains(responseLog, []byte("polling response body omitted")) {
		t.Fatalf("poll response omission marker missing: %s", responseLog)
	}
}

func TestChatGPTWebPollResponseBudgetLimitsCumulativeBytes(t *testing.T) {
	budget := newChatGPTWebPollResponseBudget(5)
	if err := budget.consume(3); err != nil {
		t.Fatalf("first response: %v", err)
	}
	if err := budget.consume(2); err != nil {
		t.Fatalf("second response: %v", err)
	}
	err := budget.consume(1)
	var limitErr *helps.ChatGPTWebResponseLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("cumulative response error = %v, want response limit error", err)
	}
}

func TestChatGPTWebPollResponseBudgetIsConcurrentSafe(t *testing.T) {
	const (
		limit    = 64
		attempts = 128
	)
	budget := newChatGPTWebPollResponseBudget(limit)
	start := make(chan struct{})
	var succeeded atomic.Int32
	var limited atomic.Int32
	var unexpected atomic.Int32
	var workers sync.WaitGroup
	workers.Add(attempts)
	for range attempts {
		go func() {
			defer workers.Done()
			<-start
			err := budget.consume(1)
			if err == nil {
				succeeded.Add(1)
				return
			}
			var limitErr *helps.ChatGPTWebResponseLimitError
			if errors.As(err, &limitErr) {
				limited.Add(1)
				return
			}
			unexpected.Add(1)
		}()
	}
	close(start)
	workers.Wait()
	if got := succeeded.Load(); got != limit {
		t.Fatalf("successful consumes = %d, want %d", got, limit)
	}
	if got := limited.Load(); got != attempts-limit {
		t.Fatalf("limited consumes = %d, want %d", got, attempts-limit)
	}
	if got := unexpected.Load(); got != 0 {
		t.Fatalf("unexpected consume errors = %d", got)
	}
}

func TestChatGPTWebTaskAndConversationPollsShareResponseBudget(t *testing.T) {
	const taskBody = `{"tasks":[]}`
	const conversationBody = `{"mapping":{}}`
	started := make(chan struct{})
	var requestCount atomic.Int32
	var release sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if requestCount.Add(1) == 2 {
			release.Do(func() { close(started) })
		}
		select {
		case <-started:
		case <-request.Context().Done():
			return
		}
		switch request.URL.Path {
		case "/backend-api/tasks":
			_, _ = io.WriteString(w, taskBody)
		case "/backend-api/conversation/shared-budget":
			_, _ = io.WriteString(w, conversationBody)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	budget := newChatGPTWebPollResponseBudget(max(len(taskBody), len(conversationBody)))
	taskResult := executor.startChatGPTWebImageTaskPoll(context.Background(), client, credential, "shared-budget", budget)
	conversationResult := executor.startChatGPTWebImageConversationPoll(context.Background(), client, credential, "shared-budget", budget)
	results := []error{(<-taskResult).err, (<-conversationResult).err}
	limitedCount := 0
	for _, resultErr := range results {
		var limitErr *helps.ChatGPTWebResponseLimitError
		if errors.As(resultErr, &limitErr) {
			limitedCount++
		}
	}
	if limitedCount != 1 {
		t.Fatalf("response limit errors = %d, results = %v", limitedCount, results)
	}
}

func TestChatGPTWebConcurrentImagePollsDoNotMutatePrimaryRequestLog(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_, _ = io.WriteString(w, `{"tasks":[]}`)
		case "/backend-api/conversation/log-isolation":
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Set("API_REQUEST", []byte("primary-request"))
	ginCtx.Set("API_RESPONSE", []byte("primary-response"))
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	executor := NewChatGPTWebExecutor(&config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "log-isolation"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	requestLog, _ := ginCtx.Get("API_REQUEST")
	responseLog, _ := ginCtx.Get("API_RESPONSE")
	if got, _ := requestLog.([]byte); !bytes.Equal(got, []byte("primary-request")) {
		t.Fatalf("primary request log mutated: %q", got)
	}
	if got, _ := responseLog.([]byte); !bytes.Equal(got, []byte("primary-response")) {
		t.Fatalf("primary response log mutated: %q", got)
	}
}

func TestPollChatGPTWebSearchDefaultHonorsContextBeforePollLimit(t *testing.T) {
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if calls.Add(1) == 3 {
			cancel()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	if executor.searchMaxPolls != chatGPTWebSearchMaxPollAttempts {
		t.Fatalf("default search max polls = %d, want %d", executor.searchMaxPolls, chatGPTWebSearchMaxPollAttempts)
	}
	if executor.searchMaxPolls > 240 || chatGPTWebSearchPollMaxBytes > 64<<20 {
		t.Fatalf("default search polling budget is too broad: polls=%d bytes=%d", executor.searchMaxPolls, chatGPTWebSearchPollMaxBytes)
	}
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.pollChatGPTWebSearch(ctx, client, credential, "pending-search")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pollChatGPTWebSearch() error = %v, want context canceled", err)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("poll calls = %d, want 3", got)
	}
}

func TestConsumeChatGPTWebImageStreamLimitsResponseSizeAndEvents(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		maxBytes  int
		maxEvents int
	}{
		{
			name:      "response bytes",
			body:      "data: {}\n\n",
			maxBytes:  4,
			maxEvents: 10,
		},
		{
			name:      "event count",
			body:      "data: {}\n\ndata: {}\n\n",
			maxBytes:  1024,
			maxEvents: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := consumeChatGPTWebImageStreamWithLimits(
				context.Background(),
				strings.NewReader(test.body),
				&helps.ChatGPTWebImageAccumulator{},
				test.maxBytes,
				test.maxEvents,
			)
			var limitErr *helps.ChatGPTWebResponseLimitError
			if !errors.As(err, &limitErr) {
				t.Fatalf("stream error = %v, want response limit error", err)
			}
		})
	}
}

func TestWaitForChatGPTWebImageIdleHasFixedDeadlineDuringProgress(t *testing.T) {
	progress := make(chan struct{}, 1)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		ticker := time.NewTicker(2 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				select {
				case progress <- struct{}{}:
				default:
				}
			}
		}
	}()
	startedAt := time.Now()
	if err := waitForChatGPTWebImageIdle(context.Background(), 30*time.Millisecond, progress); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(startedAt); elapsed < 20*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Fatalf("fixed image watcher delay = %v", elapsed)
	}
}

func TestConsumeChatGPTWebImageStreamStopsAtStructuredTerminalEvent(t *testing.T) {
	body := strings.NewReader("data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[]}}}\n\ndata: [DONE]\n\n")
	accumulator := &helps.ChatGPTWebImageAccumulator{}
	if err := consumeChatGPTWebImageStream(context.Background(), body, accumulator); err != nil {
		t.Fatalf("consumeChatGPTWebImageStream() error = %v", err)
	}
	if !accumulator.Terminal {
		t.Fatal("structured terminal event was not retained")
	}
}

func TestConsumeChatGPTWebImageStreamRequiresTransportTerminalAfterMessageTerminal(t *testing.T) {
	body := strings.NewReader("data: {\"conversation_id\":\"incomplete-image\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n")
	accumulator := &helps.ChatGPTWebImageAccumulator{}
	err := consumeChatGPTWebImageStream(context.Background(), body, accumulator)
	if !errors.Is(err, errChatGPTWebImageIncompleteStream) {
		t.Fatalf("consumeChatGPTWebImageStream() error = %v", err)
	}
	if !accumulator.Terminal || accumulator.StreamTerminal || accumulator.ConversationID != "incomplete-image" || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("incomplete stream accumulator = %+v", accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamDoesNotPollHealthyCompletedStream(t *testing.T) {
	var auxiliaryPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		auxiliaryPolls.Add(1)
		http.Error(w, "unexpected auxiliary poll", http.StatusInternalServerError)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 50 * time.Millisecond
	body := io.NopCloser(strings.NewReader(
		"data: {\"conversation_id\":\"healthy-complete\"}\n\n" +
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n" +
			"data: [DONE]\n\n",
	))
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(context.Background(), nil, nil, &fhttp.Response{Body: body})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("healthy stream accumulator = %+v", accumulator)
	}
	if got := auxiliaryPolls.Load(); got != 0 {
		t.Fatalf("auxiliary polls = %d, want 0", got)
	}
}

func TestConsumeChatGPTWebImageStreamKeepsPrimaryStreamAfterAuxiliaryAuthErrors(t *testing.T) {
	polled := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks", "/backend-api/conversation/auxiliary-auth-error":
			select {
			case polled <- struct{}{}:
			default:
			}
			http.Error(w, "auxiliary endpoint unavailable", http.StatusUnauthorized)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageMaxPolls = 1
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		defer func() { _ = streamWriter.Close() }()
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"auxiliary-auth-error\"}\n\n")
		for count := 0; count < 2; count++ {
			select {
			case <-polled:
			case <-time.After(time.Second):
				return
			}
		}
		_, _ = io.WriteString(streamWriter,
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("primary stream result = %+v", accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamKeepsPrimaryStreamAfterAuxiliaryPollBudgetLimit(t *testing.T) {
	for _, test := range []struct {
		name        string
		limitedPath string
	}{
		{name: "tasks", limitedPath: "/backend-api/tasks"},
		{name: "conversation", limitedPath: "/backend-api/conversation/auxiliary-budget-limit"},
	} {
		t.Run(test.name, func(t *testing.T) {
			polled := make(chan struct{})
			var polledOnce sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case test.limitedPath:
					_, _ = io.WriteString(w, strings.Repeat("x", 64))
					polledOnce.Do(func() { close(polled) })
				case "/backend-api/tasks", "/backend-api/conversation/auxiliary-budget-limit":
					<-request.Context().Done()
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()

			executor := NewChatGPTWebExecutor(nil, nil)
			executor.runtimeBaseURL = server.URL
			executor.imageInitialWait = 0
			executor.imagePollInterval = time.Millisecond
			executor.imageMaxPolls = 1
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			streamReader, streamWriter := io.Pipe()
			go func() {
				defer func() { _ = streamWriter.Close() }()
				_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"auxiliary-budget-limit\"}\n\n")
				select {
				case <-polled:
				case <-time.After(time.Second):
					return
				}
				time.Sleep(25 * time.Millisecond)
				_, _ = io.WriteString(streamWriter,
					"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n"+
						"data: [DONE]\n\n",
				)
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(
				ctx,
				client,
				credential,
				&fhttp.Response{Body: streamReader},
				newChatGPTWebPollResponseBudget(8),
			)
			if err != nil {
				t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
			}
			if !accumulator.Terminal || !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
				t.Fatalf("primary stream result = %+v", accumulator)
			}
		})
	}
}

func TestConsumeChatGPTWebImageStreamPreservesProtocolErrorAfterWatcherResult(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/tasks" {
			http.NotFound(w, request)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
			"conversation_id": "watcher-error",
			"status":          "completed",
			"image_gen_message": map[string]any{
				"author":   map[string]any{"role": "tool"},
				"status":   "finished_successfully",
				"metadata": map[string]any{"async_task_type": "image_gen"},
				"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
			},
		}}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	body := newChatGPTWebWatcherProtocolErrorBody()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	if err == nil || !strings.Contains(err.Error(), "image policy rejected") {
		t.Fatalf("stream error = %v", err)
	}
}

func TestConsumeChatGPTWebImageStreamClearsDisappearedPendingTask(t *testing.T) {
	var taskPolls atomic.Int32
	tasksSettled := make(chan struct{})
	var settledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			if taskPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
					"conversation_id": "disappeared-task",
					"status":          "running",
				}}})
				return
			}
			settledOnce.Do(func() { close(tasksSettled) })
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/disappeared-task":
			select {
			case <-tasksSettled:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"disappeared-task\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() < 2 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamUsesCompletedTaskWhileStreamRemainsOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/tasks" {
			http.NotFound(w, request)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
			"conversation_id": "open-stream-image",
			"status":          "completed",
			"image_gen_message": map[string]any{
				"author":   map[string]any{"role": "tool"},
				"status":   "finished_successfully",
				"metadata": map[string]any{"async_task_type": "image_gen"},
				"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
			},
		}}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"open-stream-image\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestConsumeChatGPTWebImageStreamRetriesConversationProtocolError(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/retry-protocol-error":
			if conversationPolls.Add(1) == 1 {
				_, _ = io.WriteString(w, `{broken`)
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	body := newChatGPTWebBlockingBody("data: {\"conversation_id\":\"retry-protocol-error\"}\n\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || conversationPolls.Load() < 2 {
		t.Fatalf("files = %v, conversation polls = %d", accumulator.FileIDs, conversationPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamKeepsPrimaryStreamAfterExhaustedConversationProtocolError(t *testing.T) {
	conversationPolled := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/exhausted-protocol-error":
			conversationPolled <- struct{}{}
			_, _ = io.WriteString(w, `{broken`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageMaxPolls = 2
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		defer func() { _ = streamWriter.Close() }()
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"exhausted-protocol-error\"}\n\n")
		for count := 0; count < 2; count++ {
			select {
			case <-conversationPolled:
			case <-time.After(time.Second):
				return
			}
		}
		_, _ = io.WriteString(streamWriter,
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("primary stream result = %+v", accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamKeepsPrimaryStreamAfterPartialAuxiliaryProtocolError(t *testing.T) {
	conversationPolled := make(chan struct{}, 3)
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/partial-protocol-error":
			conversationPolled <- struct{}{}
			if conversationPolls.Add(1) == 1 {
				_, _ = io.WriteString(w, `{"current_node":"partial","mapping":{"partial":{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://partial"}]}}}}}`)
				return
			}
			_, _ = io.WriteString(w, `{broken`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageMaxPolls = 3
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		defer func() { _ = streamWriter.Close() }()
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"partial-protocol-error\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://partial\"}]}}}\n\n")
		for count := 0; count < 3; count++ {
			select {
			case <-conversationPolled:
			case <-time.After(time.Second):
				return
			}
		}
		_, _ = io.WriteString(streamWriter,
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://final\"}]}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"partial", "final"}) {
		t.Fatalf("primary stream result = %+v", accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamRetriesFinalConversationRefresh(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "stream-final-refresh",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}})
		case "/backend-api/conversation/stream-final-refresh":
			poll := conversationPolls.Add(1)
			if poll < 4 {
				writeChatGPTWebImageConversation(w, "first")
				return
			}
			if poll == 4 {
				http.Error(w, "not ready", http.StatusNotFound)
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 20 * time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 8
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	body := newChatGPTWebBlockingBody("data: {\"conversation_id\":\"stream-final-refresh\"}\n\n")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) || conversationPolls.Load() < 5 {
		t.Fatalf("files = %v, conversation polls = %d", accumulator.FileIDs, conversationPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamStopsAfterStableTaskFailure(t *testing.T) {
	var taskPolls atomic.Int32
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "stable-task-failure-stream",
				"status":          "failed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{}},
				},
			}}})
		case "/backend-api/conversation/stable-task-failure-stream":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 20
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	body := newChatGPTWebBlockingBody("data: {\"conversation_id\":\"stable-task-failure-stream\"}\n\n")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	if err != nil {
		t.Fatal(err)
	}
	if !accumulator.Terminal || accumulator.FailureStatus != "failed" {
		t.Fatalf("failure accumulator = %+v", accumulator)
	}
	if got := taskPolls.Load(); got >= 8 {
		t.Fatalf("task polls = %d, stable failure was not returned promptly", got)
	}
	if got := conversationPolls.Load(); got < 1 || got >= 8 {
		t.Fatalf("conversation polls = %d, want one bounded final confirmation", got)
	}
}

func TestConsumeChatGPTWebImageStreamUsesCompletedConversationWhileStreamRemainsOpen(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/open-stream-conversation":
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"open-stream-conversation\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://streamed\"}]}}}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated", "streamed"}) {
		t.Fatalf("conversation output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestConsumeChatGPTWebImageStreamWaitsForStableTerminalConversation(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/stable-terminal-stream":
			if conversationPolls.Add(1) == 1 {
				writeTerminalChatGPTWebImageConversation(w, "first")
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"stable-terminal-stream\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("conversation output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 3 {
		t.Fatalf("conversation polls = %d, want at least 3 stable snapshots", got)
	}
}

func TestConsumeChatGPTWebImageStreamWaitsForConversationSettleWindow(t *testing.T) {
	var conversationPolls atomic.Int32
	var firstPollAt atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/delayed-terminal-image":
			conversationPolls.Add(1)
			now := time.Now()
			first := firstPollAt.Load()
			if first == 0 {
				firstPollAt.CompareAndSwap(0, now.UnixNano())
				first = firstPollAt.Load()
			}
			if now.Sub(time.Unix(0, first)) < 20*time.Millisecond {
				writeTerminalChatGPTWebImageConversation(w, "first")
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 40 * time.Millisecond
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"delayed-terminal-image\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("conversation output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 3 {
		t.Fatalf("conversation polls = %d, want delayed image plus a stable confirmation", got)
	}
}

func TestConsumeChatGPTWebImageStreamDoesNotCloseAfterRecentPrimaryProgress(t *testing.T) {
	progressSent := make(chan struct{})
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/recent-primary-progress":
			if conversationPolls.Add(1) > 1 {
				select {
				case <-progressSent:
				case <-request.Context().Done():
					return
				}
			}
			writeTerminalChatGPTWebImageConversation(w, "polled-first")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 20 * time.Millisecond
	executor.imageMaxPolls = 10
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		defer func() { _ = streamWriter.Close() }()
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"recent-primary-progress\"}\n\n")
		time.Sleep(15 * time.Millisecond)
		_, _ = io.WriteString(streamWriter, "data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"status\":\"running\"},\"content\":{\"parts\":[]}}}\n\n")
		time.Sleep(2 * time.Millisecond)
		close(progressSent)
		time.Sleep(8 * time.Millisecond)
		_, _ = io.WriteString(streamWriter,
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://stream-second\"}]}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.StreamTerminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"stream-second"}) {
		t.Fatalf("primary stream result = %+v", accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamWaitsForRecentProgressBeforeTaskFallback(t *testing.T) {
	taskStable := make(chan struct{})
	var taskStableOnce sync.Once
	var taskPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "recent-task-fallback-progress",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://task-first"}}},
				},
			}}})
			if poll >= 3 {
				taskStableOnce.Do(func() { close(taskStable) })
			}
		case "/backend-api/conversation/recent-task-fallback-progress":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 20 * time.Millisecond
	executor.imageMaxPolls = 100
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		defer func() { _ = streamWriter.Close() }()
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"recent-task-fallback-progress\"}\n\n")
		<-taskStable
		time.Sleep(15 * time.Millisecond)
		_, _ = io.WriteString(streamWriter, "data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"status\":\"running\"},\"content\":{\"parts\":[]}}}\n\n")
		time.Sleep(10 * time.Millisecond)
		_, _ = io.WriteString(streamWriter,
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"is_complete\":true,\"status\":\"finished_successfully\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://stream-late\"}]}}}\n\n"+
				"data: [DONE]\n\n",
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"stream-late"}) {
		t.Fatalf("primary stream files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamPrefersAuthoritativeConversationFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/failed-terminal-stream":
			writeFailedChatGPTWebImageConversation(w, "finished_with_error")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	body := newChatGPTWebBlockingBody(
		"data: {\"conversation_id\":\"failed-terminal-stream\"}\n\n" +
			"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://partial\"}]}}}\n\n",
	)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: body})
	if err == nil || !strings.Contains(err.Error(), "finished_with_error") {
		t.Fatalf("stream error = %v, accumulator = %+v", err, accumulator)
	}
}

func TestConsumeChatGPTWebImageStreamUsesTerminalConversationWhileTaskRemainsPending(t *testing.T) {
	var taskPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "pending-task-terminal-conversation",
				"status":          "running",
			}}})
		case "/backend-api/conversation/pending-task-terminal-conversation":
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"pending-task-terminal-conversation\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("conversation output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := taskPolls.Load(); got < 1 {
		t.Fatalf("task polls = %d, want at least 1", got)
	}
}

func TestConsumeChatGPTWebImageStreamDoesNotWaitForBlockedTaskRequest(t *testing.T) {
	taskStarted := make(chan struct{})
	taskCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			startedOnce.Do(func() { close(taskStarted) })
			<-request.Context().Done()
			canceledOnce.Do(func() { close(taskCanceled) })
		case "/backend-api/conversation/blocked-stream-task":
			select {
			case <-taskStarted:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 10 * time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"blocked-stream-task\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	select {
	case <-taskCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked task request was not canceled")
	}
}

func TestConsumeChatGPTWebImageStreamDoesNotWaitForBlockedConversationWhenTaskCompletes(t *testing.T) {
	conversationStarted := make(chan struct{})
	conversationCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			select {
			case <-conversationStarted:
			case <-request.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "blocked-stream-conversation",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/blocked-stream-conversation":
			startedOnce.Do(func() { close(conversationStarted) })
			<-request.Context().Done()
			canceledOnce.Do(func() { close(conversationCanceled) })
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 10 * time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"blocked-stream-conversation\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	select {
	case <-conversationCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked conversation request was not canceled")
	}
}

func TestConsumeChatGPTWebImageStreamWatcherHonorsRetryAfter(t *testing.T) {
	var taskPolls atomic.Int32
	var firstTaskPoll time.Time
	var secondTaskPoll time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			if poll == 1 {
				firstTaskPoll = time.Now()
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			if poll == 2 {
				secondTaskPoll = time.Now()
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/retry-after-watcher":
			if taskPolls.Load() < 2 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 250 * time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 10
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"retry-after-watcher\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	if firstTaskPoll.IsZero() || secondTaskPoll.IsZero() {
		t.Fatalf("task poll timestamps = %v, %v", firstTaskPoll, secondTaskPoll)
	}
	if delay := secondTaskPoll.Sub(firstTaskPoll); delay < 850*time.Millisecond {
		t.Fatalf("retry delay = %v, want Retry-After delay", delay)
	}
}

func TestConsumeChatGPTWebImageStreamWatcherHonorsConversationRetryAfter(t *testing.T) {
	var conversationPolls atomic.Int32
	var firstConversationPoll time.Time
	var secondConversationPoll time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/retry-after-conversation-watcher":
			poll := conversationPolls.Add(1)
			if poll == 1 {
				firstConversationPoll = time.Now()
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			if poll == 2 {
				secondConversationPoll = time.Now()
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 50 * time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 10
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"retry-after-conversation-watcher\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	if firstConversationPoll.IsZero() || secondConversationPoll.IsZero() {
		t.Fatalf("conversation poll timestamps = %v, %v", firstConversationPoll, secondConversationPoll)
	}
	if delay := secondConversationPoll.Sub(firstConversationPoll); delay < 850*time.Millisecond {
		t.Fatalf("retry delay = %v, want Retry-After delay", delay)
	}
}

func TestConsumeChatGPTWebImageStreamWaitsForDelayedSecondTask(t *testing.T) {
	var taskPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			tasks := []any{map[string]any{
				"conversation_id": "delayed-stream-task",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author": map[string]any{"role": "tool"}, "metadata": map[string]any{"async_task_type": "image_gen"},
					"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}
			if poll >= 3 {
				tasks = append(tasks, map[string]any{
					"conversation_id": "delayed-stream-task",
					"status":          "completed",
					"image_gen_message": map[string]any{
						"author": map[string]any{"role": "tool"}, "metadata": map[string]any{"async_task_type": "image_gen"},
						"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://second"}}},
					},
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": tasks})
		case "/backend-api/conversation/delayed-stream-task":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 10 * time.Millisecond
	executor.imageMaxPolls = 12
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"delayed-stream-task\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) || taskPolls.Load() < 5 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamClearsPendingWhenTasksBecomeMalformed(t *testing.T) {
	var taskPolls atomic.Int32
	tasksSettled := make(chan struct{})
	var settledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			if taskPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
					"conversation_id": "malformed-stream-task",
					"status":          "running",
				}}})
				return
			}
			settledOnce.Do(func() { close(tasksSettled) })
			_, _ = io.WriteString(w, `{broken`)
		case "/backend-api/conversation/malformed-stream-task":
			select {
			case <-tasksSettled:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"malformed-stream-task\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() != 2 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamIgnoresStalePendingTaskAfterRetryableError(t *testing.T) {
	var taskPolls atomic.Int32
	tasksSettled := make(chan struct{})
	var settledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			if taskPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
					"conversation_id": "retryable-stream-task",
					"status":          "running",
				}}})
				return
			}
			settledOnce.Do(func() { close(tasksSettled) })
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		case "/backend-api/conversation/retryable-stream-task":
			select {
			case <-tasksSettled:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 5 * time.Millisecond
	executor.imageMaxPolls = 4
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	defer func() { _ = streamWriter.Close() }()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"retryable-stream-task\"}\n\n")
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() < 2 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestConsumeChatGPTWebImageStreamIgnoresMalformedAuxiliaryResponses(t *testing.T) {
	tasksQueried := make(chan struct{})
	var closeTasksQueried sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			closeTasksQueried.Do(func() { close(tasksQueried) })
			_, _ = io.WriteString(w, `{broken`)
		case "/backend-api/conversation/healthy-stream":
			_, _ = io.WriteString(w, `{broken`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"healthy-stream\"}\n\n")
		<-tasksQueried
		_, _ = io.WriteString(streamWriter, "data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\ndata: [DONE]\n\n")
		_ = streamWriter.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("stream output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestConsumeChatGPTWebImageStreamWaitsForAllMatchingTasks(t *testing.T) {
	tasksQueried := make(chan struct{})
	var closeTasksQueried sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			closeTasksQueried.Do(func() { close(tasksQueried) })
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{
				map[string]any{
					"conversation_id": "multi-task-stream",
					"status":          "completed",
					"image_gen_message": map[string]any{
						"author": map[string]any{"role": "tool"}, "metadata": map[string]any{"async_task_type": "image_gen"},
						"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
					},
				},
				map[string]any{
					"conversation_id": "multi-task-stream",
					"status":          "running",
					"image_gen_message": map[string]any{
						"author": map[string]any{"role": "tool"}, "metadata": map[string]any{"async_task_type": "image_gen"},
						"content": map[string]any{"parts": []any{}},
					},
				},
			}})
		case "/backend-api/conversation/multi-task-stream":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	streamReader, streamWriter := io.Pipe()
	go func() {
		_, _ = io.WriteString(streamWriter, "data: {\"conversation_id\":\"multi-task-stream\"}\n\n")
		<-tasksQueried
		_, _ = io.WriteString(streamWriter, "data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://first\"},{\"asset_pointer\":\"file-service://second\"}]}}}\n\ndata: [DONE]\n\n")
		_ = streamWriter.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator, err := executor.consumeChatGPTWebImageStreamWithTaskPolling(ctx, client, credential, &fhttp.Response{Body: streamReader})
	if err != nil {
		t.Fatalf("consumeChatGPTWebImageStreamWithTaskPolling() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("stream output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestChatGPTWebBrokenErrorBodyPreservesStatusAndRetryAfter(t *testing.T) {
	response := &fhttp.Response{
		StatusCode: http.StatusServiceUnavailable,
		Header:     fhttp.Header{"Retry-After": {"9"}},
		Body:       chatGPTWebBrokenErrorBody{},
	}
	payload, err := readChatGPTWebResponseBody(response, chatGPTWebMaxJSONBodyBytes)
	if err != nil {
		t.Fatalf("readChatGPTWebResponseBody() error = %v", err)
	}
	statusErr := newChatGPTWebStatusError(response.StatusCode, "/backend-api/test", payload, response.Header)
	var status interface{ StatusCode() int }
	if !errors.As(statusErr, &status) || status.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("status error = %v", statusErr)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(statusErr, &headers) || headers.Headers().Get("Retry-After") != "9" {
		t.Fatalf("Retry-After error = %v", statusErr)
	}
	var requestPath interface{ ChatGPTWebRequestPath() string }
	if !errors.As(statusErr, &requestPath) || requestPath.ChatGPTWebRequestPath() != "/backend-api/test" {
		t.Fatalf("request path error = %v", statusErr)
	}
	var retrier interface{ RetryAfter() *time.Duration }
	if !errors.As(statusErr, &retrier) || retrier.RetryAfter() == nil || *retrier.RetryAfter() != 9*time.Second {
		t.Fatalf("retry duration = %v", statusErr)
	}
}

func TestChatGPTWebImageSettleErrorPreservesRetryMetadata(t *testing.T) {
	cause := newChatGPTWebStatusError(
		http.StatusTooManyRequests,
		"/backend-api/conversation/image-settle",
		[]byte(`{"error":"rate limited"}`),
		fhttp.Header{"Retry-After": {"7"}},
	)
	err := newChatGPTWebImageSettleError(cause)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("settle status = %v", err)
	}
	var retry interface{ RetryAfter() *time.Duration }
	if !errors.As(err, &retry) || retry.RetryAfter() == nil || *retry.RetryAfter() != 7*time.Second {
		t.Fatalf("settle Retry-After = %v", err)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(err, &headers) || headers.Headers().Get("Retry-After") != "7" {
		t.Fatalf("settle headers = %v", err)
	}
	var requestPath interface{ ChatGPTWebRequestPath() string }
	if !errors.As(err, &requestPath) || requestPath.ChatGPTWebRequestPath() != "/backend-api/conversation/image-settle" {
		t.Fatalf("settle cause = %v", err)
	}
}

func TestChatGPTWebRequestStatusErrorsSkipCredentialState(t *testing.T) {
	for _, statusCode := range []int{
		http.StatusBadRequest,
		http.StatusNotFound,
		http.StatusMethodNotAllowed,
		http.StatusConflict,
		http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType,
		http.StatusUnprocessableEntity,
	} {
		statusError := newChatGPTWebStatusError(statusCode, "/backend-api/test", []byte(`{"error":"request"}`), nil)
		var skipper interface{ SkipAuthResult() bool }
		if !errors.As(statusError, &skipper) || !skipper.SkipAuthResult() {
			t.Fatalf("status %d SkipAuthResult() error = %v", statusCode, statusError)
		}
	}

	notFound := newChatGPTWebStatusError(http.StatusNotFound, "/backend-api/test", []byte(`{"error":"missing"}`), nil)
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(notFound, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("404 RetryOtherAuth() error = %v", notFound)
	}

	for _, statusCode := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
	} {
		statusError := newChatGPTWebStatusError(statusCode, "/backend-api/test", []byte(`{"error":"auth"}`), nil)
		var skipper interface{ SkipAuthResult() bool }
		if errors.As(statusError, &skipper) && skipper.SkipAuthResult() {
			t.Fatalf("status %d unexpectedly skips auth result", statusCode)
		}
	}
}

func TestChatGPTWebAssetErrorsDoNotCoolCredentialOrLeakURL(t *testing.T) {
	const signedURL = "https://storage.example/image?sig=secret"
	cause := &url.Error{Op: http.MethodGet, URL: signedURL, Err: io.ErrUnexpectedEOF}
	transportErr := newChatGPTWebAssetTransportError(context.Background(), "download", cause)
	if strings.Contains(transportErr.Error(), signedURL) {
		t.Fatalf("transport error leaked URL: %v", transportErr)
	}
	assertChatGPTWebAssetRetryError(t, transportErr)
	for name, err := range map[string]error{
		"transport": transportErr,
		"final":     chatGPTWebFinalAssetError(transportErr),
		"committed": chatGPTWebCommittedRequestError(context.Background(), transportErr),
	} {
		t.Run(name, func(t *testing.T) {
			var gotCause *url.Error
			if !errors.As(err, &gotCause) || gotCause == cause || gotCause.URL != "<redacted>" {
				t.Fatalf("asset error cause = %#v, want sanitized URL error", gotCause)
			}
			if !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("asset error lost network cause: %v", err)
			}
			var status interface{ StatusCode() int }
			if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
				t.Fatalf("asset error status = %v, want 502", err)
			}
			if strings.Contains(err.Error(), signedURL) {
				t.Fatalf("asset error leaked URL: %v", err)
			}
		})
	}

	statusError := newChatGPTWebAssetStatusError(http.StatusBadGateway, signedURL,
		[]byte(`<Error><Message>failed https://storage.example/image?sig=secret</Message></Error>`), nil)
	if strings.Contains(statusError.Error(), "sig=secret") {
		t.Fatalf("status error leaked URL: %v", statusError)
	}
	var requestPath interface{ ChatGPTWebRequestPath() string }
	if !errors.As(statusError, &requestPath) || requestPath.ChatGPTWebRequestPath() != "/image" {
		t.Fatalf("asset request path = %#v", requestPath)
	}
	assertChatGPTWebAssetRetryError(t, statusError)
}

func TestChatGPTWebAssetTransportErrorSanitizesNestedURLErrors(t *testing.T) {
	inner := &url.Error{Op: http.MethodGet, URL: "https://storage.example/inner?sig=inner-secret", Err: io.ErrUnexpectedEOF}
	outer := &url.Error{Op: http.MethodGet, URL: "https://storage.example/outer?sig=outer-secret", Err: inner}
	err := newChatGPTWebAssetTransportError(context.Background(), "download", outer)
	for current := err; current != nil; current = errors.Unwrap(current) {
		message := current.Error()
		if strings.Contains(message, "inner-secret") || strings.Contains(message, "outer-secret") {
			t.Fatalf("nested asset error leaked a signed URL: %v", current)
		}
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("nested asset error lost network cause: %v", err)
	}
}

func TestChatGPTWebAssetStatusRetriesOnlyRecoverableFailures(t *testing.T) {
	for _, statusCode := range []int{
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusRequestTimeout,
		http.StatusConflict,
		http.StatusLocked,
		http.StatusTooEarly,
		http.StatusTooManyRequests,
		http.StatusBadGateway,
	} {
		err := newChatGPTWebAssetStatusError(statusCode, "https://storage.example/image?sig=secret", nil, nil)
		assertChatGPTWebAssetRetryError(t, err)
	}
	for _, statusCode := range []int{
		http.StatusBadRequest,
		http.StatusMethodNotAllowed,
		http.StatusRequestEntityTooLarge,
		http.StatusUnsupportedMediaType,
		http.StatusUnprocessableEntity,
	} {
		err := newChatGPTWebAssetStatusError(statusCode, "https://storage.example/image?sig=secret", nil, nil)
		assertChatGPTWebNonAuthNonRetryError(t, err)
	}
}

func assertChatGPTWebAssetRetryError(t *testing.T, err error) {
	t.Helper()
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
}

func assertChatGPTWebNonAuthNonRetryError(t *testing.T, err error) {
	t.Helper()
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if errors.As(err, &retry) && retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
}

func TestChatGPTWebStatusErrorRedactsSignedURLBody(t *testing.T) {
	jsonError := newChatGPTWebStatusError(http.StatusBadGateway, "/backend-api/files/file-1/download",
		[]byte(`{"download_url":"https://storage.example/image?sig=secret"}`), nil)
	if strings.Contains(jsonError.Error(), "sig=secret") ||
		gjson.Get(jsonError.Error(), "download_url").String() != "<redacted-signed-url>" {
		t.Fatalf("JSON status error = %q", jsonError.Error())
	}

	textError := newChatGPTWebStatusError(http.StatusBadGateway,
		"https://storage.example/image?sig=secret",
		[]byte(`<Error><Message>failed https://storage.example/image?sig=secret</Message></Error>`), nil)
	if strings.Contains(textError.Error(), "sig=secret") || textError.Error() != "<redacted-non-json-response-body>" {
		t.Fatalf("text status error = %q", textError.Error())
	}
}

func TestDoChatGPTWebSignedUploadOnlyFollowsMethodPreservingRedirects(t *testing.T) {
	t.Run("302 is not followed", func(t *testing.T) {
		var targetHits int
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			targetHits++
			w.WriteHeader(http.StatusCreated)
		}))
		defer target.Close()
		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			http.Redirect(w, request, target.URL, http.StatusFound)
		}))
		defer source.Close()

		executor := NewChatGPTWebExecutor(nil, nil)
		executor.runtimeBaseURL = source.URL
		client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, _, err := executor.doChatGPTWebAssetRequest(context.Background(), client, credential, http.MethodPut, source.URL, nil, []byte("image"), true)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusFound {
			t.Fatalf("status = %d", response.StatusCode)
		}
		if targetHits != 0 {
			t.Fatalf("target hits = %d", targetHits)
		}
	})

	t.Run("307 preserves PUT and body", func(t *testing.T) {
		var method string
		var payload []byte
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/start" {
				http.Redirect(w, request, "/target", http.StatusTemporaryRedirect)
				return
			}
			method = request.Method
			payload, _ = io.ReadAll(request.Body)
			w.WriteHeader(http.StatusCreated)
		}))
		defer server.Close()

		executor := NewChatGPTWebExecutor(nil, nil)
		executor.runtimeBaseURL = server.URL
		client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, _, err := executor.doChatGPTWebAssetRequest(context.Background(), client, credential, http.MethodPut, server.URL+"/start", nil, []byte("image"), true)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			t.Fatalf("status = %d", response.StatusCode)
		}
		if method != http.MethodPut || string(payload) != "image" {
			t.Fatalf("upload = %s %q", method, payload)
		}
	})

	t.Run("307 cross-host redirect is rejected", func(t *testing.T) {
		var targetHits int
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			targetHits++
			w.WriteHeader(http.StatusCreated)
		}))
		defer target.Close()
		targetURL, errParse := url.Parse(target.URL)
		if errParse != nil {
			t.Fatal(errParse)
		}
		targetURL.Host = "localhost:" + targetURL.Port()

		source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			http.Redirect(w, request, targetURL.String(), http.StatusTemporaryRedirect)
		}))
		defer source.Close()

		executor := NewChatGPTWebExecutor(nil, nil)
		executor.runtimeBaseURL = source.URL
		client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
		if err != nil {
			t.Fatal(err)
		}
		defer client.CloseIdleConnections()
		response, _, err := executor.doChatGPTWebAssetRequest(context.Background(), client, credential, http.MethodPut, source.URL, nil, []byte("image"), true)
		if response != nil {
			_ = response.Body.Close()
		}
		if err == nil {
			t.Fatalf("cross-host redirect error = %v", err)
		}
		if targetHits != 0 {
			t.Fatalf("target hits = %d", targetHits)
		}
	})

	t.Run("https downgrade is rejected", func(t *testing.T) {
		current, errCurrent := url.Parse("https://storage.example/upload?sig=secret")
		next, errNext := url.Parse("http://storage.example/upload?sig=secret")
		if errCurrent != nil || errNext != nil {
			t.Fatalf("parse redirect URLs: %v %v", errCurrent, errNext)
		}
		if err := validateChatGPTWebSignedUploadRedirect(current, next); err == nil || !strings.Contains(err.Error(), "downgrade") {
			t.Fatalf("downgrade redirect error = %v", err)
		}
	})
}

func TestValidateChatGPTWebAssetURLRejectsUntrustedTargets(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	for _, rawURL := range []string{
		"http://127.0.0.1/private",
		"https://127.0.0.1/private",
		"https://metadata.internal/latest",
		"https://files.oaiusercontent.com.attacker.example/image",
		"https://files.oaiusercontent.com:8443/image",
		"file:///etc/passwd",
		"https://user:pass@files.oaiusercontent.com/image",
	} {
		if _, err := executor.validateChatGPTWebAssetURL(rawURL); err == nil {
			t.Fatalf("validateChatGPTWebAssetURL(%q) accepted untrusted target", rawURL)
		}
	}
	for _, rawURL := range []string{
		"https://files.oaiusercontent.com/image",
		"https://account.blob.core.windows.net/container/image",
	} {
		if _, err := executor.validateChatGPTWebAssetURL(rawURL); err != nil {
			t.Fatalf("validateChatGPTWebAssetURL(%q) error = %v", rawURL, err)
		}
	}
}

func TestValidateChatGPTWebAssetURLResolvesSameOriginRelativePath(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = "https://chatgpt.example"
	parsed, err := executor.validateChatGPTWebAssetURL("/backend-api/files/generated/download")
	if err != nil {
		t.Fatalf("validateChatGPTWebAssetURL() error = %v", err)
	}
	if got := parsed.String(); got != "https://chatgpt.example/backend-api/files/generated/download" {
		t.Fatalf("resolved asset URL = %q", got)
	}
}

func TestSameChatGPTWebAssetOriginNormalizesDefaultPorts(t *testing.T) {
	parse := func(rawURL string) *url.URL {
		parsed, err := url.Parse(rawURL)
		if err != nil {
			t.Fatal(err)
		}
		return parsed
	}
	if !sameChatGPTWebAssetOrigin(parse("https://chatgpt.example"), parse("https://CHATGPT.example:443/path")) {
		t.Fatal("explicit HTTPS default port was not treated as the same origin")
	}
	if sameChatGPTWebAssetOrigin(parse("https://chatgpt.example"), parse("https://chatgpt.example:444/path")) {
		t.Fatal("different HTTPS port was treated as the same origin")
	}
}

func TestChatGPTWebAssetRequestHeadersAuthenticateOnlySameOriginDownloads(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = "https://chatgpt.example"
	credential := &chatgptwebauth.Credential{
		AccessToken: "account-token",
		DeviceID:    "device",
		Persona:     chatgptwebauth.DefaultPersona(),
	}
	sameOrigin, _ := url.Parse("https://chatgpt.example/backend-api/files/generated")
	external, _ := url.Parse("https://files.oaiusercontent.com/generated.png?sig=secret")

	sameOriginHeaders := executor.chatGPTWebAssetRequestHeaders(credential, http.MethodGet, sameOrigin, map[string]string{"accept": "image/png"}, false)
	if got := sameOriginHeaders["authorization"]; got != "Bearer account-token" {
		t.Fatalf("same-origin authorization = %q", got)
	}
	if got := sameOriginHeaders["x-openai-target-path"]; got != "/backend-api/files/generated" {
		t.Fatalf("same-origin target path = %q", got)
	}
	externalHeaders := executor.chatGPTWebAssetRequestHeaders(credential, http.MethodGet, external, map[string]string{"accept": "image/png"}, false)
	if _, exists := externalHeaders["authorization"]; exists {
		t.Fatal("external signed asset received the account authorization header")
	}
	if got := externalHeaders["accept"]; got != "image/png" {
		t.Fatalf("external accept = %q", got)
	}
}

func TestDownloadChatGPTWebImagesPreservesLargeErrorStatus(t *testing.T) {
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/output/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, strings.Repeat("x", chatGPTWebMaxErrorBodyBytes+1))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageSettleWait = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.downloadChatGPTWebImages(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}},
	})
	if err == nil {
		t.Fatal("expected image download error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("download error = %v", err)
	}
	var headers interface{ Headers() http.Header }
	if !errors.As(err, &headers) || headers.Headers().Get("Retry-After") != "0" {
		t.Fatalf("Retry-After error = %v", err)
	}
	if err.Error() != "<upstream-error-body-truncated>" {
		t.Fatalf("error body = %q", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestDownloadChatGPTWebImagesRejectsSedimentWithoutConversation(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.downloadChatGPTWebImages(context.Background(), nil, nil, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{{Kind: "sediment", ID: "output"}},
	})
	if err == nil || !strings.Contains(err.Error(), "conversation ID") {
		t.Fatalf("downloadChatGPTWebImages() error = %v", err)
	}
	assertChatGPTWebAssetRetryError(t, err)
}

func TestDownloadChatGPTWebImagesRejectsMissingDownloadURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/files/output/download" {
			http.NotFound(w, request)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ready"})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageSettleWait = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	_, err = executor.downloadChatGPTWebImages(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}},
	})
	if err == nil || !strings.Contains(err.Error(), "download URL") {
		t.Fatalf("downloadChatGPTWebImages() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestDownloadChatGPTWebImagesWaitsForMetadataDownloadURL(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var metadataHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/output/download":
			if metadataHits.Add(1) < 3 {
				w.WriteHeader(http.StatusAccepted)
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "processing"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageSettleWait = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	images, err := executor.downloadChatGPTWebImages(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}},
	})
	if err != nil {
		t.Fatalf("downloadChatGPTWebImages() error = %v", err)
	}
	if len(images) != 1 || metadataHits.Load() != 3 {
		t.Fatalf("images = %d, metadata hits = %d", len(images), metadataHits.Load())
	}
}

func TestDownloadChatGPTWebImagesRetriesPendingAssetInPlace(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{G: 255, A: 255})
	var assetHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/output/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			if assetHits.Add(1) < 3 {
				http.NotFound(w, request)
				return
			}
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageSettleWait = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	images, err := executor.downloadChatGPTWebImages(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{{Kind: "file", ID: "output"}},
	})
	if err != nil {
		t.Fatalf("downloadChatGPTWebImages() error = %v", err)
	}
	if len(images) != 1 || assetHits.Load() != 3 {
		t.Fatalf("images = %d, asset hits = %d", len(images), assetHits.Load())
	}
}

func TestDownloadChatGPTWebImagesPreservesDuplicateResolvedURLs(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var assetHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/first/download", "/backend-api/files/second/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			assetHits.Add(1)
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	images, err := executor.downloadChatGPTWebImages(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{
			{Kind: "file", ID: "first"},
			{Kind: "file", ID: "second"},
		},
	})
	if err != nil {
		t.Fatalf("downloadChatGPTWebImages() error = %v", err)
	}
	if len(images) != 2 || assetHits.Load() != 2 {
		t.Fatalf("downloaded images = %d, asset hits = %d", len(images), assetHits.Load())
	}
}

func TestDownloadChatGPTWebImagesLimitsResultsBeforeResolvingAssets(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var metadataHits atomic.Int32
	var assetHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/first/download", "/backend-api/files/second/download":
			metadataHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			assetHits.Add(1)
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	images, err := executor.downloadChatGPTWebImagesLimited(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{
			{Kind: "file", ID: "first"},
			{Kind: "file", ID: "second"},
		},
	}, 1)
	if err != nil {
		t.Fatalf("downloadChatGPTWebImagesLimited() error = %v", err)
	}
	if len(images) != 1 || metadataHits.Load() != 1 || assetHits.Load() != 1 {
		t.Fatalf("images=%d metadata_hits=%d asset_hits=%d", len(images), metadataHits.Load(), assetHits.Load())
	}
}

func TestDownloadChatGPTWebImagesAppliesLimitAfterSkippingPlaceholders(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{G: 255, A: 255})
	var metadataHits atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/files/generated/download":
			metadataHits.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	images, err := executor.downloadChatGPTWebImagesLimited(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		References: []helps.ChatGPTWebImageReference{
			{Kind: "file", ID: "file_upload"},
			{Kind: "file", ID: "generated"},
		},
	}, 1)
	if err != nil {
		t.Fatalf("downloadChatGPTWebImagesLimited() error = %v", err)
	}
	if len(images) != 1 || metadataHits.Load() != 1 {
		t.Fatalf("images=%d metadata_hits=%d, want one generated image", len(images), metadataHits.Load())
	}
}

func TestFinishChatGPTWebImageRejectsDoneWithoutTerminalOrConversation(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{
		response: &fhttp.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n" +
					"data: [DONE]\n\n",
			)),
		},
	}
	_, err := executor.finishChatGPTWebImage(context.Background(), nil, nil, prepared, execution)
	if err == nil || !strings.Contains(err.Error(), "explicit terminal state") {
		t.Fatalf("finishChatGPTWebImage() error = %v", err)
	}
	assertChatGPTWebAssetRetryError(t, err)
}

func TestFinishChatGPTWebImageDoesNotMaskTerminalFailureWithPartialOutput(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{
		response: &fhttp.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				"data: {\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"finish_details\":{\"type\":\"finished_with_error\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://partial\"}]}}}\n\n" +
					"data: [DONE]\n\n",
			)),
		},
	}
	_, err := executor.finishChatGPTWebImage(context.Background(), nil, nil, prepared, execution)
	if err == nil || !strings.Contains(err.Error(), "finished_with_error") {
		t.Fatalf("finishChatGPTWebImage() error = %v", err)
	}
}

func TestFinishChatGPTWebImagePollsConversationAfterIncompleteStream(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{B: 255, A: 255})
	var conversationPolls atomic.Int32
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/incomplete-image":
			conversationPolls.Add(1)
			writeTerminalChatGPTWebImageConversation(w, "generated")
		case "/backend-api/files/generated/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/asset"})
		case "/asset":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 6
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{response: &fhttp.Response{
		StatusCode: http.StatusOK,
		Body: io.NopCloser(strings.NewReader(
			"data: {\"conversation_id\":\"incomplete-image\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\",\"finish_details\":{\"type\":\"finished_successfully\"},\"is_complete\":true},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://generated\"}]}}}\n\n",
		)),
	}}
	if _, err = executor.finishChatGPTWebImage(context.Background(), client, credential, prepared, execution); err != nil {
		t.Fatalf("finishChatGPTWebImage() error = %v", err)
	}
	if got := conversationPolls.Load(); got != 1 {
		t.Fatalf("conversation polls = %d, want 1 confirming snapshot after streamed output", got)
	}
}

func TestFinishChatGPTWebImageDownloadsSedimentWithoutTaskTerminal(t *testing.T) {
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/sediment-conversation/attachment/sediment-output/download",
			"/backend-api/conversation/polled-sediment/attachment/sediment-output/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": "http://" + request.Host + "/asset"})
		case "/backend-api/conversation/sediment-conversation":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"generated": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata":    map[string]any{"async_task_type": "image_gen"},
					"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "sediment://sediment-output"}}},
				}},
			}})
		case "/backend-api/conversation/polled-sediment":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"generated": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata":    map[string]any{"async_task_type": "image_gen"},
					"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "sediment://sediment-output"}}},
				}},
			}})
		case "/asset":
			w.Header().Set("Content-Type", "image/png")
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	prepared := &chatGPTWebPreparedRequest{
		routeModel:      "gpt-image-2",
		maxImageResults: 1,
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	execution := &chatGPTWebImageExecution{
		response: &fhttp.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(
				"data: {\"conversation_id\":\"sediment-conversation\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"sediment://sediment-output\"}]}}}\n\n" +
					"data: [DONE]\n\n",
			)),
		},
	}
	payload, err := executor.finishChatGPTWebImage(context.Background(), client, credential, prepared, execution)
	if err != nil {
		t.Fatalf("finishChatGPTWebImage() error = %v", err)
	}
	if got := gjson.GetBytes(payload, "response.output.0.result").String(); got != base64.StdEncoding.EncodeToString(imageData) {
		t.Fatal("finishChatGPTWebImage() did not return the sediment image")
	}
	if got := conversationPolls.Load(); got != 1 {
		t.Fatalf("conversation polls = %d, want 1 confirming snapshot after streamed sediment", got)
	}

	executor.imageMaxPolls = 3
	polledExecution := &chatGPTWebImageExecution{
		response: &fhttp.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("data: {\"conversation_id\":\"polled-sediment\"}\n\ndata: [DONE]\n\n")),
		},
	}
	payload, err = executor.finishChatGPTWebImage(context.Background(), client, credential, prepared, polledExecution)
	if err != nil {
		t.Fatalf("finishChatGPTWebImage() polled sediment error = %v", err)
	}
	if got := gjson.GetBytes(payload, "response.output.0.result").String(); got != base64.StdEncoding.EncodeToString(imageData) {
		t.Fatal("finishChatGPTWebImage() did not return the polled sediment image")
	}
	if got := conversationPolls.Load(); got != 3 {
		t.Fatalf("conversation polls = %d, want 3 total across both requests", got)
	}
}

func TestValidateChatGPTWebDownloadedImageRejectsTruncatedPayload(t *testing.T) {
	valid := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	if err := validateChatGPTWebDownloadedImage(valid, "image/png"); err != nil {
		t.Fatalf("validateChatGPTWebDownloadedImage(valid) error = %v", err)
	}
	truncated := valid[:len(valid)-8]
	if err := validateChatGPTWebDownloadedImage(truncated, "image/png"); err == nil {
		t.Fatal("validateChatGPTWebDownloadedImage() accepted truncated PNG")
	}
}

func TestUploadChatGPTWebImageRejectsTruncatedPayloadBeforeUpload(t *testing.T) {
	valid := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	truncated := valid[:len(valid)-8]
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.uploadChatGPTWebImage(
		context.Background(),
		nil,
		nil,
		"data:image/png;base64,"+base64.StdEncoding.EncodeToString(truncated),
		"input.png",
	)
	if err == nil || !strings.Contains(err.Error(), "decode image") {
		t.Fatalf("uploadChatGPTWebImage() error = %v", err)
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("upload status error = %v", err)
	}
}

func TestValidateChatGPTWebImageEditMemory(t *testing.T) {
	if err := validateChatGPTWebImageEditMemory(image.Config{Width: 1024, Height: 1024}); err != nil {
		t.Fatalf("validateChatGPTWebImageEditMemory(small) error = %v", err)
	}
	if err := validateChatGPTWebImageEditMemory(image.Config{Width: 4096, Height: 4096}); err == nil {
		t.Fatal("validateChatGPTWebImageEditMemory() accepted oversized decoded images")
	}
}

func TestChatGPTWebRequestLogBodyRedactsTokensAndEmail(t *testing.T) {
	logged := chatGPTWebRequestLogBody("/backend-api/sentinel/chat-requirements/prepare", []byte(`{
		"p":"proof-config",
		"prepare_token":"prepare-secret",
		"nested":{
			"conduit_token":"conduit-secret",
			"email":"person@example.com",
			"image_url":"https://storage.example/image?sig=nested-secret"
		},
		"urls":["https://storage.example/other?token=array-secret"],
		"prompt":"keep this"
	}`))
	text := string(logged)
	for _, secret := range []string{
		"proof-config",
		"prepare-secret",
		"conduit-secret",
		"person@example.com",
		"nested-secret",
		"array-secret",
	} {
		if strings.Contains(text, secret) {
			t.Fatalf("sanitized request leaked %q: %s", secret, logged)
		}
	}
	if got := gjson.GetBytes(logged, "prompt").String(); got != "keep this" {
		t.Fatalf("prompt = %q", got)
	}
	if got := gjson.GetBytes(logged, "nested.email").String(); got != "pe***@example.com" {
		t.Fatalf("masked email = %q", got)
	}
	if got := gjson.GetBytes(logged, "nested.image_url").String(); got != "<redacted-signed-url>" {
		t.Fatalf("nested image URL = %q", got)
	}
	if got := gjson.GetBytes(logged, "urls.0").String(); got != "<redacted-signed-url>" {
		t.Fatalf("array URL = %q", got)
	}
}

func TestChatGPTWebDeferredHeartbeatIsSSEComment(t *testing.T) {
	prepared := &chatGPTWebPreparedRequest{
		routeModel:     "gpt-5",
		responseFormat: sdktranslator.FormatOpenAI,
	}
	payload := chatGPTWebDeferredHeartbeat(prepared, "chatcmpl-heartbeat", 123)
	if string(payload) != ": chatgpt-web upstream pending\n\n" {
		t.Fatalf("heartbeat = %q", payload)
	}
}

func TestChatGPTWebUnsupportedFunctionToolRequestsProviderFallback(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.prepareRuntimeRequest(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello","tools":[{"type":"function","name":"lookup"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
	if err == nil {
		t.Fatal("expected unsupported tool error")
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
}

func TestChatGPTWebUnsupportedSearchImageRequestsProviderFallback(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-5",
		Payload: []byte(`{
			"model":"gpt-5",
			"input":[{"role":"user","content":[{"type":"input_text","text":"find this"},{"type":"input_image","image_url":"data:image/png;base64,AAAA"}]}],
			"tools":[{"type":"web_search_preview"}],
			"tool_choice":{"type":"web_search_preview"}
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex})
	if err == nil {
		t.Fatal("expected unsupported search image error")
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
}

func TestChatGPTWebSearchIncompleteStatusesAreTerminalFailures(t *testing.T) {
	for _, status := range []string{
		"finished_partial_completion",
		"incomplete",
		"max_tokens",
		"max_output_tokens",
		"content_filter",
		"length",
		"interrupted",
		"expired",
	} {
		if !chatGPTWebSearchStatusFailed(status) {
			t.Fatalf("chatGPTWebSearchStatusFailed(%q) = false", status)
		}
	}
}

func TestChatGPTWebExecutorRejectsIncompleteConversationStream(t *testing.T) {
	server := newChatGPTWebStatusFixture(t, http.StatusOK, 0, false)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), "terminal event") {
		t.Fatalf("Execute() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestChatGPTWebExecutorRejectsOversizedTextRequestBeforeTranslation(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	payload := make([]byte, helps.ChatGPTWebMaxRequestBytes+1)
	copy(payload, `{"model":"gpt-5","input":"`)

	_, err := executor.prepareRuntimeRequest(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: payload,
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
	if err == nil {
		t.Fatal("expected oversized request error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusRequestEntityTooLarge {
		t.Fatalf("status error = %v", err)
	}
}

func TestChatGPTWebExecutorRejectsTextRequestAboveTextLimit(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	payload := []byte(`{"model":"gpt-5","input":"` + strings.Repeat("a", helps.ChatGPTWebMaxTextRequestBytes) + `"}`)

	_, err := executor.prepareRuntimeRequest(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: payload,
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
	if err == nil {
		t.Fatal("expected oversized text request error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusRequestEntityTooLarge {
		t.Fatalf("status error = %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprint(helps.ChatGPTWebMaxTextRequestBytes)) {
		t.Fatalf("text request limit error = %v", err)
	}
}

func TestChatGPTWebExecutorRejectsMalformedOversizedTextBeforeTranslation(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	payload := []byte(`{"model":"gpt-5","input":"` + strings.Repeat("a", helps.ChatGPTWebMaxTextRequestBytes))

	_, err := executor.prepareRuntimeRequest(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: payload,
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusRequestEntityTooLarge {
		t.Fatalf("status error = %v, want pre-translation 413", err)
	}
}

func TestChatGPTWebRawRequestImageDetection(t *testing.T) {
	tests := []struct {
		name   string
		format sdktranslator.Format
		body   string
	}{
		{name: "OpenAI chat", format: sdktranslator.FormatOpenAI, body: `{"messages":[{"content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}]}`},
		{name: "Responses", format: sdktranslator.FormatOpenAIResponse, body: `{"input":[{"content":[{"type":"input_image","image_url":"data:image/png;base64,QQ=="}]}]}`},
		{name: "Claude", format: sdktranslator.FormatClaude, body: `{"messages":[{"content":[{"type":"image","source":{"data":"QQ=="}}]}]}`},
		{name: "Gemini", format: sdktranslator.FormatGemini, body: `{"contents":[{"parts":[{"inlineData":{"mimeType":"image/png","data":"QQ=="}}]}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !chatGPTWebRawRequestHasImageInputs([]byte(test.body), test.format) {
				t.Fatal("image input was not detected")
			}
		})
	}
	if chatGPTWebRawRequestHasImageInputs([]byte(`{"input":"image_url is only text"}`), sdktranslator.FormatCodex) {
		t.Fatal("plain text was treated as an image input")
	}
}

func TestChatGPTWebExecutorRejectsRemoteAndInvalidMessageImagesBeforeNetwork(t *testing.T) {
	tests := []struct {
		name       string
		imageURL   string
		wantStatus int
		wantRetry  bool
	}{
		{name: "remote URL", imageURL: "https://example.test/image.png", wantStatus: http.StatusBadRequest, wantRetry: true},
		{name: "invalid base64", imageURL: "data:image/png;base64,@@@@", wantStatus: http.StatusRequestEntityTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload, errMarshal := json.Marshal(map[string]any{
				"model": "gpt-5",
				"input": []any{map[string]any{
					"role": "user",
					"content": []any{
						map[string]any{"type": "input_text", "text": "describe"},
						map[string]any{"type": "input_image", "image_url": test.imageURL},
					},
				}},
			})
			if errMarshal != nil {
				t.Fatalf("marshal request: %v", errMarshal)
			}
			executor := NewChatGPTWebExecutor(nil, nil)
			_, err := executor.prepareRuntimeRequest(context.Background(), nil, cliproxyexecutor.Request{
				Model:   "gpt-5",
				Payload: payload,
			}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
			var status interface{ StatusCode() int }
			if !errors.As(err, &status) || status.StatusCode() != test.wantStatus {
				t.Fatalf("status error = %v, want %d", err, test.wantStatus)
			}
			var retry interface{ RetryOtherAuth() bool }
			if got := errors.As(err, &retry) && retry.RetryOtherAuth(); got != test.wantRetry {
				t.Fatalf("RetryOtherAuth() = %v, want %v", got, test.wantRetry)
			}
		})
	}
}

func TestChatGPTWebExecutorRejectsTooManyMultimodalUploadsBeforeExecution(t *testing.T) {
	content := make([]any, 0, helps.ChatGPTWebMaxImageInputs+2)
	content = append(content, map[string]any{"type": "input_text", "text": "describe"})
	for range helps.ChatGPTWebMaxImageInputs + 1 {
		content = append(content, map[string]any{
			"type":      "input_image",
			"image_url": "data:image/png;base64,QQ==",
		})
	}
	payload, errMarshal := json.Marshal(map[string]any{
		"model": "gpt-5",
		"input": []any{
			map[string]any{"role": "user", "content": content},
		},
	})
	if errMarshal != nil {
		t.Fatalf("marshal request: %v", errMarshal)
	}

	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.prepareRuntimeRequest(context.Background(), nil, cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: payload,
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex}, false)
	if err == nil {
		t.Fatal("expected image input count error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusRequestEntityTooLarge {
		t.Fatalf("status error = %v", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("%d items", helps.ChatGPTWebMaxImageInputs)) {
		t.Fatalf("image count error = %v", err)
	}
}

func TestChatGPTWebExecutorPreservesRetryAfter(t *testing.T) {
	server := newChatGPTWebStatusFixture(t, http.StatusTooManyRequests, 7*time.Second, true)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"hello"}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil {
		t.Fatal("expected rate-limit error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status error = %v", err)
	}
	var retry interface{ RetryAfter() *time.Duration }
	if !errors.As(err, &retry) {
		t.Fatalf("RetryAfter interface missing: %v", err)
	}
	if retry.RetryAfter() == nil || *retry.RetryAfter() != 7*time.Second {
		t.Fatalf("RetryAfter = %v", retry.RetryAfter())
	}
	var withHeaders interface{ Headers() http.Header }
	if !errors.As(err, &withHeaders) {
		t.Fatalf("Headers interface missing: %v", err)
	}
	if got := withHeaders.Headers().Get("Retry-After"); got != "7" {
		t.Fatalf("Headers().Get(Retry-After) = %q", got)
	}
}

func TestChatGPTWebExecutorTranslatesChatCompletions(t *testing.T) {
	server := newChatGPTWebRuntimeFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL

	response, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hello"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI, ResponseFormat: sdktranslator.FormatOpenAI})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := gjson.GetBytes(response.Payload, "choices.0.message.content").String(); got != "Hello world" {
		t.Fatalf("chat completion text = %q, payload=%s", got, response.Payload)
	}
}

func TestChatGPTWebSearchPreservesConversationRolesAndUsesLatestUserQuery(t *testing.T) {
	messages := []helps.ChatGPTWebMessage{
		{Role: "system", Parts: []helps.ChatGPTWebContentPart{{Text: "Answer in Chinese."}}},
		{Role: "developer", Parts: []helps.ChatGPTWebContentPart{{Text: "Cite sources."}}},
		{Role: "user", Parts: []helps.ChatGPTWebContentPart{{Text: "Compare France and Germany."}}},
		{Role: "assistant", Parts: []helps.ChatGPTWebContentPart{{Text: "France has..."}}},
		{Role: "user", Parts: []helps.ChatGPTWebContentPart{{Text: "What about Spain?"}}},
	}
	prompt, err := chatGPTWebSearchPrompt([]helps.ChatGPTWebMessage{
		messages[0], messages[1], messages[2], messages[3], messages[4],
	})
	if err != nil {
		t.Fatalf("chatGPTWebSearchPrompt() error = %v", err)
	}
	if prompt != "What about Spain?" {
		t.Fatalf("search query = %q", prompt)
	}

	executor := NewChatGPTWebExecutor(nil, nil)
	upstreamMessages, err := executor.buildChatGPTWebConversationMessages(context.Background(), nil, nil, messages)
	if err != nil {
		t.Fatalf("buildChatGPTWebConversationMessages() error = %v", err)
	}
	applyChatGPTWebSearchMessageMetadata(upstreamMessages)
	var roles []string
	for _, message := range upstreamMessages {
		author, _ := message["author"].(map[string]any)
		roles = append(roles, fmt.Sprint(author["role"]))
	}
	if got, want := strings.Join(roles, ","), "system,developer,user,assistant,user"; got != want {
		t.Fatalf("roles = %q, want %q", got, want)
	}
	if _, ok := upstreamMessages[len(upstreamMessages)-1]["metadata"]; !ok {
		t.Fatal("latest user message is missing search metadata")
	}
}

func TestBuildChatGPTWebConversationMessagesPreservesInterleavedPartOrder(t *testing.T) {
	firstImage := "data:image/png;base64," + base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255}))
	secondImage := "data:image/png;base64," + base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{G: 255, A: 255}))
	var server *httptest.Server
	var uploadIndex atomic.Int32
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/backend-api/files":
			index := uploadIndex.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"file_id":    fmt.Sprintf("input-file-%d", index),
				"upload_url": server.URL + fmt.Sprintf("/signed-upload-%d", index),
			})
		case strings.HasPrefix(request.URL.Path, "/signed-upload-"):
			w.WriteHeader(http.StatusCreated)
		case strings.HasPrefix(request.URL.Path, "/backend-api/files/input-file-") &&
			strings.HasSuffix(request.URL.Path, "/uploaded"):
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	auth := chatGPTWebRuntimeAuth()
	credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil {
		t.Fatalf("ParseCredential() error = %v", err)
	}
	client, err := chatgptwebauth.NewClient(credential.Persona, "", credential.Cookies)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	defer client.CloseIdleConnections()

	messages, err := executor.buildChatGPTWebConversationMessages(t.Context(), client, credential, []helps.ChatGPTWebMessage{{
		Role: "user",
		Parts: []helps.ChatGPTWebContentPart{
			{Text: "first"},
			{ImageURL: firstImage},
			{Text: "second"},
			{ImageURL: secondImage},
		},
	}})
	if err != nil {
		t.Fatalf("buildChatGPTWebConversationMessages() error = %v", err)
	}
	content, _ := messages[0]["content"].(map[string]any)
	parts, _ := content["parts"].([]any)
	if len(parts) != 4 {
		t.Fatalf("parts = %#v, want 4 ordered parts", parts)
	}
	if parts[0] != "first" || parts[2] != "second" {
		t.Fatalf("text order = %#v", parts)
	}
	firstPointer, _ := parts[1].(map[string]any)
	secondPointer, _ := parts[3].(map[string]any)
	if firstPointer["asset_pointer"] != "file-service://input-file-1" ||
		secondPointer["asset_pointer"] != "file-service://input-file-2" {
		t.Fatalf("image order = %#v", parts)
	}
}

func TestChatGPTWebExecutorSearchFailureTerminalReturnsError(t *testing.T) {
	server := newChatGPTWebSearchFailureFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0

	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-5",
		Payload: []byte(`{"model":"gpt-5","input":"latest answer","tools":[{"type":"web_search_preview"}],"tool_choice":{"type":"web_search_preview"}}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), "finished_with_error") {
		t.Fatalf("Execute() error = %v", err)
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("status error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestPollChatGPTWebSearchReturnsRateLimitWithoutLooping(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/rate-limited-search" {
			http.NotFound(w, request)
			return
		}
		calls.Add(1)
		w.Header().Set("Retry-After", "7")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.pollChatGPTWebSearch(context.Background(), client, credential, "rate-limited-search")
	if err == nil {
		t.Fatal("expected rate-limit error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status error = %v", err)
	}
	var retryAfter interface{ RetryAfter() *time.Duration }
	if !errors.As(err, &retryAfter) || retryAfter.RetryAfter() == nil || *retryAfter.RetryAfter() != 7*time.Second {
		t.Fatalf("Retry-After error = %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("poll calls = %d, want 1", got)
	}
}

func TestPollChatGPTWebSearchBoundsTransientStatusRetries(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/unavailable-search" {
			http.NotFound(w, request)
			return
		}
		calls.Add(1)
		http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.pollChatGPTWebSearch(context.Background(), client, credential, "unavailable-search")
	if err == nil {
		t.Fatal("expected unavailable error")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("status error = %v", err)
	}
	if got := calls.Load(); got != chatGPTWebSearchMaxPollFailures {
		t.Fatalf("poll calls = %d, want %d", got, chatGPTWebSearchMaxPollFailures)
	}
}

func TestPollChatGPTWebSearchRetriesTransportFailure(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/transport-search" {
			http.NotFound(w, request)
			return
		}
		if calls.Add(1) == 1 {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Errorf("hijack connection: %v", err)
				return
			}
			_ = connection.Close()
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"current_node": "answer",
			"mapping": map[string]any{
				"answer": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "assistant"},
					"content":     map[string]any{"parts": []any{"answer"}},
					"is_complete": true,
				}},
			},
		})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	executor.searchMaxPolls = 3
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	result, err := executor.pollChatGPTWebSearch(context.Background(), client, credential, "transport-search")
	if err != nil {
		t.Fatalf("pollChatGPTWebSearch() error = %v", err)
	}
	if result.Text != "answer" || calls.Load() != 2 {
		t.Fatalf("result = %#v, calls = %d", result, calls.Load())
	}
}

func TestPollChatGPTWebSearchTotalAttemptsIncludeFailures(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/mixed-search" {
			http.NotFound(w, request)
			return
		}
		attempt := calls.Add(1)
		if attempt%2 == 1 {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	executor.searchMaxPolls = 3
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.pollChatGPTWebSearch(context.Background(), client, credential, "mixed-search")
	if err == nil {
		t.Fatal("expected terminal poll error")
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("poll calls = %d, want 3", got)
	}
}

func TestPollChatGPTWebSearchBoundsSuccessfulIncompleteResponses(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/pending-search" {
			http.NotFound(w, request)
			return
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.searchPollInterval = 0
	executor.searchMaxPolls = 3
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.pollChatGPTWebSearch(context.Background(), client, credential, "pending-search")
	if err == nil || !strings.Contains(err.Error(), "remained incomplete after 3 polls") {
		t.Fatalf("pollChatGPTWebSearch() error = %v", err)
	}
	assertChatGPTWebAssetRetryError(t, err)
	if got := calls.Load(); got != 3 {
		t.Fatalf("poll calls = %d, want 3", got)
	}
}

func TestPrepareChatGPTWebSearchMissingConduitSkipsCredentialState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/f/conversation/prepare" {
			http.NotFound(w, request)
			return
		}
		_, _ = io.WriteString(w, `{"unexpected":"response"}`)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.prepareChatGPTWebSearch(context.Background(), client, credential, "query")
	if err == nil || !strings.Contains(err.Error(), "missing conduit token") {
		t.Fatalf("prepareChatGPTWebSearch() error = %v", err)
	}
	assertChatGPTWebAssetRetryError(t, err)
}

func TestPrepareChatGPTWebImageMissingConduitSkipsCredentialState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/f/conversation/prepare" {
			http.NotFound(w, request)
			return
		}
		_, _ = io.WriteString(w, `{"unexpected":"response"}`)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, err = executor.prepareChatGPTWebImageConversation(
		context.Background(),
		client,
		credential,
		chatGPTWebRequirements{},
		"draw",
	)
	if err == nil || !strings.Contains(err.Error(), "missing conduit token") {
		t.Fatalf("prepareChatGPTWebImageConversation() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestPrepareChatGPTWebImageDoesNotConsumeSessionObserverToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/f/conversation/prepare" {
			http.NotFound(w, request)
			return
		}
		if got := request.Header.Get("OpenAI-Sentinel-So-Token"); got != "" {
			t.Errorf("OpenAI-Sentinel-So-Token = %q, want empty", got)
		}
		if got := request.Header.Get("OpenAI-Sentinel-Chat-Requirements-Token"); got != "requirements" {
			t.Errorf("OpenAI-Sentinel-Chat-Requirements-Token = %q", got)
		}
		_, _ = io.WriteString(w, `{"conduit_token":"conduit"}`)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	conduit, err := executor.prepareChatGPTWebImageConversation(
		context.Background(),
		client,
		credential,
		chatGPTWebRequirements{Token: "requirements", SOToken: "one-request-token"},
		"draw",
	)
	if err != nil {
		t.Fatalf("prepareChatGPTWebImageConversation() error = %v", err)
	}
	if conduit != "conduit" {
		t.Fatalf("conduit = %q", conduit)
	}
}

func TestUploadChatGPTWebImageInvalidMetadataSkipsCredentialState(t *testing.T) {
	for _, test := range []struct {
		name     string
		response string
	}{
		{name: "malformed", response: `not-json`},
		{name: "incomplete", response: `{"file_id":"file_00000000000000000000000000000000"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				if request.URL.Path != "/backend-api/files" {
					http.NotFound(w, request)
					return
				}
				_, _ = io.WriteString(w, test.response)
			}))
			defer server.Close()

			executor := NewChatGPTWebExecutor(nil, nil)
			executor.runtimeBaseURL = server.URL
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()

			_, err = executor.uploadChatGPTWebImage(
				context.Background(),
				client,
				credential,
				chatGPTWebPNGDataURL(t),
				"image.png",
			)
			if err == nil {
				t.Fatal("expected invalid upload metadata error")
			}
			var status interface{ StatusCode() int }
			if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
				t.Fatalf("status error = %v", err)
			}
			assertChatGPTWebNonAuthNonRetryError(t, err)
		})
	}
}

func TestExecuteChatGPTWebImageDoesNotRetryAfterConversationAccepted(t *testing.T) {
	var conversationPosts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_, _ = io.WriteString(w, `{"prepare_token":"prepare-token"}`)
		case "/backend-api/sentinel/chat-requirements/finalize":
			_, _ = io.WriteString(w, `{"token":"requirements-token"}`)
		case "/backend-api/f/conversation/prepare":
			_, _ = io.WriteString(w, `{"conduit_token":"conduit-token"}`)
		case "/backend-api/f/conversation":
			conversationPosts.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {invalid-json}\n\n")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	prepared := &chatGPTWebPreparedRequest{
		routeModel: "gpt-image-2",
		request: helps.ChatGPTWebRequest{
			Image: &helps.ChatGPTWebImageRequest{Prompt: "draw"},
		},
	}
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	_, _, err = executor.executeChatGPTWebImage(context.Background(), client, credential, prepared)
	if err == nil {
		t.Fatal("executeChatGPTWebImage() error = nil")
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
	if got := conversationPosts.Load(); got != 1 {
		t.Fatalf("conversation POSTs = %d, want 1", got)
	}
}

func TestPollChatGPTWebImageConversationReturnsFailureTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/failed-image" {
			http.NotFound(w, request)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
			"failed": map[string]any{"message": map[string]any{
				"author":      map[string]any{"role": "tool"},
				"create_time": 1,
				"metadata": map[string]any{
					"async_task_type": "image_gen",
					"finish_details":  map[string]any{"type": "finished_with_error"},
				},
				"content": map[string]any{"parts": []any{}},
			}},
		}})
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		ConversationID: "failed-image",
	}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "finished_with_error") {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("status error = %v", err)
	}
}

func TestPollChatGPTWebImageConversationPrefersCurrentFailureRegardlessOfResponseOrder(t *testing.T) {
	testCases := []struct {
		name              string
		taskDelay         time.Duration
		conversationDelay time.Duration
	}{
		{name: "task first", conversationDelay: 15 * time.Millisecond},
		{name: "conversation first", taskDelay: 15 * time.Millisecond},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/backend-api/tasks":
					time.Sleep(testCase.taskDelay)
					_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
						"conversation_id": "current-conversation-failed",
						"status":          "completed",
						"image_gen_message": map[string]any{
							"author":   map[string]any{"role": "tool"},
							"status":   "finished_successfully",
							"metadata": map[string]any{"async_task_type": "image_gen"},
							"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://stale-task-image"}}},
						},
					}}})
				case "/backend-api/conversation/current-conversation-failed":
					time.Sleep(testCase.conversationDelay)
					writeFailedChatGPTWebImageConversation(w, "finished_with_error")
				default:
					http.NotFound(w, request)
				}
			}))
			defer server.Close()

			executor := NewChatGPTWebExecutor(nil, nil)
			executor.runtimeBaseURL = server.URL
			executor.imageInitialWait = 0
			executor.imagePollInterval = time.Millisecond
			executor.imageSettleWait = 50 * time.Millisecond
			executor.imageMaxPolls = 8
			client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
			if err != nil {
				t.Fatal(err)
			}
			defer client.CloseIdleConnections()
			accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "current-conversation-failed"}
			err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false)
			if err == nil || !strings.Contains(err.Error(), "finished_with_error") {
				t.Fatalf("poll error = %v, accumulator = %+v", err, accumulator)
			}
		})
	}
}

func TestPollChatGPTWebImageConversationWaitsForCurrentConversationWithinTaskGrace(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "task-grace-conversation",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}})
		case "/backend-api/conversation/task-grace-conversation":
			if conversationPolls.Add(1) < 3 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			time.Sleep(15 * time.Millisecond)
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 50 * time.Millisecond
	executor.imageMaxPolls = 8
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "task-grace-conversation"}
	startedAt := time.Now()
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < 10*time.Millisecond {
		t.Fatalf("poll returned before conversation grace: %v", elapsed)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("file IDs = %v, conversation polls = %d", accumulator.FileIDs, conversationPolls.Load())
	}
	if got := conversationPolls.Load(); got < 3 {
		t.Fatalf("conversation polls = %d, want at least 3", got)
	}
}

func TestPollChatGPTWebImageConversationUsesCompletedTask(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "task-image",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/task-image":
			conversationPolls.Add(1)
			http.Error(w, "conversation polling should not be needed", http.StatusInternalServerError)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "task-image"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 3 {
		t.Fatalf("conversation polls = %d, want at least 3 while confirming and merging a stable task snapshot", got)
	}
}

func TestPollChatGPTWebImageConversationDoesNotRequireConfiguredResultCap(t *testing.T) {
	var taskPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "single-result",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://only-result"}}},
				},
			}}})
		case "/backend-api/conversation/single-result":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 6
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "single-result"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"only-result"}) || taskPolls.Load() < 3 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationDoesNotLetRetryableTaskErrorOverrideTerminalConversation(t *testing.T) {
	var taskPolls atomic.Int32
	taskResponded := make(chan struct{})
	var taskRespondedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			taskRespondedOnce.Do(func() { close(taskResponded) })
		case "/backend-api/conversation/retryable-task-error":
			select {
			case <-taskResponded:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "retryable-task-error"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() < 1 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationUsesTerminalConversationWhileTaskRemainsPending(t *testing.T) {
	var taskPolls atomic.Int32
	taskResponded := make(chan struct{})
	var taskRespondedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "pending-task-terminal-poll",
				"status":          "running",
			}}})
			taskRespondedOnce.Do(func() { close(taskResponded) })
		case "/backend-api/conversation/pending-task-terminal-poll":
			select {
			case <-taskResponded:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "pending-task-terminal-poll"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() < 1 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationDoesNotWaitForBlockedTaskRequest(t *testing.T) {
	taskStarted := make(chan struct{})
	taskCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			startedOnce.Do(func() { close(taskStarted) })
			<-request.Context().Done()
			canceledOnce.Do(func() { close(taskCanceled) })
		case "/backend-api/conversation/blocked-poll-task":
			select {
			case <-taskStarted:
			case <-request.Context().Done():
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 4
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "blocked-poll-task"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	select {
	case <-taskCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked task request was not canceled")
	}
}

func TestPollChatGPTWebImageConversationDoesNotWaitForBlockedConversationWhenTaskCompletes(t *testing.T) {
	conversationStarted := make(chan struct{})
	conversationCanceled := make(chan struct{})
	var startedOnce sync.Once
	var canceledOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			select {
			case <-conversationStarted:
			case <-request.Context().Done():
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "blocked-poll-conversation",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/blocked-poll-conversation":
			startedOnce.Do(func() { close(conversationStarted) })
			<-request.Context().Done()
			canceledOnce.Do(func() { close(conversationCanceled) })
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 10 * time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 5
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "blocked-poll-conversation"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	select {
	case <-conversationCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked conversation request was not canceled")
	}
}

func TestPollChatGPTWebImageConversationPrefersConversationOutputOverTaskFailure(t *testing.T) {
	taskServed := make(chan struct{})
	var servedOnce sync.Once
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "failed-task-successful-conversation",
				"status":          "failed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{}},
				},
			}}})
			servedOnce.Do(func() { close(taskServed) })
		case "/backend-api/conversation/failed-task-successful-conversation":
			select {
			case <-taskServed:
			case <-request.Context().Done():
				return
			}
			if conversationPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 4
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "failed-task-successful-conversation"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 2 {
		t.Fatalf("conversation polls = %d, want at least 2", got)
	}
}

func TestPollChatGPTWebImageConversationReturnsStableTaskFailurePromptly(t *testing.T) {
	var taskPolls atomic.Int32
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "stable-task-failure-poll",
				"status":          "failed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{}},
				},
			}}})
		case "/backend-api/conversation/stable-task-failure-poll":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 20
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		ConversationID: "stable-task-failure-poll",
	}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "failed") {
		t.Fatalf("poll error = %v", err)
	}
	if got := taskPolls.Load(); got >= 8 {
		t.Fatalf("task polls = %d, stable failure was not returned promptly", got)
	}
	if got := conversationPolls.Load(); got < 1 || got >= 8 {
		t.Fatalf("conversation polls = %d, want one bounded final confirmation", got)
	}
}

func TestPollChatGPTWebImageConversationReplacesPreviousCurrentNodeBranch(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/current-node-switch":
			fileID := "old-branch"
			terminal := false
			if conversationPolls.Add(1) >= 2 {
				fileID = "new-branch"
				terminal = true
			}
			metadata := map[string]any{"async_task_type": "image_gen"}
			if terminal {
				metadata["finish_details"] = map[string]any{"type": "finished_successfully"}
				metadata["is_complete"] = true
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"current_node": fileID,
				"mapping": map[string]any{
					fileID: map[string]any{
						"parent": "",
						"message": map[string]any{
							"author":      map[string]any{"role": "tool"},
							"create_time": 1,
							"metadata":    metadata,
							"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://" + fileID}}},
						},
					},
				},
			})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 5
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{
		ConversationID: "current-node-switch",
		FileIDs:        []string{"streamed"},
	}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"streamed", "new-branch"}) {
		t.Fatalf("file IDs = %v, want the initial stream output plus only the latest current_node branch", accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 2 {
		t.Fatalf("conversation polls = %d, want at least 2", got)
	}
}

func TestPollChatGPTWebImageConversationUsesStableSedimentWhenTasksAreMalformed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_, _ = io.WriteString(w, `{broken`)
		case "/backend-api/conversation/malformed-task-stable-sediment":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"generated": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata":    map[string]any{"async_task_type": "image_gen"},
					"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "sediment://generated"}}},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 4
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "malformed-task-stable-sediment"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.SedimentIDs, []string{"generated"}) {
		t.Fatalf("sediment IDs = %v", accumulator.SedimentIDs)
	}
}

func TestPollChatGPTWebImageConversationHonorsTaskRetryAfter(t *testing.T) {
	var taskPolls atomic.Int32
	var firstTaskPoll time.Time
	var secondTaskPoll time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			if poll == 1 {
				firstTaskPoll = time.Now()
				w.Header().Set("Retry-After", "1")
				http.Error(w, "rate limited", http.StatusTooManyRequests)
				return
			}
			if poll == 2 {
				secondTaskPoll = time.Now()
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/retry-after-poll":
			if taskPolls.Load() < 2 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 250 * time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 8
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "retry-after-poll"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("result files = %v", accumulator.FileIDs)
	}
	if firstTaskPoll.IsZero() || secondTaskPoll.IsZero() {
		t.Fatalf("task poll timestamps = %v, %v", firstTaskPoll, secondTaskPoll)
	}
	if delay := secondTaskPoll.Sub(firstTaskPoll); delay < 850*time.Millisecond {
		t.Fatalf("retry delay = %v, want Retry-After delay", delay)
	}
}

func TestPollChatGPTWebImageConversationMergesConversationBeforeStableTaskReturn(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "stable-task-late-conversation",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}})
		case "/backend-api/conversation/stable-task-late-conversation":
			if conversationPolls.Add(1) < 3 {
				writeChatGPTWebImageConversation(w, "first")
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 6
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "stable-task-late-conversation"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got < 3 {
		t.Fatalf("conversation polls = %d, want at least 3", got)
	}
}

func TestPollChatGPTWebImageConversationTreatsDuplicateConversationReferenceAsNoAdditionalOutput(t *testing.T) {
	var taskPolls atomic.Int32
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			taskPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "duplicate-conversation-reference",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/duplicate-conversation-reference":
			conversationPolls.Add(1)
			writeChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 0
	executor.imageMaxPolls = 20
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "duplicate-conversation-reference"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := taskPolls.Load(); got >= 10 {
		t.Fatalf("task polls = %d, duplicate conversation reference blocked task fallback", got)
	}
	if got := conversationPolls.Load(); got >= 10 {
		t.Fatalf("conversation polls = %d, duplicate reference should not require exhaustion", got)
	}
}

func TestPollChatGPTWebImageConversationRetriesAtTaskFallbackDeadline(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "fallback-final-refresh",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}})
		case "/backend-api/conversation/fallback-final-refresh":
			poll := conversationPolls.Add(1)
			if poll < 4 {
				writeChatGPTWebImageConversation(w, "first")
				return
			}
			if poll == 4 {
				http.Error(w, "not ready", http.StatusTooManyRequests)
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = 100 * time.Millisecond
	executor.imageSettleWait = 10 * time.Millisecond
	executor.imageMaxPolls = 8
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "fallback-final-refresh"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("file IDs = %v, conversation polls = %d", accumulator.FileIDs, conversationPolls.Load())
	}
	if got := conversationPolls.Load(); got < 5 {
		t.Fatalf("conversation polls = %d, want a final refresh after task fallback grace", got)
	}
}

func TestPollChatGPTWebImageConversationKeepsStableTaskAfterLaterConversationError(t *testing.T) {
	var taskPolls atomic.Int32
	taskStable := make(chan struct{})
	var taskStableOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			if poll >= 4 {
				<-request.Context().Done()
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "stable-task-conversation-error",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
			if poll == 3 {
				taskStableOnce.Do(func() { close(taskStable) })
			}
		case "/backend-api/conversation/stable-task-conversation-error":
			select {
			case <-taskStable:
				time.Sleep(10 * time.Millisecond)
				http.Error(w, "conversation is unavailable", http.StatusBadRequest)
			default:
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
			}
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 20 * time.Millisecond
	executor.imageMaxPolls = 10
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "stable-task-conversation-error"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("stable task output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
}

func TestPollChatGPTWebImageConversationWaitsForInFlightTaskAfterConversationError(t *testing.T) {
	taskStarted := make(chan struct{})
	conversationFailed := make(chan struct{})
	releaseTask := make(chan struct{})
	var taskStartedOnce sync.Once
	var conversationFailedOnce sync.Once
	var taskPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			poll := taskPolls.Add(1)
			if poll == 1 {
				taskStartedOnce.Do(func() { close(taskStarted) })
				select {
				case <-releaseTask:
				case <-request.Context().Done():
					return
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "in-flight-task-after-conversation-error",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				},
			}}})
		case "/backend-api/conversation/in-flight-task-after-conversation-error":
			select {
			case <-taskStarted:
			case <-request.Context().Done():
				return
			}
			conversationFailedOnce.Do(func() { close(conversationFailed) })
			http.Error(w, "conversation unavailable", http.StatusBadRequest)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	go func() {
		<-conversationFailed
		time.Sleep(5 * time.Millisecond)
		close(releaseTask)
	}()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageInitialWait = 0
	executor.imagePollInterval = time.Millisecond
	executor.imageSettleWait = 2 * time.Millisecond
	executor.imageMaxPolls = 8
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "in-flight-task-after-conversation-error"}
	if err = executor.pollChatGPTWebImageConversation(ctx, client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("task output = terminal %t, files %v, task polls %d", accumulator.Terminal, accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationKeepsSuccessfulTaskWhenSiblingFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{
				map[string]any{
					"conversation_id": "mixed-task-results",
					"status":          "completed",
					"image_gen_message": map[string]any{
						"author":   map[string]any{"role": "tool"},
						"status":   "finished_successfully",
						"metadata": map[string]any{"async_task_type": "image_gen"},
						"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
					},
				},
				map[string]any{
					"conversation_id": "mixed-task-results",
					"status":          "failed",
					"image_gen_message": map[string]any{
						"author":   map[string]any{"role": "tool"},
						"metadata": map[string]any{"async_task_type": "image_gen"},
						"content":  map[string]any{"parts": []any{}},
					},
				},
			}})
		case "/backend-api/conversation/mixed-task-results":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 6
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "mixed-task-results"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("file IDs = %v", accumulator.FileIDs)
	}
}

func TestPollChatGPTWebImageConversationFallsBackWhenCompletedTaskHasNoOutput(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "task-output-pending",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{}},
				},
			}}})
		case "/backend-api/conversation/task-output-pending":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"generated": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata": map[string]any{
						"async_task_type": "image_gen",
						"finish_details":  map[string]any{"type": "finished_successfully"},
						"is_complete":     true,
					},
					"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://generated"}}},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "task-output-pending"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !accumulator.Terminal || !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) {
		t.Fatalf("conversation fallback output = terminal %t, files %v", accumulator.Terminal, accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got != 2 {
		t.Fatalf("conversation polls = %d, want 2 stable snapshots", got)
	}
}

func TestPollChatGPTWebImageConversationCompletesPartialTaskResultsFromConversation(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "partial-task-image",
				"status":          "completed",
				"image_gen_message": map[string]any{
					"author":   map[string]any{"role": "tool"},
					"status":   "finished_successfully",
					"metadata": map[string]any{"async_task_type": "image_gen"},
					"content":  map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				},
			}}})
		case "/backend-api/conversation/partial-task-image":
			conversationPolls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"first": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 1,
					"metadata":    map[string]any{"async_task_type": "image_gen"},
					"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://first"}}},
				}},
				"second": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "tool"},
					"create_time": 2,
					"status":      "finished_successfully",
					"metadata":    map[string]any{"async_task_type": "image_gen", "is_complete": true},
					"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://second"}}},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "partial-task-image"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) {
		t.Fatalf("file IDs = %v, want both requested images", accumulator.FileIDs)
	}
	if got := conversationPolls.Load(); got != 2 {
		t.Fatalf("conversation polls = %d, want 2 stable snapshots", got)
	}
}

func TestPollChatGPTWebImageConversationClearsDisappearedPendingTask(t *testing.T) {
	var taskPolls atomic.Int32
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			if taskPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
					"conversation_id": "disappeared-poll-task",
					"status":          "running",
				}}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/disappeared-poll-task":
			if conversationPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "disappeared-poll-task"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() < 2 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationClearsPendingWhenTasksBecomeMalformed(t *testing.T) {
	var taskPolls atomic.Int32
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			if taskPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
					"conversation_id": "malformed-poll-task",
					"status":          "running",
				}}})
				return
			}
			_, _ = io.WriteString(w, `{broken`)
		case "/backend-api/conversation/malformed-poll-task":
			if conversationPolls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{}})
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "generated")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "malformed-poll-task"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"generated"}) || taskPolls.Load() != 2 {
		t.Fatalf("result files = %v, task polls = %d", accumulator.FileIDs, taskPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationReturnsProtocolErrorAfterPartialOutput(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/partial-poll-protocol-error":
			if conversationPolls.Add(1) == 1 {
				_, _ = io.WriteString(w, `{"current_node":"partial","mapping":{"partial":{"message":{"author":{"role":"tool"},"metadata":{"async_task_type":"image_gen"},"content":{"parts":[{"asset_pointer":"file-service://partial"}]}}}}}`)
				return
			}
			_, _ = io.WriteString(w, `{broken`)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "partial-poll-protocol-error"}
	err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false)
	if err == nil || !strings.Contains(err.Error(), "decode chatgpt web conversation") {
		t.Fatalf("partial output protocol error = %v", err)
	}
}

func TestPollChatGPTWebImageConversationDoesNotReturnEarlyReferenceWhileTaskPending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{map[string]any{
				"conversation_id": "early-reference",
				"status":          "running",
			}}})
		case "/backend-api/conversation/early-reference":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"image": map[string]any{"message": map[string]any{
					"author": map[string]any{"role": "tool"}, "metadata": map[string]any{"async_task_type": "image_gen"},
					"content": map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://preview"}}},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		ConversationID: "early-reference",
	}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "remained incomplete") {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
}

func TestPollChatGPTWebImageConversationWaitsForDelayedSecondTerminalImage(t *testing.T) {
	var conversationPolls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/backend-api/tasks":
			_ = json.NewEncoder(w).Encode(map[string]any{"tasks": []any{}})
		case "/backend-api/conversation/delayed-second-image":
			if conversationPolls.Add(1) == 1 {
				writeTerminalChatGPTWebImageConversation(w, "first")
				return
			}
			writeTerminalChatGPTWebImageConversation(w, "first", "second")
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	accumulator := &helps.ChatGPTWebImageAccumulator{ConversationID: "delayed-second-image"}
	if err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, accumulator, nil, false); err != nil {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if !reflect.DeepEqual(accumulator.FileIDs, []string{"first", "second"}) || conversationPolls.Load() != 3 {
		t.Fatalf("result files = %v, conversation polls = %d", accumulator.FileIDs, conversationPolls.Load())
	}
}

func TestPollChatGPTWebImageConversationStopsAfterMaximumPolls(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/backend-api/conversation/pending-image" {
			http.NotFound(w, request)
			return
		}
		polls.Add(1)
		_, _ = io.WriteString(w, `{"mapping":{}}`)
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	executor.imageMaxPolls = 3
	disableChatGPTWebImagePollWaits(executor)
	client, credential, err := executor.newRuntimeClient(chatGPTWebRuntimeAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()

	err = executor.pollChatGPTWebImageConversation(context.Background(), client, credential, &helps.ChatGPTWebImageAccumulator{
		ConversationID: "pending-image",
	}, nil, false)
	if err == nil || !strings.Contains(err.Error(), "remained incomplete after 3 polls") {
		t.Fatalf("pollChatGPTWebImageConversation() error = %v", err)
	}
	if got := polls.Load(); got != 3 {
		t.Fatalf("poll count = %d, want 3", got)
	}
	assertChatGPTWebAssetRetryError(t, err)
}

func TestChatGPTWebExecutorImageEditUploadsCompositedMask(t *testing.T) {
	fixture := newChatGPTWebImageEditFixture(t)
	defer fixture.server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = fixture.server.URL
	disableChatGPTWebImagePollWaits(executor)

	input := chatGPTWebPNGDataURL(t, color.NRGBA{R: 255, A: 255}, color.NRGBA{G: 255, A: 255})
	mask := chatGPTWebPNGDataURL(t, color.NRGBA{A: 0}, color.NRGBA{A: 255})
	response, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-image-2",
			"input":[{"role":"user","content":[
				{"type":"input_text","text":"edit"},
				{"type":"input_image","image_url":"` + input + `"}
			]}],
			"tools":[{"type":"image_generation","input_image_mask":{"image_url":"` + mask + `"}}]
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	expected := base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{B: 255, A: 255}))
	if got := gjson.GetBytes(response.Payload, "response.output.0.result").String(); got != expected {
		t.Fatalf("image result = %q", got)
	}

	fixture.mu.Lock()
	uploaded := bytes.Clone(fixture.uploaded)
	fixture.mu.Unlock()
	decoded, err := png.Decode(bytes.NewReader(uploaded))
	if err != nil {
		t.Fatalf("decode uploaded image: %v", err)
	}
	_, _, _, firstAlpha := decoded.At(0, 0).RGBA()
	_, _, _, secondAlpha := decoded.At(1, 0).RGBA()
	if firstAlpha != 0 || secondAlpha != 0xffff {
		t.Fatalf("uploaded alpha = (%d, %d)", firstAlpha, secondAlpha)
	}
}

func TestChatGPTWebExecutorSettlesMultipleImageResults(t *testing.T) {
	server := newChatGPTWebMultiImageFixture(t)
	defer server.Close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := executor.Execute(ctx, chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-image-2",
			"input":"draw two",
			"tools":[{"type":"image_generation"}]
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	output := gjson.GetBytes(response.Payload, "response.output").Array()
	if len(output) != 2 {
		t.Fatalf("output count = %d, payload=%s", len(output), response.Payload)
	}
	first := base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255}))
	second := base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, color.NRGBA{G: 255, A: 255}))
	if output[0].Get("result").String() != first || output[1].Get("result").String() != second {
		t.Fatalf("results = %s", response.Payload)
	}
}

func TestChatGPTWebExecutorRejectsPartialImagesWhenConversationSettleFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
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
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"partial-image\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://first-image\"}]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/partial-image":
			http.Error(w, "settle failed", http.StatusBadRequest)
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = server.URL
	disableChatGPTWebImagePollWaits(executor)
	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model:   "gpt-image-2",
		Payload: []byte(`{"model":"gpt-image-2","input":"draw","tools":[{"type":"image_generation"}]}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), "did not settle") {
		t.Fatalf("Execute() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestChatGPTWebExecutorImageStreamReturnsBeforeUpstreamCompletion(t *testing.T) {
	fixture := newChatGPTWebBlockingImageFixture(t)
	defer fixture.close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = fixture.server.URL
	disableChatGPTWebImagePollWaits(executor)
	executor.streamInitialWait = 10 * time.Millisecond
	executor.streamHeartbeat = 10 * time.Millisecond
	passthroughState := &cliproxyexecutor.ImageGenerationStreamPassthroughState{}

	startedAt := time.Now()
	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"input":"draw",
			"stream":true,
			"tools":[{"type":"image_generation"}]
		}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatCodex,
		ResponseFormat: sdktranslator.FormatCodex,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughMetadataKey:      true,
			cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey: passthroughState,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 100*time.Millisecond {
		t.Fatalf("ExecuteStream() blocked for %s", elapsed)
	}
	select {
	case <-fixture.started:
	case <-time.After(time.Second):
		t.Fatal("image conversation did not start")
	}
	if passthroughState.Enabled() {
		t.Fatal("image passthrough was enabled before the first stream payload")
	}

	for heartbeats := 0; heartbeats < 2; {
		select {
		case chunk := <-result.Chunks:
			if chunk.Err != nil {
				t.Fatalf("heartbeat chunk error = %v", chunk.Err)
			}
			if cliproxyexecutor.IsBootstrapCommitStreamChunk(chunk) {
				continue
			}
			if !strings.Contains(string(chunk.Payload), "chatgpt-web upstream pending") {
				t.Fatalf("heartbeat chunk = %q", chunk.Payload)
			}
			heartbeats++
		case <-time.After(time.Second):
			t.Fatalf("heartbeat %d was not emitted", heartbeats+1)
		}
	}
	if passthroughState.Enabled() {
		t.Fatal("comment-only heartbeat committed image passthrough")
	}
	fixture.release()

	var output strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		output.Write(chunk.Payload)
		output.WriteByte('\n')
	}
	if !strings.Contains(output.String(), `"type":"response.completed"`) ||
		!strings.Contains(output.String(), `"type":"image_generation_call"`) {
		t.Fatalf("completed image stream missing: %s", output.String())
	}
	if !passthroughState.Enabled() {
		t.Fatal("image passthrough was not enabled for semantic image output")
	}
}

func TestChatGPTWebExecutorDoesNotEnableUnrequestedImagePassthrough(t *testing.T) {
	fixture := newChatGPTWebBlockingImageFixture(t)
	defer fixture.close()
	executor := NewChatGPTWebExecutor(nil, nil)
	executor.runtimeBaseURL = fixture.server.URL
	disableChatGPTWebImagePollWaits(executor)
	executor.streamInitialWait = time.Millisecond
	passthroughState := &cliproxyexecutor.ImageGenerationStreamPassthroughState{}

	result, err := executor.ExecuteStream(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"input":"draw",
			"stream":true,
			"tools":[{"type":"image_generation"}]
		}`),
	}, cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FormatCodex,
		ResponseFormat: sdktranslator.FormatCodex,
		Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationStreamPassthroughStateMetadataKey: passthroughState,
		},
	})
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}
	select {
	case chunk := <-result.Chunks:
		if chunk.Err != nil {
			t.Fatalf("heartbeat chunk error = %v", chunk.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat was not emitted")
	}
	if passthroughState.Enabled() {
		t.Fatal("image passthrough enabled without request metadata")
	}
	fixture.release()
	for range result.Chunks {
	}
}

func TestChatGPTWebExecutorRejectsUnenforceableImageFormatBeforeUpstream(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)

	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"input":"draw",
			"tools":[{"type":"image_generation","output_format":"webp"}]
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), `output_format "webp"`) {
		t.Fatalf("Execute() error = %v", err)
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("status error = %v", err)
	}
	assertChatGPTWebAssetRetryError(t, err)
}

func TestBuildChatGPTWebImageCompletedEventRejectsOversizedOutput(t *testing.T) {
	imageData := append(chatGPTWebPNGBytes(t, color.NRGBA{A: 255}), make([]byte, chatGPTWebMaxImageResponseBytes)...)
	_, err := buildChatGPTWebImageCompletedEvent("gpt-image-2", "png", [][]byte{
		imageData,
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "image response exceeds") {
		t.Fatalf("buildChatGPTWebImageCompletedEvent() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestBuildChatGPTWebImageCompletedEventConvertsJPEGToPNG(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	source.SetNRGBA(0, 0, color.NRGBA{R: 255, A: 255})
	var jpegData bytes.Buffer
	if err := jpeg.Encode(&jpegData, source, nil); err != nil {
		t.Fatalf("encode JPEG: %v", err)
	}

	usage := map[string]any{"input_tokens": int64(7), "output_tokens": int64(0), "total_tokens": int64(7)}
	completed, err := buildChatGPTWebImageCompletedEvent("gpt-image-2", "png", [][]byte{jpegData.Bytes()}, usage)
	if err != nil {
		t.Fatalf("buildChatGPTWebImageCompletedEvent() error = %v", err)
	}
	if got := gjson.GetBytes(completed, "response.output.0.output_format").String(); got != "png" {
		t.Fatalf("output format = %q, want png", got)
	}
	if got := gjson.GetBytes(completed, "response.usage.input_tokens").Int(); got != 7 {
		t.Fatalf("input tokens = %d, want 7", got)
	}
	encoded := gjson.GetBytes(completed, "response.output.0.result").String()
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode image result: %v", err)
	}
	if got := chatGPTWebImageOutputFormat(decoded); got != "png" {
		t.Fatalf("converted image format = %q, want png", got)
	}
}

func TestBuildChatGPTWebImageCompletedEventDefaultsToPNG(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	source.SetNRGBA(0, 0, color.NRGBA{G: 255, A: 255})
	var jpegData bytes.Buffer
	if err := jpeg.Encode(&jpegData, source, nil); err != nil {
		t.Fatalf("encode JPEG: %v", err)
	}

	completed, err := buildChatGPTWebImageCompletedEvent("gpt-image-2", "", [][]byte{jpegData.Bytes()}, nil)
	if err != nil {
		t.Fatalf("buildChatGPTWebImageCompletedEvent() error = %v", err)
	}
	if got := gjson.GetBytes(completed, "response.output.0.output_format").String(); got != "png" {
		t.Fatalf("output format = %q, want png", got)
	}
	decoded, err := base64.StdEncoding.DecodeString(gjson.GetBytes(completed, "response.output.0.result").String())
	if err != nil {
		t.Fatalf("decode image result: %v", err)
	}
	if got := chatGPTWebImageOutputFormat(decoded); got != "png" {
		t.Fatalf("default image format = %q, want png", got)
	}
}

func TestChatGPTWebImageOutputFormatUsesImageBytes(t *testing.T) {
	if got := chatGPTWebImageOutputFormat(chatGPTWebPNGBytes(t, color.NRGBA{A: 255})); got != "png" {
		t.Fatalf("PNG format = %q", got)
	}
	webp := append([]byte("RIFF\x00\x00\x00\x00WEBPVP8X"), make([]byte, 16)...)
	if got := chatGPTWebImageOutputFormat(webp); got != "webp" {
		t.Fatalf("WebP format = %q", got)
	}
	if got := chatGPTWebImageOutputFormat([]byte("not-an-image")); got != "" {
		t.Fatalf("unknown format = %q", got)
	}
}

func TestChatGPTWebExecutorRejectsExactImageSizingBeforeNetwork(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	_, err := executor.Execute(context.Background(), chatGPTWebRuntimeAuth(), cliproxyexecutor.Request{
		Model: "gpt-image-2",
		Payload: []byte(`{
			"model":"gpt-5.4",
			"input":"draw",
			"tools":[{"type":"image_generation","size":"1024x1024"}]
		}`),
	}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
	if err == nil || !strings.Contains(err.Error(), "exact image size") {
		t.Fatalf("Execute() error = %v", err)
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
		t.Fatalf("StatusCode() error = %v", err)
	}
	var skipper interface{ SkipAuthResult() bool }
	if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
		t.Fatalf("SkipAuthResult() error = %v", err)
	}
	var retry interface{ RetryOtherAuth() bool }
	if !errors.As(err, &retry) || !retry.RetryOtherAuth() {
		t.Fatalf("RetryOtherAuth() error = %v", err)
	}
}

func TestChatGPTWebEncodedImageSizeEnforcesLimit(t *testing.T) {
	if _, err := chatGPTWebEncodedImageSize("data:image/png;base64,QUJDRA==", 3); err == nil {
		t.Fatal("expected encoded image size limit error")
	}
	if size, err := chatGPTWebEncodedImageSize("data:image/png;base64,QUJDRA==", 4); err != nil || size != 4 {
		t.Fatalf("encoded image size = %d, error = %v", size, err)
	}
}

func TestValidateChatGPTWebImageConfigRejectsPixelLimit(t *testing.T) {
	if err := validateChatGPTWebImageConfig(image.Config{Width: 4097, Height: 3072}); err == nil {
		t.Fatal("expected image pixel limit error")
	}
	if err := validateChatGPTWebImageConfig(image.Config{Width: 4096, Height: 3072}); err != nil {
		t.Fatalf("valid image config error = %v", err)
	}
}

func TestValidateChatGPTWebImageEditMemoryAccountsForSourceAndMask(t *testing.T) {
	if err := validateChatGPTWebImageEditMemory(image.Config{Width: 3073, Height: 2048}); err == nil {
		t.Fatal("expected image edit decoded memory limit error")
	}
	if err := validateChatGPTWebImageEditMemory(image.Config{Width: 3072, Height: 2048}); err != nil {
		t.Fatalf("valid image edit config error = %v", err)
	}
}

func TestReadChatGPTWebBoundedBody(t *testing.T) {
	if _, err := readChatGPTWebBoundedBody(strings.NewReader("four"), 3); err == nil {
		t.Fatal("expected bounded body error")
	}
	payload, err := readChatGPTWebBoundedBody(strings.NewReader("four"), 4)
	if err != nil || string(payload) != "four" {
		t.Fatalf("bounded body = %q, error = %v", payload, err)
	}
}

func newChatGPTWebStatusFixture(t *testing.T, conversationStatus int, retryAfter time.Duration, complete bool) *httptest.Server {
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
			if retryAfter > 0 {
				w.Header().Set("Retry-After", "7")
			}
			if conversationStatus == http.StatusOK {
				w.Header().Set("Content-Type", "text/event-stream")
			}
			w.WriteHeader(conversationStatus)
			if conversationStatus != http.StatusOK {
				_, _ = io.WriteString(w, `{"error":"rate limited"}`)
				return
			}
			_, _ = io.WriteString(w, "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"partial\"]}}}\n\n")
			if complete {
				_, _ = io.WriteString(w, "data: [DONE]\n\n")
			}
		default:
			http.NotFound(w, request)
		}
	}))
}

func newChatGPTWebSearchFailureFixture(t *testing.T) *httptest.Server {
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
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"failed-search\"}\n\ndata: [DONE]\n\n")
		case "/backend-api/conversation/failed-search":
			_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
				"answer": map[string]any{"message": map[string]any{
					"author":      map[string]any{"role": "assistant"},
					"create_time": 1,
					"content":     map[string]any{"parts": []any{"upstream failed"}},
					"metadata":    map[string]any{"finish_details": map[string]any{"type": "finished_with_error"}},
				}},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
}

type chatGPTWebImageEditFixture struct {
	server        *httptest.Server
	mu            sync.Mutex
	uploaded      []byte
	turnMessageID string
}

type chatGPTWebBlockingImageFixture struct {
	server        *httptest.Server
	started       chan struct{}
	releaseOnce   sync.Once
	releaseCh     chan struct{}
	mu            sync.Mutex
	turnMessageID string
}

func newChatGPTWebBlockingImageFixture(t *testing.T) *chatGPTWebBlockingImageFixture {
	t.Helper()
	fixture := &chatGPTWebBlockingImageFixture{
		started:   make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
	imageData := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	var startedOnce sync.Once
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
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
			var body struct {
				Messages []struct {
					ID string `json:"id"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode blocking image conversation request: %v", err)
			} else if len(body.Messages) > 0 {
				fixture.mu.Lock()
				fixture.turnMessageID = body.Messages[0].ID
				fixture.mu.Unlock()
			}
			startedOnce.Do(func() { close(fixture.started) })
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			select {
			case <-fixture.releaseCh:
			case <-request.Context().Done():
				return
			}
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"blocking-image\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://blocking-file\"}]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/blocking-image":
			fixture.mu.Lock()
			turnMessageID := fixture.turnMessageID
			fixture.mu.Unlock()
			writeChatGPTWebImageConversationForTurn(w, turnMessageID, true, "blocking-file")
		case "/backend-api/files/blocking-file/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": fixture.server.URL + "/blocking-image.png"})
		case "/blocking-image.png":
			_, _ = w.Write(imageData)
		default:
			http.NotFound(w, request)
		}
	}))
	return fixture
}

func (fixture *chatGPTWebBlockingImageFixture) release() {
	if fixture == nil {
		return
	}
	fixture.releaseOnce.Do(func() { close(fixture.releaseCh) })
}

func (fixture *chatGPTWebBlockingImageFixture) close() {
	if fixture == nil {
		return
	}
	fixture.release()
	if fixture.server != nil {
		fixture.server.Close()
	}
}

func newChatGPTWebImageEditFixture(t *testing.T) *chatGPTWebImageEditFixture {
	t.Helper()
	fixture := &chatGPTWebImageEditFixture{}
	outputImage := chatGPTWebPNGBytes(t, color.NRGBA{B: 255, A: 255})
	fixture.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `<html><script src="/c/build/_next/a.js"></script></html>`)
		case "/backend-api/sentinel/chat-requirements/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"prepare_token": "prepare"})
		case "/backend-api/sentinel/chat-requirements/finalize":
			_ = json.NewEncoder(w).Encode(map[string]any{"token": "requirements"})
		case "/backend-api/files":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"file_id":    "input-file",
				"upload_url": fixture.server.URL + "/signed-upload?sig=secret",
			})
		case "/signed-upload":
			payload, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read upload: %v", err)
			}
			fixture.mu.Lock()
			fixture.uploaded = payload
			fixture.mu.Unlock()
			w.WriteHeader(http.StatusCreated)
		case "/backend-api/files/input-file/uploaded":
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
		case "/backend-api/f/conversation/prepare":
			_ = json.NewEncoder(w).Encode(map[string]any{"conduit_token": "conduit"})
		case "/backend-api/f/conversation":
			var body struct {
				Messages []struct {
					ID string `json:"id"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode image edit conversation request: %v", err)
			} else if len(body.Messages) > 0 {
				fixture.mu.Lock()
				fixture.turnMessageID = body.Messages[0].ID
				fixture.mu.Unlock()
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"edit-conversation\",\"message\":{\"author\":{\"role\":\"tool\"},\"metadata\":{\"async_task_type\":\"image_gen\"},\"content\":{\"parts\":[{\"asset_pointer\":\"file-service://edited-file\"}]}}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/edit-conversation":
			fixture.mu.Lock()
			turnMessageID := fixture.turnMessageID
			fixture.mu.Unlock()
			writeChatGPTWebImageConversationForTurn(w, turnMessageID, true, "edited-file")
		case "/backend-api/files/edited-file/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": fixture.server.URL + "/edited-image"})
		case "/edited-image":
			_, _ = w.Write(outputImage)
		default:
			http.NotFound(w, request)
		}
	}))
	return fixture
}

func newChatGPTWebMultiImageFixture(t *testing.T) *httptest.Server {
	t.Helper()
	firstImage := chatGPTWebPNGBytes(t, color.NRGBA{R: 255, A: 255})
	secondImage := chatGPTWebPNGBytes(t, color.NRGBA{G: 255, A: 255})
	var server *httptest.Server
	var mu sync.Mutex
	polls := 0
	turnMessageID := ""
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
			var body struct {
				Messages []struct {
					ID string `json:"id"`
				} `json:"messages"`
			}
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Errorf("decode image conversation request: %v", err)
			} else if len(body.Messages) > 0 {
				mu.Lock()
				turnMessageID = body.Messages[0].ID
				mu.Unlock()
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"conversation_id\":\"multi-conversation\"}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		case "/backend-api/conversation/multi-conversation":
			mu.Lock()
			polls++
			current := polls
			currentTurnMessageID := turnMessageID
			mu.Unlock()
			if current <= 3 {
				writeChatGPTWebImageConversationForTurn(w, currentTurnMessageID, false, "first-file")
				return
			}
			writeChatGPTWebImageConversationForTurn(w, currentTurnMessageID, true, "first-file", "second-file")
		case "/backend-api/files/first-file/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/first-image"})
		case "/backend-api/files/second-file/download":
			_ = json.NewEncoder(w).Encode(map[string]any{"download_url": server.URL + "/second-image"})
		case "/first-image":
			_, _ = w.Write(firstImage)
		case "/second-image":
			_, _ = w.Write(secondImage)
		default:
			http.NotFound(w, request)
		}
	}))
	return server
}

func writeChatGPTWebImageConversation(w http.ResponseWriter, fileIDs ...string) {
	writeChatGPTWebImageConversationState(w, false, fileIDs...)
}

func writeTerminalChatGPTWebImageConversation(w http.ResponseWriter, fileIDs ...string) {
	writeChatGPTWebImageConversationState(w, true, fileIDs...)
}

func writeChatGPTWebImageConversationForTurn(w http.ResponseWriter, turnMessageID string, terminal bool, fileIDs ...string) {
	turnMessageID = strings.TrimSpace(turnMessageID)
	if turnMessageID == "" {
		writeChatGPTWebImageConversationState(w, terminal, fileIDs...)
		return
	}
	mapping := make(map[string]any, len(fileIDs)+1)
	mapping[turnMessageID] = map[string]any{"message": map[string]any{
		"id":          turnMessageID,
		"author":      map[string]any{"role": "user"},
		"create_time": 1,
	}}
	parentID := turnMessageID
	currentNode := turnMessageID
	for index, fileID := range fileIDs {
		metadata := map[string]any{"async_task_type": "image_gen"}
		if terminal && index == len(fileIDs)-1 {
			metadata["finish_details"] = map[string]any{"type": "finished_successfully"}
			metadata["is_complete"] = true
		}
		mapping[fileID] = map[string]any{
			"parent": parentID,
			"message": map[string]any{
				"author":      map[string]any{"role": "tool"},
				"create_time": index + 2,
				"metadata":    metadata,
				"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://" + fileID}}},
			},
		}
		parentID = fileID
		currentNode = fileID
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"current_node": currentNode, "mapping": mapping})
}

func writeFailedChatGPTWebImageConversation(w http.ResponseWriter, status string) {
	_ = json.NewEncoder(w).Encode(map[string]any{"mapping": map[string]any{
		"failed": map[string]any{"message": map[string]any{
			"author":      map[string]any{"role": "tool"},
			"create_time": 1,
			"metadata": map[string]any{
				"async_task_type": "image_gen",
				"finish_details":  map[string]any{"type": status},
			},
			"content": map[string]any{"parts": []any{}},
		}},
	}})
}

func writeChatGPTWebImageConversationState(w http.ResponseWriter, terminal bool, fileIDs ...string) {
	mapping := make(map[string]any, len(fileIDs))
	for index, fileID := range fileIDs {
		metadata := map[string]any{"async_task_type": "image_gen"}
		if terminal && index == len(fileIDs)-1 {
			metadata["finish_details"] = map[string]any{"type": "finished_successfully"}
			metadata["is_complete"] = true
		}
		mapping[fileID] = map[string]any{"message": map[string]any{
			"author":      map[string]any{"role": "tool"},
			"create_time": index + 1,
			"metadata":    metadata,
			"content":     map[string]any{"parts": []any{map[string]any{"asset_pointer": "file-service://" + fileID}}},
		}}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"mapping": mapping})
}

func chatGPTWebPNGDataURL(t *testing.T, pixels ...color.NRGBA) string {
	t.Helper()
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString(chatGPTWebPNGBytes(t, pixels...))
}

func chatGPTWebPNGBytes(t *testing.T, pixels ...color.NRGBA) []byte {
	t.Helper()
	if len(pixels) == 0 {
		pixels = []color.NRGBA{{A: 255}}
	}
	imageData := image.NewNRGBA(image.Rect(0, 0, len(pixels), 1))
	for index, pixel := range pixels {
		imageData.SetNRGBA(index, 0, pixel)
	}
	var output bytes.Buffer
	if err := png.Encode(&output, imageData); err != nil {
		t.Fatalf("encode PNG: %v", err)
	}
	return output.Bytes()
}
