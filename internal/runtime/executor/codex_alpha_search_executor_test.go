package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

type alphaSearchRoundTripper func(*http.Request) (*http.Response, error)

func (f alphaSearchRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type alphaSearchBody struct {
	io.Reader
	closed atomic.Bool
}

func (b *alphaSearchBody) Close() error { b.closed.Store(true); return nil }

func TestCodexAlphaSearchHTTPWireProtocolAndAutoTransport(t *testing.T) {
	const output = `{"output":"search output","encrypted_output":"opaque","results":[{"future":9007199254740993}],"usage":{"input_tokens":5,"output_tokens":2}}`
	wire := make(chan *http.Request, 1)
	bodyWire := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		wire <- r.Clone(context.Background())
		bodyWire <- body
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "search-upstream")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, output)
	}))
	defer server.Close()
	credential := &auth.Auth{ID: "search-wire", Provider: "codex", Attributes: map[string]string{"api_key": "fixture", "base_url": server.URL + "/v1?tenant=fixture", "websockets": "true", auth.CodexAlphaSearchAttributeKey: "true"}}
	cfg := &config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "direct"}, Codex: config.CodexConfig{PassthroughPromptCacheKey: true, IdentityConfuse: true, OptimizeMultiAgentV2: true, OrphanDelegationCompatibility: true}}
	executor := NewCodexAutoExecutor(cfg)
	raw := []byte(`{"id":"search-id","model":"alias","prompt_cache_key":"remove","input":[{"type":"agent_message","content":[{"type":"encrypted_content","encrypted_content":"opaque"}]}],"additional_tools":[{"type":"custom","name":"keep"}],"commands":[{"future":{"prompt_cache_key":"keep","number":9007199254740993}}]}`)
	ctrl := core.NewRequestBodyReleaseController(1024, []byte("released"))
	opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch, OriginalRequest: raw, Headers: http.Header{"Authorization": {"Bearer downstream"}, "Cookie": {"downstream=fixture"}, "User-Agent": {"native-client/0.153.4"}, "Session_id": {"client-session"}, "X-Openai-Internal-Codex-Responses-Lite": {"true"}}, Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}}
	resp, err := executor.Execute(core.WithDownstreamWebsocket(t.Context()), credential, core.Request{Model: "upstream-model", Payload: raw}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusAccepted || string(resp.Payload) != output || resp.Headers.Get("X-Request-Id") != "search-upstream" {
		t.Fatal("native response status, headers or body changed")
	}
	if !ctrl.Released() {
		t.Fatal("successful search did not release its original body")
	}
	r, body := <-wire, <-bodyWire
	if r.Method != http.MethodPost || r.URL.Path != "/v1/alpha/search" || r.URL.RawQuery != "tenant=fixture" {
		t.Fatal("incorrect search endpoint")
	}
	if r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("Cookie") != "" || r.Header.Get("Accept") != "application/json" || r.Header.Get("Session_id") != "client-session" {
		t.Fatal("incorrect search authentication or headers")
	}
	if r.Header.Get("Upgrade") != "" || r.Header.Get("X-OpenAI-Internal-Codex-Responses-Lite") != "" {
		t.Fatal("search inherited Responses transport metadata")
	}
	if gjson.GetBytes(body, "model").String() != "upstream-model" || gjson.GetBytes(body, "prompt_cache_key").Exists() || gjson.GetBytes(body, "stream").Exists() || gjson.GetBytes(body, "instructions").Exists() {
		t.Fatal("search used Responses request conversion")
	}
	if gjson.GetBytes(body, "commands").Raw != gjson.GetBytes(raw, "commands").Raw || gjson.GetBytes(body, "input").Raw != gjson.GetBytes(raw, "input").Raw || gjson.GetBytes(body, "additional_tools").Raw != gjson.GetBytes(raw, "additional_tools").Raw {
		t.Fatal("search mutated opaque commands or history")
	}
}

func TestCodexAlphaSearchOAuthStatusAndBodyLifecycle(t *testing.T) {
	for _, status := range []int{200, 429, 403} {
		credential := &auth.Auth{ID: "oauth-search", Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "account_id": "fixture-account"}}
		data := `{"error":{"code":"misalignment_policy_violation","message":"fixture"}}`
		if status == 200 {
			data = `{"output":"search result"}`
		}
		body := &alphaSearchBody{Reader: strings.NewReader(data)}
		ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", alphaSearchRoundTripper(func(r *http.Request) (*http.Response, error) {
			if r.URL.String() != helps.CodexAlphaSearchOAuthURL || r.Header.Get("Chatgpt-Account-Id") != "fixture-account" || r.Header.Get("Authorization") != "Bearer fixture" || r.Header.Get("Version") != "0.153.4" {
				t.Fatal("incorrect OAuth search request")
			}
			return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}, "Retry-After": {"3"}, "X-Request-Id": {"fixture"}}, Body: body}, nil
		}))
		resp, err := NewCodexExecutor(&config.Config{}).Execute(ctx, credential, core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5","id":"fixture"}`)}, core.Options{SourceFormat: translator.FormatCodexAlphaSearch})
		if !body.closed.Load() {
			t.Fatal("response body was not closed")
		}
		if status == 200 {
			if err != nil || string(resp.Payload) != data {
				t.Fatal("OAuth search failed")
			}
		} else {
			var statusError statusErrWithHeaders
			if !errors.As(err, &statusError) || statusError.StatusCode() != status || statusError.Error() != data || statusError.Headers().Get("X-Request-Id") != "fixture" {
				t.Fatal("upstream error or status was rewritten")
			}
			if status == 429 && (statusError.RetryAfter() == nil || *statusError.RetryAfter() != 3*time.Second) {
				t.Fatal("retry-after was lost")
			}
		}
	}
}

func TestCodexAlphaSearchRejectsUnsupportedCallsBeforeConnecting(t *testing.T) {
	var calls atomic.Int32
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", alphaSearchRoundTripper(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("unexpected connection")
	}))
	executor := NewCodexExecutor(&config.Config{})
	a := &auth.Auth{ID: "fixture", Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}
	req := core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5"}`)}
	opts := core.Options{SourceFormat: translator.FormatCodexAlphaSearch}
	for _, operation := range []core.RequestOperation{core.RequestOperationCount, core.RequestOperationStream} {
		if _, err := executor.PrepareProviderRequest(ctx, req, opts, operation); err == nil {
			t.Fatal("unsupported operation passed preflight")
		}
	}
	if _, err := executor.ExecuteStream(ctx, a, req, opts); err == nil {
		t.Fatal("accepted search SSE")
	}
	if _, err := executor.CountTokens(ctx, a, req, opts); err == nil {
		t.Fatal("accepted search counting")
	}
	ws := NewCodexWebsocketsExecutor(&config.Config{})
	if _, err := ws.Execute(ctx, a, req, opts); err == nil {
		t.Fatal("accepted direct search WebSocket")
	}
	if _, err := ws.ExecuteStream(ctx, a, req, opts); err == nil {
		t.Fatal("accepted search WebSocket stream")
	}
	if _, err := executor.Execute(core.WithRequiredUpstreamWebsocket(ctx), a, req, opts); err == nil {
		t.Fatal("required WebSocket silently fell back")
	}
	for _, candidate := range []*auth.Auth{nil, {Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}}, {Provider: "codex", Metadata: map[string]any{"access_token": "fixture", "auth_mode": "agentIdentity"}}} {
		if _, err := executor.Execute(ctx, candidate, req, opts); err == nil {
			t.Fatal("unsupported credential connected")
		}
	}
	missing := &auth.Auth{Provider: "codex", Metadata: map[string]any{"refresh_token": "fixture"}}
	_, err := executor.Execute(ctx, missing, req, opts)
	var local statusErr
	if !errors.As(err, &local) || !local.SkipAuthResult() || !local.RetryOtherAuth() {
		t.Fatal("missing local token was treated as an upstream credential failure")
	}
	if calls.Load() != 0 {
		t.Fatal("unsupported search opened an upstream connection")
	}
}

type alphaSearchReaderFunc func([]byte) (int, error)

func (f alphaSearchReaderFunc) Read(p []byte) (int, error) { return f(p) }

func TestCodexAlphaSearchReadFailuresKeepStatusAndClose(t *testing.T) {
	readFailure := errors.New("fixture read failure")
	for _, upstreamStatus := range []int{200, 429} {
		for _, oversized := range []bool{false, true} {
			var reader io.Reader = alphaSearchReaderFunc(func([]byte) (int, error) { return 0, readFailure })
			if oversized {
				reader = io.LimitReader(alphaSearchReaderFunc(func(p []byte) (int, error) { clear(p); return len(p), nil }), helps.CodexAlphaSearchMaxResponseBytes+1)
			}
			body := &alphaSearchBody{Reader: reader}
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", alphaSearchRoundTripper(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: upstreamStatus, Header: http.Header{"Retry-After": {"3"}}, Body: body}, nil
			}))
			_, err := NewCodexExecutor(&config.Config{}).Execute(ctx, &auth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5"}`)}, core.Options{SourceFormat: translator.FormatCodexAlphaSearch})
			var statusError statusErrWithHeaders
			want := upstreamStatus
			if want == 200 {
				want = 502
			}
			if !errors.As(err, &statusError) || statusError.StatusCode() != want || !body.closed.Load() {
				t.Fatal("response status or body lifecycle was lost")
			}
			if !oversized && !errors.Is(err, readFailure) {
				t.Fatal("read error cause was lost")
			}
			if upstreamStatus == 429 && (statusError.SkipAuthResult() || statusError.RetryAfter() == nil || *statusError.RetryAfter() != 3*time.Second) {
				t.Fatal("read failure hid real rate limiting")
			}
		}
	}
}

func TestCodexAlphaSearchCancellationClosesResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	reading := make(chan struct{})
	body := &alphaSearchBody{Reader: alphaSearchReaderFunc(func([]byte) (int, error) { close(reading); <-ctx.Done(); return 0, ctx.Err() })}
	ctx = context.WithValue(ctx, "cliproxy.roundtripper", alphaSearchRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: body}, nil
	}))
	done := make(chan error, 1)
	go func() {
		_, err := NewCodexExecutor(&config.Config{}).Execute(ctx, &auth.Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}, core.Request{Model: "gpt-5.5", Payload: []byte(`{"model":"gpt-5.5"}`)}, core.Options{SourceFormat: translator.FormatCodexAlphaSearch})
		done <- err
	}()
	select {
	case <-reading:
	case <-time.After(3 * time.Second):
		t.Fatal("response read did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || !body.closed.Load() {
			t.Fatal("cancellation did not close the response")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation leaked a response reader")
	}
}
