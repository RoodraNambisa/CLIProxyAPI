package handlers

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"github.com/tidwall/gjson"
	"golang.org/x/net/context"
)

type strictImageSizeRecordingExecutor struct {
	provider     string
	executeCalls int
	streamCalls  int
	executeErr   error
	streamErr    error
}

func (e *strictImageSizeRecordingExecutor) Identifier() string { return e.provider }

func (e *strictImageSizeRecordingExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	e.executeCalls++
	if e.executeErr != nil {
		return coreexecutor.Response{}, e.executeErr
	}
	return coreexecutor.Response{Payload: []byte(`{"id":"resp_test","object":"response","status":"completed"}`)}, nil
}

func (e *strictImageSizeRecordingExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.streamCalls++
	if e.streamErr != nil {
		return nil, e.streamErr
	}
	chunks := make(chan coreexecutor.StreamChunk, 1)
	chunks <- coreexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"object\":\"response\",\"status\":\"completed\"}}\n\n")}
	close(chunks)
	return &coreexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *strictImageSizeRecordingExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *strictImageSizeRecordingExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, nil
}

func (e *strictImageSizeRecordingExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestFilterChatGPTWebStrictImageSize(t *testing.T) {
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
		ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{
			AdaptSizeToAspectRatio: true,
			StrictSize:             true,
		},
	}}, nil)

	tests := []struct {
		name          string
		providers     []string
		handlerType   string
		payload       string
		wantProviders []string
		wantError     bool
	}{
		{
			name:          "matched keeps web",
			providers:     []string{constant.Codex, constant.ChatGPTWeb},
			handlerType:   constant.OpenaiResponse,
			payload:       `{"tools":[{"type":"image_generation","size":"1920x1080"}]}`,
			wantProviders: []string{constant.Codex, constant.ChatGPTWeb},
		},
		{
			name:          "auto keeps web",
			providers:     []string{constant.ChatGPTWeb},
			handlerType:   constant.OpenaiResponse,
			payload:       `{"tools":[{"type":"image_generation","size":"auto"}]}`,
			wantProviders: []string{constant.ChatGPTWeb},
		},
		{
			name:          "unsupported ratio excludes web",
			providers:     []string{constant.Codex, constant.ChatGPTWeb},
			handlerType:   constant.OpenaiResponse,
			payload:       `{"tools":[{"type":"image_generation","size":"1024x1536"}]}`,
			wantProviders: []string{constant.Codex},
		},
		{
			name:          "oversize excludes web",
			providers:     []string{constant.ChatGPTWeb, constant.Codex},
			handlerType:   constant.OpenaiResponse,
			payload:       `{"tools":[{"type":"image_generation","size":"4000x4000"}]}`,
			wantProviders: []string{constant.Codex},
		},
		{
			name:        "invalid web only errors",
			providers:   []string{constant.ChatGPTWeb},
			handlerType: constant.OpenaiResponse,
			payload:     `{"tools":[{"type":"image_generation","size":"square"}]}`,
			wantError:   true,
		},
		{
			name:          "codex only is unaffected",
			providers:     []string{constant.Codex},
			handlerType:   constant.OpenaiResponse,
			payload:       `{"tools":[{"type":"image_generation","size":"square"}]}`,
			wantProviders: []string{constant.Codex},
		},
		{
			name:          "native protocol is unaffected",
			providers:     []string{constant.ChatGPTWeb},
			handlerType:   "openai-images",
			payload:       `{"tools":[{"type":"image_generation","size":"square"}]}`,
			wantProviders: []string{constant.ChatGPTWeb},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, gotProviders, errMsg := handler.filterChatGPTWebStrictImageSize(
				context.Background(),
				test.providers,
				test.handlerType,
				[]byte(test.payload),
			)
			if test.wantError {
				if errMsg == nil || errMsg.StatusCode != http.StatusBadRequest {
					t.Fatalf("error = %#v, want HTTP 400", errMsg)
				}
				body := BuildErrorResponseBodyForMessage(errMsg.StatusCode, errMsg.Error.Error(), errMsg)
				if got := gjson.GetBytes(body, "error.param").String(); got != "size" {
					t.Fatalf("error param = %q; body=%s", got, body)
				}
				if got := gjson.GetBytes(body, "error.code").String(); got != "invalid_value" {
					t.Fatalf("error code = %q; body=%s", got, body)
				}
				return
			}
			if errMsg != nil {
				t.Fatalf("filter error = %v", errMsg.Error)
			}
			if !reflect.DeepEqual(gotProviders, test.wantProviders) {
				t.Fatalf("providers = %#v, want %#v", gotProviders, test.wantProviders)
			}
		})
	}
}

func TestFilterChatGPTWebStrictImageSizeDisabledPreservesProviders(t *testing.T) {
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
		ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{AdaptSizeToAspectRatio: true},
	}}, nil)
	providers := []string{constant.Codex, constant.ChatGPTWeb}
	_, got, errMsg := handler.filterChatGPTWebStrictImageSize(
		context.Background(),
		providers,
		constant.OpenaiResponse,
		[]byte(`{"tools":[{"type":"image_generation","size":"1024x1536"}]}`),
	)
	if errMsg != nil || !reflect.DeepEqual(got, providers) {
		t.Fatalf("providers, error = %#v, %#v; want unchanged", got, errMsg)
	}
}

func TestResponsesStrictImageSizeRoutesInvalidWebSizeToCodex(t *testing.T) {
	const model = "strict-image-size-responses-model"
	manager := coreauth.NewManager(nil, nil, nil)
	codexExecutor := &strictImageSizeRecordingExecutor{provider: constant.Codex}
	webExecutor := &strictImageSizeRecordingExecutor{provider: constant.ChatGPTWeb}
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(webExecutor)
	registerStrictImageSizeAuth(t, manager, constant.Codex, "strict-size-codex", model)
	registerStrictImageSizeAuth(t, manager, constant.ChatGPTWeb, "strict-size-web", model)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
		ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{
			AdaptSizeToAspectRatio: true,
			StrictSize:             true,
		},
	}}, manager)
	payload := []byte(`{"model":"strict-image-size-responses-model","tools":[{"type":"image_generation","size":"1024x1536"}]}`)

	if _, _, errMsg := handler.ExecuteWithProviders(
		context.Background(),
		[]string{constant.ChatGPTWeb, constant.Codex},
		constant.OpenaiResponse,
		model,
		payload,
		"",
	); errMsg != nil {
		t.Fatalf("non-stream execution error = %#v", errMsg)
	}
	if codexExecutor.executeCalls != 1 || webExecutor.executeCalls != 0 {
		t.Fatalf("non-stream calls codex=%d web=%d, want 1 and 0", codexExecutor.executeCalls, webExecutor.executeCalls)
	}

	data, _, errors := handler.ExecuteStreamWithProviders(
		context.Background(),
		[]string{constant.ChatGPTWeb, constant.Codex},
		constant.OpenaiResponse,
		model,
		payload,
		"",
	)
	for range data {
	}
	for errMsg := range errors {
		if errMsg != nil {
			t.Fatalf("stream execution error = %#v", errMsg)
		}
	}
	if codexExecutor.streamCalls != 1 || webExecutor.streamCalls != 0 {
		t.Fatalf("stream calls codex=%d web=%d, want 1 and 0", codexExecutor.streamCalls, webExecutor.streamCalls)
	}
}

func TestResponsesStrictImageSizeReturns400WithoutCompatibleAuth(t *testing.T) {
	const model = "strict-image-size-web-only-model"
	manager := coreauth.NewManager(nil, nil, nil)
	webExecutor := &strictImageSizeRecordingExecutor{provider: constant.ChatGPTWeb}
	manager.RegisterExecutor(webExecutor)
	registerStrictImageSizeAuth(t, manager, constant.ChatGPTWeb, "strict-size-web-only", model)
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
		ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{
			AdaptSizeToAspectRatio: true,
			StrictSize:             true,
		},
	}}, manager)
	payload := []byte(`{"model":"strict-image-size-web-only-model","tools":[{"type":"image_generation","size":"1024x1536"}]}`)
	providers := []string{constant.ChatGPTWeb, constant.Codex}

	_, _, errMsg := handler.ExecuteWithProviders(
		context.Background(),
		providers,
		constant.OpenaiResponse,
		model,
		payload,
		"",
	)
	assertStrictImageSizeError(t, errMsg)

	data, _, errors := handler.ExecuteStreamWithProviders(
		context.Background(),
		providers,
		constant.OpenaiResponse,
		model,
		payload,
		"",
	)
	if data != nil {
		for range data {
		}
	}
	assertStrictImageSizeError(t, <-errors)
	if webExecutor.executeCalls != 0 || webExecutor.streamCalls != 0 {
		t.Fatalf("web calls execute=%d stream=%d, want zero", webExecutor.executeCalls, webExecutor.streamCalls)
	}
}

func TestResponsesStrictImageSizePreservesSelectedCodexErrors(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-stream", true: "stream"}[stream], func(t *testing.T) {
			const model = "strict-image-size-codex-error-model"
			upstreamErr := &coreauth.Error{
				Code:       "codex_upstream_error",
				Message:    "codex request failed",
				HTTPStatus: http.StatusBadGateway,
				Retryable:  false,
			}
			manager := coreauth.NewManager(nil, nil, nil)
			codexExecutor := &strictImageSizeRecordingExecutor{provider: constant.Codex}
			if stream {
				codexExecutor.streamErr = upstreamErr
			} else {
				codexExecutor.executeErr = upstreamErr
			}
			webExecutor := &strictImageSizeRecordingExecutor{provider: constant.ChatGPTWeb}
			manager.RegisterExecutor(codexExecutor)
			manager.RegisterExecutor(webExecutor)
			registerStrictImageSizeAuth(t, manager, constant.Codex, "strict-size-codex-error", model)
			registerStrictImageSizeAuth(t, manager, constant.ChatGPTWeb, "strict-size-web-error", model)
			handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{Images: sdkconfig.ImagesConfig{
				ChatGPTWeb: sdkconfig.ChatGPTWebImageConfig{
					AdaptSizeToAspectRatio: true,
					StrictSize:             true,
				},
			}}, manager)
			payload := []byte(`{"model":"strict-image-size-codex-error-model","tools":[{"type":"image_generation","size":"1024x1536"}]}`)
			providers := []string{constant.ChatGPTWeb, constant.Codex}

			var errMsg *interfaces.ErrorMessage
			if stream {
				data, _, errors := handler.ExecuteStreamWithProviders(
					context.Background(), providers, constant.OpenaiResponse, model, payload, "",
				)
				if data != nil {
					for range data {
					}
				}
				errMsg = <-errors
			} else {
				_, _, errMsg = handler.ExecuteWithProviders(
					context.Background(), providers, constant.OpenaiResponse, model, payload, "",
				)
			}
			if errMsg == nil || errMsg.StatusCode != http.StatusBadGateway {
				t.Fatalf("error = %#v, want upstream HTTP 502", errMsg)
			}
			if webExecutor.executeCalls != 0 || webExecutor.streamCalls != 0 {
				t.Fatalf("web calls execute=%d stream=%d, want zero", webExecutor.executeCalls, webExecutor.streamCalls)
			}
		})
	}
}

func assertStrictImageSizeError(t *testing.T, errMsg *interfaces.ErrorMessage) {
	t.Helper()
	if errMsg == nil || errMsg.StatusCode != http.StatusBadRequest {
		t.Fatalf("error = %#v, want HTTP 400", errMsg)
	}
	body := BuildErrorResponseBodyForMessage(errMsg.StatusCode, errMsg.Error.Error(), errMsg)
	if gjson.GetBytes(body, "error.param").String() != "size" ||
		gjson.GetBytes(body, "error.code").String() != "invalid_value" {
		t.Fatalf("error body = %s, want size invalid_value", body)
	}
}

func registerStrictImageSizeAuth(t *testing.T, manager *coreauth.Manager, provider, authID, model string) {
	t.Helper()
	auth := &coreauth.Auth{ID: authID, Provider: provider, Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register %s auth: %v", provider, err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(authID) })
}
