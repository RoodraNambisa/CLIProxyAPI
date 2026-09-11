package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

type nativeImageRecoveryExecutor struct {
	ProviderExecutor
	t                          *testing.T
	prepared, calls, recovered int
	payload                    string
}

func (*nativeImageRecoveryExecutor) Identifier() string { return "codex" }

func (e *nativeImageRecoveryExecutor) check(req core.Request, opts core.Options) {
	e.t.Helper()
	if string(req.Payload) != e.payload || string(opts.OriginalRequest) != e.payload || opts.SourceFormat.String() != "openai-image" || opts.Alt != "images/generations" {
		e.t.Errorf("native format lost: req=%s original=%s format=%s alt=%s", req.Payload, opts.OriginalRequest, opts.SourceFormat, opts.Alt)
	}
}

func (e *nativeImageRecoveryExecutor) PrepareProviderRequest(_ context.Context, req core.Request, opts core.Options, _ core.RequestOperation) (any, error) {
	e.prepared++
	e.check(req, opts)
	if core.CodexNativeImageRequestFromOptions(opts).NativeResponse() {
		e.t.Error("preflight changed response mode")
	}
	return nil, nil
}

func (e *nativeImageRecoveryExecutor) Execute(_ context.Context, auth *Auth, req core.Request, opts core.Options) (core.Response, error) {
	e.calls++
	e.check(req, opts)
	if auth.Metadata["access_token"] == "expired" {
		return core.Response{}, &Error{HTTPStatus: http.StatusUnauthorized, Message: "expired token"}
	}
	return core.Response{Payload: []byte(`{"data":[{"b64_json":"aGVsbG8="}]}`)}, nil
}

func (e *nativeImageRecoveryExecutor) ExecuteStream(ctx context.Context, auth *Auth, req core.Request, opts core.Options) (*core.StreamResult, error) {
	result, err := e.Execute(ctx, auth, req, opts)
	if err != nil {
		return nil, err
	}
	ch := make(chan core.StreamChunk, 1)
	ch <- core.StreamChunk{Payload: result.Payload}
	close(ch)
	return &core.StreamResult{Chunks: ch}, nil
}

func (*nativeImageRecoveryExecutor) ShouldRecoverUnauthorized(auth *Auth, err error) bool {
	var status interface{ StatusCode() int }
	return errors.As(err, &status) && status.StatusCode() == http.StatusUnauthorized && auth.Metadata["access_token"] == "expired"
}

func (e *nativeImageRecoveryExecutor) RecoverUnauthorized(_ context.Context, auth *Auth) (*Auth, error) {
	e.recovered++
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh"
	return updated, nil
}

func TestNativeImageRequestVariantSurvivesUnauthorizedReplay(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "stream"}[stream], func(t *testing.T) {
			e := &nativeImageRecoveryExecutor{t: t, payload: `{"model":"gpt-image-2.5","prompt":"draw","n":2}`}
			manager := NewManager(nil, nil, nil)
			manager.RegisterExecutor(e)
			auth := &Auth{ID: "native-image-replay", Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": "expired"}}
			if _, err := manager.Register(t.Context(), auth); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-image-2.5"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			variant := core.NewCodexNativeImageRequest([]byte(e.payload), "images/generations")
			req := core.Request{Model: "gpt-image-2.5", Payload: []byte(`{"model":"","tools":[{"type":"image_generation"}]}`)}
			opts := core.Options{SourceFormat: translator.FormatOpenAIResponse, Stream: stream, Metadata: map[string]any{core.CodexNativeImageRequestMetadataKey: variant}}
			if stream {
				result, err := manager.ExecuteStream(t.Context(), []string{"codex"}, req, opts)
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil {
						t.Fatal(chunk.Err)
					}
				}
			} else {
				if _, err := manager.Execute(t.Context(), []string{"codex"}, req, opts); err != nil {
					t.Fatal(err)
				}
			}
			if e.calls != 2 || e.recovered != 1 || e.prepared != 1 || !variant.NativeResponse() {
				t.Fatalf("calls=%d recovered=%d prepared=%d native=%t", e.calls, e.recovered, e.prepared, variant.NativeResponse())
			}
			current, _ := manager.GetByID(auth.ID)
			if current.Unavailable || current.LastError != nil {
				t.Fatalf("recovered credential cooled down: %+v", current.LastError)
			}
		})
	}
}
