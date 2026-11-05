package executor

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestChatGPTWebImageFormatPreflight(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "unexpected request", 500) }))
	defer s.Close()
	cfg := &config.Config{}
	cfg.Images.ChatGPTWeb.IgnoreUnsupportedParams = true
	e := NewChatGPTWebExecutor(cfg, nil)
	defer e.Close()
	e.runtimeBaseURL = s.URL
	opts := core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}
	for _, tc := range []struct {
		outer, tool string
		allowed     bool
	}{{"gpt-image-2.5", "gpt-image-2.6", false}, {"gpt-image-2.6", "gpt-image-2.5", true}, {"auto", "gpt-image-2.6", true}, {"auto", "", false}} {
		body, _ := json.Marshal(map[string]any{"model": tc.outer, "input": "draw GIF", "tool_choice": map[string]any{"type": "image_generation"}, "tools": []any{map[string]any{"type": "image_generation", "model": tc.tool, "output_format": "gif"}}})
		p, err := e.PrepareProviderRequest(t.Context(), core.Request{Model: tc.outer, Payload: body}, opts, core.RequestOperationExecute)
		if tc.allowed {
			if err != nil {
				t.Fatalf("%s/%s: %v", tc.outer, tc.tool, err)
			}
			p.(*chatGPTWebPreparedRequest).discardUsageProjection()
		} else {
			var formatErr *helps.ChatGPTWebImageOutputFormatError
			if !errors.As(err, &formatErr) || !formatErr.SkipAuthResult() || formatErr.RetryOtherAuth() || core.ProviderRequestPreparationScopeOf(err) != core.ProviderRequestPreparationProviderIncompatible {
				t.Fatalf("wrong preflight: %v", err)
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatal("preflight contacted upstream")
	}
}

func TestChatGPTWebImageFormatSnapshotAndGIFInput(t *testing.T) {
	cfg := &config.Config{}
	e := NewChatGPTWebExecutor(cfg, nil)
	defer e.Close()
	opts := core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}
	req := core.Request{Model: "gpt-image-2.6", Payload: []byte(`{"model":"gpt-image-2.6","input":"bounce","tools":[{"type":"image_generation","output_format":"gif"}]}`)}
	p, err := e.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	defer p.(*chatGPTWebPreparedRequest).discardUsageProjection()
	cfg.Images.ChatGPTWeb.ModelOutputFormats = map[string][]string{"gpt-image-2.6": {"png"}}
	e.UpdateConfig(cfg)
	if _, err = e.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute); err == nil {
		t.Fatal("new request ignored hot update")
	}
	pinned := core.WithProviderPreparedRequest(opts, e.Identifier(), p)
	prepared, err := e.prepareRuntimeRequest(t.Context(), chatGPTWebRuntimeAuth(), req, pinned, false)
	if err != nil {
		t.Fatal(err)
	}
	prepared.discardUsageProjection()
	dataURL := "data:image/gif;base64," + base64.StdEncoding.EncodeToString(webGIFFixture(t))
	body, _ := json.Marshal(map[string]any{"model": "gpt-image-2.5", "input": []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "input_text", "text": "edit"}, map[string]any{"type": "input_image", "image_url": dataURL}}}}, "tools": []any{map[string]any{"type": "image_generation", "output_format": "png"}}})
	p2, err := e.PrepareProviderRequest(t.Context(), core.Request{Model: "gpt-image-2.5", Payload: body}, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatalf("GIF input was restricted: %v", err)
	}
	p2.(*chatGPTWebPreparedRequest).discardUsageProjection()
}
