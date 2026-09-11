package executor

import (
	"context"
	"testing"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestNativeImageRequestAlternativeIsProviderScoped(t *testing.T) {
	variant := NewCodexNativeImageRequest([]byte(`{"model":"gpt-image-2.5","prompt":"draw"}`), "images/generations")
	ctx := WithCodexNativeImageRequest(context.Background(), variant)
	if CodexNativeImageRequestFromContext(ctx) != variant {
		t.Fatal("context lost variant")
	}
	req := Request{Model: "mapped-image-model", Payload: []byte(`{"tools":[{"type":"image_generation"}]}`)}
	opts := Options{SourceFormat: sdktranslator.FromString("openai-response"), OriginalRequest: req.Payload,
		Metadata: map[string]any{CodexNativeImageRequestMetadataKey: variant}}
	for _, provider := range []string{"codex", "chatgpt-web", "codex"} {
		prepared, preparedOpts := PrepareImageRequestForProvider(provider, req, opts)
		if variant.NativeResponse() {
			t.Fatal("preflight changed selected response format")
		}
		if prepared.Model != req.Model {
			t.Fatal("mapped model was overwritten")
		}
		if provider == "codex" {
			if string(prepared.Payload) != string(variant.payload) || preparedOpts.SourceFormat.String() != "openai-image" || preparedOpts.Alt != "images/generations" {
				t.Fatalf("Codex did not get native variant: %+v %+v", prepared, preparedOpts)
			}
		} else if string(prepared.Payload) != string(req.Payload) || preparedOpts.Alt != "" || preparedOpts.SourceFormat != opts.SourceFormat {
			t.Fatal("Web request was modified")
		}
	}
	for _, provider := range []string{"codex", "chatgpt-web", "codex"} {
		ObserveImageResponseProvider(provider, opts)
		if variant.NativeResponse() != (provider == "codex") {
			t.Fatal("response format did not follow retry")
		}
	}
	variant.Release()
	variant.Release()
	prepared, _ := PrepareImageRequestForProvider("codex", req, opts)
	if prepared.Payload != nil {
		t.Fatal("native body was retained after release")
	}
	if string(req.Payload) != `{"tools":[{"type":"image_generation"}]}` || opts.SourceFormat.String() != "openai-response" {
		t.Fatal("provider alternative mutated original input")
	}
}
