package translator

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestGeminiCLICompatibilityFormatFailsClosed(t *testing.T) {
	registry := NewRegistry()
	payload := []byte(`{"model":"legacy"}`)

	if got := registry.TranslateRequest(FormatOpenAI, FormatGeminiCLI, "model", payload, false); !bytes.Equal(got, payload) {
		t.Fatalf("TranslateRequest() = %s, want original payload", got)
	}
	if _, err := registry.TranslateRequestChecked(FormatOpenAI, FormatGeminiCLI, "model", payload, false); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("TranslateRequestChecked() error = %v, want %v", err, ErrGeminiCLIFormatNotSupported)
	}
	if got := registry.TranslateNonStream(t.Context(), FormatGeminiCLI, FormatOpenAI, "model", payload, payload, payload, nil); !bytes.Equal(got, payload) {
		t.Fatalf("TranslateNonStream() = %s, want original payload", got)
	}
	if got := registry.TranslateStream(t.Context(), FormatGeminiCLI, FormatOpenAI, "model", payload, payload, payload, nil); len(got) != 1 || !bytes.Equal(got[0], payload) {
		t.Fatalf("TranslateStream() = %#v, want original payload chunk", got)
	}
	if got := registry.TranslateTokenCount(t.Context(), FormatOpenAI, FormatGeminiCLI, 1, payload); !bytes.Equal(got, payload) {
		t.Fatalf("TranslateTokenCount() = %s, want original payload", got)
	}

	pipeline := NewPipeline(registry)
	if _, err := pipeline.TranslateRequest(context.Background(), FormatOpenAI, FormatGeminiCLI, RequestEnvelope{Body: payload}); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("TranslateRequest() error = %v, want %v", err, ErrGeminiCLIFormatNotSupported)
	}
	if _, err := pipeline.TranslateResponse(context.Background(), FormatGeminiCLI, FormatOpenAI, ResponseEnvelope{Body: payload}, payload, payload, nil); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("TranslateResponse() error = %v, want %v", err, ErrGeminiCLIFormatNotSupported)
	}
}

func TestGeminiCLICompatibilityPipelineRunsMiddlewareBeforeRejecting(t *testing.T) {
	pipeline := NewPipeline(NewRegistry())
	requestCalled := false
	pipeline.UseRequest(func(ctx context.Context, req RequestEnvelope, next RequestHandler) (RequestEnvelope, error) {
		requestCalled = true
		return next(ctx, req)
	})
	if _, err := pipeline.TranslateRequest(t.Context(), FormatOpenAI, FormatGeminiCLI, RequestEnvelope{Body: []byte(`{}`)}); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("TranslateRequest() error = %v, want %v", err, ErrGeminiCLIFormatNotSupported)
	}
	if !requestCalled {
		t.Fatal("request middleware was skipped for retired format")
	}

	responseCalled := false
	pipeline.UseResponse(func(ctx context.Context, resp ResponseEnvelope, next ResponseHandler) (ResponseEnvelope, error) {
		responseCalled = true
		return next(ctx, resp)
	})
	if _, err := pipeline.TranslateResponse(t.Context(), FormatGeminiCLI, FormatOpenAI, ResponseEnvelope{Body: []byte(`{}`)}, nil, nil, nil); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
		t.Fatalf("TranslateResponse() error = %v, want %v", err, ErrGeminiCLIFormatNotSupported)
	}
	if !responseCalled {
		t.Fatal("response middleware was skipped for retired format")
	}
}

func TestGeminiCLICompatibilityFormatNormalizationFailsClosed(t *testing.T) {
	pipeline := NewPipeline(NewRegistry())
	for _, retiredFormat := range []Format{FromString("Gemini-CLI"), FromString(" gemini-cli ")} {
		if _, err := pipeline.TranslateRequest(context.Background(), FormatOpenAI, retiredFormat, RequestEnvelope{Body: []byte(`{}`)}); !errors.Is(err, ErrGeminiCLIFormatNotSupported) {
			t.Fatalf("TranslateRequest(%q) error = %v", retiredFormat, err)
		}
	}
}
