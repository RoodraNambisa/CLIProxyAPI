package executor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestPrepareRuntimeRequestAdaptsPinnedChatGPTWebImageSize(t *testing.T) {
	executor := NewChatGPTWebExecutor(nil, nil)
	prepared, err := executor.prepareRuntimeRequest(
		context.Background(),
		chatGPTWebRuntimeAuth(),
		cliproxyexecutor.Request{
			Model: "gpt-image-2",
			Payload: []byte(`{
				"model":"gpt-5.4",
				"input":"draw a cat",
				"tools":[{"type":"image_generation","size":"941x1672"}]
			}`),
		},
		cliproxyexecutor.Options{
			SourceFormat:   sdktranslator.FormatCodex,
			ResponseFormat: sdktranslator.FormatCodex,
			Metadata: map[string]any{
				cliproxyexecutor.ChatGPTWebImageConfigSnapshotMetadataKey: cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
					AdaptSizeToAspectRatio:     true,
					AspectRatioMaxErrorPercent: 1,
					MaxResizeEdgePixels:        3840,
				},
			},
		},
		false,
	)
	if err != nil {
		t.Fatalf("prepareRuntimeRequest() error = %v", err)
	}
	defer prepared.discardUsageProjection()
	if prepared.request.Image == nil || prepared.request.Image.Size != "" {
		t.Fatalf("prepared image request = %#v", prepared.request.Image)
	}
	if prepared.imageSizeMatch == nil || prepared.imageSizeMatch.Ratio != "9:16" ||
		prepared.imageSizeMatch.Width != 941 || prepared.imageSizeMatch.Height != 1672 {
		t.Fatalf("image size match = %#v", prepared.imageSizeMatch)
	}
	originalPrompt := prepared.request.Image.Prompt
	upstreamPrompt := chatGPTWebImageUpstreamPrompt(originalPrompt, prepared.imageSizeMatch)
	if originalPrompt != "draw a cat" || prepared.request.Image.Prompt != originalPrompt {
		t.Fatalf("original prompt changed: before=%q after=%q", originalPrompt, prepared.request.Image.Prompt)
	}
	if !strings.HasSuffix(upstreamPrompt, "Set the final image aspect ratio to 9:16.") {
		t.Fatalf("upstream prompt = %q", upstreamPrompt)
	}
}

func TestPrepareRuntimeRequestIgnoresUnmappedChatGPTWebImageSizes(t *testing.T) {
	tests := []struct {
		name      string
		size      string
		wantError bool
	}{
		{name: "unsupported ratio", size: "1024x1536"},
		{name: "oversize", size: "4000x4000"},
		{name: "invalid", size: "square", wantError: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewChatGPTWebExecutor(nil, nil)
			payload := []byte(`{"model":"gpt-5.4","input":"draw","tools":[{"type":"image_generation","size":"` + test.size + `"}]}`)
			prepared, err := executor.prepareRuntimeRequest(
				context.Background(),
				chatGPTWebRuntimeAuth(),
				cliproxyexecutor.Request{Model: "gpt-image-2", Payload: payload},
				cliproxyexecutor.Options{
					SourceFormat:   sdktranslator.FormatCodex,
					ResponseFormat: sdktranslator.FormatCodex,
					Metadata: map[string]any{
						cliproxyexecutor.ChatGPTWebImageConfigSnapshotMetadataKey: cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
							AdaptSizeToAspectRatio:     true,
							AspectRatioMaxErrorPercent: 1,
							MaxResizeEdgePixels:        3840,
						},
					},
				},
				false,
			)
			if test.wantError {
				if err == nil || !strings.Contains(err.Error(), "exact image size") {
					t.Fatalf("prepareRuntimeRequest() error = %v, want unsupported size", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepareRuntimeRequest() error = %v", err)
			}
			defer prepared.discardUsageProjection()
			if prepared.request.Image == nil || prepared.request.Image.Size != "" || prepared.imageSizeMatch != nil {
				t.Fatalf("prepared image request=%#v match=%#v", prepared.request.Image, prepared.imageSizeMatch)
			}
		})
	}
}

func TestPrepareRuntimeRequestStrictSizeRejectsBeforeNetwork(t *testing.T) {
	for _, size := range []string{"1024x1536", "4000x4000", "square"} {
		t.Run(size, func(t *testing.T) {
			executor := NewChatGPTWebExecutor(nil, nil)
			payload := []byte(`{"model":"gpt-5.4","input":"draw","tools":[{"type":"image_generation","size":"` + size + `"}]}`)
			_, err := executor.prepareRuntimeRequest(
				context.Background(),
				chatGPTWebRuntimeAuth(),
				cliproxyexecutor.Request{Model: "gpt-image-2", Payload: payload},
				cliproxyexecutor.Options{
					SourceFormat:   sdktranslator.FormatCodex,
					ResponseFormat: sdktranslator.FormatCodex,
					Metadata: map[string]any{
						cliproxyexecutor.ChatGPTWebImageConfigSnapshotMetadataKey: cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
							AdaptSizeToAspectRatio:     true,
							StrictSize:                 true,
							AspectRatioMaxErrorPercent: 1,
							MaxResizeEdgePixels:        3840,
						},
					},
				},
				false,
			)
			if err == nil {
				t.Fatal("prepareRuntimeRequest() error = nil, want strict size error")
			}
			var status interface{ StatusCode() int }
			if !errors.As(err, &status) || status.StatusCode() != http.StatusBadRequest {
				t.Fatalf("status error = %v, want 400", err)
			}
			var skipper interface{ SkipAuthResult() bool }
			if !errors.As(err, &skipper) || !skipper.SkipAuthResult() {
				t.Fatalf("SkipAuthResult() = false; error=%v", err)
			}
			var retrier interface{ RetryOtherAuth() bool }
			if !errors.As(err, &retrier) || retrier.RetryOtherAuth() {
				t.Fatalf("RetryOtherAuth() = true; error=%v", err)
			}
			if got := gjson.Get(err.Error(), "error.code").String(); got != "invalid_value" {
				t.Fatalf("error code = %q; error=%v", got, err)
			}
		})
	}
}

func TestChatGPTWebImageUpstreamPromptWithoutMatch(t *testing.T) {
	prompt := " draw "
	if got := chatGPTWebImageUpstreamPrompt(prompt, nil); got != "draw" {
		t.Fatalf("prompt = %q, want draw", got)
	}
	match := &helps.ChatGPTWebImageSizeMatch{}
	if got := chatGPTWebImageUpstreamPrompt(prompt, match); got != "draw" {
		t.Fatalf("empty ratio prompt = %q, want draw", got)
	}
}
