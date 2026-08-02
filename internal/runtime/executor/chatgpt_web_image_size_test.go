package executor

import (
	"context"
	"errors"
	"fmt"
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
				"tools":[{"type":"image_generation","size":"1200x800"}]
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
	if prepared.imageSizeMatch == nil || prepared.imageSizeMatch.Ratio != "3:2" ||
		prepared.imageSizeMatch.Width != 1200 || prepared.imageSizeMatch.Height != 800 {
		t.Fatalf("image size match = %#v", prepared.imageSizeMatch)
	}
	originalPrompt := prepared.request.Image.Prompt
	upstreamPrompt := chatGPTWebImageUpstreamPrompt(originalPrompt, "generate", prepared.imageSizeMatch)
	if originalPrompt != "draw a cat" || prepared.request.Image.Prompt != originalPrompt {
		t.Fatalf("original prompt changed: before=%q after=%q", originalPrompt, prepared.request.Image.Prompt)
	}
	wantSuffix := "Set the final image canvas to exactly 1200×800 pixels (width × height), with an aspect ratio of 3:2. If exact pixel dimensions are unavailable, preserve the 3:2 aspect ratio exactly."
	if !strings.HasSuffix(upstreamPrompt, wantSuffix) {
		t.Fatalf("upstream prompt = %q", upstreamPrompt)
	}
}

func TestPrepareRuntimeRequestIgnoresUnmappedChatGPTWebImageSizes(t *testing.T) {
	tests := []struct {
		name      string
		size      string
		wantError bool
	}{
		{name: "below minimum pixels", size: "1024x512"},
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
	for _, size := range []string{"1024x512", "4000x4000", "square"} {
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
	if got := chatGPTWebImageUpstreamPrompt(prompt, "generate", nil); got != "draw" {
		t.Fatalf("prompt = %q, want draw", got)
	}
	match := &helps.ChatGPTWebImageSizeMatch{}
	if got := chatGPTWebImageUpstreamPrompt(prompt, "generate", match); got != "draw" {
		t.Fatalf("empty ratio prompt = %q, want draw", got)
	}
}

func TestChatGPTWebImageUpstreamPromptForEdit(t *testing.T) {
	match := &helps.ChatGPTWebImageSizeMatch{Width: 1200, Height: 800, RatioWidth: 3, RatioHeight: 2, Ratio: "3:2"}
	got := chatGPTWebImageUpstreamPrompt("edit this", "edit", match)
	want := "edit this\n\nRecompose, crop, or extend the edited image so the final canvas is exactly 1200×800 pixels (width × height), with an aspect ratio of 3:2. Preserve the subject's natural proportions; do not stretch or distort the subject. If exact pixel dimensions are unavailable, preserve the 3:2 aspect ratio exactly."
	if got != want {
		t.Fatalf("prompt = %q, want %q", got, want)
	}
}

func TestPrepareRuntimeRequestEnforcesChatGPTWebMaxN(t *testing.T) {
	tests := []struct {
		name       string
		toolN      int
		maxResults int
		maxN       int
		wantError  bool
	}{
		{name: "tool count rejected", toolN: 2, maxN: 1, wantError: true},
		{name: "aggregation count rejected", toolN: 1, maxResults: 2, maxN: 1, wantError: true},
		{name: "configured count accepted", toolN: 2, maxResults: 2, maxN: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewChatGPTWebExecutor(nil, nil)
			metadata := map[string]any{
				cliproxyexecutor.ChatGPTWebImageConfigSnapshotMetadataKey: cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxN: test.maxN},
			}
			if test.maxResults > 0 {
				metadata[cliproxyexecutor.ImageGenerationMaxResultsMetadataKey] = test.maxResults
			}
			payload := []byte(fmt.Sprintf(`{"model":"gpt-5.4","input":"draw","tools":[{"type":"image_generation","n":%d}]}`, test.toolN))
			prepared, err := executor.prepareRuntimeRequest(
				context.Background(),
				chatGPTWebRuntimeAuth(),
				cliproxyexecutor.Request{Model: "gpt-image-2", Payload: payload},
				cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex, Metadata: metadata},
				false,
			)
			if test.wantError {
				if err == nil || gjson.Get(err.Error(), "error.param").String() != "n" || !strings.Contains(err.Error(), "chatgpt web") {
					t.Fatalf("prepareRuntimeRequest() error = %v, want chatgpt web n error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("prepareRuntimeRequest() error = %v", err)
			}
			defer prepared.discardUsageProjection()
		})
	}
}
