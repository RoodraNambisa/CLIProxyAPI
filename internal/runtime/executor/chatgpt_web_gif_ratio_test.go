package executor

import (
	"bytes"
	"context"
	"errors"
	"image/gif"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

func TestChatGPTWebGIFAspectRatioException(t *testing.T) {
	match := &helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 18, RatioWidth: 16, RatioHeight: 9}
	cfg := core.ChatGPTWebImageConfigSnapshot{StrictSize: true, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1, MaxImageResponseBytes: 1 << 20}
	for _, enabled := range []bool{false, true} {
		cfg.GIFAllowAspectRatioMismatch = enabled
		original := webGIFFixture(t)
		data := [][]byte{bytes.Clone(original), bytes.Clone(original)}
		outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", data, match, cfg, helps.ChatGPTWebGIFControls{FrameCount: 3})
		if !enabled {
			if err == nil || !strings.Contains(err.Error(), "image_size_mismatch") {
				t.Fatalf("strict failure: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for i, b := range data {
			animation, err := gif.DecodeAll(bytes.NewReader(b))
			if err != nil || len(animation.Image) != 3 || animation.Config.Width != 16 || outputs[i].Height != 16 || animation.Delay[0] != 18 || !bytes.Equal(b, original) {
				t.Fatalf("native animation lost: %v", err)
			}
		}
	}
	cfg.GIFAllowAspectRatioMismatch = true
	for _, format := range []string{"", "png", "jpeg", "webp"} {
		for _, input := range [][]byte{webGIFFixture(t), chatGPTWebResizeTestPNG(t, 16, 16, false)} {
			_, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), format, 100, "gpt-image-2.6", "auto", [][]byte{input}, match, cfg)
			if err == nil || !strings.Contains(err.Error(), "image_size_mismatch") {
				t.Fatalf("static format=%q bypassed strict size: %v", format, err)
			}
		}
	}
	match = &helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 32, RatioWidth: 1, RatioHeight: 1}
	data := [][]byte{webGIFFixture(t)}
	outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", data, match, cfg, helps.ChatGPTWebGIFControls{FrameCount: 3, FrameDurationMS: 200})
	if err != nil {
		t.Fatal(err)
	}
	animation, err := gif.DecodeAll(bytes.NewReader(data[0]))
	if err != nil || animation.Config.Width != 32 || outputs[0].Height != 32 || animation.Delay[0] != 20 {
		t.Fatalf("matching GIF not resized: %v", err)
	}
}

func TestChatGPTWebGIFAspectRatioExceptionKeepsValidation(t *testing.T) {
	match := &helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 18, RatioWidth: 16, RatioHeight: 9}
	cfg := core.ChatGPTWebImageConfigSnapshot{StrictSize: true, GIFAllowAspectRatioMismatch: true, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1, MaxImageResponseBytes: 1 << 20}
	for _, tc := range []struct {
		data   []byte
		frames int
	}{{webGIFFixture(t), 4}, {webGIFFixture(t)[:40], 0}, {chatGPTWebResizeTestPNG(t, 16, 16, false), 0}} {
		data := [][]byte{tc.data}
		_, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", data, match, cfg, helps.ChatGPTWebGIFControls{FrameCount: tc.frames})
		if err == nil {
			t.Fatal("invalid animation accepted")
		}
		if !bytes.Equal(data[0], tc.data) {
			t.Fatal("failed processing modified source")
		}
	}
	cfg.MaxImageResponseBytes = len(webGIFFixture(t)) - 1
	_, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", [][]byte{webGIFFixture(t)}, match, cfg)
	if err == nil || !strings.Contains(err.Error(), "image response exceeds") {
		t.Fatalf("budget bypassed: %v", err)
	}
	cfg.MaxImageResponseBytes = 1 << 20
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = prepareChatGPTWebImageOutputsWithContextAndCompression(ctx, "gif", 100, "gpt-image-2.6", "auto", [][]byte{webGIFFixture(t)}, match, cfg)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestChatGPTWebGIFAspectRatioExceptionMemory(t *testing.T) {
	image := &chatGPTWebSpooledImage{size: 1024, width: 439, height: 390, format: "gif", gifInfo: &helps.ChatGPTWebGIFInfo{Width: 439, Height: 390, Frames: 9, Pixels: 439 * 390 * 9}}
	request := &helps.ChatGPTWebImageRequest{OutputFormat: "gif", FrameDurationMS: 200}
	cfg := core.ChatGPTWebImageConfigSnapshot{StrictSize: true, GIFAllowAspectRatioMismatch: true, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1}
	match := &helps.ChatGPTWebImageSizeMatch{Width: 16, Height: 16, RatioWidth: 1, RatioHeight: 1}
	native, err := estimateChatGPTWebWholeFinalizationBytes([]*chatGPTWebSpooledImage{image}, request, nil, cfg, 10<<20)
	if err != nil {
		t.Fatal(err)
	}
	got, err := estimateChatGPTWebWholeFinalizationBytes([]*chatGPTWebSpooledImage{image}, request, match, cfg, 10<<20)
	if err != nil || got < native {
		t.Fatalf("native fallback under-reserved: got=%d native=%d err=%v", got, native, err)
	}
}

func TestChatGPTWebGIFAspectRatioExceptionSnapshot(t *testing.T) {
	cfg := &config.Config{}
	cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch = true
	tolerance := 5.0
	cfg.Images.ChatGPTWeb.GIFAspectRatioMaxErrorPercent = &tolerance
	cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
	cfg.Images.ChatGPTWeb.StrictSize = true
	cfg.Images.ChatGPTWeb.ResizeToRequestedSize = true
	e := NewChatGPTWebExecutor(cfg, nil)
	defer e.Close()
	req := core.Request{Model: "gpt-image-2.6", Payload: []byte(`{"model":"gpt-image-2.6","input":"bounce","tools":[{"type":"image_generation","output_format":"gif","size":"1024x1024"}]}`)}
	opts := core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex}
	prepared, err := e.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.(*chatGPTWebPreparedRequest).discardUsageProjection()
	cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch = false
	tolerance = 2
	e.UpdateConfig(cfg)
	current, err := e.PrepareProviderRequest(t.Context(), req, opts, core.RequestOperationExecute)
	if err != nil {
		t.Fatal(err)
	}
	defer current.(*chatGPTWebPreparedRequest).discardUsageProjection()
	if current.(*chatGPTWebPreparedRequest).imageConfigSnapshot.GIFAllowAspectRatioMismatch || current.(*chatGPTWebPreparedRequest).imageConfigSnapshot.OutputAspectRatioTolerance("gif") != 2 {
		t.Fatal("new request ignored update")
	}
	pinned := core.WithProviderPreparedRequest(opts, e.Identifier(), prepared)
	inFlight, err := e.prepareRuntimeRequest(t.Context(), chatGPTWebRuntimeAuth(), req, pinned, false)
	if err != nil {
		t.Fatal(err)
	}
	defer inFlight.discardUsageProjection()
	if !inFlight.imageConfigSnapshot.GIFAllowAspectRatioMismatch || inFlight.imageConfigSnapshot.OutputAspectRatioTolerance("gif") != 5 {
		t.Fatal("in-flight policy changed")
	}
	bad := req
	bad.Payload = bytes.ReplaceAll(req.Payload, []byte("1024x1024"), []byte("4000x4000"))
	cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch = true
	e.UpdateConfig(cfg)
	if _, err = e.PrepareProviderRequest(t.Context(), bad, opts, core.RequestOperationExecute); err == nil {
		t.Fatal("invalid requested size bypassed preflight")
	}
}

func TestChatGPTWebGIFAspectRatioIndependentTolerance(t *testing.T) {
	match := &helps.ChatGPTWebImageSizeMatch{Width: 25, Height: 24, RatioWidth: 25, RatioHeight: 24}
	for _, format := range []string{"gif", "png", "jpeg", "webp"} {
		for _, tolerance := range []float64{0, 3.9, 4.1, 5} {
			cfg := core.ChatGPTWebImageConfigSnapshot{StrictSize: true, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1, GIFAspectRatioMaxErrorPercent: &tolerance}
			data := [][]byte{webGIFFixture(t)}
			outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), format, 100, "gpt-image-2.6", "auto", data, match, cfg)
			if format != "gif" || tolerance < 4 {
				if err == nil || !strings.Contains(err.Error(), "image_size_mismatch") {
					t.Fatalf("%s/%g: %v", format, tolerance, err)
				}
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			animation, err := gif.DecodeAll(bytes.NewReader(data[0]))
			if err != nil || outputs[0].Width != 25 || animation.Config.Height != 24 || len(animation.Image) != 3 {
				t.Fatalf("GIF tolerance not used for resize: %v", err)
			}
		}
	}
	tolerance := 5.0
	snapshot := core.ChatGPTWebImageConfigSnapshot{AspectRatioMaxErrorPercent: 1, GIFAspectRatioMaxErrorPercent: &tolerance}
	clone := snapshot.Clone()
	tolerance = 0
	if clone.OutputAspectRatioTolerance("gif") != 5 || clone.OutputAspectRatioTolerance("png") != 1 {
		t.Fatal("clone did not freeze format tolerance")
	}
}
