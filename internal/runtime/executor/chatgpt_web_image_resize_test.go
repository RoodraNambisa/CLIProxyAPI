package executor

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestPrepareChatGPTWebImageOutputsResizesMatchedResult(t *testing.T) {
	for _, filter := range []string{config.ChatGPTWebResizeFilterCatmullRom, config.ChatGPTWebResizeFilterApproxBiLinear} {
		t.Run(filter, func(t *testing.T) {
			images := [][]byte{chatGPTWebResizeTestPNG(t, 12, 16, true)}
			outputs, err := prepareChatGPTWebImageOutputsWithConfig(
				"png",
				"gpt-image-2",
				"medium",
				images,
				&helps.ChatGPTWebImageSizeMatch{Width: 24, Height: 32, RatioWidth: 3, RatioHeight: 4, Ratio: "3:4"},
				cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
					AspectRatioMaxErrorPercent: 1,
					ResizeToRequestedSize:      true,
					ResizeFilter:               filter,
					MaxImageResponseBytes:      1 << 20,
				},
			)
			if err != nil {
				t.Fatalf("prepareChatGPTWebImageOutputsWithConfig() error = %v", err)
			}
			if len(outputs) != 1 || outputs[0].Width != 24 || outputs[0].Height != 32 {
				t.Fatalf("outputs = %#v", outputs)
			}
			decoded, errDecode := png.Decode(bytes.NewReader(images[0]))
			if errDecode != nil {
				t.Fatalf("decode resized PNG: %v", errDecode)
			}
			if got := color.NRGBAModel.Convert(decoded.At(12, 16)).(color.NRGBA).A; got == 255 {
				t.Fatalf("alpha = %d, want preserved transparency", got)
			}
		})
	}
}

func TestEstimateChatGPTWebImagePostProcessingBytes(t *testing.T) {
	tests := []struct {
		name            string
		sourceWidth     int
		sourceHeight    int
		targetWidth     int
		targetHeight    int
		needsResize     bool
		requestedFormat string
		wantBytes       int64
	}{
		{name: "PNG conversion", sourceWidth: 100, sourceHeight: 50, requestedFormat: "png", wantBytes: 60_000},
		{name: "JPEG conversion", sourceWidth: 100, sourceHeight: 50, requestedFormat: "jpeg", wantBytes: 80_000},
		{name: "resize PNG", sourceWidth: 100, sourceHeight: 50, targetWidth: 200, targetHeight: 100, needsResize: true, requestedFormat: "png", wantBytes: 200_000},
		{name: "resize WebP", sourceWidth: 100, sourceHeight: 50, targetWidth: 200, targetHeight: 100, needsResize: true, requestedFormat: "webp", wantBytes: 280_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotBytes := estimateChatGPTWebImagePostProcessingBytes(
				test.sourceWidth,
				test.sourceHeight,
				test.targetWidth,
				test.targetHeight,
				test.needsResize,
				test.requestedFormat,
			)
			if gotBytes != test.wantBytes {
				t.Fatalf("estimate = %d, want %d", gotBytes, test.wantBytes)
			}
		})
	}
}

func TestPrepareChatGPTWebImageOutputsOnlyWaitsForActualPostProcessing(t *testing.T) {
	original := chatGPTWebResizeTestPNG(t, 16, 16, false)
	canceledContext, cancel := context.WithCancel(t.Context())
	cancel()

	passthrough := [][]byte{append([]byte(nil), original...)}
	if _, errPrepare := prepareChatGPTWebImageOutputsWithContextAndCompression(
		canceledContext,
		"png",
		100,
		"gpt-image-2",
		"auto",
		passthrough,
		nil,
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
	); errPrepare != nil {
		t.Fatalf("passthrough with canceled context error = %v", errPrepare)
	}
	if !bytes.Equal(passthrough[0], original) {
		t.Fatal("passthrough image was modified")
	}

	converted := [][]byte{append([]byte(nil), original...)}
	_, errPrepare := prepareChatGPTWebImageOutputsWithContextAndCompression(
		canceledContext,
		"jpeg",
		90,
		"gpt-image-2",
		"auto",
		converted,
		nil,
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
	)
	if !errors.Is(errPrepare, context.Canceled) {
		t.Fatalf("conversion with canceled context error = %v, want context canceled", errPrepare)
	}
	if !bytes.Equal(converted[0], original) {
		t.Fatal("canceled conversion modified the original image")
	}
}

func TestPrepareChatGPTWebImageOutputsKeepsRatioMismatch(t *testing.T) {
	original := chatGPTWebResizeTestPNG(t, 16, 16, false)
	images := [][]byte{append([]byte(nil), original...)}
	outputs, err := prepareChatGPTWebImageOutputsWithConfig(
		"png",
		"gpt-image-2",
		"auto",
		images,
		&helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 18, RatioWidth: 16, RatioHeight: 9, Ratio: "16:9"},
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
			AspectRatioMaxErrorPercent: 1,
			ResizeToRequestedSize:      true,
			ResizeFilter:               config.ChatGPTWebResizeFilterCatmullRom,
			MaxImageResponseBytes:      1 << 20,
		},
	)
	if err != nil {
		t.Fatalf("prepareChatGPTWebImageOutputsWithConfig() error = %v", err)
	}
	if !bytes.Equal(images[0], original) {
		t.Fatal("ratio-mismatched image was modified")
	}
	if len(outputs) != 1 || outputs[0].Width != 16 || outputs[0].Height != 16 {
		t.Fatalf("outputs = %#v", outputs)
	}
}

func TestPrepareChatGPTWebImageOutputsRejectsStrictRatioMismatch(t *testing.T) {
	original := chatGPTWebResizeTestPNG(t, 16, 16, false)
	images := [][]byte{append([]byte(nil), original...)}
	_, err := prepareChatGPTWebImageOutputsWithConfig(
		"png",
		"gpt-image-2",
		"auto",
		images,
		&helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 18, RatioWidth: 16, RatioHeight: 9, Ratio: "16:9"},
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
			AspectRatioMaxErrorPercent: 1,
			ResizeToRequestedSize:      true,
			StrictSize:                 true,
			ResizeFilter:               config.ChatGPTWebResizeFilterCatmullRom,
			MaxImageResponseBytes:      1 << 20,
		},
	)
	if err == nil || !strings.Contains(err.Error(), `"code":"image_size_mismatch"`) ||
		!strings.Contains(err.Error(), "chatgpt web") {
		t.Fatalf("prepareChatGPTWebImageOutputsWithConfig() error = %v", err)
	}
	if !bytes.Equal(images[0], original) {
		t.Fatal("strict ratio failure modified the original image")
	}
	var status interface{ StatusCode() int }
	if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
		t.Fatalf("strict ratio status error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestPrepareChatGPTWebImageOutputsConvertsPNGToJPEGAndWebP(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 16, 16))
	for y := range 16 {
		for x := range 16 {
			source.SetNRGBA(x, y, color.NRGBA{R: 200, G: 40, B: 20, A: 255})
		}
	}
	for y := 8; y < 16; y++ {
		for x := 8; x < 16; x++ {
			source.SetNRGBA(x, y, color.NRGBA{R: 255, A: 0})
		}
	}
	var encodedSource bytes.Buffer
	if err := png.Encode(&encodedSource, source); err != nil {
		t.Fatalf("encode source PNG: %v", err)
	}

	t.Run("JPEG flattens transparency on white", func(t *testing.T) {
		images := [][]byte{append([]byte(nil), encodedSource.Bytes()...)}
		outputs, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"jpeg", 90, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
		)
		if err != nil {
			t.Fatalf("prepare JPEG error = %v", err)
		}
		if got := chatGPTWebImageOutputFormat(images[0]); got != "jpeg" {
			t.Fatalf("output format = %q, want jpeg", got)
		}
		if len(outputs) != 1 || outputs[0].Width != 16 || outputs[0].Height != 16 {
			t.Fatalf("outputs = %#v", outputs)
		}
		decoded, _, errDecode := image.Decode(bytes.NewReader(images[0]))
		if errDecode != nil {
			t.Fatalf("decode JPEG: %v", errDecode)
		}
		pixel := color.NRGBAModel.Convert(decoded.At(14, 14)).(color.NRGBA)
		if pixel.A != 255 || pixel.R < 220 || pixel.G < 220 || pixel.B < 220 {
			t.Fatalf("transparent pixel = %#v, want opaque white background", pixel)
		}
	})

	t.Run("JPEG accepts the lowest API compression value", func(t *testing.T) {
		images := [][]byte{append([]byte(nil), encodedSource.Bytes()...)}
		_, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"jpeg", 0, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
		)
		if err != nil {
			t.Fatalf("prepare JPEG with zero compression error = %v", err)
		}
		if got := chatGPTWebImageOutputFormat(images[0]); got != "jpeg" {
			t.Fatalf("output format = %q, want jpeg", got)
		}
	})

	t.Run("WebP preserves transparency", func(t *testing.T) {
		images := [][]byte{append([]byte(nil), encodedSource.Bytes()...)}
		outputs, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"webp", 0, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
		)
		if err != nil {
			t.Fatalf("prepare WebP error = %v", err)
		}
		if got := chatGPTWebImageOutputFormat(images[0]); got != "webp" {
			t.Fatalf("output format = %q, want webp", got)
		}
		if len(outputs) != 1 || outputs[0].Width != 16 || outputs[0].Height != 16 {
			t.Fatalf("outputs = %#v", outputs)
		}
		decoded, _, errDecode := image.Decode(bytes.NewReader(images[0]))
		if errDecode != nil {
			t.Fatalf("decode WebP: %v", errDecode)
		}
		if got := color.NRGBAModel.Convert(decoded.At(14, 14)).(color.NRGBA).A; got != 0 {
			t.Fatalf("transparent pixel alpha = %d, want 0", got)
		}
	})

	t.Run("WebP applies compression to an existing WebP", func(t *testing.T) {
		original, errEncode := encodeChatGPTWebOutputImage(source, "webp", 90, 1<<20)
		if errEncode != nil {
			t.Fatalf("encode original WebP: %v", errEncode)
		}
		images := [][]byte{append([]byte(nil), original...)}
		_, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"webp", 1, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
		)
		if err != nil {
			t.Fatalf("re-encode WebP error = %v", err)
		}
		if bytes.Equal(images[0], original) {
			t.Fatal("existing WebP was returned without applying output_compression")
		}
	})
}

func TestPrepareChatGPTWebImageOutputsUsesTransactionalBudgetFallback(t *testing.T) {
	original := chatGPTWebResizeTestPNG(t, 16, 16, true)
	decoded, _, errDecode := image.Decode(bytes.NewReader(original))
	if errDecode != nil {
		t.Fatalf("decode source PNG: %v", errDecode)
	}
	jpegCandidate, errEncode := encodeChatGPTWebOutputImage(decoded, "jpeg", 90, 1<<20)
	if errEncode != nil {
		t.Fatalf("encode candidate JPEG: %v", errEncode)
	}
	if len(jpegCandidate) <= len(original) {
		t.Fatalf("candidate length = %d, original = %d; fixture cannot exercise budget fallback", len(jpegCandidate), len(original))
	}
	limit := len(jpegCandidate) + len(original)

	t.Run("non-strict returns the complete original batch", func(t *testing.T) {
		images := [][]byte{append([]byte(nil), original...), append([]byte(nil), original...)}
		outputs, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"jpeg", 90, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: limit},
		)
		if err != nil {
			t.Fatalf("prepare non-strict batch error = %v", err)
		}
		if len(outputs) != 2 || outputs[0].Width != 16 || outputs[1].Height != 16 {
			t.Fatalf("outputs = %#v", outputs)
		}
		for index := range images {
			if !bytes.Equal(images[index], original) || chatGPTWebImageOutputFormat(images[index]) != "png" {
				t.Fatalf("image %d was partially converted", index)
			}
		}
	})

	t.Run("strict rejects the complete batch", func(t *testing.T) {
		images := [][]byte{append([]byte(nil), original...), append([]byte(nil), original...)}
		_, err := prepareChatGPTWebImageOutputsWithConfigAndCompression(
			"jpeg", 90, "gpt-image-2", "medium", images, nil,
			cliproxyexecutor.ChatGPTWebImageConfigSnapshot{StrictSize: true, MaxImageResponseBytes: limit},
		)
		if err == nil || !strings.Contains(err.Error(), `"code":"image_response_budget_exceeded"`) ||
			!strings.Contains(err.Error(), "chatgpt web") {
			t.Fatalf("prepare strict batch error = %v", err)
		}
		for index := range images {
			if !bytes.Equal(images[index], original) {
				t.Fatalf("strict failure modified image %d", index)
			}
		}
		var status interface{ StatusCode() int }
		if !errors.As(err, &status) || status.StatusCode() != http.StatusBadGateway {
			t.Fatalf("strict batch status error = %v", err)
		}
		assertChatGPTWebNonAuthNonRetryError(t, err)
	})
}

func TestResizeChatGPTWebImageFallsBackWhenEncodingExceedsBudget(t *testing.T) {
	original := chatGPTWebResizeTestPNG(t, 16, 16, false)
	resized, ok, err := resizeChatGPTWebImageToPNG(
		original,
		&helps.ChatGPTWebImageSizeMatch{Width: 64, Height: 64, RatioWidth: 1, RatioHeight: 1, Ratio: "1:1"},
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
			AspectRatioMaxErrorPercent: 1,
			ResizeFilter:               config.ChatGPTWebResizeFilterCatmullRom,
		},
		8,
	)
	if err != nil || ok || resized != nil {
		t.Fatalf("resize result bytes=%d ok=%v err=%v", len(resized), ok, err)
	}
}

func TestPrepareChatGPTWebImageOutputsRejectsOriginalBatchOverBudget(t *testing.T) {
	first := append(chatGPTWebResizeTestPNG(t, 1, 1, false), make([]byte, 600<<10)...)
	second := append(chatGPTWebResizeTestPNG(t, 1, 1, false), make([]byte, 600<<10)...)
	_, err := prepareChatGPTWebImageOutputsWithConfig(
		"png",
		"gpt-image-2",
		"auto",
		[][]byte{first, second},
		nil,
		cliproxyexecutor.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20},
	)
	if err == nil || !strings.Contains(err.Error(), "image response exceeds 1048576 bytes") {
		t.Fatalf("prepareChatGPTWebImageOutputsWithConfig() error = %v", err)
	}
	assertChatGPTWebNonAuthNonRetryError(t, err)
}

func TestChatGPTWebImageCenterCrop(t *testing.T) {
	tests := []struct {
		name       string
		bounds     image.Rectangle
		targetW    int
		targetH    int
		wantBounds image.Rectangle
	}{
		{name: "crop width", bounds: image.Rect(0, 0, 15, 10), targetW: 4, targetH: 3, wantBounds: image.Rect(1, 0, 14, 10)},
		{name: "crop height with offset", bounds: image.Rect(10, 20, 20, 35), targetW: 3, targetH: 4, wantBounds: image.Rect(10, 21, 20, 34)},
		{name: "exact", bounds: image.Rect(0, 0, 16, 9), targetW: 16, targetH: 9, wantBounds: image.Rect(0, 0, 16, 9)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := chatGPTWebImageCenterCrop(test.bounds, test.targetW, test.targetH); got != test.wantBounds {
				t.Fatalf("crop = %v, want %v", got, test.wantBounds)
			}
		})
	}
}

func BenchmarkResizeChatGPTWebImage(b *testing.B) {
	tests := []struct {
		name         string
		sourceWidth  int
		sourceHeight int
		targetWidth  int
		targetHeight int
		ratioWidth   int
		ratioHeight  int
	}{
		{name: "1024_to_3840", sourceWidth: 1024, sourceHeight: 1024, targetWidth: 3840, targetHeight: 3840, ratioWidth: 1, ratioHeight: 1},
		{name: "1536_to_3840", sourceWidth: 1536, sourceHeight: 864, targetWidth: 3840, targetHeight: 2160, ratioWidth: 16, ratioHeight: 9},
	}
	for _, test := range tests {
		b.Run(test.name, func(b *testing.B) {
			data := chatGPTWebResizeBenchmarkPNG(b, test.sourceWidth, test.sourceHeight)
			match := &helps.ChatGPTWebImageSizeMatch{
				Width: test.targetWidth, Height: test.targetHeight,
				RatioWidth: test.ratioWidth, RatioHeight: test.ratioHeight,
			}
			for _, filter := range []string{config.ChatGPTWebResizeFilterCatmullRom, config.ChatGPTWebResizeFilterApproxBiLinear} {
				b.Run(filter, func(b *testing.B) {
					snapshot := cliproxyexecutor.ChatGPTWebImageConfigSnapshot{
						AspectRatioMaxErrorPercent: 1,
						ResizeFilter:               filter,
					}
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						if _, ok, err := resizeChatGPTWebImageToPNG(data, match, snapshot, chatGPTWebMaxImageResponseBytes); err != nil || !ok {
							b.Fatalf("resize ok=%v err=%v", ok, err)
						}
					}
				})
			}
		})
	}
}

func chatGPTWebResizeTestPNG(t *testing.T, width, height int, transparent bool) []byte {
	t.Helper()
	imageData := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			alpha := uint8(255)
			if transparent && x >= width/2 && y >= height/2 {
				alpha = 64
			}
			imageData.SetNRGBA(x, y, color.NRGBA{R: uint8(x * 17), G: uint8(y * 13), B: uint8((x + y) * 7), A: alpha})
		}
	}
	var output bytes.Buffer
	if err := png.Encode(&output, imageData); err != nil {
		t.Fatalf("encode PNG: %v", err)
	}
	return output.Bytes()
}

func chatGPTWebResizeBenchmarkPNG(b *testing.B, width, height int) []byte {
	b.Helper()
	imageData := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			imageData.SetNRGBA(x, y, color.NRGBA{R: uint8(x), G: uint8(y), B: uint8(x + y), A: 255})
		}
	}
	var output bytes.Buffer
	if err := png.Encode(&output, imageData); err != nil {
		b.Fatalf("encode PNG: %v", err)
	}
	return output.Bytes()
}
