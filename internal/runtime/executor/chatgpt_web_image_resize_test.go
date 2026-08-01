package executor

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
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
