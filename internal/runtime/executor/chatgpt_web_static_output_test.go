package executor

import (
	"bytes"
	"image"
	"image/color"
	"image/gif"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestChatGPTWebStaticOutputComposesFirstPartialFrame(t *testing.T) {
	p := color.Palette{color.Black, color.White}
	frame := image.NewPaletted(image.Rect(2, 2, 4, 4), p)
	frame.SetColorIndex(2, 2, 1)
	var b bytes.Buffer
	if err := gif.EncodeAll(&b, &gif.GIF{Config: image.Config{Width: 16, Height: 16, ColorModel: p}, Image: []*image.Paletted{frame}, Delay: []int{10}}); err != nil {
		t.Fatal(err)
	}
	data := [][]byte{b.Bytes()}
	if _, err := prepareChatGPTWebImageOutputs("png", "gpt-image-2.5", "auto", data); err != nil {
		t.Fatal(err)
	}
	decoded, _, err := image.Decode(bytes.NewReader(data[0]))
	if err != nil || decoded.Bounds() != image.Rect(0, 0, 16, 16) {
		t.Fatalf("canvas: %v", err)
	}
	r, _, _, _ := decoded.At(2, 2).RGBA()
	if r != 65535 {
		t.Fatal("first frame content lost")
	}
}

func TestChatGPTWebStaticOutputFromGIF(t *testing.T) {
	for _, format := range []string{"", "png", "jpeg", "webp"} {
		for _, mismatch := range []bool{false, true} {
			data := [][]byte{webGIFFixture(t), webGIFFixture(t)}
			cfg := core.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1}
			var match *helps.ChatGPTWebImageSizeMatch
			if mismatch {
				match = &helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 18, RatioWidth: 16, RatioHeight: 9}
			}
			outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), format, 100, "gpt-image-2.5", "auto", data, match, cfg)
			if err != nil {
				t.Fatal(err)
			}
			want := format
			if want == "" {
				want = "png"
			}
			for i, b := range data {
				decoded, actual, err := image.Decode(bytes.NewReader(b))
				if err != nil || actual != want || decoded.Bounds().Dx() != 16 || outputs[i].Width != 16 {
					t.Fatalf("format=%s mismatch=%t actual=%s err=%v", want, mismatch, actual, err)
				}
			}
		}
	}
}

func TestChatGPTWebStaticOutputNativeBudgetFallback(t *testing.T) {
	data := [][]byte{webGIFFixture(t)}
	cfg := core.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 600, ResizeToRequestedSize: true, AspectRatioMaxErrorPercent: 1}
	match := &helps.ChatGPTWebImageSizeMatch{Width: 1024, Height: 1024, RatioWidth: 1, RatioHeight: 1}
	outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "png", 100, "gpt-image-2.5", "auto", data, match, cfg)
	if err != nil {
		t.Fatal(err)
	}
	decoded, actual, err := image.Decode(bytes.NewReader(data[0]))
	if err != nil || actual != "png" || decoded.Bounds().Dx() != 16 || outputs[0].Width != 16 {
		t.Fatalf("fallback format=%s err=%v", actual, err)
	}
}

func TestChatGPTWebStaticOutputNativeFallbackMemory(t *testing.T) {
	spooled := []*chatGPTWebSpooledImage{{size: 1024, width: 1024, height: 1024, format: "gif"}}
	request := &helps.ChatGPTWebImageRequest{OutputFormat: "webp"}
	cfg := core.ChatGPTWebImageConfigSnapshot{ResizeToRequestedSize: true}
	match := &helps.ChatGPTWebImageSizeMatch{Width: 16, Height: 16, RatioWidth: 1, RatioHeight: 1}
	got, err := estimateChatGPTWebWholeFinalizationBytes(spooled, request, match, cfg, 10<<20)
	if err != nil {
		t.Fatal(err)
	}
	native, err := estimateChatGPTWebWholeFinalizationBytes(spooled, request, nil, cfg, 10<<20)
	if err != nil || got < native {
		t.Fatalf("fallback reservation=%d native=%d err=%v", got, native, err)
	}
}
