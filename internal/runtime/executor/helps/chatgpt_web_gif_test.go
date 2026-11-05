package helps

import (
	"bytes"
	"context"
	"errors"
	"image"
	"image/color"
	"image/draw"
	"image/gif"
	"strings"
	"testing"
)

func gifFixture(t *testing.T) []byte {
	t.Helper()
	p := color.Palette{color.White, color.Black}
	a := &gif.GIF{Config: image.Config{ColorModel: p, Width: 8, Height: 8}, LoopCount: 0, Delay: []int{10, 20, 30}, Disposal: []byte{gif.DisposalNone, gif.DisposalPrevious, gif.DisposalBackground}}
	for i := 0; i < 3; i++ {
		f := image.NewPaletted(image.Rect(0, 0, 8, 8), p)
		f.SetColorIndex(i, i, 1)
		a.Image = append(a.Image, f)
	}
	var b bytes.Buffer
	if err := gif.EncodeAll(&b, a); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestChatGPTWebGIFInspectionAndProcessing(t *testing.T) {
	data := gifFixture(t)
	info, err := InspectChatGPTWebGIF(t.Context(), bytes.NewReader(data), 1024)
	if err != nil || info.Frames != 3 || info.Pixels != 192 {
		t.Fatalf("info=%+v err=%v", info, err)
	}
	var output bytes.Buffer
	changed, err := ProcessChatGPTWebGIF(t.Context(), data, 3, 0, 8, 8, false, &output)
	if err != nil || changed || output.Len() != 0 {
		t.Fatalf("unchanged GIF: %v %v", changed, err)
	}
	changed, err = ProcessChatGPTWebGIF(t.Context(), data, 3, 125, 16, 16, false, &output)
	if err != nil || !changed {
		t.Fatalf("resize: %v", err)
	}
	decoded, err := gif.DecodeAll(bytes.NewReader(output.Bytes()))
	if err != nil || len(decoded.Image) != 3 || decoded.Config.Width != 16 || decoded.Config.Height != 16 || decoded.LoopCount != 0 {
		t.Fatalf("output: %+v %v", decoded, err)
	}
	for _, delay := range decoded.Delay {
		if delay != 13 {
			t.Fatalf("delay=%d", delay)
		}
	}
	if _, err := InspectChatGPTWebGIF(t.Context(), bytes.NewReader(data), 128); err == nil {
		t.Fatal("total decoded pixel budget ignored")
	}
	if _, err := InspectChatGPTWebGIF(t.Context(), bytes.NewReader(data[:len(data)-3]), 1024); err == nil {
		t.Fatal("truncated tail accepted")
	}
	if _, err := ProcessChatGPTWebGIF(t.Context(), data, 4, 0, 0, 0, false, &output); err == nil {
		t.Fatal("wrong frame count accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := InspectChatGPTWebGIF(ctx, bytes.NewReader(data), 1024); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestChatGPTWebGIFProtocolControlsAndAuxiliaryReferences(t *testing.T) {
	request, err := ParseChatGPTWebRequest([]byte(`{"model":"gpt-image-2.6","input":"bounce","tools":[{"type":"image_generation","output_format":"gif","frame_count":9,"frame_duration_ms":180}]}`))
	if err != nil || request.Image == nil || request.Image.FrameCount != 9 || request.Image.FrameDurationMS != 180 {
		t.Fatalf("parse: %+v %v", request, err)
	}
	for _, fields := range []string{`"output_format":"png","frame_count":3`, `"output_format":"gif","frame_count":1`, `"output_format":"gif","frame_count":65`, `"output_format":"gif","frame_count":2.5`, `"output_format":"gif","frame_duration_ms":0`, `"output_format":"gif","frame_duration_ms":2001`, `"output_format":"gif","output_compression":80`, `"output_format":"gif","partial_images":1`} {
		if _, err := ParseChatGPTWebRequestWithOptions([]byte(`{"input":"bounce","tools":[{"type":"image_generation",`+fields+`}]}`), ChatGPTWebParseOptions{IgnoreUnsupportedImageParams: true}); err == nil {
			t.Errorf("accepted %s", fields)
		}
	}
	acc := &ChatGPTWebImageAccumulator{}
	events := []string{
		`{"message":{"author":{"role":"tool","name":"image_tool.variant"},"status":"finished_successfully","metadata":{"image_gen_title":"fixture"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"sediment://gif_fixture","mime_type":"image/gif","width":439,"height":390,"metadata":{"generation":{"width":439,"height":390,"animation":{"rows":3,"columns":3,"frame_count":9,"frame_duration_ms":180},"spritesheet":{"asset_pointer":"sediment://sheet_fixture"},"gif_speed_variants":{"2":"sediment://fast_fixture"}}}}]}}}`,
		`{"p":"/message/content/parts/0/metadata/generation/spritesheet","o":"add","v":{"asset_pointer":"sediment://sheet_fixture"}}`,
		`{"p":"/message/content/parts/0/metadata/generation/gif_speed_variants/2/asset_pointer","o":"replace","v":"sediment://fast_fixture"}`,
	}
	for _, event := range events {
		if _, err := acc.Apply([]byte(event)); err != nil {
			t.Fatal(err)
		}
	}
	if len(acc.References) != 1 || acc.References[0].ID != "gif_fixture" {
		t.Fatalf("auxiliary output captured: %+v", acc.References)
	}
	if prompt := ChatGPTWebGIFPrompt("bounce", 9, 180); !strings.Contains(prompt, "9 frames") || !strings.Contains(prompt, "180") {
		t.Fatalf("prompt: %s", prompt)
	}
}

func TestChatGPTWebGIFResizeCompositesDisposalAndTransparency(t *testing.T) {
	p := color.Palette{color.Transparent, color.RGBA{R: 255, A: 255}, color.RGBA{G: 255, A: 255}, color.RGBA{B: 255, A: 255}, color.White}
	a := &gif.GIF{Config: image.Config{ColorModel: p, Width: 16, Height: 4}, Delay: []int{10, 20, 30, 40}, LoopCount: 2, Disposal: []byte{gif.DisposalNone, gif.DisposalPrevious, gif.DisposalBackground, gif.DisposalNone}}
	for i := range 4 {
		bounds := image.Rect(i*4, 0, (i+1)*4, 4)
		frame := image.NewPaletted(bounds, p)
		draw.Draw(frame, bounds, image.NewUniform(p[i+1]), image.Point{}, draw.Src)
		a.Image = append(a.Image, frame)
	}
	var source, output bytes.Buffer
	if err := gif.EncodeAll(&source, a); err != nil {
		t.Fatal(err)
	}
	if _, err := ProcessChatGPTWebGIF(t.Context(), source.Bytes(), 4, 0, 32, 8, true, &output); err != nil {
		t.Fatal(err)
	}
	result, err := gif.DecodeAll(&output)
	if err != nil {
		t.Fatal(err)
	}
	if result.LoopCount != 2 || result.Delay[3] != 40 {
		t.Fatal("playback settings changed")
	}
	for i, want := range [][]int{{1, 0, 0, 0}, {1, 2, 0, 0}, {1, 0, 3, 0}, {1, 0, 0, 4}} {
		for column, index := range want {
			got := color.NRGBAModel.Convert(result.Image[i].At(column*8+3, 3)).(color.NRGBA)
			wantColor := color.NRGBAModel.Convert(p[index]).(color.NRGBA)
			if got != wantColor {
				t.Fatalf("frame %d column %d: got %v want %v", i, column, got, wantColor)
			}
		}
	}
}
