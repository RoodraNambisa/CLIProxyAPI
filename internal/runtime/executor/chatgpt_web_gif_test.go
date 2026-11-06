package executor

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/gif"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
	"github.com/tidwall/gjson"
)

func webGIFFixture(t *testing.T) []byte {
	t.Helper()
	p := color.Palette{color.White, color.Black}
	a := &gif.GIF{Config: image.Config{ColorModel: p, Width: 16, Height: 16}, Delay: []int{18, 18, 18}, LoopCount: 0}
	for i := 0; i < 3; i++ {
		f := image.NewPaletted(image.Rect(0, 0, 16, 16), p)
		f.SetColorIndex(i, i, 1)
		a.Image = append(a.Image, f)
	}
	var b bytes.Buffer
	if err := gif.EncodeAll(&b, a); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}

func TestChatGPTWebGIFOutputsRemainAnimated(t *testing.T) {
	original := webGIFFixture(t)
	for _, resize := range []bool{false, true} {
		images := [][]byte{bytes.Clone(original)}
		cfg := core.ChatGPTWebImageConfigSnapshot{MaxImageResponseBytes: 1 << 20, ResizeToRequestedSize: resize, StrictSize: true, AspectRatioMaxErrorPercent: 1}
		var match *helps.ChatGPTWebImageSizeMatch
		if resize {
			match = &helps.ChatGPTWebImageSizeMatch{Width: 32, Height: 32, RatioWidth: 1, RatioHeight: 1}
		}
		outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", images, match, cfg, helps.ChatGPTWebGIFControls{FrameCount: 3})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := buildPreparedChatGPTWebImageCompletedEvent("gpt-image-2.6", "gif", images, outputs, nil)
		if err != nil {
			t.Fatal(err)
		}
		if gjson.GetBytes(payload, "response.output.0.output_format").String() != "gif" {
			t.Fatal("wrong output MIME")
		}
		decoded, err := base64.StdEncoding.DecodeString(gjson.GetBytes(payload, "response.output.0.result").String())
		if err != nil {
			t.Fatal(err)
		}
		animation, err := gif.DecodeAll(bytes.NewReader(decoded))
		if err != nil || len(animation.Image) != 3 || animation.Delay[1] != 18 || animation.LoopCount != 0 {
			t.Fatalf("lost animation: %v", err)
		}
		want := 16
		if resize {
			want = 32
		}
		if animation.Config.Width != want || animation.Config.Height != want {
			t.Fatal("wrong animation size")
		}
		if !resize && !bytes.Equal(original, decoded) {
			t.Fatal("unchanged GIF was re-encoded")
		}
	}
}

func TestChatGPTWebGIFRejectsStaticOrInvalidAnimation(t *testing.T) {
	for _, data := range [][]byte{chatGPTWebResizeTestPNG(t, 16, 16, false), webGIFFixture(t)[:40]} {
		_, err := prepareChatGPTWebImageOutputs("gif", "gpt-image-2.6", "auto", [][]byte{data})
		if err == nil {
			t.Fatal("static or incomplete output accepted as GIF")
		}
		assertChatGPTWebNonAuthNonRetryError(t, chatGPTWebCommittedRequestError(t.Context(), err))
	}
	data := webGIFFixture(t)
	_, _, _, err := processChatGPTWebGIFOutput(t.Context(), data, helps.ChatGPTWebGIFControls{FrameCount: 4}, nil, core.ChatGPTWebImageConfigSnapshot{}, 1<<20)
	if err == nil || !strings.Contains(err.Error(), "image_animation_mismatch") {
		t.Fatalf("mismatch: %v", err)
	}
	var estimated int64
	err = validateChatGPTWebDownloadedImageWithAdmission(t.Context(), data, "image/gif", func(_ context.Context, n int64) (func(), error) { estimated = n; return func() {}, nil })
	if err != nil || estimated < int64(3*16*16*8) {
		t.Fatalf("frame memory not counted: %d %v", estimated, err)
	}
}

func TestChatGPTWebGIFBudgetAndCancellation(t *testing.T) {
	data := webGIFFixture(t)
	for _, duration := range []int{0, 200} {
		_, _, _, err := processChatGPTWebGIFOutput(t.Context(), data, helps.ChatGPTWebGIFControls{FrameDurationMS: duration}, nil, core.ChatGPTWebImageConfigSnapshot{}, 1)
		if err == nil || strings.Contains(err.Error(), "image_animation_mismatch") {
			t.Fatalf("output budget was not reported distinctly: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, _, err := processChatGPTWebGIFOutput(ctx, data, helps.ChatGPTWebGIFControls{}, nil, core.ChatGPTWebImageConfigSnapshot{}, len(data)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	info := helps.ChatGPTWebGIFInfo{Width: 3840, Height: 3840, Frames: 2, Pixels: 2}
	if got := chatGPTWebGIFWorkingBytes(info, 16, 16); got < 3840*3840*8 {
		t.Fatalf("partial frames omitted full compositing canvases: %d", got)
	}
}

func TestChatGPTWebGIFExecutorDownloadsNativeAnimation(t *testing.T) {
	for _, tc := range []struct {
		name              string
		strict, exception bool
	}{
		{name: "native"}, {name: "strict mismatch", strict: true}, {name: "GIF exception", strict: true, exception: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := webGIFFixture(t)
			base := newChatGPTWebImageFixture(t)
			t.Cleanup(base.Close)
			var submissions, downloads atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/backend-api/f/conversation":
					submissions.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
						return
					}
					prompt := gjson.GetBytes(body, "messages.0.content.parts.0").String()
					if !strings.Contains(prompt, "3 frames") || !strings.Contains(prompt, "180 milliseconds") {
						t.Errorf("animation controls missing from prompt: %s", prompt)
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, `data: {"message":{"author":{"role":"tool","name":"image_tool.variant"},"status":"finished_successfully","metadata":{"async_task_type":"image_gen"},"content":{"content_type":"multimodal_text","parts":[{"content_type":"image_asset_pointer","asset_pointer":"file-service://generated-gif","mime_type":"image/gif","width":16,"height":16,"metadata":{"generation":{"width":16,"height":16,"animation":{"rows":1,"columns":3,"frame_count":3,"frame_duration_ms":180},"spritesheet":{"asset_pointer":"file-service://sheet"},"gif_speed_variants":{"2":"file-service://fast"}}}}]}}}`+"\n\n")
					_, _ = io.WriteString(w, `data: {"message":{"author":{"role":"assistant"},"status":"finished_successfully","end_turn":true,"content":{"content_type":"text","parts":["Here is the animation."]}}}`+"\n\ndata: [DONE]\n\n")
				case "/backend-api/files/generated-gif/download":
					_ = json.NewEncoder(w).Encode(map[string]string{"download_url": server.URL + "/native-gif"})
				case "/native-gif":
					downloads.Add(1)
					w.Header().Set("Content-Type", "image/gif")
					_, _ = w.Write(data)
				default:
					base.Config.Handler.ServeHTTP(w, r)
				}
			}))
			t.Cleanup(server.Close)
			cfg := &config.Config{}
			cfg.Images.ChatGPTWeb.AdaptSizeToAspectRatio = true
			cfg.Images.ChatGPTWeb.StrictSize = tc.strict
			cfg.Images.ChatGPTWeb.ResizeToRequestedSize = tc.strict
			cfg.Images.ChatGPTWeb.GIFAllowAspectRatioMismatch = tc.exception
			e := NewChatGPTWebExecutor(cfg, nil)
			t.Cleanup(func() { _ = e.Close() })
			e.runtimeBaseURL = server.URL
			result, err := e.Execute(t.Context(), chatGPTWebRuntimeAuth(), core.Request{Model: "gpt-image-2.6", Payload: []byte(`{"model":"gpt-image-2.6","input":"blue ball bouncing","tools":[{"type":"image_generation","output_format":"gif","size":"1536x864","frame_count":3,"frame_duration_ms":180}]}`)}, core.Options{SourceFormat: sdktranslator.FormatCodex, ResponseFormat: sdktranslator.FormatCodex})
			if got := fmt.Sprintf("%d/%d", submissions.Load(), downloads.Load()); got != "1/1" {
				t.Fatalf("submission/download counts = %s, error=%v", got, err)
			}
			if tc.strict && !tc.exception {
				if err == nil || !strings.Contains(err.Error(), "image_size_mismatch") {
					t.Fatalf("missing strict ratio failure: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			output, err := base64.StdEncoding.DecodeString(gjson.GetBytes(result.Payload, "response.output.0.result").String())
			if err != nil || !bytes.Equal(output, data) {
				t.Fatal("executor changed the native GIF")
			}
			if got := gjson.GetBytes(result.Payload, "response.tool_usage.image_gen.output_tokens").Int(); got != 321 {
				t.Fatalf("GIF sheet usage = %d, want 321 from the delivered 16x16 frames in the 1x3 upstream layout", got)
			}
			if got := fmt.Sprintf("%d/%d", submissions.Load(), downloads.Load()); got != "1/1" {
				t.Fatalf("submission/download counts = %s, error=%v", got, err)
			}
		})
	}
}

func TestChatGPTWebGIFUsageKeepsDeliveredSizeAndIgnoresPlaybackSpeed(t *testing.T) {
	for _, tc := range []struct {
		resize   bool
		duration int
		want     int64
	}{{false, 200, 479}, {true, 200, 974}, {true, 1000, 974}} {
		data := [][]byte{webGIFFixture(t)}
		cfg := core.ChatGPTWebImageConfigSnapshot{ResizeToRequestedSize: tc.resize, StrictSize: true, AspectRatioMaxErrorPercent: 1, MaxImageResponseBytes: 16 << 20}
		match := &helps.ChatGPTWebImageSizeMatch{Width: 512, Height: 512, RatioWidth: 1, RatioHeight: 1}
		outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "gif", 100, "gpt-image-2.6", "auto", data, match, cfg, helps.ChatGPTWebGIFControls{FrameCount: 3, FrameDurationMS: tc.duration})
		if err != nil {
			t.Fatal(err)
		}
		got, err := helps.ChatGPTWebImageUsageOutputTokenCount(outputs[0], "medium")
		if err != nil || got != tc.want {
			t.Fatalf("resize=%t duration=%d: tokens=%d err=%v want=%d", tc.resize, tc.duration, got, err, tc.want)
		}
	}
	data := [][]byte{webGIFFixture(t)}
	outputs, err := prepareChatGPTWebImageOutputsWithContextAndCompression(t.Context(), "png", 100, "gpt-image-2.6", "auto", data, nil, core.ChatGPTWebImageConfigSnapshot{})
	if err != nil || outputs[0].Animation.FrameCount != 0 {
		t.Fatalf("static delivery retained GIF frame billing: %v", err)
	}
}
