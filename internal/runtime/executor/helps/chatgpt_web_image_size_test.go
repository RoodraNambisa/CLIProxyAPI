package helps

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestChatGPTWebImageSizeFromResponsesPayload(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		wantSize  string
		wantImage bool
	}{
		{name: "no tool", payload: `{"tools":[{"type":"web_search"}]}`},
		{name: "missing size", payload: `{"tools":[{"type":"image_generation"}]}`, wantImage: true},
		{name: "auto", payload: `{"tools":[{"type":"image_generation","size":"auto"}]}`, wantSize: "auto", wantImage: true},
		{name: "explicit", payload: `{"tools":[{"type":"image_generation","size":"1920x1080"}]}`, wantSize: "1920x1080", wantImage: true},
		{name: "non string", payload: `{"tools":[{"type":"image_generation","size":1024}]}`, wantSize: "1024", wantImage: true},
		{name: "invalid json", payload: `{`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotSize, gotImage := ChatGPTWebImageSizeFromResponsesPayload([]byte(test.payload))
			if gotSize != test.wantSize || gotImage != test.wantImage {
				t.Fatalf("size, image = %q, %v; want %q, %v", gotSize, gotImage, test.wantSize, test.wantImage)
			}
		})
	}
}

func TestChatGPTWebStrictImageSizeErrorPayload(t *testing.T) {
	payload := ChatGPTWebStrictImageSizeErrorPayload("1024x1536", 3840, 1)
	if got := gjson.GetBytes(payload, "error.type").String(); got != "invalid_request_error" {
		t.Fatalf("error type = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.param").String(); got != "size" {
		t.Fatalf("error param = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "invalid_value" {
		t.Fatalf("error code = %q; payload=%s", got, payload)
	}
}

func TestResolveChatGPTWebImageSize(t *testing.T) {
	tests := []struct {
		name       string
		size       string
		maxEdge    int
		maxError   float64
		wantState  ChatGPTWebImageSizeDisposition
		wantRatio  string
		wantWidth  int
		wantHeight int
	}{
		{name: "empty", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeUnspecified},
		{name: "auto", size: "AUTO", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeUnspecified},
		{name: "square", size: "1024x1024", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "1:1", wantWidth: 1024, wantHeight: 1024},
		{name: "portrait", size: "960x1280", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "3:4", wantWidth: 960, wantHeight: 1280},
		{name: "story", size: "941x1672", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "9:16", wantWidth: 941, wantHeight: 1672},
		{name: "landscape", size: "1280x960", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "4:3", wantWidth: 1280, wantHeight: 960},
		{name: "wide", size: "1920x1080", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "16:9", wantWidth: 1920, wantHeight: 1080},
		{name: "boundary accepted", size: "101x100", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeMatched, wantRatio: "1:1", wantWidth: 101, wantHeight: 100},
		{name: "boundary rejected", size: "101x100", maxEdge: 3840, maxError: 0.99, wantState: ChatGPTWebImageSizeIgnored},
		{name: "unsupported ratio", size: "1024x1536", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeIgnored},
		{name: "oversize", size: "4000x4000", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeIgnored},
		{name: "malformed", size: "1024-by-1024", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeInvalid},
		{name: "zero", size: "0x1024", maxEdge: 3840, maxError: 1, wantState: ChatGPTWebImageSizeInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, state := ResolveChatGPTWebImageSize(test.size, test.maxEdge, test.maxError)
			if state != test.wantState {
				t.Fatalf("state = %v, want %v; match=%#v", state, test.wantState, got)
			}
			if got.Ratio != test.wantRatio || got.Width != test.wantWidth || got.Height != test.wantHeight {
				t.Fatalf("match = %#v, want ratio=%q size=%dx%d", got, test.wantRatio, test.wantWidth, test.wantHeight)
			}
		})
	}
}

func TestChatGPTWebImageRatioMatches(t *testing.T) {
	if !ChatGPTWebImageRatioMatches(901, 1600, 9, 16, 1) {
		t.Fatal("rounded 9:16 result should match")
	}
	if ChatGPTWebImageRatioMatches(920, 1600, 9, 16, 1) {
		t.Fatal("out-of-tolerance result should not match")
	}
	if ChatGPTWebImageRatioMatches(0, 1600, 9, 16, 1) {
		t.Fatal("invalid dimensions should not match")
	}
}
