package helps

import (
	"strings"
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

func TestChatGPTWebImageControlsFromResponsesPayload(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		wantSize  string
		wantN     int
		wantImage bool
	}{
		{name: "no tool", payload: `{"tools":[{"type":"web_search"}]}`, wantN: 1},
		{name: "defaults", payload: `{"tools":[{"type":"image_generation"}]}`, wantN: 1, wantImage: true},
		{name: "explicit", payload: `{"tools":[{"type":"image_generation","size":"1200x800","n":2}]}`, wantSize: "1200x800", wantN: 2, wantImage: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotSize, gotN, gotImage := ChatGPTWebImageControlsFromResponsesPayload([]byte(test.payload))
			if gotSize != test.wantSize || gotN != test.wantN || gotImage != test.wantImage {
				t.Fatalf("controls = %q, %d, %v; want %q, %d, %v", gotSize, gotN, gotImage, test.wantSize, test.wantN, test.wantImage)
			}
		})
	}
}

func TestChatGPTWebStrictImageSizeErrorPayload(t *testing.T) {
	payload := ChatGPTWebStrictImageSizeErrorPayload("1025x1024", 3840)
	if got := gjson.GetBytes(payload, "error.type").String(); got != "invalid_request_error" {
		t.Fatalf("error type = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.param").String(); got != "size" {
		t.Fatalf("error param = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.code").String(); got != "invalid_value" {
		t.Fatalf("error code = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.message").String(); !strings.Contains(got, "chatgpt web") || strings.Contains(got, "1:1") {
		t.Fatalf("error message = %q; payload=%s", got, payload)
	}
}

func TestChatGPTWebImageNErrorPayload(t *testing.T) {
	payload := ChatGPTWebImageNErrorPayload(2, 1)
	if got := gjson.GetBytes(payload, "error.param").String(); got != "n" {
		t.Fatalf("error param = %q; payload=%s", got, payload)
	}
	if got := gjson.GetBytes(payload, "error.message").String(); !strings.Contains(got, "chatgpt web") {
		t.Fatalf("error message = %q; payload=%s", got, payload)
	}
}

func TestResolveChatGPTWebImageSize(t *testing.T) {
	tests := []struct {
		name       string
		size       string
		maxEdge    int
		wantState  ChatGPTWebImageSizeDisposition
		wantRatio  string
		wantWidth  int
		wantHeight int
	}{
		{name: "empty", maxEdge: 3840, wantState: ChatGPTWebImageSizeUnspecified},
		{name: "auto", size: "AUTO", maxEdge: 3840, wantState: ChatGPTWebImageSizeUnspecified},
		{name: "square", size: "1024x1024", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "1:1", wantWidth: 1024, wantHeight: 1024},
		{name: "arbitrary landscape", size: "1200x800", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "3:2", wantWidth: 1200, wantHeight: 800},
		{name: "arbitrary portrait", size: "1024x1536", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "2:3", wantWidth: 1024, wantHeight: 1536},
		{name: "large reduced ratio", size: "1984x800", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "62:25", wantWidth: 1984, wantHeight: 800},
		{name: "minimum pixels", size: "1024x640", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "8:5", wantWidth: 1024, wantHeight: 640},
		{name: "maximum pixels", size: "3840x2160", maxEdge: 3840, wantState: ChatGPTWebImageSizeMatched, wantRatio: "16:9", wantWidth: 3840, wantHeight: 2160},
		{name: "not multiple of sixteen", size: "1025x1024", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
		{name: "below minimum pixels", size: "800x800", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
		{name: "above maximum pixels", size: "3840x2176", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
		{name: "ratio above three", size: "1936x640", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
		{name: "configured edge", size: "2064x1024", maxEdge: 2048, wantState: ChatGPTWebImageSizeIgnored},
		{name: "oversize", size: "4000x4000", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
		{name: "malformed", size: "1024-by-1024", maxEdge: 3840, wantState: ChatGPTWebImageSizeInvalid},
		{name: "zero", size: "0x1024", maxEdge: 3840, wantState: ChatGPTWebImageSizeIgnored},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, state := ResolveChatGPTWebImageSize(test.size, test.maxEdge)
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
