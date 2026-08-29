package helps

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"strings"
	"testing"

	imagewebp "github.com/gen2brain/webp"
)

func TestDetectChatGPTWebImageMIME(t *testing.T) {
	tests := []struct {
		name     string
		mimeType string
		encode   func(*bytes.Buffer, image.Image) error
	}{
		{name: "JPEG", mimeType: "image/jpeg", encode: func(output *bytes.Buffer, source image.Image) error {
			return jpeg.Encode(output, source, &jpeg.Options{Quality: 90})
		}},
		{name: "PNG", mimeType: "image/png", encode: func(output *bytes.Buffer, source image.Image) error {
			return png.Encode(output, source)
		}},
		{name: "GIF", mimeType: "image/gif", encode: func(output *bytes.Buffer, source image.Image) error {
			return gif.Encode(output, source, nil)
		}},
		{name: "WebP", mimeType: "image/webp", encode: func(output *bytes.Buffer, source image.Image) error {
			return imagewebp.Encode(output, source, imagewebp.Options{Quality: 90, Exact: true})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := image.NewNRGBA(image.Rect(0, 0, 2, 3))
			source.SetNRGBA(0, 0, color.NRGBA{R: 0xff, G: 0x40, B: 0x20, A: 0xff})
			var encoded bytes.Buffer
			if err := tt.encode(&encoded, source); err != nil {
				t.Fatalf("encode image: %v", err)
			}
			config, got, err := DetectChatGPTWebImageMIME(bytes.NewReader(encoded.Bytes()))
			if err != nil {
				t.Fatalf("DetectChatGPTWebImageMIME() error = %v", err)
			}
			if got != tt.mimeType || config.Width != 2 || config.Height != 3 {
				t.Fatalf("detected MIME/config = %q %dx%d", got, config.Width, config.Height)
			}
		})
	}
}

func TestNormalizeChatGPTWebImageDataURLMIMERewritesOnlyPrefix(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 1, 1))
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, source); err != nil {
		t.Fatalf("encode PNG: %v", err)
	}
	payload := base64.StdEncoding.EncodeToString(encoded.Bytes())
	input := "data:IMAGE/JPEG;CHARSET=binary;base64," + payload

	normalized, err := NormalizeChatGPTWebImageDataURLMIME(input, ChatGPTWebMaxImageBytes)
	if err != nil {
		t.Fatalf("NormalizeChatGPTWebImageDataURLMIME() error = %v", err)
	}
	const prefix = "data:image/png;base64,"
	if !strings.HasPrefix(normalized, prefix) {
		t.Fatalf("normalized data URL prefix = %q", normalized[:min(len(normalized), len(prefix))])
	}
	if strings.TrimPrefix(normalized, prefix) != payload {
		t.Fatal("normalization changed the base64 payload")
	}
}

func TestNormalizeChatGPTWebImageDataURLMIMERejectsNonImage(t *testing.T) {
	input := "data:image/jpeg;base64," + base64.StdEncoding.EncodeToString([]byte("not an image"))
	if _, err := NormalizeChatGPTWebImageDataURLMIME(input, ChatGPTWebMaxImageBytes); err == nil {
		t.Fatal("NormalizeChatGPTWebImageDataURLMIME() accepted non-image content")
	}
}

func TestCanonicalChatGPTWebImageMIME(t *testing.T) {
	tests := map[string]string{
		"IMAGE/JPEG; charset=binary": "image/jpeg",
		"image/jpg":                  "image/jpeg",
		"image/png":                  "image/png",
		"image/gif":                  "image/gif",
		"image/webp":                 "image/webp",
		"image/svg+xml":              "",
		"not a mime":                 "",
	}
	for input, want := range tests {
		if got := CanonicalChatGPTWebImageMIME(input); got != want {
			t.Errorf("CanonicalChatGPTWebImageMIME(%q) = %q, want %q", input, got, want)
		}
	}
}
