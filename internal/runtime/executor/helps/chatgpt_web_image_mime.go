package helps

import (
	"encoding/base64"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"mime"
	"strings"

	_ "golang.org/x/image/webp"
)

// ChatGPTWebImageMIMEMismatchError reports a supported image whose declared
// MIME type differs from the format detected from its bytes.
type ChatGPTWebImageMIMEMismatchError struct {
	Declared string
	Detected string
}

func (err *ChatGPTWebImageMIMEMismatchError) Error() string {
	if err == nil {
		return "image MIME type does not match content"
	}
	return fmt.Sprintf("image MIME type mismatch: declared %q, detected %q", err.Declared, err.Detected)
}

// CanonicalChatGPTWebImageMIME normalizes supported image MIME aliases.
func CanonicalChatGPTWebImageMIME(value string) string {
	mediaType, _, errParse := mime.ParseMediaType(strings.TrimSpace(value))
	if errParse != nil {
		mediaType = strings.TrimSpace(strings.SplitN(value, ";", 2)[0])
	}
	switch strings.ToLower(mediaType) {
	case "image/jpeg", "image/jpg":
		return "image/jpeg"
	case "image/png":
		return "image/png"
	case "image/gif":
		return "image/gif"
	case "image/webp":
		return "image/webp"
	default:
		return ""
	}
}

// DetectChatGPTWebImageMIME decodes image metadata and returns a canonical MIME type.
func DetectChatGPTWebImageMIME(reader io.Reader) (image.Config, string, error) {
	if reader == nil {
		return image.Config{}, "", errors.New("image content is empty")
	}
	imageConfig, format, errDecode := image.DecodeConfig(reader)
	if errDecode != nil {
		return image.Config{}, "", errDecode
	}
	var mimeType string
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "jpeg":
		mimeType = "image/jpeg"
	case "png":
		mimeType = "image/png"
	case "gif":
		mimeType = "image/gif"
	case "webp":
		mimeType = "image/webp"
	}
	if mimeType == "" {
		return image.Config{}, "", fmt.Errorf("unsupported image format %q", format)
	}
	return imageConfig, mimeType, nil
}

// NormalizeChatGPTWebImageDataURLMIME replaces only a data URL's MIME prefix.
// The base64 payload is retained verbatim.
func NormalizeChatGPTWebImageDataURLMIME(value string, maxBytes int) (string, error) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(strings.ToLower(trimmed), "data:") {
		return value, nil
	}
	comma := strings.IndexByte(trimmed, ',')
	if comma < 0 {
		return "", errors.New("invalid image data URL")
	}
	metadata := trimmed[5:comma]
	if !strings.Contains(strings.ToLower(metadata), ";base64") {
		return "", errors.New("image data URL must use base64 encoding")
	}
	declared := metadata
	if semicolon := strings.IndexByte(declared, ';'); semicolon >= 0 {
		declared = declared[:semicolon]
	}
	declared = CanonicalChatGPTWebImageMIME(declared)
	if declared == "" {
		return "", errors.New("image data URL has an unsupported MIME type")
	}
	payload := trimmed[comma+1:]
	if _, errSize := ChatGPTWebEncodedImageSize(payload, maxBytes); errSize != nil {
		return "", errSize
	}
	_, detected, errDetect := DetectChatGPTWebImageMIME(base64.NewDecoder(base64.StdEncoding, strings.NewReader(payload)))
	if errDetect != nil {
		return "", fmt.Errorf("decode image: %w", errDetect)
	}
	if declared == detected {
		return trimmed, nil
	}
	return "data:" + detected + ";base64," + payload, nil
}
