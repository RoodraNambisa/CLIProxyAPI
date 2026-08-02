package helps

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
)

// ChatGPTWebImageSizeDisposition describes how an explicit image size should be handled.
type ChatGPTWebImageSizeDisposition uint8

const (
	ChatGPTWebImageSizeUnspecified ChatGPTWebImageSizeDisposition = iota
	ChatGPTWebImageSizeMatched
	ChatGPTWebImageSizeIgnored
	ChatGPTWebImageSizeInvalid

	ChatGPTWebImageMinPixels     = 655_360
	ChatGPTWebImageMaxPixels     = 8_294_400
	ChatGPTWebImageHardMaxEdge   = 3840
	ChatGPTWebImageMinRatioScale = 3
)

// ChatGPTWebImageSizeMatch contains an explicit target and its reduced ratio.
type ChatGPTWebImageSizeMatch struct {
	Width       int
	Height      int
	RatioWidth  int
	RatioHeight int
	Ratio       string
}

// ChatGPTWebImageSizeFromResponsesPayload returns the first image_generation size.
func ChatGPTWebImageSizeFromResponsesPayload(payload []byte) (string, bool) {
	size, _, hasImage := ChatGPTWebImageControlsFromResponsesPayload(payload)
	return size, hasImage
}

// ChatGPTWebImageControlsFromResponsesPayload returns the first image_generation size and count.
func ChatGPTWebImageControlsFromResponsesPayload(payload []byte) (string, int, bool) {
	var request struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &request) != nil {
		return "", 1, false
	}
	for _, rawTool := range request.Tools {
		var tool struct {
			Type string          `json:"type"`
			Size json.RawMessage `json:"size"`
			N    json.RawMessage `json:"n"`
		}
		if json.Unmarshal(rawTool, &tool) != nil || !strings.EqualFold(strings.TrimSpace(tool.Type), "image_generation") {
			continue
		}
		n := 1
		if len(tool.N) > 0 && string(tool.N) != "null" {
			var parsedN int
			if json.Unmarshal(tool.N, &parsedN) == nil {
				n = parsedN
			}
		}
		if len(tool.Size) == 0 || string(tool.Size) == "null" {
			return "", n, true
		}
		var size string
		if json.Unmarshal(tool.Size, &size) == nil {
			return size, n, true
		}
		return string(tool.Size), n, true
	}
	return "", 1, false
}

// ChatGPTWebStrictImageSizeErrorPayload builds an OpenAI-compatible size error.
func ChatGPTWebStrictImageSizeErrorPayload(size string, maxEdge int) []byte {
	maxEdge = effectiveChatGPTWebImageMaxEdge(maxEdge)
	message := "Invalid value for 'size': " + strconv.Quote(strings.TrimSpace(size)) +
		" cannot be handled by chatgpt web image generation. Width and height must be positive multiples of 16, " +
		"the aspect ratio must be between 1:3 and 3:1, each edge must not exceed " + strconv.Itoa(maxEdge) +
		" pixels, and the total pixel count must be between " + strconv.Itoa(ChatGPTWebImageMinPixels) +
		" and " + strconv.Itoa(ChatGPTWebImageMaxPixels) + "."
	payload, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message,
		"type":    "invalid_request_error",
		"param":   "size",
		"code":    "invalid_value",
	}})
	if err != nil {
		return []byte(`{"error":{"message":"Invalid image size for chatgpt web","type":"invalid_request_error","param":"size","code":"invalid_value"}}`)
	}
	return payload
}

// ChatGPTWebImageNErrorPayload builds an OpenAI-compatible image count error.
func ChatGPTWebImageNErrorPayload(n, maxN int) []byte {
	message := "Invalid value for 'n': " + strconv.Itoa(n) +
		" exceeds the chatgpt web image generation limit of " + strconv.Itoa(maxN) + "."
	payload, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message,
		"type":    "invalid_request_error",
		"param":   "n",
		"code":    "invalid_value",
	}})
	if err != nil {
		return []byte(`{"error":{"message":"Invalid image count for chatgpt web","type":"invalid_request_error","param":"n","code":"invalid_value"}}`)
	}
	return payload
}

// ResolveChatGPTWebImageSize validates an explicit WxH target and reduces its ratio.
func ResolveChatGPTWebImageSize(size string, maxEdge int) (ChatGPTWebImageSizeMatch, ChatGPTWebImageSizeDisposition) {
	size = strings.ToLower(strings.TrimSpace(size))
	if size == "" || size == "auto" {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeUnspecified
	}
	parts := strings.Split(size, "x")
	if len(parts) != 2 {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeInvalid
	}
	width, widthSyntax, widthFits := parseImageDimension(parts[0])
	height, heightSyntax, heightFits := parseImageDimension(parts[1])
	if !widthSyntax || !heightSyntax {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeInvalid
	}
	if !widthFits || !heightFits || width <= 0 || height <= 0 {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeIgnored
	}
	maxEdge = effectiveChatGPTWebImageMaxEdge(maxEdge)
	pixels := int64(width) * int64(height)
	if width%16 != 0 || height%16 != 0 || width > maxEdge || height > maxEdge ||
		int64(width) > int64(height)*ChatGPTWebImageMinRatioScale ||
		int64(height) > int64(width)*ChatGPTWebImageMinRatioScale ||
		pixels < ChatGPTWebImageMinPixels || pixels > ChatGPTWebImageMaxPixels {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeIgnored
	}
	divisor := greatestCommonDivisor(width, height)
	ratioWidth := width / divisor
	ratioHeight := height / divisor
	return ChatGPTWebImageSizeMatch{
		Width:       width,
		Height:      height,
		RatioWidth:  ratioWidth,
		RatioHeight: ratioHeight,
		Ratio:       strconv.Itoa(ratioWidth) + ":" + strconv.Itoa(ratioHeight),
	}, ChatGPTWebImageSizeMatched
}

// ChatGPTWebImageRatioErrorPercent returns the relative cross-product error for two ratios.
func ChatGPTWebImageRatioErrorPercent(width, height, ratioWidth, ratioHeight int) float64 {
	if width <= 0 || height <= 0 || ratioWidth <= 0 || ratioHeight <= 0 {
		return math.Inf(1)
	}
	return math.Abs(float64(width)*float64(ratioHeight)-float64(height)*float64(ratioWidth)) /
		(float64(height) * float64(ratioWidth)) * 100
}

// ChatGPTWebImageRatioMatches reports whether dimensions fit a ratio within tolerance.
func ChatGPTWebImageRatioMatches(width, height, ratioWidth, ratioHeight int, maxErrorPercent float64) bool {
	if math.IsNaN(maxErrorPercent) || maxErrorPercent < 0 {
		return false
	}
	return ChatGPTWebImageRatioErrorPercent(width, height, ratioWidth, ratioHeight) <= maxErrorPercent
}

func parseImageDimension(value string) (int, bool, bool) {
	if value == "" {
		return 0, false, false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return 0, false, false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, strconv.IntSize)
	if err != nil {
		return 0, true, false
	}
	return int(parsed), true, true
}

func effectiveChatGPTWebImageMaxEdge(maxEdge int) int {
	if maxEdge <= 0 || maxEdge > ChatGPTWebImageHardMaxEdge {
		return ChatGPTWebImageHardMaxEdge
	}
	return maxEdge
}

func greatestCommonDivisor(left, right int) int {
	for right != 0 {
		left, right = right, left%right
	}
	if left <= 0 {
		return 1
	}
	return left
}
