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
)

// ChatGPTWebImageSizeMatch contains an explicit target and its closest Web-supported ratio.
type ChatGPTWebImageSizeMatch struct {
	Width       int
	Height      int
	RatioWidth  int
	RatioHeight int
	Ratio       string
}

var chatGPTWebSupportedImageRatios = [...]struct {
	width  int
	height int
}{
	{width: 1, height: 1},
	{width: 3, height: 4},
	{width: 9, height: 16},
	{width: 4, height: 3},
	{width: 16, height: 9},
}

// ChatGPTWebImageSizeFromResponsesPayload returns the first image_generation size.
func ChatGPTWebImageSizeFromResponsesPayload(payload []byte) (string, bool) {
	var request struct {
		Tools []json.RawMessage `json:"tools"`
	}
	if len(payload) == 0 || json.Unmarshal(payload, &request) != nil {
		return "", false
	}
	for _, rawTool := range request.Tools {
		var tool struct {
			Type string          `json:"type"`
			Size json.RawMessage `json:"size"`
		}
		if json.Unmarshal(rawTool, &tool) != nil || !strings.EqualFold(strings.TrimSpace(tool.Type), "image_generation") {
			continue
		}
		if len(tool.Size) == 0 || string(tool.Size) == "null" {
			return "", true
		}
		var size string
		if json.Unmarshal(tool.Size, &size) == nil {
			return size, true
		}
		return string(tool.Size), true
	}
	return "", false
}

// ChatGPTWebStrictImageSizeErrorPayload builds an OpenAI-compatible size error.
func ChatGPTWebStrictImageSizeErrorPayload(size string, maxEdge int, maxErrorPercent float64) []byte {
	message := "Invalid value for 'size': " + strconv.Quote(strings.TrimSpace(size)) +
		" cannot be handled by ChatGPT Web strict size mode. Use auto or a positive WxH size matching " +
		"one of 1:1, 3:4, 9:16, 4:3, or 16:9 within " +
		strconv.FormatFloat(maxErrorPercent, 'f', -1, 64) + "% and with each edge no greater than " +
		strconv.Itoa(maxEdge) + " pixels."
	payload, err := json.Marshal(map[string]any{"error": map[string]any{
		"message": message,
		"type":    "invalid_request_error",
		"param":   "size",
		"code":    "invalid_value",
	}})
	if err != nil {
		return []byte(`{"error":{"message":"Invalid image size","type":"invalid_request_error","param":"size","code":"invalid_value"}}`)
	}
	return payload
}

// ResolveChatGPTWebImageSize maps an explicit WxH target to the nearest supported ratio.
func ResolveChatGPTWebImageSize(size string, maxEdge int, maxErrorPercent float64) (ChatGPTWebImageSizeMatch, ChatGPTWebImageSizeDisposition) {
	size = strings.ToLower(strings.TrimSpace(size))
	if size == "" || size == "auto" {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeUnspecified
	}
	parts := strings.Split(size, "x")
	if len(parts) != 2 {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeInvalid
	}
	width, okWidth := parsePositiveImageDimension(parts[0])
	height, okHeight := parsePositiveImageDimension(parts[1])
	if !okWidth || !okHeight {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeInvalid
	}
	if width > maxEdge || height > maxEdge {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeIgnored
	}

	bestIndex := -1
	bestError := math.MaxFloat64
	for index, ratio := range chatGPTWebSupportedImageRatios {
		errorPercent := ChatGPTWebImageRatioErrorPercent(width, height, ratio.width, ratio.height)
		if errorPercent < bestError {
			bestIndex = index
			bestError = errorPercent
		}
	}
	if bestIndex < 0 || bestError > maxErrorPercent {
		return ChatGPTWebImageSizeMatch{}, ChatGPTWebImageSizeIgnored
	}
	ratio := chatGPTWebSupportedImageRatios[bestIndex]
	return ChatGPTWebImageSizeMatch{
		Width:       width,
		Height:      height,
		RatioWidth:  ratio.width,
		RatioHeight: ratio.height,
		Ratio:       strconv.Itoa(ratio.width) + ":" + strconv.Itoa(ratio.height),
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

func parsePositiveImageDimension(value string) (int, bool) {
	if value == "" {
		return 0, false
	}
	for index := range value {
		if value[index] < '0' || value[index] > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.Atoi(value)
	return parsed, err == nil && parsed > 0
}
