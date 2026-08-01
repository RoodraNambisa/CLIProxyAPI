package helps

import (
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
		errorPercent := math.Abs(float64(width)*float64(ratio.height)-float64(height)*float64(ratio.width)) /
			(float64(height) * float64(ratio.width)) * 100
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
