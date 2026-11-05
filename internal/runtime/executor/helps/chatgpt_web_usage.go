package helps

import (
	"fmt"
	"math"
	"strings"
)

func chatGPTWebTextTokenSegments(request ChatGPTWebRequest) []string {
	segments := make([]string, 0, len(request.Messages)*2+2)
	for _, message := range request.Messages {
		if role := strings.TrimSpace(message.Role); role != "" {
			segments = append(segments, role)
		}
		for _, part := range message.Parts {
			if text := strings.TrimSpace(part.Text); text != "" {
				segments = append(segments, text)
			}
		}
	}
	if effort := strings.TrimSpace(request.ReasoningEffort); effort != "" {
		segments = append(segments, effort)
	}
	if request.WebSearch {
		segments = append(segments, "web_search")
	}
	return segments
}

func isUTF8RuneStart(value byte) bool {
	return value&0xc0 != 0x80
}

// ChatGPTWebImageTokenCount estimates OpenAI image-input token units from the
// model, detail level, and decoded dimensions.
func ChatGPTWebImageTokenCount(model, detail string, width, height int) int64 {
	if width <= 0 || height <= 0 {
		return 0
	}
	model = normalizeChatGPTWebUsageModel(model)
	detail = strings.ToLower(strings.TrimSpace(detail))

	if strings.HasPrefix(model, "gpt-image-") || model == "chatgpt-image-latest" {
		return chatGPTWebGPTImageTokenCount(width, height)
	}
	if profile, ok := chatGPTWebPatchProfile(model, detail); ok {
		if profile.low {
			return int64(math.Round(256 * profile.multiplier))
		}
		patches := chatGPTWebResizedPatchCount(width, height, profile.patchBudget, profile.maxDimension)
		return int64(math.Round(float64(patches) * profile.multiplier))
	}

	baseTokens, tileTokens := chatGPTWebTileRates(model)
	if detail == "low" {
		return int64(baseTokens)
	}
	return int64(baseTokens + chatGPTWebTileCount(width, height, 768)*tileTokens)
}

type chatGPTWebPatchTokenProfile struct {
	patchBudget  int
	maxDimension int
	multiplier   float64
	low          bool
}

func chatGPTWebPatchProfile(model, detail string) (chatGPTWebPatchTokenProfile, bool) {
	profile := chatGPTWebPatchTokenProfile{multiplier: 1}
	if detail == "low" {
		profile.low = true
	}
	switch {
	case strings.HasPrefix(model, "gpt-5.6"):
		if detail == "" || detail == "auto" || detail == "original" {
			return profile, true
		}
		profile.patchBudget, profile.maxDimension = 2500, 2048
		return profile, true
	case strings.HasPrefix(model, "gpt-5.5"):
		if detail == "" || detail == "auto" || detail == "original" {
			profile.patchBudget, profile.maxDimension = 10000, 6000
		} else {
			profile.patchBudget, profile.maxDimension = 2500, 2048
		}
		return profile, true
	case strings.HasPrefix(model, "gpt-5.4-mini"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 1.62
		return profile, true
	case strings.HasPrefix(model, "gpt-5.4-nano"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 2.46
		return profile, true
	case strings.HasPrefix(model, "gpt-5.4"):
		if detail == "original" {
			profile.patchBudget, profile.maxDimension = 10000, 6000
		} else {
			profile.patchBudget, profile.maxDimension = 2500, 2048
		}
		return profile, true
	case strings.HasPrefix(model, "gpt-5-mini"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 1.62
		return profile, true
	case strings.HasPrefix(model, "gpt-5-nano"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 2.46
		return profile, true
	case strings.HasPrefix(model, "gpt-5.2"), strings.HasPrefix(model, "gpt-5.3-codex"),
		strings.HasPrefix(model, "gpt-5.3-mini"), strings.HasPrefix(model, "gpt-5-codex-mini"),
		strings.HasPrefix(model, "gpt-5.1-codex-mini"):
		profile.patchBudget, profile.maxDimension = 1536, 2048
		return profile, true
	case strings.HasPrefix(model, "gpt-4.1-mini"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 1.62
		return profile, true
	case strings.HasPrefix(model, "gpt-4.1-nano"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 2.46
		return profile, true
	case strings.HasPrefix(model, "o4-mini"):
		profile.patchBudget, profile.maxDimension, profile.multiplier = 1536, 2048, 1.72
		return profile, true
	default:
		return chatGPTWebPatchTokenProfile{}, false
	}
}

func chatGPTWebResizedPatchCount(width, height, patchBudget, maxDimension int) int {
	scale := 1.0
	if maxDimension > 0 {
		maxSide := math.Max(float64(width), float64(height))
		if maxSide > float64(maxDimension) {
			scale = float64(maxDimension) / maxSide
		}
	}
	scaledWidth := float64(width) * scale
	scaledHeight := float64(height) * scale
	patches := chatGPTWebPatchCount(scaledWidth, scaledHeight)
	if patchBudget <= 0 || patches <= patchBudget {
		return patches
	}

	shrink := math.Sqrt(float64(32*32*patchBudget) / (scaledWidth * scaledHeight))
	widthRatio := scaledWidth * shrink / 32
	heightRatio := scaledHeight * shrink / 32
	adjustment := math.Min(math.Floor(widthRatio)/widthRatio, math.Floor(heightRatio)/heightRatio)
	if adjustment > 0 && !math.IsNaN(adjustment) {
		shrink *= adjustment
	}
	patches = chatGPTWebPatchCount(scaledWidth*shrink, scaledHeight*shrink)
	return min(patches, patchBudget)
}

func chatGPTWebPatchCount(width, height float64) int {
	return int(math.Ceil(width/32) * math.Ceil(height/32))
}

func chatGPTWebTileRates(model string) (baseTokens, tileTokens int) {
	switch {
	case strings.HasPrefix(model, "gpt-4o-mini"):
		return 2833, 5667
	case strings.HasPrefix(model, "gpt-5-chat-latest"), strings.HasPrefix(model, "gpt-5"):
		return 70, 140
	case strings.HasPrefix(model, "o1"), strings.HasPrefix(model, "o3"):
		return 75, 150
	case strings.Contains(model, "computer-use-preview"):
		return 65, 129
	default:
		return 85, 170
	}
}

func chatGPTWebGPTImageTokenCount(width, height int) int64 {
	return int64(65 + chatGPTWebTileCount(width, height, 512)*129)
}

// ChatGPTWebImageOutputTokenCount estimates output tokens using the selected image profile.
func ChatGPTWebImageOutputTokenCount(model, requestedQuality, autoQuality string, width, height int) (int64, error) {
	if width <= 0 || height <= 0 {
		return 0, fmt.Errorf("image output dimensions are invalid")
	}
	model = normalizeChatGPTWebUsageModel(model)
	modern := model == "gpt-image-2.5" || strings.HasPrefix(model, "gpt-image-2.5-") ||
		model == "gpt-image-2.6" || strings.HasPrefix(model, "gpt-image-2.6-")
	if model != "gpt-image-2" && !modern {
		return 0, fmt.Errorf("image output token estimation is not available for model %q", model)
	}
	quality := strings.ToLower(strings.TrimSpace(requestedQuality))
	if quality == "" || quality == "auto" {
		quality = normalizeChatGPTWebOutputQuality(autoQuality)
	}
	factor := 0
	switch quality {
	case "low":
		factor = 16
	case "medium":
		factor = 48
	case "high":
		factor = 96
	case "xhigh":
		if modern {
			factor = 64
		}
	case "max":
		if modern {
			factor = 96
		}
	}
	if modern && quality == "medium" {
		factor = 24
	}
	if modern && quality == "high" {
		factor = 48
	}
	if factor == 0 {
		return 0, fmt.Errorf("unsupported image output quality %q", quality)
	}
	minor := float64(factor) * float64(min(width, height)) / float64(max(width, height))
	rounded := math.Round(minor)
	if modern {
		rounded = math.RoundToEven(minor)
	}
	grid := int64(factor) * max(1, int64(rounded))
	const maxInt64 = int64(^uint64(0) >> 1)
	if int64(width) > maxInt64/int64(height) {
		return 0, fmt.Errorf("image output dimensions exceed token arithmetic limits")
	}
	pixels := int64(width) * int64(height)
	if pixels > maxInt64/grid-2_000_000 {
		return 0, fmt.Errorf("image output dimensions exceed token arithmetic limits")
	}
	numerator := grid * (2_000_000 + pixels)
	tokens := numerator / 4_000_000
	if numerator%4_000_000 != 0 {
		tokens++
	}
	multiplier := int64(10)
	if model == "gpt-image-2.6" || strings.HasPrefix(model, "gpt-image-2.6-") {
		switch quality {
		case "low":
			multiplier = 42
		case "medium":
			multiplier = 33
		}
	}
	if tokens > maxInt64/multiplier {
		return 0, fmt.Errorf("image output tokens exceed arithmetic limits")
	}
	numerator = tokens * multiplier
	tokens = numerator / 10
	if numerator%10 != 0 {
		tokens++
	}
	return tokens, nil
}

func chatGPTWebTileCount(width, height, shortestSide int) int {
	maxSide := math.Max(float64(width), float64(height))
	fitScale := math.Max(1, maxSide/2048)
	fitWidth := float64(width) / fitScale
	fitHeight := float64(height) / fitScale
	shortSide := math.Min(fitWidth, fitHeight)
	if shortSide <= 0 {
		return 0
	}
	scale := float64(shortestSide) / shortSide
	tilesWide := int(math.Ceil(fitWidth * scale / 512))
	tilesHigh := int(math.Ceil(fitHeight * scale / 512))
	return tilesWide * tilesHigh
}

func normalizeChatGPTWebUsageModel(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	for _, version := range []string{"6", "5", "4", "3", "2", "1"} {
		prefix := "gpt-5-" + version
		if model == prefix || strings.HasPrefix(model, prefix+"-") {
			return "gpt-5." + version + strings.TrimPrefix(model, prefix)
		}
	}
	return model
}
