package helps

import "fmt"

const ChatGPTWebImageUsageEstimator = "model-profiles-sheet-v1"

// ChatGPTWebImageAnimation contains bounded layout metadata, without image payloads.
type ChatGPTWebImageAnimation struct {
	Rows       int
	Columns    int
	FrameCount int
}

func (a ChatGPTWebImageAnimation) validLayout() bool {
	return a.Rows > 0 && a.Columns > 0 && a.Rows <= ChatGPTWebMaxGIFFrames &&
		a.Columns <= ChatGPTWebMaxGIFFrames && a.Rows*a.Columns <= ChatGPTWebMaxGIFFrames &&
		a.FrameCount >= 0 && a.FrameCount <= a.Rows*a.Columns
}

func mergeChatGPTWebImageAnimation(current, incoming ChatGPTWebImageAnimation) ChatGPTWebImageAnimation {
	if !incoming.validLayout() {
		return current
	}
	if !current.validLayout() {
		return incoming
	}
	if current.Rows == incoming.Rows && current.Columns == incoming.Columns && current.FrameCount == 0 {
		current.FrameCount = incoming.FrameCount
	}
	return current
}

// ForFrames keeps verified layout information or infers a compact sheet from the delivered GIF.
func (a ChatGPTWebImageAnimation) ForFrames(frames, width, height int) ChatGPTWebImageAnimation {
	if frames < 2 || frames > ChatGPTWebMaxGIFFrames {
		return ChatGPTWebImageAnimation{}
	}
	if a.validLayout() && a.Rows*a.Columns >= frames && (a.FrameCount == 0 || a.FrameCount == frames) {
		a.FrameCount = frames
		return a
	}
	columns, rows := 4, 2
	if frames > 8 {
		columns = 3
		for columns*columns < frames {
			columns++
		}
		rows = (frames + columns - 1) / columns
	}
	// Without upstream metadata, orient the compact sheet towards a square canvas.
	if width > height {
		columns, rows = rows, columns
	}
	return ChatGPTWebImageAnimation{Rows: rows, Columns: columns, FrameCount: frames}
}

// ChatGPTWebImageUsageOutputTokenCount preserves final-frame sizing for GIF accounting.
func ChatGPTWebImageUsageOutputTokenCount(image ChatGPTWebUsageImage, autoQuality string) (int64, error) {
	width, height := image.Width, image.Height
	if image.Animation.FrameCount != 0 {
		if image.Animation.FrameCount < 2 || image.Animation.FrameCount > ChatGPTWebMaxGIFFrames {
			return 0, fmt.Errorf("image output animation frame count is invalid")
		}
		layout := image.Animation.ForFrames(image.Animation.FrameCount, width, height)
		maxInt := int(^uint(0) >> 1)
		if width <= 0 || height <= 0 || width > maxInt/layout.Columns || height > maxInt/layout.Rows {
			return 0, fmt.Errorf("image output sheet dimensions are invalid")
		}
		width *= layout.Columns
		height *= layout.Rows
	}
	return ChatGPTWebImageOutputTokenCount(image.Model, image.Quality, autoQuality, width, height)
}

func chatGPTWebImageAnimationFromPart(part map[string]any) ChatGPTWebImageAnimation {
	metadata, _ := part["metadata"].(map[string]any)
	generation, _ := metadata["generation"].(map[string]any)
	value := chatGPTWebImageAnimationValues(generation["animation"])
	if !value.validLayout() {
		return ChatGPTWebImageAnimation{}
	}
	return value
}

// OutputReferences matches download selection so layout metadata cannot shift between results.
func (accumulator *ChatGPTWebImageAccumulator) OutputReferences(maxResults int) []ChatGPTWebImageReference {
	references := chatGPTWebImageAccumulatorReferences(accumulator)
	selected := make([]ChatGPTWebImageReference, 0, len(references))
	for _, reference := range references {
		if reference.Kind != "file" && reference.Kind != "sediment" || reference.Kind == "file" && reference.ID == "file_upload" {
			continue
		}
		selected = append(selected, reference)
		if maxResults > 0 && len(selected) >= maxResults {
			break
		}
	}
	return selected
}
