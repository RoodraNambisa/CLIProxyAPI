package helps

import (
	"strconv"
	"strings"
)

type chatGPTWebImageUsagePart struct {
	kind      string
	id        string
	animation ChatGPTWebImageAnimation
}

func chatGPTWebImageUsagePartFromValue(value any) chatGPTWebImageUsagePart {
	part, _ := value.(map[string]any)
	kind, id := chatGPTWebImagePointerKindID(stringFromAny(part["asset_pointer"]))
	if chatGPTWebImageVisuallyHidden(part) || chatGPTWebImageNoAuthPlaceholder(part) || chatGPTWebImagePointerIncomplete(part) {
		return chatGPTWebImageUsagePart{}
	}
	return chatGPTWebImageUsagePart{kind: kind, id: id, animation: chatGPTWebImageAnimationFromPart(part)}
}

func (accumulator *ChatGPTWebImageAccumulator) captureImageUsageMessage(value any) {
	message, ok := value.(map[string]any)
	if !ok || message["author"] == nil || message["content"] == nil {
		return
	}
	accumulator.usageMessageID = stringFromAny(message["id"])
	accumulator.usageParts = nil
	content, _ := message["content"].(map[string]any)
	parts, _ := content["parts"].([]any)
	accumulator.captureImageUsageParts(parts)
}

func (accumulator *ChatGPTWebImageAccumulator) captureImageUsageParts(parts []any) {
	accumulator.usageParts = make(map[int]chatGPTWebImageUsagePart)
	for index, part := range parts {
		if index >= chatGPTWebMaxImageOutputReferences {
			break
		}
		accumulator.usageParts[index] = chatGPTWebImageUsagePartFromValue(part)
	}
}

// captureImageUsagePatch retains only bounded per-part identifiers and numbers.
// It never creates an output reference or downloads an auxiliary spritesheet.
func (accumulator *ChatGPTWebImageAccumulator) captureImageUsagePatch(event map[string]any) {
	path := strings.TrimSpace(stringFromAny(event["p"]))
	if path == "/message/id" {
		id := stringFromAny(event["v"])
		if id != accumulator.usageMessageID {
			accumulator.usageMessageID, accumulator.usageParts = id, nil
		}
		return
	}
	if strings.HasPrefix(path, "/message/author") {
		accumulator.usageParts = nil
		return
	}
	if !webMessageCanContainGeneratedImage(accumulator.role) || !accumulator.imageTool {
		return
	}
	path = strings.TrimPrefix(path, "/message")
	if path == "/content/parts" {
		parts, _ := event["v"].([]any)
		accumulator.captureImageUsageParts(parts)
		return
	}
	const prefix = "/content/parts/"
	if !strings.HasPrefix(path, prefix) {
		return
	}
	segments := strings.SplitN(strings.TrimPrefix(path, prefix), "/", 2)
	index, err := strconv.Atoi(segments[0])
	if err != nil || index < 0 || index >= chatGPTWebMaxImageOutputReferences {
		return
	}
	if accumulator.usageParts == nil {
		accumulator.usageParts = make(map[int]chatGPTWebImageUsagePart)
	}
	field := ""
	if len(segments) == 2 {
		field = segments[1]
	}
	value := event["v"]
	if stringFromAny(event["o"]) == "remove" {
		value = nil
	}
	part := accumulator.usageParts[index]
	switch field {
	case "":
		part = chatGPTWebImageUsagePartFromValue(value)
	case "asset_pointer":
		kind, id := chatGPTWebImagePointerKindID(stringFromAny(value))
		if kind != part.kind || id != part.id {
			part = chatGPTWebImageUsagePart{kind: kind, id: id}
		}
	case "metadata":
		part.animation = chatGPTWebImageAnimationFromPart(map[string]any{"metadata": value})
	case "metadata/generation":
		part.animation = chatGPTWebImageAnimationFromPart(map[string]any{"metadata": map[string]any{"generation": value}})
	case "metadata/generation/animation":
		part.animation = chatGPTWebImageAnimationValues(value)
	case "metadata/generation/animation/rows":
		part.animation.Rows = chatGPTWebImageAnimationNumber(value)
	case "metadata/generation/animation/columns":
		part.animation.Columns = chatGPTWebImageAnimationNumber(value)
	case "metadata/generation/animation/frame_count":
		part.animation.FrameCount = chatGPTWebImageAnimationNumber(value)
	default:
		return
	}
	accumulator.usageParts[index] = part
}

func chatGPTWebImageAnimationNumber(value any) int {
	number, ok := integerFromAny(value)
	if !ok || number < 0 || number > ChatGPTWebMaxGIFFrames {
		return 0
	}
	return number
}

func chatGPTWebImageAnimationValues(value any) ChatGPTWebImageAnimation {
	animation, _ := value.(map[string]any)
	return ChatGPTWebImageAnimation{
		Rows:       chatGPTWebImageAnimationNumber(animation["rows"]),
		Columns:    chatGPTWebImageAnimationNumber(animation["columns"]),
		FrameCount: chatGPTWebImageAnimationNumber(animation["frame_count"]),
	}
}

func (accumulator *ChatGPTWebImageAccumulator) applyImageUsageParts() {
	for _, part := range accumulator.usageParts {
		if !part.animation.validLayout() || part.id == "" {
			continue
		}
		for index := range accumulator.References {
			reference := &accumulator.References[index]
			if reference.Kind == part.kind && reference.ID == part.id {
				reference.Animation = mergeChatGPTWebImageAnimation(reference.Animation, part.animation)
				break
			}
		}
	}
}
