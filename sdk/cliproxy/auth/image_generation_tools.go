package auth

import (
	"strings"

	"github.com/tidwall/gjson"
)

// PayloadHasImageGenerationTool reports whether a request contains a supported Codex image tool form.
func PayloadHasImageGenerationTool(payload []byte) bool {
	if len(payload) == 0 {
		return false
	}
	tools := gjson.GetBytes(payload, "tools")
	if !tools.IsArray() {
		return false
	}
	for _, tool := range tools.Array() {
		if ToolHasImageGeneration(tool) {
			return true
		}
	}
	return false
}

// PayloadMaySelectImageGenerationTool reports whether the declared image tool
// remains selectable after applying an explicit tool_choice.
func PayloadMaySelectImageGenerationTool(payload []byte) bool {
	if !PayloadHasImageGenerationTool(payload) {
		return false
	}
	choice := gjson.GetBytes(payload, "tool_choice")
	if !choice.Exists() {
		return true
	}
	if choice.Type == gjson.String {
		switch strings.ToLower(strings.TrimSpace(choice.String())) {
		case "", "auto", "required", "image_generation":
			return true
		default:
			return false
		}
	}
	if !choice.IsObject() {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(choice.Get("type").String()), "allowed_tools") {
		return allowedToolsContainImageGeneration(choice)
	}
	return ToolChoiceSelectsImageGeneration(choice)
}

// PayloadExplicitlySelectsImageGenerationTool reports whether a request forces
// one of the supported image generation forms.
func PayloadExplicitlySelectsImageGenerationTool(payload []byte) bool {
	if !PayloadHasImageGenerationTool(payload) {
		return false
	}
	choice := gjson.GetBytes(payload, "tool_choice")
	if choice.IsObject() {
		if strings.EqualFold(strings.TrimSpace(choice.Get("type").String()), "allowed_tools") {
			return false
		}
		return toolChoiceForcesImageGeneration(payload, choice)
	}
	if choice.Type != gjson.String ||
		!strings.EqualFold(strings.TrimSpace(choice.String()), "required") {
		return false
	}
	tools := gjson.GetBytes(payload, "tools")
	declared := tools.Array()
	return len(declared) == 1 && toolForcesImageGeneration(declared[0])
}

func toolChoiceForcesImageGeneration(payload []byte, choice gjson.Result) bool {
	if !strings.EqualFold(strings.TrimSpace(choice.Get("type").String()), "namespace") {
		return ToolChoiceSelectsImageGeneration(choice)
	}
	if !strings.EqualFold(strings.TrimSpace(choice.Get("name").String()), "image_gen") {
		return false
	}
	for _, tool := range gjson.GetBytes(payload, "tools").Array() {
		if !strings.EqualFold(strings.TrimSpace(tool.Get("type").String()), "namespace") ||
			!strings.EqualFold(strings.TrimSpace(tool.Get("name").String()), "image_gen") {
			continue
		}
		return toolForcesImageGeneration(tool)
	}
	return false
}

func allowedToolsContainImageGeneration(choice gjson.Result) bool {
	for _, path := range []string{"tools", "allowed_tools.tools"} {
		for _, tool := range choice.Get(path).Array() {
			if ToolHasImageGeneration(tool) {
				return true
			}
		}
	}
	return false
}

func toolForcesImageGeneration(tool gjson.Result) bool {
	if !ToolHasImageGeneration(tool) {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(tool.Get("type").String()), "namespace") {
		return true
	}
	nestedTools := tool.Get("tools").Array()
	return len(nestedTools) == 1 && IsImageGenerationNamespaceMember(nestedTools[0])
}

// ToolChoiceSelectsImageGeneration reports whether an explicit tool choice
// selects one of the supported image generation forms.
func ToolChoiceSelectsImageGeneration(choice gjson.Result) bool {
	switch strings.TrimSpace(choice.Get("type").String()) {
	case "image_generation":
		return true
	case "function":
		return imageGenerationFunctionName(choice) == "image_gen.imagegen"
	case "namespace":
		return strings.TrimSpace(choice.Get("name").String()) == "image_gen"
	default:
		return false
	}
}

// ToolHasImageGeneration reports whether one tool entry exposes Codex image generation.
func ToolHasImageGeneration(tool gjson.Result) bool {
	switch tool.Get("type").String() {
	case "image_generation":
		return true
	case "function":
		return imageGenerationFunctionName(tool) == "image_gen.imagegen"
	case "namespace":
		if strings.TrimSpace(tool.Get("name").String()) != "image_gen" {
			return false
		}
		for _, nestedTool := range tool.Get("tools").Array() {
			if IsImageGenerationNamespaceMember(nestedTool) {
				return true
			}
		}
	}
	return false
}

// IsImageGenerationNamespaceMember reports whether a function is image_gen.imagegen inside its namespace.
func IsImageGenerationNamespaceMember(tool gjson.Result) bool {
	return tool.Get("type").String() == "function" && imageGenerationFunctionName(tool) == "imagegen"
}

func imageGenerationFunctionName(tool gjson.Result) string {
	name := strings.TrimSpace(tool.Get("name").String())
	if name == "" {
		name = strings.TrimSpace(tool.Get("function.name").String())
	}
	return name
}
