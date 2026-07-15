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
