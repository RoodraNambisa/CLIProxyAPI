package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/translator/common"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const openAIToolResultImageOmittedText = "[image omitted: unsupported by upstream]"

func OpenAIToolResultPolicyFromRequest(req core.Request) (textOnly, bound bool) {
	modalities, bound := coreauth.ResolvedModelInputModalities(req)
	return len(modalities) == 1 && modalities[0] == "text", bound
}

// ShouldNormalizeOpenAIToolResultsForModel supports direct executor callers
// without a Manager binding. A selected upstream name wins over an alias pool;
// an ambiguous pool opts in only when every matching model is text-only.
func ShouldNormalizeOpenAIToolResultsForModel(compat *config.OpenAICompatibility, upstreamModel, requestedModel string) bool {
	if compat == nil || compat.Disabled {
		return false
	}
	textOnly := func(model config.OpenAICompatibilityModel) bool {
		modalities := model.GetInputModalities()
		return len(modalities) == 1 && modalities[0] == "text"
	}
	for _, exact := range []bool{true, false} {
		for _, model := range compat.Models {
			name, selected := strings.TrimSpace(model.Name), strings.TrimSpace(upstreamModel)
			matches := selected != "" && strings.EqualFold(name, selected)
			if !exact {
				parsed := thinking.ParseSuffix(name)
				matches = selected != "" && !parsed.HasSuffix && strings.EqualFold(parsed.ModelName, thinking.ParseSuffix(selected).ModelName)
			}
			if matches {
				return textOnly(model)
			}
		}
	}
	requested := strings.TrimSpace(thinking.ParseSuffix(requestedModel).ModelName)
	matched := false
	for _, model := range compat.Models {
		if requested == "" || !strings.EqualFold(requested, strings.TrimSpace(thinking.ParseSuffix(model.Alias).ModelName)) {
			continue
		}
		matched = true
		if !textOnly(model) {
			return false
		}
	}
	return matched
}

// NormalizeOpenAIToolResultsTextOnly changes protocol-level tool content only.
// Business JSON and stringified JSON stay opaque, and the original request is
// never mutated because a retry may select a model that accepts tool images.
func NormalizeOpenAIToolResultsTextOnly(payload []byte) []byte {
	return normalizeToolContentArray(payload, "messages", "content", func(item gjson.Result) bool {
		return item.Get("role").String() == "tool"
	})
}

// NormalizeResponsesToolResultsTextOnly covers the native compact request body.
func NormalizeResponsesToolResultsTextOnly(payload []byte) []byte {
	return normalizeToolContentArray(payload, "input", "output", func(item gjson.Result) bool {
		switch item.Get("type").String() {
		case "function_call_output", "custom_tool_call_output":
			return true
		}
		return false
	})
}

func normalizeToolContentArray(payload []byte, arrayPath, contentField string, eligible func(gjson.Result) bool) []byte {
	itemsValue := gjson.GetBytes(payload, arrayPath)
	if !itemsValue.IsArray() || !gjson.ValidBytes(payload) {
		return payload
	}
	var items [][]byte
	changed := false
	itemsValue.ForEach(func(_, item gjson.Result) bool {
		raw := []byte(item.Raw)
		if eligible(item) {
			content := item.Get(contentField)
			if content.Exists() && content.Type != gjson.String {
				if updated, err := sjson.SetBytes(raw, contentField, flattenOpenAIToolResultContent(content)); err == nil {
					raw, changed = updated, true
				}
			}
		}
		items = append(items, raw)
		return true
	})
	if !changed {
		return payload
	}
	if updated, err := sjson.SetRawBytes(payload, arrayPath, common.JoinRawArray(items)); err == nil {
		return updated
	}
	return payload
}

func flattenOpenAIToolResultContent(content gjson.Result) string {
	if !content.IsArray() {
		return openAIToolResultPartText(content)
	}
	var parts []string
	content.ForEach(func(_, item gjson.Result) bool {
		parts = append(parts, openAIToolResultPartText(item))
		return true
	})
	return strings.Join(parts, "\n\n")
}

func openAIToolResultPartText(part gjson.Result) string {
	if part.Type == gjson.String {
		return part.Str
	}
	if part.IsObject() {
		switch strings.ToLower(strings.TrimSpace(part.Get("type").String())) {
		case "image", "image_url", "input_image":
			return openAIToolResultImageOmittedText
		case "text", "input_text", "output_text":
			if text := part.Get("text"); text.Type == gjson.String {
				return text.Str
			}
		}
	}
	return part.Raw
}
