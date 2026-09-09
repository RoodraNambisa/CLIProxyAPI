package thinking

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// stripInferredClaudeSummaryActivation removes a globally inferred adaptive
// mode when the selected API-key model supports only manual extended thinking.
// The exact model-aware summary pass can then activate enabled thinking with a
// valid budget, or leave thinking absent when max_tokens cannot accommodate it.
func stripInferredClaudeSummaryActivation(body []byte, modelInfo *registry.ModelInfo) []byte {
	if modelInfo == nil || modelInfo.Thinking == nil || len(modelInfo.Thinking.Levels) > 0 || modelInfo.Thinking.Min <= 0 {
		return body
	}
	if !strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, "thinking.type").String()), "adaptive") {
		return body
	}

	for _, path := range []string{
		"thinking.type",
		"thinking.budget_tokens",
		"thinking.display",
		"output_config.effort",
	} {
		body, _ = sjson.DeleteBytes(body, path)
	}
	for _, path := range []string{"thinking", "output_config"} {
		if object := gjson.GetBytes(body, path); object.Exists() && object.IsObject() && len(object.Map()) == 0 {
			body, _ = sjson.DeleteBytes(body, path)
		}
	}
	return body
}
