package thinking

import (
	"strings"
	"unsafe"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ApplySummaryConfig writes canonical summary intent in the target protocol.
func ApplySummaryConfig(body []byte, format string, config SummaryConfig) []byte {
	return ApplySummaryConfigForModel(body, format, "", config)
}

// ApplySummaryConfigForModel writes canonical summary intent in the target
// protocol and uses target model capabilities when a valid target request must
// activate thinking before it can request summaries.
func ApplySummaryConfigForModel(body []byte, format, model string, config SummaryConfig) []byte {
	return applySummaryConfigForModel(body, format, model, nil, config)
}

// applySummaryConfigForModel uses the resolved model definition when execution
// selected a configured API-key model whose capability is not globally visible.
func applySummaryConfigForModel(body []byte, format, model string, modelInfo *registry.ModelInfo, config SummaryConfig) []byte {
	return applySummaryConfigForProvider(body, format, model, "", modelInfo, config)
}

// applySummaryConfigForProvider uses the execution provider identity for Chat
// dialects whose visibility controls are not part of the OpenAI wire format.
func applySummaryConfigForProvider(body []byte, format, model, provider string, modelInfo *registry.ModelInfo, config SummaryConfig) []byte {
	normalized := strings.ToLower(strings.TrimSpace(format))
	if config.Mode == SummaryUnspecified || !summaryFormatSupported(normalized) || len(body) == 0 || !gjson.ValidBytes(body) {
		return body
	}

	enabled := config.Mode == SummaryEnabled
	switch normalized {
	case "openai":
		body = applyOpenAIChatSummaryConfig(body, provider, enabled)
	case "claude":
		// Display requires active thinking. Hiding summaries must not activate
		// thinking; showing them may select a valid mode from model capabilities.
		if enabled && !gjson.GetBytes(body, "thinking.type").Exists() {
			body = enableClaudeThinkingForSummary(body, model, modelInfo)
		}
		if !claudeThinkingAcceptsDisplay(body) {
			return body
		}
		value := "omitted"
		if enabled {
			value = "summarized"
		}
		body, _ = sjson.SetBytes(body, "thinking.display", value)
	case "gemini":
		body, _ = sjson.SetBytes(body, "generationConfig.thinkingConfig.includeThoughts", enabled)
		for _, path := range []string{
			"generationConfig.thinkingConfig.include_thoughts",
			"generation_config.thinking_config.include_thoughts",
			"generation_config.thinking_config.includeThoughts",
		} {
			body, _ = sjson.DeleteBytes(body, path)
		}
	case "antigravity":
		body, _ = sjson.SetBytes(body, "request.generationConfig.thinkingConfig.includeThoughts", enabled)
		for _, path := range []string{
			"request.generationConfig.thinkingConfig.include_thoughts",
			"request.generationConfig.thinking_config.include_thoughts",
			"request.generationConfig.thinking_config.includeThoughts",
		} {
			body, _ = sjson.DeleteBytes(body, path)
		}
	case "interactions":
		// Google Interactions only accepts auto or none. OpenAI's concise and
		// detailed selectors therefore collapse to the supported enabled value.
		value := "none"
		if enabled {
			value = "auto"
		}
		body, _ = sjson.SetBytes(body, "generation_config.thinking_summaries", value)
		body, _ = sjson.DeleteBytes(body, "generation_config.thinkingSummaries")
	case "openai-response", "codex":
		body = applyResponsesSummaryConfig(body, config)
	}
	return body
}

// applyResponsesSummaryConfig skips mutations that would only copy the body.
// Read each path segment separately to match SJSON's first-parent lookup when
// duplicate reasoning objects exist. Borrowed views stay within this call;
// changed output is still independently owned by the original SJSON writers.
func applyResponsesSummaryConfig(body []byte, config SummaryConfig) []byte {
	reasoning := gjson.Get(unsafe.String(unsafe.SliceData(body), len(body)), "reasoning")
	summary := reasoning.Get("summary")
	legacy := reasoning.Get("generate_summary")
	if config.Mode == SummaryEnabled {
		detail := normalizedSummaryDetail(config.Detail)
		if summary.Raw != `"`+detail+`"` {
			body, _ = sjson.SetBytes(body, "reasoning.summary", detail)
		}
	} else if summary.Exists() {
		// Omission disables summaries; explicit null is not accepted by every
		// Responses-compatible backend.
		body, _ = sjson.DeleteBytes(body, "reasoning.summary")
	}
	if legacy.Exists() {
		body, _ = sjson.DeleteBytes(body, "reasoning.generate_summary")
	}
	if config.Mode == SummaryEnabled {
		return body
	}
	if summary.Exists() || legacy.Exists() {
		reasoning = gjson.Get(unsafe.String(unsafe.SliceData(body), len(body)), "reasoning")
	}
	if reasoning.IsObject() && strings.TrimSpace(reasoning.Raw[1:len(reasoning.Raw)-1]) == "" {
		body, _ = sjson.DeleteBytes(body, "reasoning")
	}
	return body
}

// applyOpenAIChatSummaryConfig writes only documented Chat visibility controls.
//
// OpenAI Chat Completions exposes reasoning_effort but no reasoning summary or
// visibility parameter. DeepSeek and Kimi Chat return reasoning_content while
// thinking is active, but likewise document no independent hide/show switch.
// Summary intent must therefore never invent or overwrite thinking effort for
// those dialects. OpenRouter is the exception: reasoning.exclude is its
// documented "reason but hide" control, and include_reasoning is its deprecated
// inverse alias. Unknown OpenAI-compatible providers are handled conservatively
// by updating those fields only when the payload already carries them.
//
// Docs:
// https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create
// https://openrouter.ai/docs/guides/best-practices/reasoning-tokens
// https://api-docs.deepseek.com/guides/thinking_mode
// https://platform.kimi.ai/docs/api/chat
func applyOpenAIChatSummaryConfig(body []byte, provider string, enabled bool) []byte {
	if isOpenRouterProvider(provider) || gjson.GetBytes(body, "reasoning.exclude").IsBool() {
		body, _ = sjson.SetBytes(body, "reasoning.exclude", !enabled)
	}
	if gjson.GetBytes(body, "include_reasoning").IsBool() {
		body, _ = sjson.SetBytes(body, "include_reasoning", enabled)
	}
	return body
}

func isOpenRouterProvider(provider string) bool {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "openrouter" {
		return true
	}
	for _, part := range strings.FieldsFunc(provider, func(r rune) bool {
		return r == '-' || r == '_' || r == '/' || r == '.' || r == ':'
	}) {
		if part == "openrouter" {
			return true
		}
	}
	return false
}

func enableClaudeThinkingForSummary(body []byte, model string, resolvedModelInfo *registry.ModelInfo) []byte {
	modelInfo := resolvedModelInfo
	if modelInfo == nil {
		baseModel := ParseSuffix(model).ModelName
		if baseModel == "" {
			baseModel = ParseSuffix(gjson.GetBytes(body, "model").String()).ModelName
		}
		modelInfo = registry.LookupModelInfo(baseModel, "claude")
	}
	if modelInfo == nil || modelInfo.Thinking == nil {
		return body
	}

	if len(modelInfo.Thinking.Levels) > 0 {
		body, _ = sjson.SetBytes(body, "thinking.type", "adaptive")
		body, _ = sjson.DeleteBytes(body, "thinking.budget_tokens")
		return body
	}

	budget := modelInfo.Thinking.Min
	if budget <= 0 {
		return body
	}
	if maxTokens := gjson.GetBytes(body, "max_tokens"); maxTokens.Exists() && maxTokens.Int() <= int64(budget) {
		return body
	}
	body, _ = sjson.SetBytes(body, "thinking.type", "enabled")
	body, _ = sjson.SetBytes(body, "thinking.budget_tokens", budget)
	return body
}

func normalizedSummaryDetail(detail string) string {
	switch strings.ToLower(strings.TrimSpace(detail)) {
	case "concise":
		return "concise"
	case "detailed":
		return "detailed"
	default:
		return "auto"
	}
}
