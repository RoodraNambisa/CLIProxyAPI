package thinking

import (
	"strings"
	"unsafe"

	"github.com/tidwall/gjson"
)

// SummaryMode represents whether the client explicitly requested reasoning summaries.
type SummaryMode int

const (
	SummaryUnspecified SummaryMode = iota
	SummaryDisabled
	SummaryEnabled
)

// SummaryConfig is the provider-neutral reasoning-summary visibility intent.
// Detail preserves protocols that distinguish auto, concise, and detailed summaries.
type SummaryConfig struct {
	Mode   SummaryMode
	Detail string
}

// ExtractSummaryConfig reads protocol-specific summary visibility intent.
//
// OpenAI Chat is the one protocol where effort implies summaries: chat
// completions has no summary field of its own, and clients that send
// reasoning_effort have always received reasoning summaries here, so treating a
// non-none effort as an explicit request preserves that contract. Every other
// protocol carries a dedicated summary field, so effort alone means nothing.
func ExtractSummaryConfig(body []byte, format string) SummaryConfig {
	normalized := strings.ToLower(strings.TrimSpace(format))
	if normalized == "codex" || normalized == "openai-response" {
		config, ok := responsesSummaryConfig(body, "reasoning.summary")
		if !ok {
			config, ok = responsesSummaryConfig(body, "reasoning.generate_summary")
		}
		// Absence already means unspecified, even for malformed input. Validate
		// the complete document before accepting any extracted visibility intent.
		if ok && gjson.ValidBytes(body) {
			return config
		}
		return SummaryConfig{}
	}
	// Check the format first so unsupported targets skip whole-body validation.
	if !summaryFormatSupported(normalized) || len(body) == 0 || !gjson.ValidBytes(body) {
		return SummaryConfig{}
	}

	switch normalized {
	case "openai":
		body = openAISummaryFields(body)
		if config, ok := extractOpenAIExplicitSummaryConfig(body); ok {
			return config
		}
		if effort := gjson.GetBytes(body, "reasoning_effort"); effort.Type == gjson.String {
			value := strings.ToLower(strings.TrimSpace(effort.String()))
			if value == "" {
				return SummaryConfig{}
			}
			if value == "none" {
				return SummaryConfig{Mode: SummaryDisabled}
			}
			return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}
		}
	case "claude":
		// Anthropic only accepts display alongside active adaptive/manual thinking.
		if !claudeThinkingAcceptsDisplay(body) {
			return SummaryConfig{}
		}
		if config, ok := claudeSummaryConfig(body, "thinking.display"); ok {
			return config
		}
	case "gemini":
		if config, ok := firstSummaryBoolConfig(body, []string{
			"generationConfig.thinkingConfig.includeThoughts",
			"generationConfig.thinkingConfig.include_thoughts",
			"generation_config.thinking_config.include_thoughts",
			"generation_config.thinking_config.includeThoughts",
		}); ok {
			return config
		}
	case "antigravity":
		if config, ok := firstSummaryBoolConfig(body, []string{
			"request.generationConfig.thinkingConfig.includeThoughts",
			"request.generationConfig.thinkingConfig.include_thoughts",
			"request.generationConfig.thinking_config.includeThoughts",
			"request.generationConfig.thinking_config.include_thoughts",
		}); ok {
			return config
		}
	case "interactions":
		for _, path := range []string{
			"generation_config.thinking_summaries",
			"generation_config.thinkingSummaries",
		} {
			if config, ok := interactionsSummaryConfig(body, path); ok {
				return config
			}
		}
		// Existing Interactions translators accept the OpenAI-style top-level
		// compatibility object. Keep the official generation_config selector
		// authoritative when both are present.
		if config, ok := interactionsSummaryConfig(body, "reasoning.summary"); ok {
			return config
		}
		if config, ok := firstSummaryBoolConfig(body, []string{
			"generation_config.thinking_config.include_thoughts",
			"generation_config.thinking_config.includeThoughts",
			"generation_config.thinkingConfig.include_thoughts",
			"generation_config.thinkingConfig.includeThoughts",
		}); ok {
			return config
		}
	}

	return SummaryConfig{}
}

// ExtractExplicitSummaryConfig reads only explicit visibility controls from a
// provider payload. Unlike ExtractSummaryConfig, OpenAI Chat reasoning_effort
// is not treated as a summary proxy. This lets executor post-processing tell
// whether a request normalizer retained or removed the translated target field.
func ExtractExplicitSummaryConfig(body []byte, format string) SummaryConfig {
	normalized := strings.ToLower(strings.TrimSpace(format))
	if normalized != "openai" {
		return ExtractSummaryConfig(body, normalized)
	}
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return SummaryConfig{}
	}
	config, _ := extractOpenAIExplicitSummaryConfig(openAISummaryFields(body))
	return config
}

// openAISummaryFields keeps unrelated messages out of repeated alias lookups.
// Callers validate the original document first. Preserve duplicate fields and
// their original encoding so deep-path lookup semantics remain unchanged.
func openAISummaryFields(body []byte) []byte {
	// The borrowed view is confined to this call. Selected values are copied
	// into the small result; no reference to the caller's body escapes.
	root := gjson.Parse(unsafe.String(unsafe.SliceData(body), len(body)))
	if !root.IsObject() {
		return nil
	}
	var fields []byte
	root.ForEach(func(key, value gjson.Result) bool {
		switch key.String() {
		case "extra_body", "google", "thinking", "reasoning", "generationConfig", "generation_config", "include_reasoning", "reasoning_effort":
			if len(fields) == 0 {
				fields = append(fields, '{')
			} else {
				fields = append(fields, ',')
			}
			fields = append(fields, key.Raw...)
			fields = append(fields, ':')
			fields = append(fields, value.Raw...)
		}
		return true
	})
	if len(fields) > 0 {
		fields = append(fields, '}')
	}
	return fields
}

// summaryFormatSupported reports whether a protocol carries summary visibility
// intent that this package can read or write.
func summaryFormatSupported(format string) bool {
	switch format {
	case "openai", "openai-response", "codex", "claude", "gemini", "antigravity", "interactions":
		return true
	default:
		return false
	}
}

// claudeThinkingAcceptsDisplay reports whether the body carries an active
// thinking block that can hold a display field.
func claudeThinkingAcceptsDisplay(body []byte) bool {
	switch strings.ToLower(strings.TrimSpace(gjson.GetBytes(body, "thinking.type").String())) {
	case "adaptive":
		return true
	case "enabled":
		// This runs before ApplyThinking normalizes the request, so a missing
		// budget_tokens is an unfinished body rather than inactive thinking. CPA
		// also accepts -1 as its compatibility representation for auto thinking.
		budget := gjson.GetBytes(body, "thinking.budget_tokens")
		if budget.Type != gjson.Number {
			return true
		}
		value := budget.Int()
		return value == -1 || value > 0
	default:
		return false
	}
}

func extractOpenAIExplicitSummaryConfig(body []byte) (SummaryConfig, bool) {
	// Google's documented Chat Completions extension is the authoritative
	// explicit visibility control when present, ahead of CPA compatibility
	// aliases and Chat's reasoning_effort fallback.
	for _, path := range []string{
		"extra_body.google.thinking_config.include_thoughts",
		"extra_body.google.thinking_config.includeThoughts",
		"extra_body.google.thinkingConfig.include_thoughts",
		"extra_body.google.thinkingConfig.includeThoughts",
		"extra_body.extra_body.google.thinking_config.include_thoughts",
		"extra_body.extra_body.google.thinking_config.includeThoughts",
		"google.thinking_config.include_thoughts",
		"google.thinking_config.includeThoughts",
		"thinking.includeThoughts",
		"thinking.include_thoughts",
		"reasoning.includeThoughts",
		"reasoning.include_thoughts",
		"generationConfig.thinkingConfig.includeThoughts",
		"generationConfig.thinkingConfig.include_thoughts",
		"generation_config.thinking_config.include_thoughts",
		"generation_config.thinking_config.includeThoughts",
	} {
		if config, ok := summaryBoolConfig(body, path); ok {
			return config, true
		}
	}

	for _, path := range []string{
		"reasoning.summary",
		"reasoning.generate_summary",
	} {
		if config, ok := responsesSummaryConfig(body, path); ok {
			return config, true
		}
	}

	// reasoning.exclude is OpenRouter's documented "reason but hide" bit, not an
	// OpenAI wire field; include_reasoning is its documented legacy alias
	// (include_reasoning: false is equivalent to reasoning: {exclude: true}).
	// Only accept actual JSON booleans.
	if exclude := gjson.GetBytes(body, "reasoning.exclude"); exclude.IsBool() {
		if exclude.Bool() {
			return SummaryConfig{Mode: SummaryDisabled}, true
		}
		return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
	}
	if include := gjson.GetBytes(body, "include_reasoning"); include.IsBool() {
		if include.Bool() {
			return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
		}
		return SummaryConfig{Mode: SummaryDisabled}, true
	}
	// OpenRouter's reasoning.enabled turns reasoning on "with no exclusions", so
	// it also decides visibility when no dedicated bit was sent.
	if enabled := gjson.GetBytes(body, "reasoning.enabled"); enabled.IsBool() {
		if enabled.Bool() {
			return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
		}
		return SummaryConfig{Mode: SummaryDisabled}, true
	}
	return SummaryConfig{}, false
}

func firstSummaryBoolConfig(body []byte, paths []string) (SummaryConfig, bool) {
	for _, path := range paths {
		if config, ok := summaryBoolConfig(body, path); ok {
			return config, true
		}
	}
	return SummaryConfig{}, false
}

func summaryBoolConfig(body []byte, path string) (SummaryConfig, bool) {
	switch value := gjson.GetBytes(body, path); value.Type {
	case gjson.True:
		return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
	case gjson.False:
		return SummaryConfig{Mode: SummaryDisabled}, true
	default:
		return SummaryConfig{}, false
	}
}

func responsesSummaryConfig(body []byte, path string) (SummaryConfig, bool) {
	value := gjson.GetBytes(body, path)
	if value.Raw == "" {
		return SummaryConfig{}, false
	}
	if value.Type == gjson.Null {
		return SummaryConfig{Mode: SummaryDisabled}, true
	}
	if value.Type != gjson.String {
		return SummaryConfig{}, false
	}

	raw := strings.ToLower(strings.TrimSpace(value.String()))
	switch raw {
	case "auto", "concise", "detailed":
		return SummaryConfig{Mode: SummaryEnabled, Detail: raw}, true
	case "none":
		// Compatibility with clients that expose a none enum; the OpenAI wire
		// representation disables summaries by omitting the field.
		return SummaryConfig{Mode: SummaryDisabled}, true
	default:
		return SummaryConfig{}, false
	}
}

func claudeSummaryConfig(body []byte, path string) (SummaryConfig, bool) {
	value := gjson.GetBytes(body, path)
	if value.Type != gjson.String {
		return SummaryConfig{}, false
	}
	switch strings.ToLower(strings.TrimSpace(value.String())) {
	case "summarized":
		return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
	case "omitted":
		return SummaryConfig{Mode: SummaryDisabled}, true
	default:
		return SummaryConfig{}, false
	}
}

func interactionsSummaryConfig(body []byte, path string) (SummaryConfig, bool) {
	value := gjson.GetBytes(body, path)
	if value.Type != gjson.String {
		return SummaryConfig{}, false
	}
	switch strings.ToLower(strings.TrimSpace(value.String())) {
	case "auto":
		return SummaryConfig{Mode: SummaryEnabled, Detail: "auto"}, true
	case "none":
		return SummaryConfig{Mode: SummaryDisabled}, true
	default:
		return SummaryConfig{}, false
	}
}
