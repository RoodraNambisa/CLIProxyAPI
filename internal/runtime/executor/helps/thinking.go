package helps

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// RequestSummaryConfig resolves visibility independently from provider-specific
// effort normalization, without retaining or copying the source payloads.
func RequestSummaryConfig(body []byte, req core.Request, opts core.Options, fromFormat, toFormat string) thinking.SummaryConfig {
	original := opts.OriginalRequest
	if len(original) == 0 {
		original = req.Payload
	}
	return translatedRequestSummaryConfig(body, req.Payload, original, req.Model, fromFormat, toFormat)
}

// ApplyThinkingWithSourcePayload retains visibility that could not be expressed
// until the model-aware pass, while respecting normalized target fields.
func ApplyThinkingWithSourcePayload(body, currentSource, originalSource []byte, model, fromFormat, toFormat, provider string) ([]byte, error) {
	summary := translatedRequestSummaryConfig(body, currentSource, originalSource, model, fromFormat, toFormat)
	return thinking.ApplyThinkingWithSummary(body, model, fromFormat, toFormat, provider, summary)
}

func translatedRequestSummaryConfig(body, currentSource, originalSource []byte, model, fromFormat, toFormat string) thinking.SummaryConfig {
	from, to := sdktranslator.FromString(fromFormat), sdktranslator.FromString(toFormat)
	fromFormat = strings.ToLower(strings.TrimSpace(fromFormat))
	toFormat = strings.ToLower(strings.TrimSpace(toFormat))
	if fromFormat == "" {
		fromFormat = toFormat
	}
	if fromFormat != toFormat && !sdktranslator.HasRequestTransformer(from, to) {
		// Fallback bodies retain their source shape and must not gain target fields.
		return thinking.SummaryConfig{}
	}

	target := thinking.ExtractExplicitSummaryConfig(body, toFormat)
	if fromFormat == toFormat {
		target = thinking.ExtractSummaryConfig(body, toFormat)
	}
	if target.Mode != thinking.SummaryUnspecified {
		return target
	}
	current := thinking.ExtractSummaryConfig(currentSource, fromFormat)
	if current.Mode == thinking.SummaryUnspecified {
		return thinking.ExtractSummaryConfig(originalSource, fromFormat)
	}
	if fromFormat == toFormat {
		// An explicit native field was removed by the final request normalizer.
		return thinking.SummaryConfig{}
	}

	candidate := thinking.ApplySummaryConfigForModel(body, toFormat, model, current)
	if thinking.ExtractExplicitSummaryConfig(candidate, toFormat).Mode != thinking.SummaryUnspecified {
		// Translation could represent this intent, so a missing target field was
		// deliberately removed. Do not restore it from the original source.
		return thinking.SummaryConfig{}
	}
	// Claude may need a suffix to activate thinking before display is valid;
	// Chat dialects may need the execution provider's visibility extension.
	return current
}
