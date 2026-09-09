package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	chatclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/claude/openai/chat-completions"
	codexclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/claude"
	geminiclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/gemini/claude"
	interactionsclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/interactions/claude"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// TranslateRequestWithAPIKeyModelCompatibility selects explicit compatibility
// converters without putting private policy markers into request JSON.
func TranslateRequestWithAPIKeyModelCompatibility(from, to sdktranslator.Format, model string, payload []byte, stream, isCompat bool) []byte {
	if !isCompat {
		return sdktranslator.TranslateRequest(from, to, model, payload, stream)
	}
	var translated []byte
	switch {
	case from == sdktranslator.FormatClaude && to == sdktranslator.FormatCodex:
		translated = codexclaude.ConvertClaudeRequestToCodexWithCompat(model, payload, stream)
	case from == sdktranslator.FormatClaude && to == sdktranslator.FormatGemini:
		translated = geminiclaude.ConvertClaudeRequestToGeminiWithCompat(model, payload, stream)
	case from == sdktranslator.FormatClaude && to == sdktranslator.FormatInteractions:
		translated = interactionsclaude.ConvertClaudeRequestToInteractionsWithCompat(model, payload, stream)
	case from == sdktranslator.FormatOpenAI && to == sdktranslator.FormatClaude:
		translated = chatclaude.ConvertOpenAIRequestToClaudeWithCompat(model, payload, stream)
	default:
		return sdktranslator.TranslateRequest(from, to, model, payload, stream)
	}
	return thinking.ApplySummaryConfigForModel(translated, to.String(), model, thinking.ExtractSummaryConfig(payload, from.String()))
}
