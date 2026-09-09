package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	codexclaude "github.com/router-for-me/CLIProxyAPI/v6/internal/translator/codex/claude"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// TranslateRequestWithAPIKeyModelCompatibility selects explicit compatibility
// converters without putting private policy markers into request JSON.
func TranslateRequestWithAPIKeyModelCompatibility(from, to sdktranslator.Format, model string, payload []byte, stream, isCompat bool) []byte {
	if !isCompat || from != sdktranslator.FormatClaude || to != sdktranslator.FormatCodex {
		return sdktranslator.TranslateRequest(from, to, model, payload, stream)
	}
	translated := codexclaude.ConvertClaudeRequestToCodexWithCompat(model, payload, stream)
	return thinking.ApplySummaryConfigForModel(translated, to.String(), model, thinking.ExtractSummaryConfig(payload, from.String()))
}
