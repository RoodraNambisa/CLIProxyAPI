package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// TranslateRequestWithCodexMultiAgentV2 adapts plaintext collaboration history
// before translating Responses to another protocol. Native ciphertext is opaque.
func TranslateRequestWithCodexMultiAgentV2(ctx context.Context, headers http.Header, cfg *config.Config, from, to sdktranslator.Format, model string, payload []byte, stream bool) ([]byte, error) {
	if from == sdktranslator.FormatCodex || from == sdktranslator.FormatOpenAIResponse {
		policy := SnapshotCodexMultiAgentPolicy(ctx, headers, cfg != nil && cfg.Codex.OptimizeMultiAgentV2)
		if policy.Enabled && ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		var errNormalize error
		payload, errNormalize = NormalizeCodexMultiAgentInput(payload, policy.Enabled, to)
		if errNormalize != nil {
			return nil, errNormalize
		}
	}
	return sdktranslator.TranslateRequest(from, to, model, payload, stream), nil
}
