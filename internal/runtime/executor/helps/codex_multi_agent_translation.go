package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// TranslateRequestWithCodexMultiAgentV2 adapts plaintext collaboration history
// before translating Responses to another protocol. Native ciphertext is opaque.
func TranslateRequestWithCodexMultiAgentV2(ctx context.Context, headers http.Header, cfg *config.Config, from, to sdktranslator.Format, model string, payload []byte, stream bool, isCompat ...bool) ([]byte, error) {
	payload, errNormalize := NormalizeCodexMultiAgentRequest(ctx, headers, cfg, from, to, payload)
	if errNormalize != nil {
		return nil, errNormalize
	}
	return TranslateRequestWithAPIKeyModelCompatibility(from, to, model, payload, stream, len(isCompat) > 0 && isCompat[0]), nil
}

// NormalizeCodexMultiAgentRequest keeps source and target semantics separate
// from delegated executors and their transport-specific configuration.
func NormalizeCodexMultiAgentRequest(ctx context.Context, headers http.Header, cfg *config.Config, from, target sdktranslator.Format, payload []byte) ([]byte, error) {
	if from == sdktranslator.FormatCodex || from == sdktranslator.FormatOpenAIResponse {
		policy := SnapshotCodexMultiAgentPolicy(ctx, headers, cfg != nil && cfg.Codex.OptimizeMultiAgentV2)
		if policy.Enabled && ctx != nil && ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if policy.Enabled && !policy.ToolsPrepared {
			payload = PrepareCodexCollaborationTools(payload, "")
		}
		return NormalizeCodexMultiAgentInput(payload, policy.Enabled, target)
	}
	return payload, nil
}
