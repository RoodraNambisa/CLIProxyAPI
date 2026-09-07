package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// CodexPlaintextResponsePolicy snapshots the caller gate for foreign-provider
// responses. Only known collaboration calls gain a missing plaintext marker.
func CodexPlaintextResponsePolicy(ctx context.Context, headers http.Header, cfg *config.Config, responseFormat sdktranslator.Format) CodexMultiAgentResponsePolicy {
	if responseFormat != sdktranslator.FormatOpenAIResponse && responseFormat != sdktranslator.FormatCodex {
		return CodexMultiAgentResponsePolicy{}
	}
	policy := SnapshotCodexMultiAgentPolicy(ctx, headers, cfg != nil && cfg.Codex.OptimizeMultiAgentV2)
	return CodexMultiAgentResponsePolicy{PlaintextCalls: policy.Enabled}
}

// TranslateNonStream adds plaintext metadata after restoring source tool names.
func (policy CodexMultiAgentResponsePolicy) TranslateNonStream(ctx context.Context, from, to sdktranslator.Format, model string, original, request, response []byte, param *any) []byte {
	return policy.Rewrite(sdktranslator.TranslateNonStream(ctx, from, to, model, original, request, response, param))
}

// TranslateStream rewrites complete translator events without retaining bodies.
func (policy CodexMultiAgentResponsePolicy) TranslateStream(ctx context.Context, from, to sdktranslator.Format, model string, original, request, response []byte, param *any) [][]byte {
	chunks := sdktranslator.TranslateStream(ctx, from, to, model, original, request, response, param)
	for index := range chunks {
		chunks[index] = policy.RewriteSSEChunk(chunks[index])
	}
	return chunks
}
