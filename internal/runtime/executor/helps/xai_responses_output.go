package helps

import (
	"context"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v6/sdk/translator"
)

// XAIResponsesOutputPolicy keeps output identities and the caller policy local
// to the prepared request. Tool schemas and request bodies are not retained.
type XAIResponsesOutputPolicy struct {
	namespaces XAIResponseToolNamespaces
	codex      CodexMultiAgentResponsePolicy
}

func NewXAIResponsesOutputPolicy(original, translated []byte, responseFormat sdktranslator.Format, codex CodexMultiAgentResponsePolicy) XAIResponsesOutputPolicy {
	if responseFormat != sdktranslator.FormatCodex && responseFormat != sdktranslator.FormatOpenAIResponse {
		return XAIResponsesOutputPolicy{}
	}
	return XAIResponsesOutputPolicy{namespaces: NewXAIResponseToolNamespaces(original, translated), codex: codex}
}

func (policy XAIResponsesOutputPolicy) Rewrite(payload []byte) []byte {
	return policy.codex.Rewrite(policy.namespaces.Restore(payload))
}

func (policy XAIResponsesOutputPolicy) TranslateNonStream(ctx context.Context, from, to sdktranslator.Format, model string, original, request, response []byte, param *any) []byte {
	return policy.Rewrite(sdktranslator.TranslateNonStream(ctx, from, to, model, original, request, response, param))
}

func (policy XAIResponsesOutputPolicy) TranslateStream(ctx context.Context, from, to sdktranslator.Format, model string, original, request, response []byte, param *any) [][]byte {
	chunks := sdktranslator.TranslateStream(ctx, from, to, model, original, request, response, param)
	if len(policy.namespaces) == 0 && !policy.codex.PlaintextCalls && !policy.codex.NamespaceOptimized {
		return chunks
	}
	for index := range chunks {
		chunks[index] = rewriteCodexSSEChunk(chunks[index], policy.Rewrite)
	}
	return chunks
}
