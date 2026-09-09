package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// ApplyRequestThinking uses the selected credential's declaration when bound by
// the manager. Direct executor calls retain the existing registry lookup path.
// Applied payload rules remain authoritative over the field roles they own;
// the model suffix still has its existing precedence over effort.
func ApplyRequestThinking(body []byte, req core.Request, opts core.Options, fromFormat, toFormat, provider string, payloadAuthority ...PayloadThinkingAuthority) ([]byte, error) {
	original := opts.OriginalRequest
	if len(original) == 0 {
		original = req.Payload
	}
	var authority PayloadThinkingAuthority
	if len(payloadAuthority) > 0 {
		authority = payloadAuthority[0]
	}
	summary := thinking.SummaryConfig{}
	if !authority.Summary {
		summary = translatedRequestSummaryConfig(body, req.Payload, original, req.Model, fromFormat, toFormat)
	}
	if info, ok := cliproxyauth.ResolvedAPIKeyModelInfo(req); ok {
		var source []byte
		if !authority.Effort {
			source = original
		}
		return thinking.ApplyThinkingWithModelInfoAndSummary(body, source, req.Model, fromFormat, toFormat, provider, info, summary)
	}
	return thinking.ApplyThinkingWithSummary(body, req.Model, fromFormat, toFormat, provider, summary)
}
