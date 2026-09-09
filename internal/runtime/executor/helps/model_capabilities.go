package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// ApplyRequestThinking uses the selected credential's declaration when bound by
// the manager. Direct executor calls retain the existing registry lookup path.
// A true payloadAuthority makes applied rules authoritative over source-body
// intent; the model suffix still has its existing precedence.
func ApplyRequestThinking(body []byte, req core.Request, opts core.Options, fromFormat, toFormat, provider string, payloadAuthority ...bool) ([]byte, error) {
	if info, ok := cliproxyauth.ResolvedAPIKeyModelInfo(req); ok {
		var source []byte
		if len(payloadAuthority) == 0 || !payloadAuthority[0] {
			source = opts.OriginalRequest
			if len(source) == 0 {
				source = req.Payload
			}
		}
		return thinking.ApplyThinkingWithModelInfoAndSummary(body, source, req.Model, fromFormat, toFormat, provider, info, thinking.ExtractSummaryConfig(body, toFormat))
	}
	return thinking.ApplyThinking(body, req.Model, fromFormat, toFormat, provider)
}
