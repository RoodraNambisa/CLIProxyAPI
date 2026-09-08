package helps

import (
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

// ApplyRequestThinking uses the selected credential's declaration when bound by
// the manager. Direct executor calls retain the existing registry lookup path.
func ApplyRequestThinking(body []byte, req core.Request, opts core.Options, fromFormat, toFormat, provider string) ([]byte, error) {
	if info, ok := cliproxyauth.ResolvedAPIKeyModelInfo(req); ok {
		source := opts.OriginalRequest
		if len(source) == 0 {
			source = req.Payload
		}
		return thinking.ApplyThinkingWithModelInfo(body, source, req.Model, fromFormat, toFormat, provider, info)
	}
	return thinking.ApplyThinking(body, req.Model, fromFormat, toFormat, provider)
}
