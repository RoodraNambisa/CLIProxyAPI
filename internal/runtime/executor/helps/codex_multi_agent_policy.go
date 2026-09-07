package helps

import (
	"context"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const CodexMultiAgentPolicyGinKey = "codex_multi_agent_v2_policy"

type codexMultiAgentPolicyContextKey struct{}

// CodexMultiAgentPolicy contains only immutable client preparation decisions.
type CodexMultiAgentPolicy struct {
	Enabled       bool
	ToolsPrepared bool
	ClientVersion string
}

// NewCodexMultiAgentPolicy uses the real caller identity, never outbound defaults.
func NewCodexMultiAgentPolicy(headers http.Header, enabled bool) CodexMultiAgentPolicy {
	userAgent := codexMultiAgentUserAgent(headers)
	official := userAgent == "codex_cli_rs" || strings.HasPrefix(userAgent, "codex_cli_rs/") ||
		strings.HasPrefix(userAgent, "codex-tui/") || strings.HasPrefix(userAgent, "Codex Desktop/")
	policy := CodexMultiAgentPolicy{Enabled: enabled && official}
	if official {
		if _, suffix, ok := strings.Cut(userAgent, "/"); ok {
			policy.ClientVersion, _, _ = strings.Cut(suffix, " ")
		}
	}
	return policy
}

func codexMultiAgentUserAgent(headers http.Header) string {
	var result string
	for key, values := range headers {
		if !strings.EqualFold(key, "User-Agent") {
			continue
		}
		for _, value := range values {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if result != "" && value != result {
				return ""
			}
			result = value
		}
	}
	return result
}

// SnapshotCodexMultiAgentPolicy reuses API preparation first, then the Manager's
// logical request setting. Direct executor callers can supply a config fallback.
func SnapshotCodexMultiAgentPolicy(ctx context.Context, headers http.Header, enabled bool) CodexMultiAgentPolicy {
	if ctx != nil {
		if policy, ok := ctx.Value(codexMultiAgentPolicyContextKey{}).(CodexMultiAgentPolicy); ok {
			return policy
		}
		if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil {
			if raw, exists := c.Get(CodexMultiAgentPolicyGinKey); exists {
				if policy, ok := raw.(CodexMultiAgentPolicy); ok {
					return policy
				}
			}
			if c.Request != nil {
				headers = c.Request.Header
			}
		}
	}
	if captured, ok := cliproxyauth.CodexMultiAgentV2RequestSetting(ctx); ok {
		enabled = captured
	}
	return NewCodexMultiAgentPolicy(headers, enabled)
}

// CaptureCodexMultiAgentPolicyContext copies one prepared API turn out of Gin.
// The next WebSocket turn may update Gin without changing this context's policy.
func CaptureCodexMultiAgentPolicyContext(ctx context.Context) context.Context {
	if ctx == nil {
		return nil
	}
	c, _ := ctx.Value("gin").(*gin.Context)
	if c == nil {
		return ctx
	}
	raw, exists := c.Get(CodexMultiAgentPolicyGinKey)
	if !exists {
		return ctx
	}
	policy, ok := raw.(CodexMultiAgentPolicy)
	if !ok {
		return ctx
	}
	return context.WithValue(ctx, codexMultiAgentPolicyContextKey{}, policy)
}
