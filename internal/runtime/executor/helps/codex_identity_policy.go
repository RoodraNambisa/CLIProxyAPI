package helps

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func CodexIdentityPriority(auth *coreauth.Auth) int {
	if auth == nil {
		return 0
	}
	n, _ := strconv.Atoi(strings.TrimSpace(auth.Attributes["priority"]))
	return n
}

func CodexCredentialUserAgent(auth *coreauth.Auth) string {
	if auth == nil {
		return ""
	}
	for key, value := range auth.Attributes {
		if strings.EqualFold(key, "header:User-Agent") && codexauth.ValidSoftwareIdentityUserAgent(value) {
			return value
		}
	}
	return ""
}

func ResolveCodexIdentityPolicy(policies *config.CodexIdentityPolicySet, auth *coreauth.Auth) config.CodexIdentityPolicy {
	mode := "device"
	if auth != nil {
		mode = string(codexauth.EffectiveFingerprintMode(auth.Metadata))
	}
	policy := policies.Resolve(CodexIdentityPriority(auth), mode, CodexCredentialUserAgent(auth))
	if auth != nil && strings.TrimSpace(auth.Attributes["api_key"]) != "" {
		policy.SpoofSessionIdentity = false
		policy.FingerprintMode = "off"
		policy.EnforceSoftwareIdentity = false
	}
	return policy
}

func ApplyCodexSoftwarePolicy(headers http.Header, policy config.CodexIdentityPolicy, model string) {
	if !policy.EnforceSoftwareIdentity {
		if policy.HasUserAgentOverride() {
			headers.Set("User-Agent", policy.UserAgent)
		}
		return
	}
	identity := codexauth.ResolveSoftwareIdentityForModel(policy.UserAgent, model)
	for _, key := range []string{"User-Agent", "Originator", "Version"} {
		for existing := range headers {
			if strings.EqualFold(key, existing) {
				delete(headers, existing)
			}
		}
	}
	headers.Set("User-Agent", identity.UserAgent)
	headers.Set("Originator", identity.Originator)
	headers.Set("Version", identity.Version)
}

type codexIdentityConnectionKey struct{}

func WithCodexIdentityConnection(ctx context.Context, p config.CodexIdentityPolicy, h http.Header) context.Context {
	raw, _ := json.Marshal(p)
	sum := sha256.New()
	_, _ = sum.Write(raw)
	for _, key := range []string{"User-Agent", "Originator", "Version", "X-Codex-Installation-Id", "Session-Id", "Thread-Id", "X-Codex-Window-Id"} {
		_, _ = sum.Write([]byte{0})
		_, _ = sum.Write([]byte(h.Get(key)))
	}
	var digest [32]byte
	copy(digest[:], sum.Sum(nil))
	return context.WithValue(ctx, codexIdentityConnectionKey{}, digest)
}
func CodexIdentityConnectionDigest(ctx context.Context) [32]byte {
	v, _ := ctx.Value(codexIdentityConnectionKey{}).([32]byte)
	return v
}
