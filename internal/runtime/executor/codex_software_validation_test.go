package executor

import (
	"net/http"
	"testing"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCodexSoftwareIdentityInvalidCredentialFallsBackToGlobal(t *testing.T) {
	for _, enforce := range []bool{false, true} {
		cfg := &config.Config{Codex: config.CodexConfig{EnforceSoftwareIdentity: &enforce}, CodexHeaderDefaults: config.CodexHeaderDefaults{UserAgent: "Codex Desktop/0.153.4 (Linux; x86_64) (Codex Desktop; 26.9.0)"}}
		auth := &coreauth.Auth{Provider: "codex", Attributes: map[string]string{"header:User-Agent": "codex-tui/0.153.4 bad\x00"}}
		headers := http.Header{"User-Agent": {auth.Attributes["header:User-Agent"]}}
		applyCodexSoftwareIdentity(headers, auth, cfg)
		if headers.Get("User-Agent") != cfg.CodexHeaderDefaults.UserAgent {
			t.Fatal("invalid credential UA did not fall back to global")
		}
		cfg.CodexHeaderDefaults.UserAgent = "codex-tui/0.153.4 bad\x7f"
		headers.Set("User-Agent", auth.Attributes["header:User-Agent"])
		applyCodexSoftwareIdentity(headers, auth, cfg)
		if headers.Get("User-Agent") != codexauth.DefaultUserAgent {
			t.Fatal("invalid global UA did not fall back to built-in")
		}
	}
}
