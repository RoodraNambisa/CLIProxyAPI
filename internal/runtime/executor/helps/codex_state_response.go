package helps

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type CodexManagedStateUse struct {
	Credential codexstate.Credential
	Version    uint64
	Value      string
	manager    *codexstate.Manager
}

func codexStateRequestKey(c codexstate.Credential) string {
	return c.ID + "\x00" + c.Owner + "\x00" + c.Model + "\x00" + c.Plan
}

// ManagedStateUse binds observation to the actual applied header, never to a
// manually supplied matching value or an acquisition probe.
func ManagedStateUse(ctx context.Context, a *auth.Auth, model string, headers http.Header) CodexManagedStateUse {
	if IsStateProbe(ctx) || headers.Get("X-Codex-Turn-State") == "" || !core.HasCodexStateChoice(ctx) {
		return CodexManagedStateUse{}
	}
	c := StateCredential(a, model)
	manager, requestKey := codexstate.Default, codexStateRequestKey(c)
	if diagnostic, _ := ctx.Value(codexStateDiagnosticKey{}).(*codexStateDiagnostic); diagnostic != nil && diagnostic.mode == "acquired" {
		manager, requestKey = codexstate.Diagnostic, "diagnostic:"+requestKey
	}
	choice, ok := core.CodexStateChoiceForRequest(ctx, requestKey)
	if !ok || choice.Version == 0 || choice.Value == "" || choice.Value != headers.Get("X-Codex-Turn-State") {
		return CodexManagedStateUse{}
	}
	return CodexManagedStateUse{Credential: c, Version: choice.Version, Value: choice.Value, manager: manager}
}

// Observe accepts original upstream metadata before any response-model rewrite.
// It does not alter the business response or cause the business request to replay.
func (u CodexManagedStateUse) Observe(headers http.Header, payload []byte) bool {
	manager := u.manager
	if manager == nil {
		manager = codexstate.Default
	}
	if u.Version == 0 || !manager.WatchesResponses() {
		return false
	}
	model := ""
	if len(payload) > 0 {
		root := gjson.ParseBytes(payload)
		if root.Get("response").IsObject() {
			root = root.Get("response")
		}
		model = root.Get("model").String()
	}
	return manager.ObserveResponse(u.Credential, u.Version, u.Value, model, len(headers.Get("X-Codex-Turn-State"))) != ""
}
