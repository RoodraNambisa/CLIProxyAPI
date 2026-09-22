package helps

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type CodexManagedStateUse struct {
	Credential  codexstate.Credential
	Version     uint64
	Value       string
	manager     *codexstate.Manager
	observation *core.CodexStateObservationPolicy
	cookie      *codexstate.CookieSelection
	response    *cookieResponseObservation
}

type cookieResponseObservation struct {
	mu      sync.Mutex
	headers http.Header
}

// CookieNeedsReconnect is used only between WebSocket turns, never midstream.
func (u CodexManagedStateUse) CookieNeedsReconnect() bool {
	if u.cookie == nil || u.manager == nil {
		return false
	}
	p := config.CodexStateOverrideConfig{}
	if u.observation != nil {
		p.CookieMaxAgeSeconds = u.observation.CookieMaxAgeSeconds
	}
	return !u.manager.CookieConnectionValid(u.Credential, *u.cookie, time.Now(), p)
}

func codexStateRequestKey(c codexstate.Credential) string {
	return c.ID + "\x00" + c.Owner + "\x00" + c.Model + "\x00" + c.Plan
}

// ManagedStateUse binds observation to the actual applied header, never to a
// manually supplied matching value or an acquisition probe.
func ManagedStateUse(ctx context.Context, a *auth.Auth, model string, headers http.Header) CodexManagedStateUse {
	if IsStateProbe(ctx) || !core.HasCodexStateChoice(ctx) {
		return CodexManagedStateUse{}
	}
	c := StateCredential(a, model)
	manager, requestKey := codexstate.Default, codexStateRequestKey(c)
	if diagnostic, _ := ctx.Value(codexStateDiagnosticKey{}).(*codexStateDiagnostic); diagnostic != nil && diagnostic.mode == "acquired" {
		manager, requestKey = codexstate.Diagnostic, "diagnostic:"+requestKey
	}
	choice, ok := core.CodexStateChoiceForRequest(ctx, requestKey)
	if !ok || choice.Version == 0 {
		return CodexManagedStateUse{}
	}
	if choice.Cookie != nil {
		return CodexManagedStateUse{Credential: c, Version: choice.Version, cookie: choice.Cookie, manager: manager, observation: choice.Observation, response: &cookieResponseObservation{}}
	}
	if choice.Value == "" || choice.Value != headers.Get("X-Codex-Turn-State") {
		return CodexManagedStateUse{}
	}
	return CodexManagedStateUse{Credential: c, Version: choice.Version, Value: choice.Value, manager: manager, observation: choice.Observation}
}

// Observe accepts original upstream metadata before any response-model rewrite.
// It does not alter the business response or cause the business request to replay.
func (u CodexManagedStateUse) Observe(headers http.Header, payload []byte) bool {
	manager := u.manager
	if manager == nil {
		manager = codexstate.Default
	}
	if u.Version == 0 {
		return false
	}
	if u.cookie == nil && u.observation != nil && !u.observation.ModelMismatch && !u.observation.LengthMismatch {
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
	if u.cookie != nil {
		policy := config.CodexStateOverrideConfig{}
		if u.observation != nil {
			policy.InvalidateOnModelMismatch = u.observation.ModelMismatch
			policy.InvalidateOnStateLengthMismatch = u.observation.LengthMismatch
			policy.Lengths = u.observation.Lengths
			policy.MissingReturnedState = u.observation.MissingReturnedState
			policy.MatchModel = new(u.observation.MatchModel)
		}
		completed := gjson.GetBytes(payload, "type").String() == "response.completed" || gjson.GetBytes(payload, "response.status").String() == "completed" || gjson.GetBytes(payload, "status").String() == "completed"
		if u.response != nil {
			u.response.mu.Lock()
			defer u.response.mu.Unlock()
			if headers != nil {
				u.response.headers = headers.Clone()
				// Apply explicit deletions and auxiliary deltas at receipt, but do
				// not classify incomplete or failed requests as rule failures.
				if manager.ObserveCookie(u.Credential, *u.cookie, policy, headers, "", false) {
					return true
				}
			}
			if !completed {
				return false
			}
			headers = u.response.headers.Clone()
			headers.Del("Set-Cookie")
		}
		return manager.ObserveCookie(u.Credential, *u.cookie, policy, headers, model, completed)
	}
	if u.observation != nil {
		policy := config.CodexStateOverrideConfig{InvalidateOnModelMismatch: u.observation.ModelMismatch, InvalidateOnStateLengthMismatch: u.observation.LengthMismatch, Lengths: u.observation.Lengths, MissingReturnedState: u.observation.MissingReturnedState}
		if headers == nil {
			policy.MissingReturnedState = "ignore"
		}
		return manager.ObserveResponseForPolicy(u.Credential, u.Version, u.Value, model, len(headers.Get("X-Codex-Turn-State")), policy) != ""
	}
	return manager.ObserveResponse(u.Credential, u.Version, u.Value, model, len(headers.Get("X-Codex-Turn-State"))) != ""
}
