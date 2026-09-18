package helps

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	codexauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexstate"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	auth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

type stateProbeKey struct{}
type stateCaptureKey struct{}
type codexStateDiagnosticKey struct{}
type codexStateDiagnostic struct {
	mode, value string
	observe     func(string, string)
}

// WithCodexStateDiagnostic applies temporary management-test options after normal header guards.
func WithCodexStateDiagnostic(ctx context.Context, mode, value string, observe func(string, string)) context.Context {
	return context.WithValue(ctx, codexStateDiagnosticKey{}, &codexStateDiagnostic{mode, value, observe})
}

func WithStateCapture(ctx context.Context, capture *codexstate.Result) context.Context {
	return context.WithValue(WithStateProbe(ctx), stateCaptureKey{}, capture)
}
func CaptureStateCompletion(ctx context.Context, headers http.Header, data []byte) {
	capture, _ := ctx.Value(stateCaptureKey{}).(*codexstate.Result)
	if capture == nil {
		return
	}
	root := gjson.GetBytes(data, "response")
	capture.Completed = root.Get("status").String() == "completed"
	capture.State = headers.Get("X-Codex-Turn-State")
	capture.Status = 200
	capture.Model = root.Get("model").String()
	capture.Tokens = root.Get("usage.total_tokens").Int()
	for _, item := range root.Get("output").Array() {
		if item.Get("type").String() == "message" {
			for _, content := range item.Get("content").Array() {
				if content.Get("type").String() == "output_text" {
					capture.Answer += content.Get("text").String()
				}
			}
		}
	}
}

func WithStateProbe(ctx context.Context) context.Context {
	return context.WithValue(ctx, stateProbeKey{}, true)
}
func IsStateProbe(ctx context.Context) bool { return ctx != nil && ctx.Value(stateProbeKey{}) == true }

func StateCredential(a *auth.Auth, model string) codexstate.Credential {
	if a == nil {
		return codexstate.Credential{}
	}
	owner := codexauth.EffectiveRequestAccountID(a.Metadata)
	if owner == "" {
		owner = a.ID
	}
	plan := strings.TrimSpace(a.Attributes["plan_type"])
	if plan == "" {
		plan = codexauth.EffectivePlanType(a.Metadata)
	}
	return codexstate.Credential{ID: a.ID, Name: a.FileName, Owner: owner, Instance: a.RuntimeInstanceID(), Model: thinking.ParseSuffix(model).ModelName, Plan: config.NormalizeCodexStatePlanType(plan)}
}

// ManagedStateModels uses the credential's registered catalog, including exclusions.
func ManagedStateModels(cfg *config.Config, a *auth.Auth) []codexstate.Credential {
	if cfg == nil || !cfg.Codex.StateOverride.Enabled || cfg.Codex.ResolvedTurnStatePolicy() == config.CodexTurnStatePolicyStrip || a == nil || a.Disabled || a.Status == auth.StatusDisabled || a.RuntimeInstanceRetired() || a.ExecutionProvider() != "codex" || a.Attributes["api_key"] != "" {
		return nil
	}
	c := cfg.Codex.StateOverride
	if slices.Contains(c.ExcludedCredentials, a.ID) || slices.Contains(c.ExcludedCredentials, a.Index) || slices.Contains(c.ExcludedCredentials, a.FileName) {
		return nil
	}
	priority := 0
	if p := a.Attributes["priority"]; p != "" {
		priority, _ = strconv.Atoi(p)
	} else {
		switch p := a.Metadata["priority"].(type) {
		case int:
			priority = p
		case float64:
			priority = int(p)
		case string:
			priority, _ = strconv.Atoi(p)
		}
	}
	included := slices.Contains(c.IncludedCredentials, a.ID) || slices.Contains(c.IncludedCredentials, a.Index) || slices.Contains(c.IncludedCredentials, a.FileName)
	// Explicit credentials extend the priority scope. Empty selectors preserve all-credential scope.
	if (len(c.Priorities) > 0 || len(c.IncludedCredentials) > 0) && !included && !slices.Contains(c.Priorities, priority) {
		return nil
	}
	result := []codexstate.Credential{}
	seen := map[string]bool{}
	for _, info := range registry.GetGlobalRegistry().GetModelsForClient(a.ID) {
		if info == nil {
			continue
		}
		model := info.UpstreamID
		if model == "" {
			model = info.ID
		}
		model = thinking.ParseSuffix(model).ModelName
		lower := strings.ToLower(model)
		if strings.Contains(lower, "image") || strings.Contains(lower, "audio") || strings.Contains(lower, "video") || strings.Contains(lower, "realtime") || strings.Contains(lower, "search") {
			continue
		}
		if len(info.SupportedOutputModalities) > 0 {
			text := false
			for _, v := range info.SupportedOutputModalities {
				text = text || strings.EqualFold(v, "text")
			}
			if !text {
				continue
			}
		}
		if len(c.Models) > 0 && !slices.Contains(c.Models, model) && !slices.Contains(c.Models, info.ID) {
			continue
		}
		if seen[model] {
			continue
		}
		seen[model] = true
		item := StateCredential(a, model)
		item.Route = info.ID
		result = append(result, item)
	}
	return result
}

type stateUnavailableError struct{ body string }

func (e stateUnavailableError) Error() string             { return e.body }
func (stateUnavailableError) StatusCode() int             { return 429 }
func (stateUnavailableError) SkipAuthResult() bool        { return true }
func (stateUnavailableError) RetryOtherAuth() bool        { return false }
func (stateUnavailableError) PreserveErrorResponse() bool { return true }

// ApplyManagedState runs after client headers and account guards. It never changes session IDs.
func ApplyManagedState(ctx context.Context, cfg *config.Config, a *auth.Auth, model string, headers http.Header) (err error) {
	var diagnostic *codexStateDiagnostic
	if ctx != nil {
		diagnostic, _ = ctx.Value(codexStateDiagnosticKey{}).(*codexStateDiagnostic)
	}
	source := "none"
	if headers.Get("X-Codex-Turn-State") != "" {
		source = "configured"
	}
	defer func() {
		if diagnostic != nil && diagnostic.observe != nil {
			value := headers.Get("X-Codex-Turn-State")
			if err != nil {
				value = ""
			}
			diagnostic.observe(source, value)
		}
	}()
	setState := func(value string) {
		for name := range headers {
			if strings.EqualFold(name, "X-Codex-Turn-State") {
				delete(headers, name)
			}
		}
		if value != "" {
			headers.Set("X-Codex-Turn-State", value)
		}
	}
	if IsStateProbe(ctx) {
		setState("")
		source = "none"
		return nil
	}
	if diagnostic != nil {
		switch diagnostic.mode {
		case "none":
			setState("")
			source = "none"
			return nil
		case "custom":
			setState(diagnostic.value)
			source = "custom"
			return nil
		}
	}
	requireManaged := diagnostic != nil && diagnostic.mode == "managed"
	unavailable := func() error {
		source = "unavailable"
		return errors.New("no valid managed State is available for this credential and upstream model")
	}
	if cfg == nil || !cfg.Codex.StateOverride.Enabled || cfg.Codex.ResolvedTurnStatePolicy() == config.CodexTurnStatePolicyStrip {
		if requireManaged {
			return unavailable()
		}
		return nil
	}
	c := StateCredential(a, model)
	inScope := false
	for _, allowed := range ManagedStateModels(cfg, a) {
		if allowed.Model == c.Model {
			inScope = true
			break
		}
	}
	if !inScope {
		if requireManaged {
			return unavailable()
		}
		return nil
	}
	choice := core.CodexStateForRequest(ctx, codexStateRequestKey(c), func() core.CodexStateChoice {
		clientState := headers.Get("X-Codex-Turn-State")
		if requireManaged {
			clientState = ""
		}
		value, policy, version, eligible := codexstate.Default.PickVersion(c, clientState, time.Now())
		return core.CodexStateChoice{Value: value, Policy: policy, Version: version, Eligible: eligible}
	})
	state, policy, eligible := choice.Value, choice.Policy, choice.Eligible
	if !eligible {
		if requireManaged {
			return unavailable()
		}
		return nil
	}
	if state != "" {
		setState(state)
		source = "managed"
		return nil
	}
	if requireManaged {
		return unavailable()
	}
	if policy == "" {
		return nil
	}
	log.WithFields(log.Fields{"auth_id": a.ID, "model": c.Model, "reason": "codex_state_unavailable", "action": policy}).Warn("Codex request has no valid managed state")
	if policy != "error" {
		return nil
	}
	source = "unavailable"
	resolved := cfg.Codex.StateOverride.Resolved()
	body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": resolved.ErrorType, "code": resolved.ErrorCode, "message": resolved.ErrorMessage}})
	return stateUnavailableError{body: string(body)}
}

// ObserveManagedStateCompletion counts only requests that actually sent a managed value.
func ObserveManagedStateCompletion(ctx context.Context, a *auth.Auth, model string, headers http.Header) {
	if !IsStateProbe(ctx) && headers.Get("X-Codex-Turn-State") != "" {
		codexstate.Default.Complete(StateCredential(a, model), headers.Get("X-Codex-Turn-State"))
	}
}
