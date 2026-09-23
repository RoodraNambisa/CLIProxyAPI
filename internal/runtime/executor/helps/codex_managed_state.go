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
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
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
	c := codexstate.Credential{ID: a.ID, Name: a.FileName, Owner: owner, Instance: a.RuntimeInstanceID(), Model: thinking.ParseSuffix(model).ModelName, Plan: config.NormalizeCodexStatePlanType(plan), ShortID: a.Index, Priority: StateCredentialPriority(a)}
	if c.Model != "" {
		for _, info := range registry.GetGlobalRegistry().GetModelsForClient(a.ID) {
			if info == nil {
				continue
			}
			upstream := info.UpstreamID
			if upstream == "" {
				upstream = info.ID
			}
			if thinking.ParseSuffix(upstream).ModelName == c.Model {
				c.Aliases = append(c.Aliases, thinking.ParseSuffix(info.ID).ModelName)
			}
		}
	}
	return c
}

// StateCredentialAvailable validates a live Codex OAuth credential independently of automatic scope.
func StateCredentialAvailable(a *auth.Auth) bool {
	return a != nil && !a.Disabled && a.Status != auth.StatusDisabled && !a.RuntimeInstanceRetired() && a.ExecutionProvider() == "codex" && a.Attributes["api_key"] == ""
}

// ManagedStateCredentialEligible checks credential scope without requiring a registered model.
func ManagedStateCredentialEligible(cfg *config.Config, a *auth.Auth) bool {
	if cfg == nil || !cfg.Codex.StateOverride.Enabled || !StateCredentialAvailable(a) {
		return false
	}
	c := cfg.Codex.StateOverride
	if c.Rules != nil {
		return c.MatchesCredential(StateCredential(a, "").Scope())
	}
	if slices.Contains(c.ExcludedCredentials, a.ID) || slices.Contains(c.ExcludedCredentials, a.Index) || slices.Contains(c.ExcludedCredentials, a.FileName) {
		return false
	}
	priority := StateCredentialPriority(a)
	included := slices.Contains(c.IncludedCredentials, a.ID) || slices.Contains(c.IncludedCredentials, a.Index) || slices.Contains(c.IncludedCredentials, a.FileName)
	// Explicit credentials extend the priority scope. Empty selectors preserve all-credential scope.
	if (len(c.Priorities) > 0 || len(c.IncludedCredentials) > 0) && !included && !slices.Contains(c.Priorities, priority) {
		return false
	}
	return true
}

// ManagedStateModels uses the credential's registered catalog, including exclusions.
func ManagedStateModels(cfg *config.Config, a *auth.Auth) []codexstate.Credential {
	if !ManagedStateCredentialEligible(cfg, a) {
		return nil
	}
	c := cfg.Codex.StateOverride
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
		if !StateTextModel(model) {
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
		if c.Rules == nil && len(c.Models) > 0 && !slices.Contains(c.Models, model) && !slices.Contains(c.Models, info.ID) {
			continue
		}
		if seen[model] {
			continue
		}
		seen[model] = true
		item := StateCredential(a, model)
		item.Route = info.ID
		if c.Rules != nil {
			if _, _, ok := c.PolicyFor(item.Scope()); !ok {
				continue
			}
		}
		result = append(result, item)
	}
	return result
}

// StateTextModel excludes non-text protocols from both managed and manual acquisition.
func StateTextModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" {
		return false
	}
	for _, kind := range []string{"image", "audio", "video", "realtime", "search"} {
		if strings.Contains(model, kind) {
			return false
		}
	}
	return true
}

// StateCredentialPriority shares the effective default-zero priority with option discovery.
func StateCredentialPriority(a *auth.Auth) int {
	if a == nil {
		return 0
	}
	if p := a.Attributes["priority"]; p != "" {
		value, _ := strconv.Atoi(p)
		return value
	}
	switch p := a.Metadata["priority"].(type) {
	case int:
		return p
	case float64:
		return int(p)
	case string:
		value, _ := strconv.Atoi(p)
		return value
	}
	return 0
}

type stateUnavailableError struct {
	body   string
	status int
}

func (e stateUnavailableError) Error() string { return e.body }
func (e stateUnavailableError) StatusCode() int {
	if e.status != 0 {
		return e.status
	}
	return http.StatusTooManyRequests
}
func (stateUnavailableError) SkipAuthResult() bool        { return true }
func (stateUnavailableError) RetryOtherAuth() bool        { return true }
func (stateUnavailableError) PreserveErrorResponse() bool { return true }

// ApplyManagedState runs after client headers and account guards. It never changes session IDs.
func ApplyManagedState(ctx context.Context, cfg *config.Config, a *auth.Auth, model string, headers http.Header, targetURLs ...string) (err error) {
	if guard := core.ResponseGuardConfigFromContext(ctx); cfg != nil && guard != nil {
		copy := *cfg
		copy.Codex.ResponseGuard = *guard
		cfg = &copy
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var diagnostic *codexStateDiagnostic
	if ctx != nil {
		diagnostic, _ = ctx.Value(codexStateDiagnosticKey{}).(*codexStateDiagnostic)
	}
	targetRespectsState := sdkaccess.CredentialTargetRespectsStatePolicy(ctx)
	if diagnostic == nil && sdkaccess.CredentialTargetAuthID(ctx) != "" && !targetRespectsState {
		diagnostic = &codexStateDiagnostic{mode: "auto"}
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
	targetURL := "https://chatgpt.com/backend-api/codex/responses"
	if a != nil && a.Attributes["base_url"] != "" {
		targetURL = strings.TrimRight(a.Attributes["base_url"], "/") + "/responses"
	}
	if len(targetURLs) > 0 {
		targetURL = targetURLs[0]
	}
	if IsStateProbe(ctx) {
		source = "none"
		return applyStateProbeHeaders(ctx, headers, targetURL)
	}
	if handled, errCookie := applyCookieDiagnostic(ctx, cfg, a, model, headers, targetURL); handled {
		source = "cookie"
		return errCookie
	}
	if diagnostic != nil {
		switch diagnostic.mode {
		case "auto":
			setState("")
			source = "none"
		case "none":
			setState("")
			source = "none"
		case "custom":
			setState(diagnostic.value)
			source = "custom"
			return nil
		case "acquired":
			source = "unavailable"
			if cfg == nil || !StateCredentialAvailable(a) {
				return errors.New("no valid manually acquired State is available for this credential and upstream model")
			}
			c := StateCredential(a, model)
			policy, _ := codexstate.DiagnosticPolicy(cfg.Codex.ManagedStateConfig(), c)
			choice := core.RefreshCodexStateForRequest(ctx, "diagnostic:"+codexStateRequestKey(c), func(previous core.CodexStateChoice) bool {
				return codexstate.Diagnostic.StateSelectionValid(c, previous.Value, previous.Version, previous.ExpiresAt, time.Now(), policy)
			}, func() core.CodexStateChoice {
				value, missing, version, eligible := codexstate.Diagnostic.PickVersionForPolicy(c, "", time.Now(), policy)
				return core.CodexStateChoice{Value: value, Policy: missing, Version: version, Eligible: eligible, ExpiresAt: codexstate.Diagnostic.StateExpiry(c, version), Observation: stateObservationPolicy(policy)}
			})
			if !choice.Eligible || choice.Value == "" {
				return errors.New("no valid manually acquired State is available for this credential and upstream model")
			}
			setState(choice.Value)
			source = "acquired"
			return nil
		}
	}
	requireManaged := diagnostic != nil && diagnostic.mode == "managed"
	unavailable := func() error {
		source = "unavailable"
		return errors.New("no valid managed State is available for this credential and upstream model")
	}
	if cfg == nil || !cfg.Codex.StateOverride.Enabled {
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
	if !inScope && requireManaged && ManagedStateCredentialEligible(cfg, a) && ManagedStatePairAllowed(cfg, c) && codexstate.Default.HasManual(c) {
		inScope = true
	}
	// Fixed tests may request unregistered models. Opt-in State rules still
	// apply to the matching credential/model without registering it for routing.
	if !inScope && targetRespectsState && ManagedStateCredentialEligible(cfg, a) && ManagedStatePairAllowed(cfg, c) && StateTextModel(c.Model) {
		inScope = true
	}
	if !inScope {
		if requireManaged {
			return unavailable()
		}
		return nil
	}
	resolved, _, _ := cfg.Codex.ManagedStateConfig().PolicyFor(c.Scope())
	if cfg.Codex.StateOverride.Rules == nil {
		// Legacy scope was already checked against the exact registered alias.
		// Do not recheck its spelling after resolving the upstream model.
		resolved = cfg.Codex.ManagedStateConfig().ApplyResponseAcceptance(c.Scope(), cfg.Codex.StateOverride.ForCredential(c.Plan, c.Model))
	}
	if diagnostic != nil && diagnostic.mode == "none" && !resolved.CookieOnly() {
		return nil
	}
	if cfg.Codex.ResolvedTurnStatePolicy() == config.CodexTurnStatePolicyStrip && !resolved.CookieOnly() {
		if requireManaged {
			return unavailable()
		}
		return nil
	}
	if resolved.CookieOnly() {
		if cfg.Codex.AutoCookie {
			return errors.New("Cookie-only management conflicts with codex.auto-cookie")
		}
		setState("")
		if d, ok := ctx.Value(cookieDiagnosticKey{}).(cookieDiagnostic); ok && d.mode == "none" {
			source = "none"
			return nil
		}
		codexstate.StripManagedCookies(headers)
		choice := core.RefreshCodexStateForRequest(ctx, codexStateRequestKey(c), func(previous core.CodexStateChoice) bool {
			return previous.Cookie != nil && previous.Cookie.URL == targetURL && codexstate.Default.CookieSelectionValid(c, *previous.Cookie, time.Now(), resolved)
		}, func() core.CodexStateChoice {
			selection, missing, eligible := codexstate.Default.PickCookie(c, targetURL, time.Now(), resolved)
			return core.CodexStateChoice{Policy: missing, Eligible: eligible, Version: selection.Version, Cookie: &selection, Observation: stateObservationPolicy(resolved)}
		})
		if choice.Cookie != nil && choice.Cookie.Header != "" {
			existing := headers.Get("Cookie")
			if existing != "" {
				existing += "; "
			}
			headers.Set("Cookie", existing+choice.Cookie.Header)
			source = "cookie"
			observeCookieSelection(ctx, "managed", *choice.Cookie)
			return nil
		}
		source = "unavailable"
		if requireManaged {
			return errors.New("no valid managed Cookie is available for this credential")
		}
		if diagnostic != nil && diagnostic.mode == "auto" {
			return nil
		}
		if choice.Policy == "error" || choice.Policy == "hide" {
			body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": resolved.ErrorType, "code": resolved.ErrorCode, "message": resolved.ErrorMessage}})
			if choice.Policy == "hide" && targetRespectsState {
				body = []byte(`{"error":{"type":"service_unavailable_error","code":"auth_unavailable","message":"Selected credential has no valid Cookie"}}`)
				return stateUnavailableError{body: string(body), status: 503}
			}
			return stateUnavailableError{body: string(body)}
		}
		return nil
	}
	// A retry may reuse its mutable header map. Remove a retired managed value
	// before the missing-only mode considers it an explicit client value.
	if previous, ok := core.CodexStateChoiceForRequest(ctx, codexStateRequestKey(c)); ok && previous.Version != 0 && previous.Value != "" && headers.Get("X-Codex-Turn-State") == previous.Value && !codexstate.Default.StateSelectionValid(c, previous.Value, previous.Version, previous.ExpiresAt, time.Now(), resolved) {
		setState("")
	}
	choice := core.RefreshCodexStateForRequest(ctx, codexStateRequestKey(c), func(previous core.CodexStateChoice) bool {
		return codexstate.Default.StateSelectionValid(c, previous.Value, previous.Version, previous.ExpiresAt, time.Now(), resolved)
	}, func() core.CodexStateChoice {
		clientState := headers.Get("X-Codex-Turn-State")
		if requireManaged {
			clientState = ""
		}
		value, policy, version, eligible := codexstate.Default.PickVersionForPolicy(c, clientState, time.Now(), resolved)
		return core.CodexStateChoice{Value: value, Policy: policy, Version: version, Eligible: eligible, ExpiresAt: codexstate.Default.StateExpiry(c, version), Observation: stateObservationPolicy(resolved)}
	})
	state, policy, eligible := choice.Value, choice.Policy, choice.Eligible
	if !eligible && targetRespectsState {
		// Pausing acquisition must not bypass an explicitly requested admission check.
		policy, eligible = resolved.MissingPolicy, true
	}
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
	if diagnostic != nil && diagnostic.mode == "auto" {
		return nil
	}
	log.WithFields(log.Fields{"auth_id": a.ID, "model": c.Model, "reason": "codex_state_unavailable", "action": policy}).Warn("Codex request has no valid managed state")
	if policy != "error" && policy != "hide" {
		return nil
	}
	source = "unavailable"
	if policy == "hide" && targetRespectsState {
		body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "service_unavailable_error", "code": "auth_unavailable", "message": "Selected credential has no valid State for model " + c.Model}})
		return stateUnavailableError{body: string(body), status: http.StatusServiceUnavailable}
	}
	body, _ := json.Marshal(map[string]any{"error": map[string]string{"type": resolved.ErrorType, "code": resolved.ErrorCode, "message": resolved.ErrorMessage}})
	return stateUnavailableError{body: string(body)}
}

// ObserveManagedStateCompletion counts only requests that actually sent a managed value.
func ObserveManagedStateCompletion(ctx context.Context, a *auth.Auth, model string, headers http.Header) {
	if !IsStateProbe(ctx) && headers.Get("X-Codex-Turn-State") != "" {
		manager := codexstate.Default
		if ctx != nil {
			if diagnostic, _ := ctx.Value(codexStateDiagnosticKey{}).(*codexStateDiagnostic); diagnostic != nil && diagnostic.mode == "acquired" {
				manager = codexstate.Diagnostic
			}
		}
		manager.Complete(StateCredential(a, model), headers.Get("X-Codex-Turn-State"))
	}
}

// ManagedStatePairAllowed checks the full scope for explicit diagnostic pairs.
func ManagedStatePairAllowed(cfg *config.Config, c codexstate.Credential) bool {
	_, _, ok := cfg.Codex.ManagedStateConfig().PolicyFor(c.Scope())
	return ok
}
func ResolveStateModel(authID, requested string) string {
	model := thinking.ParseSuffix(strings.TrimSpace(requested)).ModelName
	for _, info := range registry.GetGlobalRegistry().GetModelsForClient(authID) {
		if info != nil && thinking.ParseSuffix(info.ID).ModelName == model {
			if info.UpstreamID != "" {
				return thinking.ParseSuffix(info.UpstreamID).ModelName
			}
			break
		}
	}
	return model
}

func stateObservationPolicy(policy config.CodexStateOverrideConfig) *core.CodexStateObservationPolicy {
	return &core.CodexStateObservationPolicy{AcceptedReturnedModels: slices.Clone(policy.AcceptedReturnedModels), ReturnedLengthMode: policy.ReturnedLengthMode, MatchModel: policy.MatchModel == nil || *policy.MatchModel, ModelMismatch: policy.InvalidateOnModelMismatch, LengthMismatch: policy.InvalidateOnStateLengthMismatch, Lengths: slices.Clone(policy.Lengths), MissingReturnedState: policy.MissingReturnedState, Strategy: policy.Strategy, CookieMaxAgeSeconds: policy.CookieMaxAgeSeconds}
}
