package management

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type proxyRegionFieldView struct {
	Value       string `json:"value"`
	Source      string `json:"source"`
	State       string `json:"state"`
	FollowProxy bool   `json:"follow_proxy"`
	ProxyValue  string `json:"proxy_value,omitempty"`
	ProxySource string `json:"proxy_source,omitempty"`
}

type proxyRegionApplicationView struct {
	Kind         string               `json:"kind"`
	EvaluatedAt  time.Time            `json:"evaluated_at"`
	AuthDisabled bool                 `json:"auth_disabled"`
	BindingID    string               `json:"binding_id,omitempty"`
	RouteSource  string               `json:"route_source,omitempty"`
	Scope        string               `json:"scope,omitempty"`
	Timezone     proxyRegionFieldView `json:"timezone"`
	Language     proxyRegionFieldView `json:"language"`
	CacheState   string               `json:"cache_state,omitempty"`
	RefreshState string               `json:"refresh_state,omitempty"`
	Reason       string               `json:"reason,omitempty"`
}

func regionSettingsSource(raw any, key string) string {
	b, _ := json.Marshal(raw)
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(b, &fields)
	if string(fields["mode"]) == `"inherit"` {
		return "provider_settings"
	}
	if _, ok := fields[key]; ok {
		return "credential_settings"
	}
	return "provider_settings"
}

func regionFieldState(follow bool, source string) string {
	if !follow {
		return "not_following"
	}
	switch source {
	case "proxy_pool", "proxy_node", "credential_proxy", "global_proxy", "fresh_cache":
		return "ready"
	case "stale_cache":
		return "stale"
	default:
		return "fallback"
	}
}

// proxyRegionApplication resolves saved configuration only. It never acquires a
// binding, enqueues a lookup, or claims that an upstream request has been sent.
func (h *Handler) proxyRegionApplication(ctx context.Context, auth *coreauth.Auth, expectedBinding string) proxyRegionApplicationView {
	view := proxyRegionApplicationView{Kind: "configuration_preview", EvaluatedAt: time.Now(),
		Timezone: proxyRegionFieldView{State: "unavailable"}, Language: proxyRegionFieldView{State: "unavailable"}}
	cfg := h.currentConfig()
	manager, authManager := h.proxyPoolRuntimeManager(), h.coreAuthRuntimeManager()
	if cfg == nil || manager == nil || authManager == nil || auth == nil {
		view.Reason = "unavailable"
		return view
	}
	view.AuthDisabled = auth.Disabled
	resolved, err := authManager.PreviewProxyAuth(auth)
	if expectedBinding != "" && (err != nil || resolved.Pending || resolved.BindingID != expectedBinding) {
		view.Reason = "route_changed"
		return view
	}
	view.BindingID, view.RouteSource = resolved.BindingID, resolved.Source
	routed := auth.Clone()
	routed.ProxyURL, routed.RuntimeProxyURL = resolved.URL, ""
	target := proxyregion.ChatGPTScope
	if auth.Provider == "codex" {
		target = helps.ResolveCodexUpstream(routed, cfg).BaseURL
	}
	route := proxyregion.Route{ProxyURL: helps.ResolvedRequestProxy(cfg, routed, target), BindingID: resolved.BindingID, OwnerID: auth.ID, Scope: proxyregion.ScopeForTarget(target)}
	if err != nil || resolved.Pending {
		route.ProxyURL, route.BindingID = "direct", ""
		view.Reason = "route_unresolved"
	}
	view.Scope = route.Scope
	service := manager.IPRegions()
	cached := service.Preview(route)
	manual, _ := service.Manual(route)
	tzSource, langSource := service.ManualSources(route)
	view.CacheState, view.RefreshState = cached.CacheState, cached.RefreshState
	if view.Reason == "" {
		view.Reason = cached.Reason
	}
	view.Timezone.ProxyValue, view.Language.ProxyValue = cached.Timezone, cached.SuggestedLanguage
	if cached.Timezone != "" {
		view.Timezone.ProxySource = cached.CacheState + "_cache"
	}
	if cached.SuggestedLanguage != "" {
		view.Language.ProxySource = cached.CacheState + "_cache"
	}
	if manual.Timezone != "" {
		view.Timezone.ProxyValue, view.Timezone.ProxySource = manual.Timezone, tzSource
	}
	if manual.Language != "" {
		view.Language.ProxyValue, view.Language.ProxySource = manual.Language, langSource
	}
	switch auth.Provider {
	case "chatgpt-web":
		policy, errPolicy := config.ApplyWebRegionOverride(cfg.ChatGPTWeb, auth.Metadata[config.WebRegionMetadataKey])
		credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
		if errPolicy != nil || errCredential != nil {
			view.Reason = "credential_invalid"
			return view
		}
		chatgptwebauth.ResolveCredentialPersona(credential, auth.ID)
		resolver := chatgptwebauth.NewSharedBrowserRegionResolver(ctx, service)
		result := resolver.Apply(route, credential.Persona, &policy, false)
		view.Timezone.Value, view.Timezone.Source = result.Timezone, result.TimezoneSource
		view.Language.Value, view.Language.Source = result.Language, result.LanguageSource
		view.Timezone.FollowProxy, view.Language.FollowProxy = policy.BrowserRegion.AutoTimezone, policy.BrowserRegion.AutoLanguage
		view.Timezone.State = regionFieldState(view.Timezone.FollowProxy, result.TimezoneSource)
		view.Language.State = regionFieldState(view.Language.FollowProxy, result.LanguageSource)
		if view.Timezone.Source == "configured" {
			view.Timezone.Source = regionSettingsSource(auth.Metadata[config.WebRegionMetadataKey], "timezone")
		}
		if view.Language.Source == "configured" {
			view.Language.Source = regionSettingsSource(auth.Metadata[config.WebRegionMetadataKey], "language")
		}
	case "codex":
		result := helps.SelectCodexTimeContext(ctx, service, auth, route, cfg.Codex.TimeContext, false)
		view.Timezone.Value, view.Timezone.Source = result.Timezone, result.Source
		view.Timezone.FollowProxy = result.Mode == "proxy" && result.Source != "disabled"
		view.Language.State = "not_applicable"
		switch result.Source {
		case "disabled":
			view.Timezone.State = "disabled"
		case "manual":
			view.Timezone.State = "fixed"
			view.Timezone.Source = regionSettingsSource(auth.Metadata[config.CodexTimeContextMetadataKey], "timezone")
		case "preserved":
			view.Timezone.State = "preserved"
		case "fallback":
			view.Timezone.State = "fallback"
		case "proxy":
			view.Timezone.Source = result.Region.CacheState + "_cache"
			view.Timezone.State = regionFieldState(true, view.Timezone.Source)
		default:
			view.Timezone.State = regionFieldState(true, result.Source)
		}
		if result.Reason == "invalid_policy" {
			view.Timezone.State, view.Reason = "unavailable", result.Reason
		}
	default:
		view.Timezone.State, view.Language.State = "not_applicable", "not_applicable"
	}
	needsLookup := (view.Timezone.FollowProxy && manual.Timezone == "") || (view.Language.FollowProxy && manual.Language == "")
	if !needsLookup && view.Reason != "route_unresolved" && view.Reason != "invalid_policy" {
		view.Reason = ""
	}
	return view
}

func proxyRegionSearchMatch(search string, values ...string) bool {
	search = strings.ToLower(strings.TrimSpace(search))
	if search == "" {
		return true
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), search) {
			return true
		}
	}
	return false
}
