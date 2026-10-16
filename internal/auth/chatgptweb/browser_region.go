package chatgptweb

import (
	"context"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxyregion"
)

type BrowserRegionSnapshot struct {
	Timezone          string `json:"timezone"`
	OffsetMinutes     int    `json:"timezone_offset_minutes"`
	Language          string `json:"language"`
	TimezoneSource    string `json:"timezone_source"`
	LanguageSource    string `json:"language_source"`
	ObservationSource string `json:"observation_source,omitempty"`
	Country           string `json:"country,omitempty"`
	Region            string `json:"region,omitempty"`
	CacheState        string `json:"cache_state"`
	CacheAgeSeconds   int64  `json:"cache_age_seconds"`
	RefreshState      string `json:"refresh_state"`
	Reason            string `json:"reason,omitempty"`
	RetryAfterSeconds int64  `json:"retry_after_seconds,omitempty"`
	OverrideLanguage  bool   `json:"-"`
}

// BrowserRegionResolver applies Web policy to shared proxy metadata. It owns no
// provider-specific query queue or cache.
type BrowserRegionResolver struct {
	mu     sync.RWMutex
	policy config.ChatGPTWebConfig
	shared *proxyregion.Service
	owns   bool
}

func NewBrowserRegionResolver(parent context.Context) *BrowserRegionResolver {
	return NewSharedBrowserRegionResolver(parent, nil)
}
func NewSharedBrowserRegionResolver(parent context.Context, shared *proxyregion.Service) *BrowserRegionResolver {
	r := &BrowserRegionResolver{shared: shared}
	if shared == nil {
		r.shared = proxyregion.New(parent, config.ProxyIPConfig{})
		r.owns = true
	}
	return r
}
func (r *BrowserRegionResolver) Close() {
	if r != nil && r.owns {
		r.shared.Close()
	}
}
func (r *BrowserRegionResolver) UpdateConfig(cfg config.ChatGPTWebConfig) {
	if cfg.Validate() != nil {
		return
	}
	r.mu.Lock()
	r.policy = cfg
	r.mu.Unlock()
	if r.owns {
		_ = r.shared.UpdateConfig((&config.Config{ChatGPTWeb: cfg}).EffectiveProxyIP())
	}
}
func (r *BrowserRegionResolver) Resolve(proxy string, p Persona) BrowserRegionSnapshot {
	return r.Apply(proxyregion.Route{ProxyURL: proxy}, p, nil, true)
}
func (r *BrowserRegionResolver) Preview(proxy string, p Persona) BrowserRegionSnapshot {
	return r.Apply(proxyregion.Route{ProxyURL: proxy}, p, nil, false)
}
func (r *BrowserRegionResolver) Refresh(proxy string) string {
	return r.shared.Refresh(proxyregion.Route{ProxyURL: proxy})
}
func (r *BrowserRegionResolver) Shared() *proxyregion.Service { return r.shared }
func (r *BrowserRegionResolver) Apply(route proxyregion.Route, p Persona, override *config.ChatGPTWebConfig, enqueue bool) BrowserRegionSnapshot {
	return r.ApplyContext(context.Background(), route, p, override, enqueue)
}
func (r *BrowserRegionResolver) ResolveContext(ctx context.Context, proxy string, p Persona) BrowserRegionSnapshot {
	if policy, ok := ctx.Value(regionPolicyKey{}).(regionPolicy); ok {
		route := policy.Route
		if route.ProxyURL != proxy {
			route.BindingID = ""
		}
		route.ProxyURL = proxy
		return r.ApplyContext(ctx, route, p, &policy.Config, true)
	}
	return r.ApplyContext(ctx, proxyregion.Route{ProxyURL: proxy}, p, nil, true)
}
func (r *BrowserRegionResolver) ApplyContext(ctx context.Context, route proxyregion.Route, p Persona, override *config.ChatGPTWebConfig, enqueue bool) BrowserRegionSnapshot {
	r.mu.RLock()
	cfg := r.policy
	r.mu.RUnlock()
	if override != nil {
		cfg = *override
	}
	settings := cfg.BrowserRegion.Resolved()
	zone := cfg.ResolvedTimezone(time.Now())
	result := BrowserRegionSnapshot{Timezone: zone.Timezone, OffsetMinutes: zone.OffsetMinutes, Language: p.Language, TimezoneSource: "configured", LanguageSource: "credential", CacheState: "missing", RefreshState: "idle"}
	if settings.Language != "" {
		result.Language = settings.Language
		result.LanguageSource = "configured"
		result.OverrideLanguage = true
	}
	value := r.shared.Preview(route)
	if enqueue && (settings.AutoTimezone || settings.AutoLanguage) {
		value = r.shared.ResolveForRequest(ctx, route)
	}
	result.CacheState = value.CacheState
	result.CacheAgeSeconds = value.CacheAgeSeconds
	result.RefreshState = value.RefreshState
	result.Reason = value.Reason
	result.RetryAfterSeconds = value.RetryAfterSeconds
	result.ObservationSource = value.Source
	result.Country = value.Country
	result.Region = value.Region
	if value.Timezone != "" {
		if settings.AutoTimezone {
			result.Timezone = value.Timezone
			result.OffsetMinutes = (config.ChatGPTWebConfig{Timezone: value.Timezone}).ResolvedTimezone(time.Now()).OffsetMinutes
			result.TimezoneSource = value.CacheState + "_cache"
		}
		if settings.AutoLanguage && value.SuggestedLanguage != "" {
			result.Language = value.SuggestedLanguage
			result.LanguageSource = value.CacheState + "_cache"
			result.OverrideLanguage = true
		}
	}
	return result
}
