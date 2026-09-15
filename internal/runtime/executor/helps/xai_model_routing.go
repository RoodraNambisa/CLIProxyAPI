package helps

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/thinking"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

const (
	XAICatalogSourcesKey = "xai_model_catalog_sources"
	XAIModelRoutesKey    = "xai_model_routes"
)

func xaiDecodeRoutingField(auth *coreauth.Auth, key string, target any) error {
	if auth == nil || auth.Metadata[key] == nil {
		return nil
	}
	raw, err := json.Marshal(auth.Metadata[key])
	if err != nil || len(raw) > 1024*1024 || json.Unmarshal(raw, target) != nil {
		return fmt.Errorf("invalid %s in Grok credential", key)
	}
	return nil
}

func XAICredentialModelRoutes(auth *coreauth.Auth) ([]config.XAIModelRoute, error) {
	var routes []config.XAIModelRoute
	if err := xaiDecodeRoutingField(auth, XAIModelRoutesKey, &routes); err != nil {
		return nil, err
	}
	return config.NormalizeXAIModelRoutes(routes)
}

func xaiReferenceURL(reference string, auth *coreauth.Auth, cfg *config.Config) string {
	if reference == "default" {
		return ResolveXAIUpstream(auth, cfg).BaseURL
	}
	if baseURL, ok := config.XAIBaseURLForMode(reference); ok {
		return baseURL
	}
	return reference
}

// XAICatalogEndpoints resolves only administrator-configured sources. Empty
// account settings inherit globals; empty globals query the account's default.
func XAICatalogEndpoints(auth *coreauth.Auth, cfg *config.Config) ([]string, error) {
	var sources []string
	if err := xaiDecodeRoutingField(auth, XAICatalogSourcesKey, &sources); err != nil {
		return nil, err
	}
	if len(sources) == 0 && cfg != nil {
		sources = cfg.XAI.ModelCatalogSources
	}
	sources, err := config.NormalizeXAICatalogSources(sources)
	if err != nil {
		return nil, err
	}
	if len(sources) == 0 {
		sources = []string{"default"}
	}
	endpoints := make([]string, 0, len(sources))
	seen := make(map[string]bool)
	for _, source := range sources {
		endpoint := strings.TrimRight(xaiReferenceURL(source, auth, cfg), "/") + "/models"
		if !seen[endpoint] {
			endpoints = append(endpoints, endpoint)
			seen[endpoint] = true
		}
	}
	return endpoints, nil
}

// ResolveXAIModelUpstream applies account rules before global rules, then the
// existing default URL. Catalog membership is never treated as a routing rule.
func ResolveXAIModelUpstream(auth *coreauth.Auth, cfg *config.Config, model string) (XAIUpstream, error) {
	model = thinking.ParseSuffix(strings.TrimSpace(model)).ModelName
	accountRules, err := XAICredentialModelRoutes(auth)
	if err != nil {
		return XAIUpstream{}, err
	}
	rules := [][]config.XAIModelRoute{accountRules, nil}
	if cfg != nil {
		rules[1] = cfg.XAI.ModelRoutes
	}
	for scope, entries := range rules {
		for _, wildcard := range []bool{false, true} {
			for _, entry := range entries {
				for _, pattern := range entry.Models {
					if model == "" || strings.Contains(pattern, "*") != wildcard || !matchModelPattern(pattern, model) {
						continue
					}
					baseURL := xaiReferenceURL(entry.Upstream, auth, cfg)
					source := "global-rule"
					if scope == 0 {
						source = "credential-rule"
					}
					return XAIUpstream{BaseURL: baseURL, Mode: xaiBaseURLMode(baseURL), Source: source}, nil
				}
			}
		}
	}
	return ResolveXAIUpstream(auth, cfg), nil
}

// XAIModelAPIOnlyBaseURL preserves the existing compact/WS transport fallback.
func XAIModelAPIOnlyBaseURL(auth *coreauth.Auth, cfg *config.Config, model string) (string, error) {
	upstream, err := ResolveXAIModelUpstream(auth, cfg, model)
	if err != nil {
		return "", err
	}
	return xaiAPIOnlyURL(upstream.BaseURL), nil
}
