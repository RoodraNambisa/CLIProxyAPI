package config

import (
	"fmt"
	"strings"
)

const (
	XAIMaxCatalogSources = 8
	XAIMaxModelRoutes    = 64
)

// XAIModelRoute chooses an upstream after credential selection and model aliases.
// Exact model IDs take precedence over wildcard patterns within the same scope.
type XAIModelRoute struct {
	Models   []string `yaml:"models" json:"models"`
	Upstream string   `yaml:"upstream" json:"upstream"`
}

// NormalizeXAIUpstreamReference accepts a built-in node, the account's default,
// or an explicitly configured relay. Remote model catalogs cannot add endpoints.
func NormalizeXAIUpstreamReference(raw string) (string, error) {
	value := strings.TrimSpace(raw)
	mode := strings.ToLower(value)
	if mode == "default" {
		return mode, nil
	}
	if _, ok := XAIBaseURLForMode(mode); ok && mode != "" {
		return mode, nil
	}
	if len(value) > 2048 || value == "" {
		return "", fmt.Errorf("Grok upstream must be a node name or an HTTP(S) base URL")
	}
	return NormalizeXAIBaseURL(value)
}

func NormalizeXAICatalogSources(sources []string) ([]string, error) {
	if len(sources) > XAIMaxCatalogSources {
		return nil, fmt.Errorf("Grok supports at most %d model catalog sources", XAIMaxCatalogSources)
	}
	out := make([]string, 0, len(sources))
	seen := make(map[string]bool)
	for _, source := range sources {
		normalized, err := NormalizeXAIUpstreamReference(source)
		if err != nil {
			return nil, err
		}
		if !seen[normalized] {
			out = append(out, normalized)
			seen[normalized] = true
		}
	}
	return out, nil
}

func NormalizeXAIModelRoutes(routes []XAIModelRoute) ([]XAIModelRoute, error) {
	if len(routes) > XAIMaxModelRoutes {
		return nil, fmt.Errorf("Grok supports at most %d model routes", XAIMaxModelRoutes)
	}
	out := make([]XAIModelRoute, 0, len(routes))
	for i, route := range routes {
		upstream, err := NormalizeXAIUpstreamReference(route.Upstream)
		if err != nil {
			return nil, fmt.Errorf("Grok model route %d: %w", i+1, err)
		}
		if len(route.Models) == 0 || len(route.Models) > 64 {
			return nil, fmt.Errorf("Grok model route %d must contain 1–64 model patterns", i+1)
		}
		models := make([]string, 0, len(route.Models))
		seen := make(map[string]bool)
		for _, raw := range route.Models {
			model := strings.TrimSpace(raw)
			if model == "" || len(model) > 256 || strings.ContainsAny(model, " \t\r\n\x00,?[]\\") {
				return nil, fmt.Errorf("Grok model route %d has an invalid model pattern; only * is a wildcard", i+1)
			}
			if !seen[model] {
				models = append(models, model)
				seen[model] = true
			}
		}
		out = append(out, XAIModelRoute{Models: models, Upstream: upstream})
	}
	return out, nil
}
