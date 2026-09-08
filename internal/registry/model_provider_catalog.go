package registry

import (
	"strings"
	"time"
)

// GetAvailableModelsForProviders returns one catalog snapshot using only the
// allowed providers' metadata. An empty allowlist returns an empty catalog.
func (r *ModelRegistry) GetAvailableModelsForProviders(handlerType string, allowedProviders []string) []map[string]any {
	allowed := make(map[string]struct{}, len(allowedProviders))
	for _, provider := range allowedProviders {
		if provider = strings.ToLower(strings.TrimSpace(provider)); provider != "" {
			allowed[provider] = struct{}{}
		}
	}
	models := make([]map[string]any, 0)
	if len(allowed) == 0 {
		return models
	}

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	now := time.Now()
	for _, registration := range r.models {
		if registration == nil {
			continue
		}
		var selected string
		var selectedCount int
		for provider, count := range registration.Providers {
			if _, ok := allowed[provider]; !ok || registration.InfoByProvider[provider] == nil {
				continue
			}
			if !providerHasCatalogAvailability(registration, provider, count, r.clientProviders, now) {
				continue
			}
			// Match GetModelProviders ordering without falling back to global metadata.
			if selected == "" || count > selectedCount || (count == selectedCount && provider < selected) {
				selected, selectedCount = provider, count
			}
		}
		if selected != "" {
			if model := r.convertModelToMap(registration.InfoByProvider[selected], handlerType); model != nil {
				models = append(models, model)
			}
		}
	}
	return models
}
