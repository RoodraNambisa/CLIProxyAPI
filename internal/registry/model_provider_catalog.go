package registry

import (
	"strings"
	"time"
)

// ModelCatalogSnapshot keeps formatted models and capability lookup data from
// the same registry state. All fields are owned by the caller.
type ModelCatalogSnapshot struct {
	Models    []map[string]any
	Metadata  map[string]*ModelInfo
	Providers map[string][]string
}

// GetOpenAIModelCatalog captures the unrestricted catalog and capability
// metadata together, retaining the existing global metadata selection rules.
func (r *ModelRegistry) GetOpenAIModelCatalog() ModelCatalogSnapshot {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	now := time.Now()
	models, _ := r.buildAvailableModelsLocked("openai", now)
	catalog := ModelCatalogSnapshot{
		Models:    models,
		Metadata:  make(map[string]*ModelInfo, len(models)),
		Providers: make(map[string][]string, len(models)),
	}
	for _, model := range models {
		id, _ := model["id"].(string)
		if registration := r.models[id]; registration != nil {
			catalog.Metadata[id] = cloneModelInfo(registration.Info)
			catalog.Providers[id] = r.modelProvidersLocked(id, now)
		}
	}
	return catalog
}

// GetAvailableModelsForProviders returns one catalog snapshot using only the
// allowed providers' metadata. An empty allowlist returns an empty catalog.
func (r *ModelRegistry) GetAvailableModelsForProviders(handlerType string, allowedProviders []string) []map[string]any {
	return r.GetModelCatalogForProviders(handlerType, allowedProviders).Models
}

// GetModelCatalogForProviders also snapshots the metadata needed to derive
// client capabilities without performing later unscoped registry lookups.
func (r *ModelRegistry) GetModelCatalogForProviders(handlerType string, allowedProviders []string) ModelCatalogSnapshot {
	allowed := make(map[string]struct{}, len(allowedProviders))
	for _, provider := range allowedProviders {
		if provider = strings.ToLower(strings.TrimSpace(provider)); provider != "" {
			allowed[provider] = struct{}{}
		}
	}
	catalog := ModelCatalogSnapshot{
		Models:    make([]map[string]any, 0),
		Metadata:  make(map[string]*ModelInfo),
		Providers: make(map[string][]string),
	}
	if len(allowed) == 0 {
		return catalog
	}

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	now := time.Now()
	for id, registration := range r.models {
		if registration == nil {
			continue
		}
		var selected string
		var selectedCount int
		var providers []string
		for provider, count := range registration.Providers {
			if _, ok := allowed[provider]; !ok || registration.InfoByProvider[provider] == nil {
				continue
			}
			if !providerHasCatalogAvailability(registration, provider, count, r.clientProviders, now) {
				continue
			}
			providers = append(providers, provider)
			// Match GetModelProviders ordering without falling back to global metadata.
			if selected == "" || count > selectedCount || (count == selectedCount && provider < selected) {
				selected, selectedCount = provider, count
			}
		}
		if selected != "" {
			if model := r.convertModelToMap(registration.InfoByProvider[selected], handlerType); model != nil {
				catalog.Models = append(catalog.Models, model)
				catalog.Metadata[id] = cloneModelInfo(registration.InfoByProvider[selected])
				catalog.Providers[id] = providers
			}
		}
	}
	return catalog
}
