package registry

import (
	"sort"
	"strings"
	"time"
)

// GetModelCatalogForClients intersects a credential allowlist with an optional
// provider allowlist. All metadata is taken from eligible credentials, so a
// shared model ID cannot import capabilities from a forbidden priority tier.
func (r *ModelRegistry) GetModelCatalogForClients(handlerType string, clientIDs, providerIDs []string) ModelCatalogSnapshot {
	catalog := ModelCatalogSnapshot{Models: []map[string]any{}, Metadata: map[string]*ModelInfo{}, Providers: map[string][]string{}}
	allowedProviders := make(map[string]bool, len(providerIDs))
	for _, provider := range providerIDs {
		allowedProviders[strings.ToLower(strings.TrimSpace(provider))] = true
	}
	clients := append([]string(nil), clientIDs...)
	sort.Strings(clients)
	seen := make(map[string]bool, len(clients))
	counts := make(map[string]map[string]int)
	owners := make(map[string]string)
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	availability, now := r.availabilityLocked(), time.Now()
	for _, client := range clients {
		if seen[client] {
			continue
		}
		seen[client] = true
		provider := r.clientProviders[client]
		if provider == "" || len(allowedProviders) > 0 && !allowedProviders[provider] {
			continue
		}
		for id, info := range r.clientModelInfos[client] {
			if info == nil || !availability.Available(client, id, now) {
				continue
			}
			registration := r.models[id]
			if registration == nil {
				continue
			}
			if reason, suspended := registration.SuspendedClients[client]; suspended && !modelSuspensionIsQuotaOnly(reason) && !modelSuspensionIsNotFound(reason) {
				continue
			}
			if counts[id] == nil {
				counts[id] = make(map[string]int)
			}
			counts[id][provider]++
			if catalog.Metadata[id] == nil || client == registration.infoClientID {
				catalog.Metadata[id], owners[id] = cloneModelInfo(info), client
			} else if owners[id] != registration.infoClientID {
				selected := r.clientProviders[owners[id]]
				if counts[id][provider] > counts[id][selected] || counts[id][provider] == counts[id][selected] && provider < selected {
					catalog.Metadata[id], owners[id] = cloneModelInfo(info), client
				}
			}
		}
	}
	modelIDs := make([]string, 0, len(catalog.Metadata))
	for id := range catalog.Metadata {
		modelIDs = append(modelIDs, id)
	}
	sort.Strings(modelIDs)
	for _, id := range modelIDs {
		if model := r.convertModelToMap(catalog.Metadata[id], handlerType); model != nil {
			catalog.Models = append(catalog.Models, model)
			for provider := range counts[id] {
				catalog.Providers[id] = append(catalog.Providers[id], provider)
			}
			sort.Slice(catalog.Providers[id], func(i, j int) bool {
				left, right := catalog.Providers[id][i], catalog.Providers[id][j]
				return counts[id][left] > counts[id][right] || counts[id][left] == counts[id][right] && left < right
			})
		}
	}
	return catalog
}
