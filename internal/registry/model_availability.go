package registry

import (
	"maps"
	"time"
)

// ClientModelAvailability is an immutable view of temporarily gated routes.
// A present route is available only before its deadline; zero means unavailable.
// Original registrations remain intact for background acquisition and diagnostics.
type ClientModelAvailability map[string]map[string]time.Time

func (a ClientModelAvailability) Available(clientID, modelID string, now time.Time) bool {
	expires, gated := a[clientID][modelID]
	return !gated || now.Before(expires)
}

// SetClientModelAvailability installs a lock-free snapshot reader. The reader
// must not call registry methods or mutate a previously published snapshot.
func (r *ModelRegistry) SetClientModelAvailability(reader func() ClientModelAvailability) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.clientModelAvailability = reader
	r.invalidateAvailableModelsCacheLocked()
}

func (r *ModelRegistry) availabilityLocked() ClientModelAvailability {
	if r.clientModelAvailability == nil {
		return nil
	}
	return r.clientModelAvailability()
}

func (r *ModelRegistry) ClientModelAvailable(clientID, modelID string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.availabilityLocked().Available(clientID, modelID, time.Now())
}

func (r *ModelRegistry) ModelHasAvailabilityGate(modelID string) bool {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	for _, routes := range r.availabilityLocked() {
		if _, gated := routes[modelID]; gated {
			return true
		}
	}
	return false
}

// GetRoutingModelProviders keeps known routes resolvable while every State is
// missing. Selection returns an availability error, rather than unknown model.
func (r *ModelRegistry) GetRoutingModelProviders(modelID string) []string {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.modelProvidersWithAvailabilityLocked(modelID, time.Now(), nil)
}

// GetSelectableModelsForClient is the user-facing view. Internal scope discovery
// must use GetModelsForClient so a hidden route can acquire State and recover.
func (r *ModelRegistry) GetSelectableModelsForClient(clientID string) []*ModelInfo {
	models := r.GetModelsForClient(clientID)
	r.mutex.RLock()
	availability := r.availabilityLocked()
	r.mutex.RUnlock()
	now := time.Now()
	result := models[:0]
	for _, info := range models {
		if availability.Available(clientID, info.ID, now) {
			result = append(result, info)
		}
	}
	return result
}

// Exclude gated clients independently of quota/cooldown suspension. Restoring
// State must never clear an unrelated upstream failure or cooldown.
func (r *ModelRegistry) availableRegistrationLocked(modelID string, registration *ModelRegistration, availability ClientModelAvailability, now time.Time) *ModelRegistration {
	var filtered *ModelRegistration
	for clientID := range availability {
		if r.clientModelInfos[clientID][modelID] == nil || availability.Available(clientID, modelID, now) {
			continue
		}
		if filtered == nil {
			copyRegistration := *registration
			filtered = &copyRegistration
			filtered.Providers = maps.Clone(registration.Providers)
			filtered.SuspendedClients = maps.Clone(registration.SuspendedClients)
			filtered.QuotaExceededClients = maps.Clone(registration.QuotaExceededClients)
		}
		filtered.Count--
		if provider := r.clientProviders[clientID]; provider != "" {
			filtered.Providers[provider]--
		}
		delete(filtered.SuspendedClients, clientID)
		delete(filtered.QuotaExceededClients, clientID)
	}
	if filtered != nil {
		return filtered
	}
	return registration
}
