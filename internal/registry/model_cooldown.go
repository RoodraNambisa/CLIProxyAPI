package registry

import "time"

// ClearClientModelCooldowns repairs both transient registry projections without
// re-registering models or clearing another client's state.
func (r *ModelRegistry) ClearClientModelCooldowns(clientID string, modelIDs []string) bool {
	if r == nil || clientID == "" || len(modelIDs) == 0 {
		return false
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	changed := false
	for _, modelID := range modelIDs {
		registration := r.models[modelID]
		if registration == nil {
			continue
		}
		_, quota := registration.QuotaExceededClients[clientID]
		_, suspended := registration.SuspendedClients[clientID]
		if !quota && !suspended {
			continue
		}
		delete(registration.QuotaExceededClients, clientID)
		delete(registration.SuspendedClients, clientID)
		registration.LastUpdated = time.Now()
		changed = true
	}
	if changed {
		r.invalidateAvailableModelsCacheLocked()
	}
	return changed
}
