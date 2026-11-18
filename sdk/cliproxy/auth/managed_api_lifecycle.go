package auth

import (
	"reflect"
	"strings"
)

func managedAPIAuth(a *Auth) bool {
	return a != nil && a.Attributes["config_id"] != "" && a.Attributes["config_usage_index"] != ""
}

func managedAPIExecutionEquivalent(a, b *Auth) bool {
	if a.ID != b.ID || a.Provider != b.Provider || a.ProxyURL != b.ProxyURL || !reflect.DeepEqual(a.Metadata, b.Metadata) {
		return false
	}
	ignored := func(key string) bool {
		return strings.HasPrefix(key, "config_") || key == "priority" || key == "weight" || key == "models_hash" || strings.HasPrefix(key, "excluded_models")
	}
	for key, value := range a.Attributes {
		if !ignored(key) && b.Attributes[key] != value {
			return false
		}
	}
	for key, value := range b.Attributes {
		if !ignored(key) && a.Attributes[key] != value {
			return false
		}
	}
	return true
}

// Draining prevents a stale selection from starting another execution. Existing
// execution contexts finish before their generation-specific resources close.
func (a *Auth) beginManagedAPIDrain() {
	if a == nil || a.instanceState == nil {
		return
	}
	a.instanceState.mu.Lock()
	a.instanceState.draining = true
	a.instanceState.mu.Unlock()
}

func (m *Manager) finishManagedAPIReplacement(old *Auth, notify func()) {
	if old == nil || old.instanceState == nil {
		if notify != nil {
			notify()
		}
		return
	}
	m.invalidateSessionAffinity(old.ID)
	m.runSessionCleanupCallback(old.ID, "auth_replaced", "auth_hook", notify)
	old.instanceState.mu.Lock()
	idle := old.instanceState.executionsIdle
	old.instanceState.mu.Unlock()
	finish := func() {
		if idle != nil {
			<-idle
		}
		old.retireInstance()
		m.waitForRefreshExecutions(old)
		m.closeRuntimeAuthSessions(old, "auth_replaced", true)
		old.instanceState.completeCleanup()
	}
	if idle == nil {
		finish()
	} else {
		go finish()
	}
}
