package proxypool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type credentialBindingWriter interface {
	AuthSource
	LockProxyBindingMutation(context.Context, *coreauth.Auth) (context.Context, func(), error)
	MutateRuntimeMetadataIfCurrent(context.Context, *coreauth.Auth, func(*coreauth.Auth)) (*coreauth.Auth, bool, error)
}

func rememberedNodeID(raw string) string {
	parsed, err := url.Parse(raw)
	if err == nil {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		raw = parsed.String()
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func memoryForBinding(binding Binding, rawURL string) *coreauth.ProxyBindingMemory {
	if binding.Direct {
		return &coreauth.ProxyBindingMemory{Version: 1, Direct: true}
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil
	}
	port, err := strconv.Atoi(parsed.Port())
	if err != nil || port < 1 || port > 65535 {
		return nil
	}
	return &coreauth.ProxyBindingMemory{Version: 1, NodeID: rememberedNodeID(rawURL), Port: port, Placeholders: append([]string(nil), binding.PlaceholderValues...)}
}

// restoreCredentialBinding is called under the credential's binding lock.
// The credential reference wins over a different server-local sidecar binding.
func (m *Manager) restoreCredentialBinding(snapshot *configSnapshot, auth *coreauth.Auth, targets []config.ProxyRuleTargetConfig) error {
	memory := coreauth.ReadProxyBindingMemory(auth)
	m.mu.RLock()
	current, exists := m.bindings[auth.ID]
	m.mu.RUnlock()
	hasCurrent := exists && bindingCredentialGenerationMatches(current.CredentialUID, coreauth.ChatGPTWebCredentialUID(auth)) && bindingMatchesRuleTargets(current, targets)
	if hasCurrent {
		if current.Direct && (memory == nil || memory.Direct) {
			return nil
		}
		if raw, valid := m.bindingURL(snapshot, current); valid && (memory == nil || rememberedNodeID(raw) == memory.NodeID) {
			return nil
		}
	}
	if memory != nil {
		for _, target := range targets {
			if target.Direct {
				if memory.Direct {
					_, err := m.resolveDirectBinding(snapshot, auth.ID, coreauth.ChatGPTWebCredentialUID(auth))
					return err
				}
				continue
			}
			pool, ok := snapshot.pools[strings.ToLower(target.Pool)]
			if !ok || memory.Direct {
				continue
			}
			for _, entry := range pool.entries {
				candidate := Binding{AuthID: auth.ID, CredentialUID: coreauth.ChatGPTWebCredentialUID(auth), Pool: pool.config.Name, Entry: entry.config.ID,
					PlaceholderValues: append([]string(nil), memory.Placeholders...), BoundAt: m.now().UTC()}
				if entry.ports.Count() > 0 {
					if !entry.ports.Contains(memory.Port) {
						continue
					}
					candidate.Port = memory.Port
				}
				raw, valid := resolveBindingURL(pool, candidate)
				if !valid || rememberedNodeID(raw) != memory.NodeID {
					continue
				}
				id, err := randomBindingID(m.random)
				if err != nil {
					return err
				}
				candidate.ID = id
				// ResolvePoolBinding will run the normal health check before use.
				return m.saveBindingForSnapshot(snapshot, candidate)
			}
		}
	}
	// No portable match: keep the existing allocator, load balancing and failover.
	return nil
}

func (m *Manager) lockCredentialMemory(ctx context.Context, auth *coreauth.Auth) (context.Context, *coreauth.Auth, func(), error) {
	snapshot := m.snapshot()
	if snapshot == nil || auth.Metadata == nil {
		return ctx, auth, func() {}, nil
	}
	_, matched := config.MatchProxyRuleTargets(snapshot.rules, auth.Provider, authPriority(auth))
	m.mu.RLock()
	writer, ok := m.auths.(credentialBindingWriter)
	m.mu.RUnlock()
	if !matched || !ok || auth.RuntimeInstanceID() == "" {
		return ctx, auth, func() {}, nil
	}
	locked, unlock, err := writer.LockProxyBindingMutation(ctx, auth)
	if err != nil {
		return ctx, auth, func() {}, err
	}
	current, exists := writer.GetByID(auth.ID)
	if !exists || current.RuntimeInstanceID() != auth.RuntimeInstanceID() {
		unlock()
		return ctx, auth, func() {}, &UnavailableError{}
	}
	return locked, current, unlock, nil
}

// RememberCredentialBinding is called after Resolve released its binding lock.
// The canonical lock order is credential mutation -> binding -> configuration.
// Reading the binding again under these locks prevents a late request from
// overwriting a newer rebind or a retired credential's metadata.
func (m *Manager) RememberCredentialBinding(ctx context.Context, auth *coreauth.Auth, bindingID string) (*coreauth.Auth, error) {
	if auth == nil || auth.Metadata == nil || auth.RuntimeInstanceID() == "" || strings.EqualFold(auth.Attributes["runtime_only"], "true") {
		return auth, nil
	}
	m.mu.RLock()
	source, ok := m.auths.(credentialBindingWriter)
	binding, found := m.bindings[auth.ID]
	m.mu.RUnlock()
	if !ok || !found || (!binding.Direct && binding.ID != bindingID) || (binding.Direct && bindingID != "") {
		return auth, nil
	}
	lockedCtx, unlockAuth, err := source.LockProxyBindingMutation(ctx, auth)
	if err != nil {
		return nil, err
	}
	defer unlockAuth()
	unlockBinding, err := m.lockBinding(ctx, auth.ID)
	if err != nil {
		return nil, err
	}
	defer unlockBinding()
	current, exists := source.GetByID(auth.ID)
	if !exists || current.RuntimeInstanceID() != auth.RuntimeInstanceID() || current.RuntimeInstanceRetired() {
		return nil, &UnavailableError{Cause: errors.New("credential changed before proxy binding persistence")}
	}
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	snapshot := m.snapshot()
	m.mu.RLock()
	binding, found = m.bindings[auth.ID]
	m.mu.RUnlock()
	if !found || (!binding.Direct && binding.ID != bindingID) || (binding.Direct && bindingID != "") {
		return current, nil
	}
	targets, matched := config.MatchProxyRuleTargets(snapshot.rules, current.Provider, authPriority(current))
	if !matched || !bindingMatchesRuleTargets(binding, targets) || current.ProxyURL != "" {
		return current, nil
	}
	raw, valid := m.bindingURL(snapshot, binding)
	if !valid {
		return current, nil
	}
	memory := memoryForBinding(binding, raw)
	if memory == nil {
		return current, nil
	}
	data, err := json.Marshal(memory)
	if err != nil {
		return nil, err
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, err
	}
	if coreauth.ReadProxyBindingMemory(&coreauth.Auth{Metadata: map[string]any{coreauth.ProxyBindingMemoryKey: value}}) == nil {
		return nil, &UnavailableError{Pool: binding.Pool, Cause: errors.New("proxy binding reference exceeds portable limits")}
	}
	// No write, scheduler update, or token change for an unchanged binding.
	if reflect.DeepEqual(coreauth.ReadProxyBindingMemory(current), memory) {
		return current, nil
	}
	updated, applied, err := source.MutateRuntimeMetadataIfCurrent(lockedCtx, current, func(candidate *coreauth.Auth) {
		if candidate.Metadata == nil {
			candidate.Metadata = make(map[string]any)
		}
		candidate.Metadata[coreauth.ProxyBindingMemoryKey] = value
	})
	if err != nil {
		return nil, &UnavailableError{Pool: binding.Pool, Cause: err}
	}
	if !applied {
		return nil, &UnavailableError{Pool: binding.Pool, Cause: errors.New("credential changed during proxy binding persistence")}
	}
	return updated, nil
}
