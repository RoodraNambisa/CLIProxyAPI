package proxypool

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"sort"
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
		if raw, valid := m.bindingURL(snapshot, current); valid && (memory == nil || rememberedNodeID(raw) == memory.NodeID) {
			return nil
		}
	}
	if memory != nil {
		for _, target := range targets {
			pool, ok := snapshot.pools[strings.ToLower(target.Pool)]
			if target.Direct || !ok || !pool.config.RememberCredentialBinding {
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
	if hasCurrent {
		if _, valid := m.bindingURL(snapshot, current); valid {
			return nil
		}
	}
	return m.seedCredentialBinding(snapshot, auth, targets)
}

// Initial selection is deterministic across replicas, independent of local
// load, credential filenames and token rotation. Explicit rebind remains random.
func (m *Manager) seedCredentialBinding(snapshot *configSnapshot, auth *coreauth.Auth, targets []config.ProxyRuleTargetConfig) error {
	seed := stableProxyCredentialSeed(auth)
	ordered := append([]config.ProxyRuleTargetConfig(nil), targets...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority > ordered[j].Priority
		}
		return rememberedNodeID(seed+proxyRuleTargetKey(ordered[i])) < rememberedNodeID(seed+proxyRuleTargetKey(ordered[j]))
	})
	if len(ordered) == 0 || ordered[0].Direct {
		return nil
	}
	pool, ok := snapshot.pools[strings.ToLower(ordered[0].Pool)]
	if !ok || !pool.config.RememberCredentialBinding {
		return nil
	}
	// Sorting by the configured template also keeps entry renames/reordering
	// from changing an as-yet-unwritten selection on another server.
	pool.entries = append([]runtimeEntry(nil), pool.entries...)
	sort.Slice(pool.entries, func(i, j int) bool {
		a, b := pool.entries[i], pool.entries[j]
		return a.config.URLTemplate+"|"+a.config.Ports < b.config.URLTemplate+"|"+b.config.Ports
	})
	count := poolCandidateCount(pool)
	if count == 0 {
		return nil
	}
	sum := sha256.Sum256([]byte(seed + "/node"))
	ordinal := int(binary.BigEndian.Uint64(sum[:8]) % uint64(count))
	source := &credentialBindingRandom{seed: seed + "/session"}
	binding, _, err := m.bindingAtOrdinalWithSource(pool, auth.ID, ordinal, source)
	if err != nil {
		return err
	}
	binding.CredentialUID = coreauth.ChatGPTWebCredentialUID(auth)
	return m.saveBindingForSnapshot(snapshot, binding)
}

func stableProxyCredentialSeed(auth *coreauth.Auth) string {
	for _, key := range []string{"account_id", "user_id", "email", "credential_uid"} {
		if value, ok := auth.Metadata[key].(string); ok && strings.TrimSpace(value) != "" {
			return "proxy-binding-v1/" + strings.ToLower(auth.Provider) + "/" + key + "/" + strings.ToLower(strings.TrimSpace(value))
		}
	}
	return "proxy-binding-v1/" + strings.ToLower(auth.Provider) + "/" + auth.ID
}

type credentialBindingRandom struct {
	seed    string
	counter uint64
	pending []byte
}

func (r *credentialBindingRandom) Read(target []byte) (int, error) {
	for i := range target {
		if len(r.pending) == 0 {
			sum := sha256.Sum256([]byte(r.seed + "/" + strconv.FormatUint(r.counter, 10)))
			r.counter++
			r.pending = sum[:]
		}
		target[i] = r.pending[0]
		r.pending = r.pending[1:]
	}
	return len(target), nil
}

func (m *Manager) lockCredentialMemory(ctx context.Context, auth *coreauth.Auth) (context.Context, *coreauth.Auth, func(), error) {
	snapshot := m.snapshot()
	if snapshot == nil || auth.Metadata == nil {
		return ctx, auth, func() {}, nil
	}
	targets, _ := config.MatchProxyRuleTargets(snapshot.rules, auth.Provider, authPriority(auth))
	enabled := false
	for _, target := range targets {
		if pool, ok := snapshot.pools[strings.ToLower(target.Pool)]; ok && pool.config.RememberCredentialBinding {
			enabled = true
			break
		}
	}
	m.mu.RLock()
	writer, ok := m.auths.(credentialBindingWriter)
	m.mu.RUnlock()
	if !enabled || !ok || auth.RuntimeInstanceID() == "" {
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
	if !ok || !found || binding.ID != bindingID || binding.Direct {
		return auth, nil
	}
	snapshot := m.snapshot()
	pool, exists := snapshot.pools[strings.ToLower(binding.Pool)]
	if !exists || !pool.config.RememberCredentialBinding {
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
	snapshot = m.snapshot()
	m.mu.RLock()
	binding, found = m.bindings[auth.ID]
	m.mu.RUnlock()
	if !found || binding.ID != bindingID || binding.Direct {
		return current, nil
	}
	pool, exists = snapshot.pools[strings.ToLower(binding.Pool)]
	targets, matched := config.MatchProxyRuleTargets(snapshot.rules, current.Provider, authPriority(current))
	if !exists || !pool.config.RememberCredentialBinding || !matched || !bindingMatchesRuleTargets(binding, targets) || current.ProxyURL != "" {
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
