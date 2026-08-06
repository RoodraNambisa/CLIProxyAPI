package auth

import (
	"path/filepath"
	"sort"
	"strings"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func authBackingPathKey(auth *Auth, cfg *internalconfig.Config) string {
	if auth == nil {
		return ""
	}
	path := ""
	if auth.Attributes != nil {
		path = strings.TrimSpace(auth.Attributes["path"])
	}
	if path == "" {
		path = strings.TrimSpace(auth.FileName)
	}
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) && cfg != nil && strings.TrimSpace(cfg.AuthDir) != "" {
		path = filepath.Join(cfg.AuthDir, path)
	}
	return authfileguard.PathIdentity(path)
}

func authRelevantToChatGPTWebDependencyIndex(auth *Auth) bool {
	if auth == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		return ChatGPTWebCredentialUID(auth) != ""
	}
	return ChatGPTWebLinkedSourceUID(auth) != ""
}

func chatGPTWebIdentityIndexKeys(auth *Auth) []string {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return nil
	}
	reference := NewChatGPTWebCredentialReference(auth)
	keys := make([]string, 0, 5)
	for prefix, value := range map[string]string{
		"account:":  reference.accountHash,
		"user:":     reference.userHash,
		"subject:":  reference.subjectHash,
		"identity:": reference.identityHash,
	} {
		if value != "" {
			keys = append(keys, prefix+value)
		}
	}
	if email := chatGPTWebRegistrationEmail(auth); email != "" {
		keys = append(keys, "email:"+email)
	}
	sort.Strings(keys)
	return keys
}

func (m *Manager) removeChatGPTWebIdentityIndexLocked(id string) {
	for _, key := range m.chatGPTWebIdentityKeysByID[id] {
		ids := m.chatGPTWebIdentityIDs[key]
		delete(ids, id)
		if len(ids) == 0 {
			delete(m.chatGPTWebIdentityIDs, key)
		}
	}
	delete(m.chatGPTWebIdentityKeysByID, id)
}

func (m *Manager) addChatGPTWebIdentityIndexLocked(auth *Auth) {
	if auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	id := strings.TrimSpace(auth.ID)
	m.removeChatGPTWebIdentityIndexLocked(id)
	keys := chatGPTWebIdentityIndexKeys(auth)
	if len(keys) == 0 {
		return
	}
	m.chatGPTWebIdentityKeysByID[id] = keys
	for _, key := range keys {
		addIDToDependencySet(m.chatGPTWebIdentityIDs, key, id)
	}
}

func (m *Manager) installPersistedAuthIndexLocked(auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	m.persistedAuthsByID[strings.TrimSpace(auth.ID)] = auth.Clone()
}

// recordPersistedAuthSave updates the indexed store view after a successful
// save. Runtime records remain authoritative for dependency and identity
// lookups, while persisted-only records must update those indexes immediately.
func (m *Manager) recordPersistedAuthSave(auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	id := strings.TrimSpace(auth.ID)
	m.mu.Lock()
	m.installPersistedAuthIndexLocked(auth)
	m.persistedAuthRevision++
	if m.auths[id] == nil {
		if authRelevantToChatGPTWebDependencyIndex(auth) {
			m.addDependencyAuthIndexLocked(auth)
		} else {
			m.removeDependencyAuthIndexLocked(id)
		}
		if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
			m.addChatGPTWebIdentityIndexLocked(auth)
		} else {
			m.removeChatGPTWebIdentityIndexLocked(id)
		}
	}
	m.authIndexRevision++
	m.mu.Unlock()
}

func (m *Manager) removePersistedAuthIndexLocked(id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	delete(m.persistedAuthsByID, id)
}

// recordPersistedAuthDeleteLocked removes one record from the authoritative
// store view and drops persisted-only dependency indexes when no runtime record
// remains. The caller must hold m.mu for writing.
func (m *Manager) recordPersistedAuthDeleteLocked(id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	m.persistedAuthRevision++
	if m.auths[id] == nil {
		m.removePersistedDependencyAuthLocked(id)
		return
	}
	m.removePersistedAuthIndexLocked(id)
}

func addIDToDependencySet(index map[string]map[string]struct{}, key, id string) {
	key = strings.TrimSpace(key)
	id = strings.TrimSpace(id)
	if key == "" || id == "" {
		return
	}
	ids := index[key]
	if ids == nil {
		ids = make(map[string]struct{})
		index[key] = ids
	}
	ids[id] = struct{}{}
}

func removeIDFromDependencySet(index map[string]map[string]struct{}, key, id string) {
	ids := index[strings.TrimSpace(key)]
	if ids == nil {
		return
	}
	delete(ids, strings.TrimSpace(id))
	if len(ids) == 0 {
		delete(index, strings.TrimSpace(key))
	}
}

func (m *Manager) removeDependencyAuthIndexLocked(id string) {
	indexed := m.dependencyAuthsByID[id]
	if indexed == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(indexed.Provider), "codex") {
		removeIDFromDependencySet(m.dependencySourceIDs, ChatGPTWebCredentialUID(indexed), id)
	}
	removeIDFromDependencySet(m.dependencyDependentIDs, ChatGPTWebLinkedSourceUID(indexed), id)
	delete(m.dependencyAuthsByID, id)
}

func (m *Manager) addDependencyAuthIndexLocked(auth *Auth) {
	if !authRelevantToChatGPTWebDependencyIndex(auth) {
		return
	}
	id := strings.TrimSpace(auth.ID)
	m.removeDependencyAuthIndexLocked(id)
	m.dependencyAuthsByID[id] = auth.Clone()
	if strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
		addIDToDependencySet(m.dependencySourceIDs, ChatGPTWebCredentialUID(auth), id)
	}
	addIDToDependencySet(m.dependencyDependentIDs, ChatGPTWebLinkedSourceUID(auth), id)
}

func (m *Manager) removeAuthIndexesLocked(id string) {
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	if pathKey := m.backingPathByAuthID[id]; pathKey != "" {
		if ids := m.backingPathAuthIDs[pathKey]; ids != nil {
			delete(ids, id)
			if len(ids) == 0 {
				delete(m.backingPathAuthIDs, pathKey)
			}
		}
		delete(m.backingPathByAuthID, id)
	}
	if providerKey := m.providerByAuthID[id]; providerKey != "" {
		if ids := m.providerAuthIDs[providerKey]; ids != nil {
			delete(ids, id)
			if len(ids) == 0 {
				delete(m.providerAuthIDs, providerKey)
			}
		}
		delete(m.providerByAuthID, id)
	}
	delete(m.authIndexesByID, id)
	delete(m.authPlanTypesByID, id)
	m.removeDependencyAuthIndexLocked(id)
	m.removeChatGPTWebIdentityIndexLocked(id)
}

func (m *Manager) addAuthIndexesLocked(auth *Auth, cfg *internalconfig.Config) {
	if auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	id := strings.TrimSpace(auth.ID)
	if index := strings.TrimSpace(auth.Index); index != "" {
		m.authIndexesByID[id] = index
	}
	if planType := strings.TrimSpace(internalcodex.EffectivePlanType(auth.Metadata)); planType != "" {
		m.authPlanTypesByID[id] = planType
	}
	if providerKey := strings.ToLower(strings.TrimSpace(auth.Provider)); providerKey != "" {
		ids := m.providerAuthIDs[providerKey]
		if ids == nil {
			ids = make(map[string]struct{})
			m.providerAuthIDs[providerKey] = ids
		}
		ids[id] = struct{}{}
		m.providerByAuthID[id] = providerKey
	}
	if pathKey := authBackingPathKey(auth, cfg); pathKey != "" {
		ids := m.backingPathAuthIDs[pathKey]
		if ids == nil {
			ids = make(map[string]struct{})
			m.backingPathAuthIDs[pathKey] = ids
		}
		ids[id] = struct{}{}
		m.backingPathByAuthID[id] = pathKey
	}
	if authRelevantToChatGPTWebDependencyIndex(auth) {
		m.addDependencyAuthIndexLocked(auth)
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		m.addChatGPTWebIdentityIndexLocked(auth)
	}
}

// installAuthLocked atomically installs an auth and updates all Manager indexes.
// The caller must hold m.mu for writing.
func (m *Manager) installAuthLocked(id string, auth *Auth) {
	if m == nil || auth == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		id = strings.TrimSpace(auth.ID)
	}
	if id == "" {
		return
	}
	m.removeAuthIndexesLocked(id)
	m.auths[id] = auth
	m.addAuthIndexesLocked(auth, m.currentConfig())
	m.authIndexRevision++
}

// removeAuthLocked atomically removes an auth and its Manager index entries.
// The caller must hold m.mu for writing.
func (m *Manager) removeAuthLocked(id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	removed := m.auths[id]
	delete(m.auths, id)
	m.removeAuthIndexesLocked(id)
	m.removeAPIKeyModelAliasForAuthLocked(removed)
	m.authIndexRevision++
}

func (m *Manager) installPersistedDependencyAuthLocked(auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" || m.auths[auth.ID] != nil {
		return
	}
	m.installPersistedAuthIndexLocked(auth)
	if authRelevantToChatGPTWebDependencyIndex(auth) {
		m.addDependencyAuthIndexLocked(auth)
	} else {
		m.removeDependencyAuthIndexLocked(auth.ID)
	}
	if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		m.addChatGPTWebIdentityIndexLocked(auth)
	}
	m.authIndexRevision++
}

func (m *Manager) removePersistedDependencyAuthLocked(id string) {
	if m == nil || strings.TrimSpace(id) == "" || m.auths[id] != nil {
		return
	}
	m.removePersistedAuthIndexLocked(id)
	m.removeDependencyAuthIndexLocked(id)
	m.removeChatGPTWebIdentityIndexLocked(id)
	m.authIndexRevision++
}

func (m *Manager) rebuildBackingPathIndexLocked(cfg *internalconfig.Config) {
	m.backingPathAuthIDs = make(map[string]map[string]struct{})
	m.backingPathByAuthID = make(map[string]string)
	m.backingPathAuthDir = ""
	if cfg != nil {
		m.backingPathAuthDir = strings.TrimSpace(cfg.AuthDir)
	}
	for _, auth := range m.auths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		if pathKey := authBackingPathKey(auth, cfg); pathKey != "" {
			ids := m.backingPathAuthIDs[pathKey]
			if ids == nil {
				ids = make(map[string]struct{})
				m.backingPathAuthIDs[pathKey] = ids
			}
			ids[auth.ID] = struct{}{}
			m.backingPathByAuthID[auth.ID] = pathKey
		}
	}
	m.authIndexRevision++
}

// rebuildAuthIndexesLocked rebuilds the runtime path index and the dependency
// view. Persisted records are installed first so runtime records win by ID.
// The caller must hold m.mu for writing.
func (m *Manager) rebuildAuthIndexesLocked(persisted []*Auth, complete bool) {
	m.backingPathAuthIDs = make(map[string]map[string]struct{})
	m.backingPathByAuthID = make(map[string]string)
	m.providerAuthIDs = make(map[string]map[string]struct{})
	m.providerByAuthID = make(map[string]string)
	m.authIndexesByID = make(map[string]string)
	m.authPlanTypesByID = make(map[string]string)
	m.dependencyAuthsByID = make(map[string]*Auth)
	m.dependencySourceIDs = make(map[string]map[string]struct{})
	m.dependencyDependentIDs = make(map[string]map[string]struct{})
	m.chatGPTWebIdentityIDs = make(map[string]map[string]struct{})
	m.chatGPTWebIdentityKeysByID = make(map[string][]string)
	m.persistedAuthsByID = make(map[string]*Auth, len(persisted))
	m.persistedAuthRevision++
	for _, auth := range persisted {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		m.persistedAuthsByID[strings.TrimSpace(auth.ID)] = auth.Clone()
		if authRelevantToChatGPTWebDependencyIndex(auth) {
			m.addDependencyAuthIndexLocked(auth)
		}
		if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
			m.addChatGPTWebIdentityIndexLocked(auth)
		}
	}
	cfg := m.currentConfig()
	m.backingPathAuthDir = ""
	if cfg != nil {
		m.backingPathAuthDir = strings.TrimSpace(cfg.AuthDir)
	}
	for _, auth := range m.auths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		m.addAuthIndexesLocked(auth, cfg)
		if !authRelevantToChatGPTWebDependencyIndex(auth) {
			m.removeDependencyAuthIndexLocked(auth.ID)
		}
	}
	m.dependencyIndexComplete = complete
	m.chatGPTWebIdentityComplete = complete
	m.authIndexRevision++
}

func (m *Manager) replaceAuthsLocked(auths map[string]*Auth, persisted []*Auth, complete bool) {
	if auths == nil {
		auths = make(map[string]*Auth)
	}
	m.auths = auths
	m.rebuildAuthIndexesLocked(persisted, complete)
}

// AuthsForBackingPath returns current runtime auth snapshots associated with a
// normalized backing file path. Multiple auths are returned in stable ID order.
func (m *Manager) AuthsForBackingPath(path string) []*Auth {
	if m == nil || strings.TrimSpace(path) == "" {
		return nil
	}
	key := authfileguard.PathIdentity(path)
	if key == "" {
		return nil
	}
	m.mu.RLock()
	ids := m.backingPathAuthIDs[key]
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	auths := make([]*Auth, 0, len(ordered))
	for _, id := range ordered {
		if auth := m.auths[id]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return auths
}

// ChatGPTWebDependencyIndexSnapshot returns the current dependency graph and
// whether it includes a successful complete persistence-store enumeration.
func (m *Manager) ChatGPTWebDependencyIndexSnapshot() (*ChatGPTWebDependencyGraph, bool) {
	if m == nil {
		return BuildChatGPTWebDependencyGraph(nil), false
	}
	m.mu.RLock()
	auths := make([]*Auth, 0, len(m.dependencyAuthsByID))
	for _, auth := range m.dependencyAuthsByID {
		if auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	complete := m.dependencyIndexComplete
	m.mu.RUnlock()
	return BuildChatGPTWebDependencyGraph(auths), complete
}

// ChatGPTWebSourceByCredentialUID resolves one Codex source directly from the
// incremental dependency index. Duplicate UIDs are reported as ambiguous.
func (m *Manager) ChatGPTWebSourceByCredentialUID(uid string) (*Auth, bool, bool) {
	if m == nil || strings.TrimSpace(uid) == "" {
		return nil, false, false
	}
	uid = strings.TrimSpace(uid)
	m.mu.RLock()
	ids := m.dependencySourceIDs[uid]
	complete := m.dependencyIndexComplete
	if len(ids) != 1 {
		ambiguous := len(ids) > 1
		m.mu.RUnlock()
		return nil, ambiguous, complete
	}
	var source *Auth
	for id := range ids {
		if indexed := m.dependencyAuthsByID[id]; indexed != nil {
			source = indexed.Clone()
		}
	}
	m.mu.RUnlock()
	return source, false, complete
}

// ChatGPTWebDependentsForSource resolves linked Web credentials in time
// proportional to the number of dependents for this source UID.
func (m *Manager) ChatGPTWebDependentsForSource(source *Auth) ([]*Auth, bool, bool) {
	uid := ChatGPTWebCredentialUID(source)
	if m == nil || uid == "" {
		return nil, false, false
	}
	m.mu.RLock()
	complete := m.dependencyIndexComplete
	if len(m.dependencySourceIDs[uid]) > 1 {
		m.mu.RUnlock()
		return nil, true, complete
	}
	ids := m.dependencyDependentIDs[uid]
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	dependents := make([]*Auth, 0, len(ordered))
	for _, id := range ordered {
		indexed := m.dependencyAuthsByID[id]
		if indexed != nil && ChatGPTWebLinkedSourceMatches(indexed, source) {
			dependents = append(dependents, indexed.Clone())
		}
	}
	m.mu.RUnlock()
	return dependents, false, complete
}

// markChatGPTWebDependencyIndexDirtyLocked invalidates the authoritative store
// view after a persistence outcome becomes uncertain. The caller must hold
// m.mu for writing.
func (m *Manager) markChatGPTWebDependencyIndexDirtyLocked() {
	m.dependencyIndexComplete = false
	m.chatGPTWebIdentityComplete = false
	m.authIndexRevision++
	m.persistedAuthRevision++
}

// MarkChatGPTWebDependencyIndexDirty forces dependency-sensitive operations to
// refresh from persistence before making destructive decisions.
func (m *Manager) MarkChatGPTWebDependencyIndexDirty() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.markChatGPTWebDependencyIndexDirtyLocked()
	m.mu.Unlock()
}

// ChatGPTWebAuthsByEmail returns runtime ChatGPT Web auths for one normalized
// email. The completeness flag is false when a persisted-only candidate exists
// or the persistence index is stale.
func (m *Manager) ChatGPTWebAuthsByEmail(email string) ([]*Auth, bool) {
	if m == nil {
		return nil, false
	}
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		m.mu.RLock()
		complete := m.chatGPTWebIdentityComplete
		m.mu.RUnlock()
		return nil, complete
	}
	m.mu.RLock()
	ids := m.chatGPTWebIdentityIDs["email:"+email]
	ordered := make([]string, 0, len(ids))
	complete := m.chatGPTWebIdentityComplete
	for id := range ids {
		ordered = append(ordered, id)
		if m.auths[id] == nil {
			complete = false
		}
	}
	sort.Strings(ordered)
	auths := make([]*Auth, 0, len(ordered))
	for _, id := range ordered {
		if auth := m.auths[id]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return auths, complete
}

// PersistedAuthByID resolves an auth from the latest complete persistence
// index without cloning unrelated records. The completeness flag is false
// after an external mutation has invalidated the index.
func (m *Manager) PersistedAuthByID(id string) (*Auth, bool) {
	if m == nil || strings.TrimSpace(id) == "" {
		return nil, false
	}
	id = strings.TrimSpace(id)
	m.mu.RLock()
	auth := m.persistedAuthsByID[id]
	complete := m.chatGPTWebIdentityComplete
	if auth != nil {
		auth = auth.Clone()
	}
	m.mu.RUnlock()
	return auth, complete
}

func (m *Manager) persistedAuthIndexSnapshot() ([]*Auth, bool) {
	if m == nil {
		return nil, false
	}
	m.mu.RLock()
	complete := m.chatGPTWebIdentityComplete && m.dependencyIndexComplete
	ids := make([]string, 0, len(m.persistedAuthsByID))
	for id := range m.persistedAuthsByID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	auths := make([]*Auth, 0, len(ids))
	for _, id := range ids {
		if auth := m.persistedAuthsByID[id]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return auths, complete
}

// ChatGPTWebIdentityConflicts returns runtime candidates that represent the
// same account as incoming. It avoids cloning unrelated credentials.
func (m *Manager) ChatGPTWebIdentityConflicts(incoming *Auth, excludedID string) ([]*Auth, bool) {
	if m == nil || incoming == nil {
		return nil, false
	}
	keys := chatGPTWebIdentityIndexKeys(incoming)
	m.mu.RLock()
	candidateIDs := make(map[string]struct{})
	complete := m.chatGPTWebIdentityComplete
	for _, key := range keys {
		for id := range m.chatGPTWebIdentityIDs[key] {
			if id == excludedID {
				continue
			}
			candidateIDs[id] = struct{}{}
			if m.auths[id] == nil {
				complete = false
			}
		}
	}
	ordered := make([]string, 0, len(candidateIDs))
	for id := range candidateIDs {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	conflicts := make([]*Auth, 0, len(ordered))
	for _, id := range ordered {
		if candidate := m.auths[id]; candidate != nil && ChatGPTWebCredentialIdentityConflict(candidate, incoming) {
			conflicts = append(conflicts, candidate.Clone())
		}
	}
	m.mu.RUnlock()
	return conflicts, complete
}

// ChatGPTWebAuthIDsByEmail returns only matching runtime IDs for refresh-lock
// coordination. The operation is proportional to the matching accounts.
func (m *Manager) ChatGPTWebAuthIDsByEmail(email string) []string {
	auths, _ := m.ChatGPTWebAuthsByEmail(email)
	ids := make([]string, 0, len(auths))
	for _, auth := range auths {
		if auth != nil {
			ids = append(ids, auth.ID)
		}
	}
	return ids
}

// ChatGPTWebAuths returns only runtime ChatGPT Web credentials in stable ID
// order. It avoids cloning unrelated providers for provider-owned maintenance.
func (m *Manager) ChatGPTWebAuths() []*Auth {
	return m.AuthsForProviders(chatgptwebauth.Provider)
}

// AuthsForProviders returns runtime credentials for the requested providers in
// stable ID order without cloning unrelated credentials.
func (m *Manager) AuthsForProviders(providers ...string) []*Auth {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	uniqueIDs := make(map[string]struct{})
	for _, provider := range providers {
		providerKey := strings.ToLower(strings.TrimSpace(provider))
		for id := range m.providerAuthIDs[providerKey] {
			uniqueIDs[id] = struct{}{}
		}
	}
	ids := make([]string, 0, len(uniqueIDs))
	for id := range uniqueIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	auths := make([]*Auth, 0, len(ids))
	for _, id := range ids {
		if auth := m.auths[id]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return auths
}

// AuthIndexSnapshot returns the immutable public auth indexes without cloning
// credential metadata.
func (m *Manager) AuthIndexSnapshot() map[string]string {
	if m == nil {
		return map[string]string{}
	}
	m.mu.RLock()
	indexes := make(map[string]string, len(m.authIndexesByID))
	for id, index := range m.authIndexesByID {
		indexes[id] = index
	}
	m.mu.RUnlock()
	return indexes
}
