package auth

import (
	"path/filepath"
	"sort"
	"strings"

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
	m.removeDependencyAuthIndexLocked(id)
}

func (m *Manager) addAuthIndexesLocked(auth *Auth, cfg *internalconfig.Config) {
	if auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	id := strings.TrimSpace(auth.ID)
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
	delete(m.auths, id)
	m.removeAuthIndexesLocked(id)
	m.authIndexRevision++
}

func (m *Manager) installPersistedDependencyAuthLocked(auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" || m.auths[auth.ID] != nil {
		return
	}
	if authRelevantToChatGPTWebDependencyIndex(auth) {
		m.addDependencyAuthIndexLocked(auth)
	} else {
		m.removeDependencyAuthIndexLocked(auth.ID)
	}
	m.authIndexRevision++
}

func (m *Manager) removePersistedDependencyAuthLocked(id string) {
	if m == nil || strings.TrimSpace(id) == "" || m.auths[id] != nil {
		return
	}
	m.removeDependencyAuthIndexLocked(id)
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
	m.dependencyAuthsByID = make(map[string]*Auth)
	m.dependencySourceIDs = make(map[string]map[string]struct{})
	m.dependencyDependentIDs = make(map[string]map[string]struct{})
	for _, auth := range persisted {
		if auth == nil || strings.TrimSpace(auth.ID) == "" || !authRelevantToChatGPTWebDependencyIndex(auth) {
			continue
		}
		m.addDependencyAuthIndexLocked(auth)
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

// MarkChatGPTWebDependencyIndexDirty forces dependency-sensitive operations to
// refresh from persistence before making destructive decisions.
func (m *Manager) MarkChatGPTWebDependencyIndexDirty() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.dependencyIndexComplete = false
	m.authIndexRevision++
	m.mu.Unlock()
}
