package auth

import (
	"encoding/json"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

var managementCatalogAttributeKeys = [...]string{
	"auth_kind",
	"credential_uid",
	"note",
	"path",
	"plan_type",
	"priority",
	"refresh_strategy",
	"runtime_only",
	"source_auth_id",
	"source_credential_uid",
}

var managementCatalogMetadataKeys = [...]string{
	"auth_kind",
	"credential_uid",
	"deletion_state",
	"email",
	"lifecycle_reason",
	"lifecycle_state",
	"note",
	"plan_type",
	"priority",
	"refresh_strategy",
	"source_auth_id",
	"source_credential_uid",
}

type providerRequestRetryEntry struct {
	provider    string
	override    int
	hasOverride bool
}

type providerRequestRetryAggregate struct {
	authCount      int
	defaultCount   int
	overrideCounts map[int]int
	maxOverride    int
}

func (m *Manager) removeProviderSchedulingIndexesLocked(id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return
	}
	entry, ok := m.providerRetryByAuthID[id]
	if ok {
		delete(m.providerRetryByAuthID, id)
		aggregate := m.providerRetryAggregates[entry.provider]
		if aggregate != nil {
			aggregate.authCount--
			if entry.hasOverride {
				if count := aggregate.overrideCounts[entry.override]; count <= 1 {
					delete(aggregate.overrideCounts, entry.override)
					if entry.override == aggregate.maxOverride {
						aggregate.maxOverride = 0
						for value, remaining := range aggregate.overrideCounts {
							if remaining > 0 && value > aggregate.maxOverride {
								aggregate.maxOverride = value
							}
						}
					}
				} else {
					aggregate.overrideCounts[entry.override] = count - 1
				}
			} else {
				aggregate.defaultCount--
			}
			if aggregate.authCount <= 0 {
				delete(m.providerRetryAggregates, entry.provider)
			}
		}
	}
	if ok {
		if ids := m.providerPrefixedAuthIDs[entry.provider]; ids != nil {
			delete(ids, id)
			if len(ids) == 0 {
				delete(m.providerPrefixedAuthIDs, entry.provider)
			}
		}
	}
}

func (m *Manager) addProviderSchedulingIndexesLocked(auth *Auth) {
	if m == nil || auth == nil {
		return
	}
	id := strings.TrimSpace(auth.ID)
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	if id == "" || provider == "" {
		return
	}
	override, hasOverride := auth.RequestRetryOverride()
	if override < 0 {
		override = 0
	}
	m.providerRetryByAuthID[id] = providerRequestRetryEntry{
		provider:    provider,
		override:    override,
		hasOverride: hasOverride,
	}
	aggregate := m.providerRetryAggregates[provider]
	if aggregate == nil {
		aggregate = &providerRequestRetryAggregate{overrideCounts: make(map[int]int)}
		m.providerRetryAggregates[provider] = aggregate
	}
	aggregate.authCount++
	if hasOverride {
		aggregate.overrideCounts[override]++
		if override > aggregate.maxOverride {
			aggregate.maxOverride = override
		}
	} else {
		aggregate.defaultCount++
	}
	if strings.TrimSpace(auth.Prefix) != "" {
		ids := m.providerPrefixedAuthIDs[provider]
		if ids == nil {
			ids = make(map[string]struct{})
			m.providerPrefixedAuthIDs[provider] = ids
		}
		ids[id] = struct{}{}
	}
}

func managementCatalogScalar(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case bool:
		return typed, true
	case int:
		return typed, true
	case int8:
		return typed, true
	case int16:
		return typed, true
	case int32:
		return typed, true
	case int64:
		return typed, true
	case uint:
		return typed, true
	case uint8:
		return typed, true
	case uint16:
		return typed, true
	case uint32:
		return typed, true
	case uint64:
		return typed, true
	case float32:
		return typed, true
	case float64:
		return typed, true
	case json.Number:
		return typed, true
	default:
		return nil, false
	}
}

func managementCatalogErrorCode(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' || char == ':' {
			continue
		}
		return ""
	}
	return value
}

func managementAuthCatalogSummary(auth *Auth, planType string) *Auth {
	if auth == nil {
		return nil
	}
	summary := &Auth{
		ID:            strings.TrimSpace(auth.ID),
		Index:         strings.TrimSpace(auth.Index),
		Provider:      strings.TrimSpace(auth.Provider),
		FileName:      strings.TrimSpace(auth.FileName),
		Label:         strings.TrimSpace(auth.Label),
		Status:        auth.Status,
		StatusMessage: "",
		Disabled:      auth.Disabled,
		Unavailable:   auth.Unavailable,
	}
	if strings.TrimSpace(auth.StatusMessage) != "" {
		summary.StatusMessage = "problem"
	}
	for _, key := range managementCatalogAttributeKeys {
		if value := strings.TrimSpace(auth.Attributes[key]); value != "" {
			if summary.Attributes == nil {
				summary.Attributes = make(map[string]string)
			}
			summary.Attributes[key] = value
		}
	}
	for _, key := range managementCatalogMetadataKeys {
		if value, ok := managementCatalogScalar(auth.Metadata[key]); ok {
			if summary.Metadata == nil {
				summary.Metadata = make(map[string]any)
			}
			summary.Metadata[key] = value
		}
	}
	if summary.Metadata == nil && strings.TrimSpace(planType) != "" {
		summary.Metadata = make(map[string]any, 1)
	}
	if summary.Metadata != nil {
		if _, explicit := summary.Metadata["plan_type"]; !explicit && strings.TrimSpace(planType) != "" {
			summary.Metadata["plan_type"] = strings.TrimSpace(planType)
		}
	}
	if auth.LastError != nil {
		summary.LastError = &Error{
			Code:       managementCatalogErrorCode(auth.LastError.Code),
			Retryable:  auth.LastError.Retryable,
			HTTPStatus: auth.LastError.HTTPStatus,
		}
	}
	return summary
}

func (m *Manager) updateManagementAuthCatalogLocked(auth *Auth) {
	if m == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	m.refreshChatGPTWebImageQuotaCheckLocked(auth, time.Now())
	if m.managementAuthCatalog == nil {
		m.managementAuthCatalog = make(map[string]*Auth)
	}
	id := strings.TrimSpace(auth.ID)
	next := managementAuthCatalogSummary(auth, m.authPlanTypesByID[id])
	nextUsage := usageAuthInfoLocked(auth, strings.TrimSpace(auth.Index))
	if reflect.DeepEqual(m.managementAuthCatalog[id], next) && m.usageAuthCatalog[id] == nextUsage {
		return
	}
	m.managementAuthCatalog[id] = next
	if m.usageAuthCatalog == nil {
		m.usageAuthCatalog = make(map[string]UsageAuthInfo)
	}
	m.usageAuthCatalog[id] = nextUsage
	m.managementCatalogRevision++
}

func (m *Manager) removeManagementAuthCatalogLocked(id string) {
	if m == nil {
		return
	}
	id = strings.TrimSpace(id)
	if id == "" || m.managementAuthCatalog[id] == nil {
		return
	}
	delete(m.managementAuthCatalog, id)
	delete(m.usageAuthCatalog, id)
	m.managementCatalogRevision++
}

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

func authManagedFileIndexKeys(auth *Auth) []string {
	if auth == nil {
		return nil
	}
	values := []string{auth.FileName, auth.ID}
	if auth.Attributes != nil {
		values = append(values, auth.Attributes["path"])
	}
	keys := make(map[string]struct{}, len(values)*2)
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		cleaned := filepath.ToSlash(filepath.Clean(value))
		cleaned = strings.TrimPrefix(cleaned, "./")
		if cleaned == "" || cleaned == "." {
			continue
		}
		keys[strings.ToLower(cleaned)] = struct{}{}
		if base := filepath.Base(cleaned); base != "" && base != "." {
			keys[strings.ToLower(filepath.ToSlash(base))] = struct{}{}
		}
	}
	ordered := make([]string, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Strings(ordered)
	return ordered
}

func managedFileLookupKey(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	cleaned := filepath.ToSlash(filepath.Clean(name))
	cleaned = strings.TrimPrefix(cleaned, "./")
	if cleaned == "" || cleaned == "." {
		return ""
	}
	return strings.ToLower(cleaned)
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
	m.removeProviderSchedulingIndexesLocked(id)
	delete(m.chatGPTWebImageBlockedIDs, id)
	m.removeChatGPTWebImageQuotaCheckLocked(id)
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
	if index := m.authIndexesByID[id]; index != "" {
		removeIDFromDependencySet(m.authIDsByIndex, index, id)
	}
	delete(m.authIndexesByID, id)
	for _, key := range m.managedFileKeysByAuthID[id] {
		removeIDFromDependencySet(m.managedFileAuthIDs, key, id)
	}
	delete(m.managedFileKeysByAuthID, id)
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
		addIDToDependencySet(m.authIDsByIndex, index, id)
	}
	if keys := authManagedFileIndexKeys(auth); len(keys) > 0 {
		m.managedFileKeysByAuthID[id] = keys
		for _, key := range keys {
			addIDToDependencySet(m.managedFileAuthIDs, key, id)
		}
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
	m.addProviderSchedulingIndexesLocked(auth)
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
	m.updateManagementAuthCatalogLocked(auth)
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
	m.removeManagementAuthCatalogLocked(id)
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

type managerAuthIndexState struct {
	backingPathAuthIDs         map[string]map[string]struct{}
	backingPathByAuthID        map[string]string
	backingPathAuthDir         string
	providerAuthIDs            map[string]map[string]struct{}
	providerByAuthID           map[string]string
	providerPrefixedAuthIDs    map[string]map[string]struct{}
	providerRetryByAuthID      map[string]providerRequestRetryEntry
	providerRetryAggregates    map[string]*providerRequestRetryAggregate
	chatGPTWebImageBlockedIDs  map[string]string
	chatGPTWebImageQuotaChecks map[string]chatGPTWebImageQuotaCheck
	chatGPTWebImageQuotaHeap   chatGPTWebImageQuotaCheckHeap
	chatGPTWebQuotaGeneration  uint64
	authIndexesByID            map[string]string
	authIDsByIndex             map[string]map[string]struct{}
	managedFileAuthIDs         map[string]map[string]struct{}
	managedFileKeysByAuthID    map[string][]string
	managementAuthCatalog      map[string]*Auth
	usageAuthCatalog           map[string]UsageAuthInfo
	authPlanTypesByID          map[string]string
	dependencyAuthsByID        map[string]*Auth
	dependencySourceIDs        map[string]map[string]struct{}
	dependencyDependentIDs     map[string]map[string]struct{}
	dependencyIndexComplete    bool
	chatGPTWebIdentityIDs      map[string]map[string]struct{}
	chatGPTWebIdentityKeysByID map[string][]string
	persistedAuthsByID         map[string]*Auth
	chatGPTWebIdentityComplete bool
}

func newManagerAuthIndexBuilder(auths map[string]*Auth) *Manager {
	return &Manager{
		auths:                      auths,
		backingPathAuthIDs:         make(map[string]map[string]struct{}),
		backingPathByAuthID:        make(map[string]string),
		providerAuthIDs:            make(map[string]map[string]struct{}),
		providerByAuthID:           make(map[string]string),
		providerPrefixedAuthIDs:    make(map[string]map[string]struct{}),
		providerRetryByAuthID:      make(map[string]providerRequestRetryEntry),
		providerRetryAggregates:    make(map[string]*providerRequestRetryAggregate),
		chatGPTWebImageBlockedIDs:  make(map[string]string),
		chatGPTWebImageQuotaChecks: make(map[string]chatGPTWebImageQuotaCheck),
		authIndexesByID:            make(map[string]string),
		authIDsByIndex:             make(map[string]map[string]struct{}),
		managedFileAuthIDs:         make(map[string]map[string]struct{}),
		managedFileKeysByAuthID:    make(map[string][]string),
		managementAuthCatalog:      make(map[string]*Auth),
		usageAuthCatalog:           make(map[string]UsageAuthInfo),
		authPlanTypesByID:          make(map[string]string),
		dependencyAuthsByID:        make(map[string]*Auth),
		dependencySourceIDs:        make(map[string]map[string]struct{}),
		dependencyDependentIDs:     make(map[string]map[string]struct{}),
		chatGPTWebIdentityIDs:      make(map[string]map[string]struct{}),
		chatGPTWebIdentityKeysByID: make(map[string][]string),
		persistedAuthsByID:         make(map[string]*Auth),
	}
}

func buildManagerAuthIndexState(auths map[string]*Auth, persisted []*Auth, complete bool, cfg *internalconfig.Config) managerAuthIndexState {
	if auths == nil {
		auths = make(map[string]*Auth)
	}
	builder := newManagerAuthIndexBuilder(auths)
	for _, auth := range persisted {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		builder.persistedAuthsByID[strings.TrimSpace(auth.ID)] = auth.Clone()
		if authRelevantToChatGPTWebDependencyIndex(auth) {
			builder.addDependencyAuthIndexLocked(auth)
		}
		if strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
			builder.addChatGPTWebIdentityIndexLocked(auth)
		}
	}
	if cfg != nil {
		builder.backingPathAuthDir = strings.TrimSpace(cfg.AuthDir)
	}
	for _, auth := range auths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		builder.addAuthIndexesLocked(auth, cfg)
		if !authRelevantToChatGPTWebDependencyIndex(auth) {
			builder.removeDependencyAuthIndexLocked(auth.ID)
		}
	}
	return managerAuthIndexState{
		backingPathAuthIDs:         builder.backingPathAuthIDs,
		backingPathByAuthID:        builder.backingPathByAuthID,
		backingPathAuthDir:         builder.backingPathAuthDir,
		providerAuthIDs:            builder.providerAuthIDs,
		providerByAuthID:           builder.providerByAuthID,
		providerPrefixedAuthIDs:    builder.providerPrefixedAuthIDs,
		providerRetryByAuthID:      builder.providerRetryByAuthID,
		providerRetryAggregates:    builder.providerRetryAggregates,
		chatGPTWebImageBlockedIDs:  builder.chatGPTWebImageBlockedIDs,
		chatGPTWebImageQuotaChecks: builder.chatGPTWebImageQuotaChecks,
		chatGPTWebImageQuotaHeap:   builder.chatGPTWebImageQuotaHeap,
		chatGPTWebQuotaGeneration:  builder.chatGPTWebQuotaGeneration,
		authIndexesByID:            builder.authIndexesByID,
		authIDsByIndex:             builder.authIDsByIndex,
		managedFileAuthIDs:         builder.managedFileAuthIDs,
		managedFileKeysByAuthID:    builder.managedFileKeysByAuthID,
		managementAuthCatalog:      builder.managementAuthCatalog,
		usageAuthCatalog:           builder.usageAuthCatalog,
		authPlanTypesByID:          builder.authPlanTypesByID,
		dependencyAuthsByID:        builder.dependencyAuthsByID,
		dependencySourceIDs:        builder.dependencySourceIDs,
		dependencyDependentIDs:     builder.dependencyDependentIDs,
		dependencyIndexComplete:    complete,
		chatGPTWebIdentityIDs:      builder.chatGPTWebIdentityIDs,
		chatGPTWebIdentityKeysByID: builder.chatGPTWebIdentityKeysByID,
		persistedAuthsByID:         builder.persistedAuthsByID,
		chatGPTWebIdentityComplete: complete,
	}
}

func (m *Manager) applyManagerAuthIndexStateLocked(state managerAuthIndexState) {
	m.backingPathAuthIDs = state.backingPathAuthIDs
	m.backingPathByAuthID = state.backingPathByAuthID
	m.backingPathAuthDir = state.backingPathAuthDir
	m.providerAuthIDs = state.providerAuthIDs
	m.providerByAuthID = state.providerByAuthID
	m.providerPrefixedAuthIDs = state.providerPrefixedAuthIDs
	m.providerRetryByAuthID = state.providerRetryByAuthID
	m.providerRetryAggregates = state.providerRetryAggregates
	m.chatGPTWebImageBlockedIDs = state.chatGPTWebImageBlockedIDs
	m.chatGPTWebImageQuotaChecks = state.chatGPTWebImageQuotaChecks
	m.chatGPTWebImageQuotaHeap = state.chatGPTWebImageQuotaHeap
	m.chatGPTWebQuotaGeneration = state.chatGPTWebQuotaGeneration
	m.authIndexesByID = state.authIndexesByID
	m.authIDsByIndex = state.authIDsByIndex
	m.managedFileAuthIDs = state.managedFileAuthIDs
	m.managedFileKeysByAuthID = state.managedFileKeysByAuthID
	if !reflect.DeepEqual(m.managementAuthCatalog, state.managementAuthCatalog) || !reflect.DeepEqual(m.usageAuthCatalog, state.usageAuthCatalog) {
		m.managementAuthCatalog = state.managementAuthCatalog
		m.usageAuthCatalog = state.usageAuthCatalog
		m.managementCatalogRevision++
	}
	m.authPlanTypesByID = state.authPlanTypesByID
	m.dependencyAuthsByID = state.dependencyAuthsByID
	m.dependencySourceIDs = state.dependencySourceIDs
	m.dependencyDependentIDs = state.dependencyDependentIDs
	m.dependencyIndexComplete = state.dependencyIndexComplete
	m.chatGPTWebIdentityIDs = state.chatGPTWebIdentityIDs
	m.chatGPTWebIdentityKeysByID = state.chatGPTWebIdentityKeysByID
	m.persistedAuthsByID = state.persistedAuthsByID
	m.chatGPTWebIdentityComplete = state.chatGPTWebIdentityComplete
	m.persistedAuthRevision++
	m.authIndexRevision++
}

// rebuildAuthIndexesLocked rebuilds the runtime path index and the dependency
// view. Persisted records are installed first so runtime records win by ID.
// The caller must hold m.mu for writing.
func (m *Manager) rebuildAuthIndexesLocked(persisted []*Auth, complete bool) {
	cfg := m.currentConfig()
	state := buildManagerAuthIndexState(m.auths, persisted, complete, cfg)
	m.applyManagerAuthIndexStateLocked(state)
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

// ChatGPTWebDependencyGraphForAuths builds the dependency view needed to
// render the supplied auths without cloning unrelated credentials. The cost is
// proportional to the targets and their linked sources or dependents.
func (m *Manager) ChatGPTWebDependencyGraphForAuths(auths []*Auth) (*ChatGPTWebDependencyGraph, bool) {
	if m == nil || len(auths) == 0 {
		return BuildChatGPTWebDependencyGraph(nil), false
	}
	targets := make(map[string]*Auth, len(auths))
	neededIDs := make(map[string]struct{}, len(auths))
	for _, auth := range auths {
		if auth == nil || strings.TrimSpace(auth.ID) == "" {
			continue
		}
		id := strings.TrimSpace(auth.ID)
		targets[id] = auth
		neededIDs[id] = struct{}{}
	}
	m.mu.RLock()
	for _, auth := range targets {
		if uid := ChatGPTWebCredentialUID(auth); uid != "" && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			for id := range m.dependencySourceIDs[uid] {
				neededIDs[id] = struct{}{}
			}
			for id := range m.dependencyDependentIDs[uid] {
				neededIDs[id] = struct{}{}
			}
		}
		if uid := ChatGPTWebLinkedSourceUID(auth); uid != "" {
			for id := range m.dependencySourceIDs[uid] {
				neededIDs[id] = struct{}{}
			}
		}
	}
	ids := make([]string, 0, len(neededIDs))
	for id := range neededIDs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	graphAuths := make([]*Auth, 0, len(ids))
	for _, id := range ids {
		if target := targets[id]; target != nil {
			graphAuths = append(graphAuths, target.Clone())
			continue
		}
		if indexed := m.dependencyAuthsByID[id]; indexed != nil {
			graphAuths = append(graphAuths, indexed.Clone())
		}
	}
	complete := m.dependencyIndexComplete
	m.mu.RUnlock()
	return BuildChatGPTWebDependencyGraph(graphAuths), complete
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

// AuthIDsForProviders returns stable runtime IDs without cloning credentials.
func (m *Manager) AuthIDsForProviders(providers ...string) []string {
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
	m.mu.RUnlock()
	sort.Strings(ids)
	return ids
}

// AuthIDs returns all runtime IDs in stable order without cloning credentials.
func (m *Manager) AuthIDs() []string {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	ids := make([]string, 0, len(m.auths))
	for id := range m.auths {
		ids = append(ids, id)
	}
	m.mu.RUnlock()
	sort.Strings(ids)
	return ids
}

// ManagementAuthCatalogRevision returns the revision of list-visible auth fields.
func (m *Manager) ManagementAuthCatalogRevision() uint64 {
	if m == nil {
		return 0
	}
	m.mu.RLock()
	revision := m.managementCatalogRevision
	m.mu.RUnlock()
	return revision
}

// ManagementAuthCatalogSnapshot returns non-sensitive auth summaries for list filtering.
func (m *Manager) ManagementAuthCatalogSnapshot() (uint64, []*Auth) {
	if m == nil {
		return 0, nil
	}
	m.mu.RLock()
	revision := m.managementCatalogRevision
	ids := make([]string, 0, len(m.managementAuthCatalog))
	for id := range m.managementAuthCatalog {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	auths := make([]*Auth, 0, len(ids))
	for _, id := range ids {
		if auth := m.managementAuthCatalog[id]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return revision, auths
}

// GetByIDs clones only the requested runtime credentials in request order.
func (m *Manager) GetByIDs(ids []string) []*Auth {
	if m == nil || len(ids) == 0 {
		return nil
	}
	m.mu.RLock()
	auths := make([]*Auth, 0, len(ids))
	for _, id := range ids {
		if auth := m.auths[strings.TrimSpace(id)]; auth != nil {
			auths = append(auths, auth.Clone())
		}
	}
	m.mu.RUnlock()
	return auths
}

// GetByAuthIndex returns the first stable runtime credential for an auth index.
func (m *Manager) GetByAuthIndex(index string) (*Auth, bool) {
	if m == nil || strings.TrimSpace(index) == "" {
		return nil, false
	}
	index = strings.TrimSpace(index)
	m.mu.RLock()
	ids := m.authIDsByIndex[index]
	ordered := make([]string, 0, len(ids))
	for id := range ids {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	var auth *Auth
	for _, id := range ordered {
		if current := m.auths[id]; current != nil {
			auth = current.Clone()
			break
		}
	}
	m.mu.RUnlock()
	return auth, auth != nil
}

// GetByAuthIndexes resolves the first stable runtime credential for each index.
func (m *Manager) GetByAuthIndexes(indexes []string) map[string]*Auth {
	result := make(map[string]*Auth, len(indexes))
	if m == nil || len(indexes) == 0 {
		return result
	}
	m.mu.RLock()
	for _, rawIndex := range indexes {
		index := strings.TrimSpace(rawIndex)
		if index == "" || result[index] != nil {
			continue
		}
		ids := m.authIDsByIndex[index]
		ordered := make([]string, 0, len(ids))
		for id := range ids {
			ordered = append(ordered, id)
		}
		sort.Strings(ordered)
		for _, id := range ordered {
			if auth := m.auths[id]; auth != nil {
				result[index] = auth.Clone()
				break
			}
		}
	}
	m.mu.RUnlock()
	return result
}

// AuthsForManagedFileName returns indexed filename candidates in stable ID order.
func (m *Manager) AuthsForManagedFileName(name string) []*Auth {
	if m == nil {
		return nil
	}
	key := managedFileLookupKey(name)
	if key == "" {
		return nil
	}
	m.mu.RLock()
	ids := m.managedFileAuthIDs[key]
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
