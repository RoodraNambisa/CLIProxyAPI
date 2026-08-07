package auth

import (
	"sort"
	"strings"
)

// UsageAuthInfo is the non-sensitive credential metadata exposed to usage
// management endpoints. It deliberately excludes tokens, cookies, model maps,
// and arbitrary credential metadata.
type UsageAuthInfo struct {
	AuthIndex   string
	ID          string
	Name        string
	Provider    string
	Label       string
	Status      string
	Disabled    bool
	AccountType string
	Account     string
	Email       string
}

// ListUsageAuthInfos returns lightweight summaries for all runtime credentials
// without cloning credential payloads. Callers apply their requested ordering.
func (m *Manager) ListUsageAuthInfos() []UsageAuthInfo {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	ids := make([]string, 0, len(m.usageAuthCatalog))
	for id := range m.usageAuthCatalog {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]UsageAuthInfo, 0, len(ids))
	for _, id := range ids {
		items = append(items, m.usageAuthCatalog[id])
	}
	m.mu.RUnlock()
	return items
}

// UsageAuthCatalogSnapshot returns the revision and lightweight usage metadata.
func (m *Manager) UsageAuthCatalogSnapshot() (uint64, []UsageAuthInfo) {
	if m == nil {
		return 0, nil
	}
	m.mu.RLock()
	revision := m.managementCatalogRevision
	ids := make([]string, 0, len(m.usageAuthCatalog))
	for id := range m.usageAuthCatalog {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	items := make([]UsageAuthInfo, 0, len(ids))
	for _, id := range ids {
		items = append(items, m.usageAuthCatalog[id])
	}
	m.mu.RUnlock()
	return revision, items
}

// UsageAuthInfoByIndex returns one lightweight runtime credential summary.
func (m *Manager) UsageAuthInfoByIndex(authIndex string) (UsageAuthInfo, bool) {
	if m == nil {
		return UsageAuthInfo{}, false
	}
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" {
		return UsageAuthInfo{}, false
	}
	m.mu.RLock()
	var result UsageAuthInfo
	found := false
	for id := range m.authIDsByIndex[authIndex] {
		if info, ok := m.usageAuthCatalog[id]; ok && (!found || id < result.ID) {
			result = info
			found = true
		}
	}
	m.mu.RUnlock()
	return result, found
}

func usageAuthInfoLocked(auth *Auth, authIndex string) UsageAuthInfo {
	if auth == nil {
		return UsageAuthInfo{}
	}
	name := strings.TrimSpace(auth.FileName)
	if name == "" {
		name = strings.TrimSpace(auth.ID)
	}
	email := authUsageEmail(auth)
	accountType, account := auth.AccountInfo()
	// API keys must never be copied into usage-management summaries.
	if strings.EqualFold(accountType, "api_key") {
		account = ""
	}
	return UsageAuthInfo{
		AuthIndex:   strings.TrimSpace(authIndex),
		ID:          strings.TrimSpace(auth.ID),
		Name:        name,
		Provider:    strings.TrimSpace(auth.Provider),
		Label:       strings.TrimSpace(auth.Label),
		Status:      string(auth.Status),
		Disabled:    auth.Disabled,
		AccountType: strings.TrimSpace(accountType),
		Account:     strings.TrimSpace(account),
		Email:       email,
	}
}

func authUsageEmail(auth *Auth) string {
	if auth == nil {
		return ""
	}
	if value, ok := auth.Metadata["email"].(string); ok {
		if email := strings.TrimSpace(value); email != "" {
			return email
		}
	}
	if email := strings.TrimSpace(auth.Attributes["email"]); email != "" {
		return email
	}
	return strings.TrimSpace(auth.Attributes["account_email"])
}
