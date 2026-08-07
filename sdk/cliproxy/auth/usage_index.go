package auth

import (
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
	items := make([]UsageAuthInfo, 0, len(m.authIndexesByID))
	for id, authIndex := range m.authIndexesByID {
		if auth := m.auths[id]; auth != nil {
			items = append(items, usageAuthInfoLocked(auth, authIndex))
		}
	}
	m.mu.RUnlock()
	return items
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
		if auth := m.auths[id]; auth != nil && (!found || id < result.ID) {
			result = usageAuthInfoLocked(auth, authIndex)
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
