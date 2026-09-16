package auth

import (
	"net/http"
	"strings"

	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
)

const RoutingAliasMetadataKey = "routing_alias"

// ExecutionProvider returns the runtime provider, including named compatibility routes.
func (a *Auth) ExecutionProvider() string { return executorKeyFromAuth(a) }

// CredentialRoutingAlias is independent of filename, priority and model prefix.
func CredentialRoutingAlias(auth *Auth) string {
	if auth == nil {
		return ""
	}
	raw, _ := auth.Metadata[RoutingAliasMetadataKey].(string)
	alias, _ := sdkaccess.NormalizeCredentialTarget(raw)
	return alias
}

// ResolveCredentialTarget refuses ambiguous aliases instead of choosing one.
// Only the selected record is cloned; bulk credential metadata is not copied.
func (m *Manager) ResolveCredentialTarget(selector string) (*Auth, error) {
	selector, err := sdkaccess.NormalizeCredentialTarget(selector)
	if err != nil {
		return nil, &Error{Code: "invalid_credential_target", Message: err.Error(), HTTPStatus: http.StatusBadRequest}
	}
	if m == nil {
		return nil, &Error{Code: "credential_target_unavailable", Message: "Credential targeting is unavailable", HTTPStatus: http.StatusServiceUnavailable}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var selected *Auth
	for _, auth := range m.auths {
		if auth == nil || auth.Index != selector && CredentialRoutingAlias(auth) != selector {
			continue
		}
		if selected != nil && selected.ID != auth.ID {
			return nil, &Error{Code: "credential_target_ambiguous", Message: "Credential alias is ambiguous; use its credential ID", HTTPStatus: http.StatusConflict}
		}
		selected = auth
	}
	if selected == nil {
		return nil, &Error{Code: "credential_target_not_found", Message: "Selected credential was not found", HTTPStatus: http.StatusNotFound}
	}
	if selected.Disabled || selected.Status == StatusDisabled || IsRetiredGeminiCLIAuth(selected) || ChatGPTWebAuthRetainedForDependents(selected) {
		return nil, &Error{Code: "credential_target_disabled", Message: "Selected credential is disabled or retired", HTTPStatus: http.StatusForbidden}
	}
	return selected.Clone(), nil
}

// NormalizeCredentialRoutingAlias also accepts an empty value to clear an alias.
func NormalizeCredentialRoutingAlias(raw string) (string, error) {
	if strings.TrimSpace(raw) == "" {
		return "", nil
	}
	return sdkaccess.NormalizeCredentialTarget(raw)
}
