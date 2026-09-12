package auth

import (
	"path"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/managementdiag"
)

// LogIdentity returns a bounded display name without paths or authentication material.
// File names intentionally remain recognizable in operational logs, including email names.
func (a *Auth) LogIdentity() logging.CredentialIdentity {
	if a == nil {
		return logging.CredentialIdentity{}
	}
	name := strings.TrimSpace(a.Label)
	if file := strings.TrimSpace(a.FileName); file != "" {
		name = path.Base(strings.ReplaceAll(file, `\`, "/"))
	}
	for _, key := range []string{"api_key", "access_token", "refresh_token", "id_token"} {
		for _, value := range []any{a.Attributes[key], a.Metadata[key]} {
			if secret, ok := value.(string); ok && secret != "" {
				name = strings.ReplaceAll(name, secret, "<redacted-key>")
			}
		}
	}
	name, _ = managementdiag.ProcessText(name, managementdiag.DetailLevelFull, 512)
	name = strings.NewReplacer("\n", " ", "\t", " ").Replace(name)
	return logging.CredentialIdentity{Provider: a.Provider, Index: a.EnsureIndex(), Name: name}
}
