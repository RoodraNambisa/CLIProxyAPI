package store

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

// A manager-owned portable binding update is already installed in runtime.
// Remote-store mirrors must not echo it back as a credential replacement.
func markProxyBindingMemoryPersistence(ctx context.Context, auth *coreauth.Auth, path string, data []byte) {
	if !authfileguard.ManagerOwnedPersistence(ctx) || coreauth.ReadProxyBindingMemory(auth) == nil || len(data) == 0 {
		return
	}
	authfileguard.MarkManagerPersistedGeneration(path, coreauth.SourceHashFromBytes(data))
}
