package api

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestServerDerivedCodexHandlerPolicyFollowsSuccessfulReload(t *testing.T) {
	server := newTestServerWithConfig(t, func(cfg *config.Config) { cfg.Codex.OrphanDelegationCompatibility = true })
	previous := server.handlers.ConfigSnapshot()
	if !previous.CodexOrphanDelegationCompatibility {
		t.Fatal("startup lost derived Codex policy")
	}
	next, err := config.Clone(server.currentConfig())
	if err != nil {
		t.Fatal(err)
	}
	next.Codex.OrphanDelegationCompatibility = false
	if err := server.UpdateClients(next); err != nil {
		t.Fatal(err)
	}
	if server.handlers.ConfigSnapshot().CodexOrphanDelegationCompatibility || !previous.CodexOrphanDelegationCompatibility {
		t.Fatal("reload failed to isolate old/new handler policy")
	}
	if next.SDKConfig.CodexOrphanDelegationCompatibility {
		t.Fatal("derived field contaminated saved config")
	}
}
