package watcher

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestCredentialRequestRetryReloadEmitsOnlyCredentialModification(t *testing.T) {
	cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "test", BaseURL: "https://example.test"}}}
	initial := snapshotConfigAuths(cfg, t.TempDir())
	if len(initial) != 1 {
		t.Fatal("missing initial credential")
	}
	id := initial[0].ID
	watcher := &Watcher{currentAuths: map[string]*coreauth.Auth{id: initial[0]}, authQueue: make(chan AuthUpdate, 1)}
	for _, retry := range []*int{new(2), new(0), nil} {
		cfg.CodexKey[0].RequestRetry = retry
		next := snapshotConfigAuths(cfg, t.TempDir())
		updates, stats := watcher.prepareAuthUpdatesLockedWithStats(next, false)
		if len(updates) != 1 || stats.modified != 1 || stats.added != 0 || stats.deleted != 0 || updates[0].Action != AuthUpdateActionModify || updates[0].ID != id {
			t.Fatal("retry edit was ignored or replaced the credential identity")
		}
		if value, present := updates[0].Auth.RequestRetryOverride(); present != (retry != nil) || (retry != nil && value != *retry) {
			t.Fatal("reload notification lost its retry override")
		}
		if unchanged, stats := watcher.prepareAuthUpdatesLockedWithStats(snapshotConfigAuths(cfg, t.TempDir()), false); len(unchanged) != 0 || stats.unchanged != 1 {
			t.Fatal("unchanged retry config generated another credential update")
		}
	}
}
