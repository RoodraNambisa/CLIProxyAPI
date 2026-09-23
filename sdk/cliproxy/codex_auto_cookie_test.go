package cliproxy

import (
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/codexcookie"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestAutoCookieServiceLifecycle(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	a, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{ID: t.Name(), Provider: "codex", Metadata: map[string]any{"account_id": "owner"}})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.Codex.AutoCookie = true
	s := &Service{cfg: cfg, coreManager: manager}
	s.syncCodexState(cfg)
	t.Cleanup(func() { codexcookie.Default.Sync(false, nil) })
	target := "https://chatgpt.com/backend-api/codex/responses"
	store := codexcookie.Default.Acquire(a.ID, helps.StateCredential(a, "").Owner)
	store.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=first; Path=/"}})
	updated := a.Clone()
	updated.Metadata["access_token"] = "refreshed"
	if _, err = manager.Update(coreauth.WithSkipPersist(t.Context()), updated); err != nil {
		t.Fatal(err)
	}
	s.syncCodexState(cfg)
	if store.Header(target) != "__oailb=first" {
		t.Fatal("refresh removed cookies")
	}
	if err = s.deleteCoreAuth(coreauth.WithSkipPersist(t.Context()), a.ID); err != nil {
		t.Fatal(err)
	}
	store.StoreResponse(target, http.Header{"Set-Cookie": {"__oailb=late; Path=/"}})
	if store.Header(target) != "" {
		t.Fatal("delete failed to retire jar immediately")
	}
	s.syncCodexState(&config.Config{})
}
