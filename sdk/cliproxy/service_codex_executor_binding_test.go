package cliproxy

import (
	"net/http"
	"sync"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestCodexExecutorBindingReadsConfigurationSnapshot(t *testing.T) {
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, nil, nil)}
	auth := &coreauth.Auth{ID: "config-binding-fixture", Provider: "codex"}
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 200 {
			cfg := &config.Config{}
			cfg.CodexHeaderDefaults.BetaFeatures = "snapshot-fixture"
			service.cfgMu.Lock()
			service.cfg = cfg
			service.cfgMu.Unlock()
		}
	})
	workers.Go(func() {
		for range 200 {
			service.ensureExecutorsForAuthWithMode(auth, true)
		}
	})
	workers.Wait()
	service.ensureExecutorsForAuthWithMode(auth, true)
	exec, ok := service.coreManager.Executor("codex")
	if !ok {
		t.Fatal("Codex executor was not bound")
	}
	preparer, ok := exec.(interface {
		PrepareRequest(*http.Request, *coreauth.Auth) error
	})
	if !ok {
		t.Fatal("Codex executor lost request preparation")
	}
	r, err := http.NewRequest(http.MethodPost, "https://fixture.invalid/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = preparer.PrepareRequest(r, auth); err != nil {
		t.Fatal(err)
	}
	if r.Header.Get("X-Codex-Beta-Features") != "snapshot-fixture" {
		t.Fatal("final binding did not use the latest configuration")
	}
}

func TestEnsureExecutorsForAuth_CodexDoesNotReplaceInNormalMode(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "codex-auth-1",
		Provider: "codex",
		Status:   coreauth.StatusActive,
	}

	service.ensureExecutorsForAuth(auth)
	firstExecutor, okFirst := service.coreManager.Executor("codex")
	if !okFirst || firstExecutor == nil {
		t.Fatal("expected codex executor after first bind")
	}

	service.ensureExecutorsForAuth(auth)
	secondExecutor, okSecond := service.coreManager.Executor("codex")
	if !okSecond || secondExecutor == nil {
		t.Fatal("expected codex executor after second bind")
	}

	if firstExecutor != secondExecutor {
		t.Fatal("expected codex executor to stay unchanged in normal mode")
	}
}

func TestEnsureExecutorsForAuthWithMode_CodexForceReplace(t *testing.T) {
	service := &Service{
		cfg:         &config.Config{},
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	auth := &coreauth.Auth{
		ID:       "codex-auth-2",
		Provider: "codex",
		Status:   coreauth.StatusActive,
	}

	service.ensureExecutorsForAuth(auth)
	firstExecutor, okFirst := service.coreManager.Executor("codex")
	if !okFirst || firstExecutor == nil {
		t.Fatal("expected codex executor after first bind")
	}

	service.ensureExecutorsForAuthWithMode(auth, true)
	secondExecutor, okSecond := service.coreManager.Executor("codex")
	if !okSecond || secondExecutor == nil {
		t.Fatal("expected codex executor after forced rebind")
	}

	if firstExecutor == secondExecutor {
		t.Fatal("expected codex executor replacement in force mode")
	}
}
