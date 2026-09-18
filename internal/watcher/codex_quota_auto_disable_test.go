package watcher

import (
	"context"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type quotaDisableWatchExecutor struct {
	coreauth.ProviderExecutor
	inspect func(context.Context)
}

func (*quotaDisableWatchExecutor) Identifier() string { return "codex" }
func (e *quotaDisableWatchExecutor) Execute(ctx context.Context, auth *coreauth.Auth, _ core.Request, _ core.Options) (core.Response, error) {
	observer := core.CodexQuotaObserverFromContext(ctx)
	observer(auth.ID, auth.RuntimeInstanceID(), "http", http.Header{
		"X-Codex-Active-Limit": {"premium"}, "X-Codex-Primary-Window-Minutes": {"300"}, "X-Codex-Primary-Used-Percent": {"99"},
	})
	e.inspect(ctx)
	return core.Response{Payload: []byte(`{"model":"quota-fixture","output":[]}`)}, nil
}

func TestCodexQuotaAutoDisableWatcherEchoPreservesActiveRequest(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	threshold := 5.0
	cfg := &config.Config{AuthDir: authDir, Codex: config.CodexConfig{
		ObserveQuota: true, QuotaAutoDisable: config.CodexQuotaAutoDisableConfig{Enabled: true,
			Rules: []config.CodexQuotaAutoDisableRule{{FiveHourRemainingPercent: &threshold}},
		},
	}}
	manager.SetConfig(cfg)
	w := &Watcher{authDir: authDir, config: cfg, lastAuthHashes: make(map[string]string), fileAuthsByPath: make(map[string]map[string]*coreauth.Auth), currentAuths: make(map[string]*coreauth.Auth)}
	exec := &quotaDisableWatchExecutor{}
	manager.RegisterExecutor(exec)
	a, err := manager.Register(t.Context(), &coreauth.Auth{ID: "quota.json", FileName: "quota.json", Provider: "codex", Metadata: map[string]any{"type": "codex", "access_token": "fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(authDir, "quota.json")
	w.addOrUpdateClient(path)
	queue := make(chan AuthUpdate, 1)
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()
	registry.GetGlobalRegistry().RegisterClient(a.ID, "codex", []*registry.ModelInfo{{ID: "quota-fixture"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(a.ID) })
	exec.inspect = func(ctx context.Context) {
		w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
		select {
		case update := <-queue:
			t.Fatalf("manager-owned disable dispatched a destructive replacement: %+v", update.Action)
		case <-time.After(50 * time.Millisecond):
		}
		if ctx.Err() != nil {
			t.Fatal("active request canceled by persistence echo")
		}
		w.clientsMutex.RLock()
		adopted := w.currentAuths[a.ID].Clone()
		w.clientsMutex.RUnlock()
		if !adopted.Disabled || adopted.StatusMessage == "" {
			t.Fatal("watcher did not adopt disabled state and reason")
		}
	}
	if _, err = manager.Execute(t.Context(), []string{"codex"}, core.Request{Model: "quota-fixture", Payload: []byte(`{}`)}, core.Options{}); err != nil {
		t.Fatal(err)
	}
	restarted := coreauth.NewManager(store, nil, nil)
	restarted.SetConfig(cfg)
	if err = restarted.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	current, _ := restarted.GetByID(a.ID)
	if current == nil || !current.Disabled || current.StatusMessage == "" {
		t.Fatal("restart lost persisted disable state")
	}
}
