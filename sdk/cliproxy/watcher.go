package cliproxy

import (
	"context"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func defaultWatcherFactory(configPath, authDir string, reload func(*config.Config)) (*WatcherWrapper, error) {
	w, err := watcher.NewWatcher(configPath, authDir, reload)
	if err != nil {
		return nil, err
	}

	return &WatcherWrapper{
		start: func(ctx context.Context) error {
			return w.Start(ctx)
		},
		stop: func() error {
			return w.Stop()
		},
		setConfig: func(cfg *config.Config) {
			w.SetConfig(cfg)
		},
		snapshotAuths:        func() []*coreauth.Auth { return w.SnapshotCoreAuths() },
		seedCurrentFileAuths: func(auths []*coreauth.Auth) { w.SeedCurrentFileAuths(auths) },
		setUpdateQueue: func(queue chan<- watcher.AuthUpdate) {
			w.SetAuthUpdateQueue(queue)
		},
		dispatchRuntimeUpdate: func(update watcher.AuthUpdate) watcher.RuntimeAuthUpdateResult {
			return w.DispatchRuntimeAuthUpdateResult(update)
		},
		waitForAuthUpdates: func(ctx context.Context) error {
			return w.WaitForAuthUpdates(ctx)
		},
	}, nil
}
