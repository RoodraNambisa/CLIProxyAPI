// Package watcher watches config/auth files and triggers hot reloads.
// It supports cross-platform fsnotify event handling.
package watcher

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"gopkg.in/yaml.v3"

	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// storePersister captures persistence-capable token store methods used by the watcher.
type storePersister interface {
	PersistConfig(ctx context.Context) error
	PersistAuthFiles(ctx context.Context, message string, paths ...string) error
}

type authDeleteFinalizer interface {
	FinalizeAuthFileDeletion(ctx context.Context, id string) error
}

type authDeleteAtBaseDirFinalizer interface {
	FinalizeAuthFileDeletionAtBaseDir(ctx context.Context, baseDir string, id string) error
}

type authDirProvider interface {
	AuthDir() string
}

// Watcher manages file watching for configuration and authentication files
type Watcher struct {
	configPath           string
	authDir              string
	config               *config.Config
	clientsMutex         sync.RWMutex
	configReloadMu       sync.Mutex
	configReloadTimer    *time.Timer
	serverUpdateMu       sync.Mutex
	serverUpdateTimer    *time.Timer
	serverUpdateLast     time.Time
	serverUpdatePend     bool
	stopped              atomic.Bool
	reloadCallback       func(*config.Config)
	watcher              *fsnotify.Watcher
	lastAuthHashes       map[string]string
	lastAuthContents     map[string]*coreauth.Auth
	fileAuthsByPath      map[string]map[string]*coreauth.Auth
	retiredAuthPaths     map[string]struct{}
	retiredDeleteHashes  map[string]string
	retiredDeleteStates  map[string]*authfileguard.DeleteGeneration
	retiredDeleteSeq     uint64
	retiredDeletes       map[string]uint64
	authRetryBase        time.Duration
	authRetryMu          sync.Mutex
	authRetryTimers      map[*time.Timer]struct{}
	authRetryWG          sync.WaitGroup
	authWorkMu           sync.Mutex
	authWorkContext      context.Context
	authWorkCancel       context.CancelFunc
	authWorkWG           sync.WaitGroup
	eventMu              sync.Mutex
	eventCancel          context.CancelFunc
	eventDone            chan struct{}
	eventInitialized     bool
	eventPathsAdded      bool
	lastRemoveTimes      map[string]time.Time
	lastConfigHash       string
	authQueue            chan<- AuthUpdate
	currentAuths         map[string]*coreauth.Auth
	runtimeAuths         map[string]*coreauth.Auth
	dispatchMu           sync.Mutex
	dispatchLifecycleMu  sync.Mutex
	dispatchCond         *sync.Cond
	pendingUpdates       map[string]AuthUpdate
	pendingOrder         []string
	dispatchCancel       context.CancelFunc
	dispatchDone         chan struct{}
	dependencyTimer      *time.Timer
	dependencyPending    bool
	dependencyGeneration uint64
	dependencyDebounce   time.Duration
	dependencyMaxDelay   time.Duration
	dependencyFirstAt    time.Time
	storePersister       storePersister
	mirroredAuthDir      string
	oldConfigYaml        []byte
}

// AuthUpdateAction represents the type of change detected in auth sources.
type AuthUpdateAction string

const (
	AuthUpdateActionAdd                             AuthUpdateAction = "add"
	AuthUpdateActionModify                          AuthUpdateAction = "modify"
	AuthUpdateActionDelete                          AuthUpdateAction = "delete"
	AuthUpdateActionBarrier                         AuthUpdateAction = "barrier"
	AuthUpdateActionReconcileChatGPTWebDependencies AuthUpdateAction = "reconcile_chatgpt_web_dependencies"
)

// AuthUpdate describes an incremental change to auth configuration.
type AuthUpdate struct {
	Action  AuthUpdateAction
	ID      string
	Auth    *coreauth.Auth
	Applied chan struct{}
}

// RuntimeAuthUpdateResult describes how a runtime update was handled by the watcher.
type RuntimeAuthUpdateResult struct {
	Enqueued bool
	Consumed bool
	Fallback *AuthUpdate
}

const (
	// replaceCheckDelay is a short delay to allow atomic replace (rename) to settle
	// before deciding whether a Remove event indicates a real deletion.
	replaceCheckDelay        = 50 * time.Millisecond
	configReloadDebounce     = 150 * time.Millisecond
	authRemoveDebounceWindow = 1 * time.Second
	serverUpdateDebounce     = 1 * time.Second
	authPersistenceRetryBase = 250 * time.Millisecond
	authDependencyDebounce   = authRemoveDebounceWindow
	authDependencyMaxDelay   = 5 * authDependencyDebounce
)

var authPersistenceShutdownWait = 5 * time.Second

// NewWatcher creates a new file watcher instance
func NewWatcher(configPath, authDir string, reloadCallback func(*config.Config)) (*Watcher, error) {
	watcher, errNewWatcher := fsnotify.NewWatcher()
	if errNewWatcher != nil {
		return nil, errNewWatcher
	}
	w := &Watcher{
		configPath:          configPath,
		authDir:             authDir,
		reloadCallback:      reloadCallback,
		watcher:             watcher,
		lastAuthHashes:      make(map[string]string),
		fileAuthsByPath:     make(map[string]map[string]*coreauth.Auth),
		retiredAuthPaths:    make(map[string]struct{}),
		retiredDeleteHashes: make(map[string]string),
		retiredDeleteStates: make(map[string]*authfileguard.DeleteGeneration),
	}
	w.dispatchCond = sync.NewCond(&w.dispatchMu)
	if store := sdkAuth.GetTokenStore(); store != nil {
		if persister, ok := store.(storePersister); ok {
			w.storePersister = persister
			log.Debug("persistence-capable token store detected; watcher will propagate persisted changes")
		}
		if provider, ok := store.(authDirProvider); ok {
			if fixed := strings.TrimSpace(provider.AuthDir()); fixed != "" {
				w.mirroredAuthDir = fixed
				log.Debugf("mirrored auth directory locked to %s", fixed)
			}
		}
	}
	return w, nil
}

// Start begins watching the configuration file and authentication directory
func (w *Watcher) Start(ctx context.Context) error {
	return w.start(ctx)
}

// Stop stops the file watcher
func (w *Watcher) Stop() error {
	w.stopEventLoop()
	w.stopAuthPersistenceRetryTimers()
	w.stopAuthPersistenceTasks()
	w.stopDispatch()
	w.stopConfigReloadTimer()
	w.stopServerUpdateTimer()
	return w.watcher.Close()
}

// SetConfig updates the current configuration
func (w *Watcher) SetConfig(cfg *config.Config) {
	w.clientsMutex.Lock()
	defer w.clientsMutex.Unlock()
	w.config = cfg
	w.oldConfigYaml, _ = yaml.Marshal(cfg)
}

// SetAuthUpdateQueue sets the queue used to emit auth updates.
func (w *Watcher) SetAuthUpdateQueue(queue chan<- AuthUpdate) {
	w.setAuthUpdateQueue(queue)
}

// DispatchRuntimeAuthUpdate allows external runtime providers (e.g., websocket-driven auths)
// to push auth updates through the same queue used by file/config watchers.
// Returns true if the update was enqueued; false if no queue is configured.
func (w *Watcher) DispatchRuntimeAuthUpdate(update AuthUpdate) bool {
	return w.DispatchRuntimeAuthUpdateResult(update).Enqueued
}

// DispatchRuntimeAuthUpdateResult preserves rejected updates and translated fallbacks for SDK integration.
func (w *Watcher) DispatchRuntimeAuthUpdateResult(update AuthUpdate) RuntimeAuthUpdateResult {
	return w.dispatchRuntimeAuthUpdate(update)
}

// SnapshotCoreAuths converts current clients snapshot into core auth entries.
func (w *Watcher) SnapshotCoreAuths() []*coreauth.Auth {
	w.clientsMutex.RLock()
	cfg := w.config
	w.clientsMutex.RUnlock()
	return snapshotCoreAuths(cfg, w.authDir)
}
