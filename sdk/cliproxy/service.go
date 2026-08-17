// Package cliproxy provides the core service implementation for the CLI Proxy API.
// It includes service lifecycle management, authentication handling, file watching,
// and integration with various AI service providers through a unified interface.
package cliproxy

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/api"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	internalcodex "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/codex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	executorhelps "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	internalusage "github.com/router-for-me/CLIProxyAPI/v6/internal/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/wsrelay"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	sdkusage "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Service wraps the proxy server lifecycle so external programs can embed the CLI proxy.
// It manages the complete lifecycle including authentication, file watching, HTTP server,
// and integration with various AI service providers.
type Service struct {
	// cfg holds the current application configuration.
	cfg *config.Config

	// cfgMu protects concurrent access to the configuration.
	cfgMu sync.RWMutex

	// runtimeConfigApplyMu serializes complete runtime configuration updates
	// from management writes and file watcher reloads.
	runtimeConfigApplyMu sync.Mutex
	runtimeConfigDigest  [sha256.Size]byte
	runtimeConfigHashed  bool

	// configPath is the path to the configuration file.
	configPath string

	// tokenProvider handles loading token-based clients.
	tokenProvider TokenClientProvider

	// apiKeyProvider handles loading API key-based clients.
	apiKeyProvider APIKeyClientProvider

	// watcherFactory creates file watcher instances.
	watcherFactory WatcherFactory

	// hooks provides lifecycle callbacks.
	hooks Hooks

	// serverOptions contains additional server configuration options.
	serverOptions []api.ServerOption

	// server is the HTTP API server instance.
	server *api.Server

	// pprofServer manages the optional pprof HTTP debug server.
	pprofServer *pprofServer

	// serverErr channel for server startup/shutdown errors.
	serverErr chan error

	// startupState gates proxy routes until essential routing initialization is complete.
	startupState *api.StartupState

	// watcher handles file system monitoring.
	watcher *WatcherWrapper

	// watcherCancel cancels the watcher context.
	watcherCancel context.CancelFunc

	// authUpdates channel for authentication updates.
	authUpdates chan watcher.AuthUpdate

	// authQueueMu protects the auth update consumer lifecycle.
	authQueueMu sync.Mutex

	// authQueueStop cancels the auth update queue processing.
	authQueueStop context.CancelFunc

	// authQueueDone is closed when the auth update consumer exits.
	authQueueDone chan struct{}

	// authQueueWaitObserved is used by shutdown tests to observe the consumer join.
	authQueueWaitObserved func()

	// authQueueStoppedObserved is used by shutdown tests to observe a completed join.
	authQueueStoppedObserved func()

	// modelSyncMu protects the background model sync worker pool state.
	modelSyncMu sync.Mutex

	// modelSyncCancel stops the optional background model sync workers.
	modelSyncCancel context.CancelFunc

	// modelSyncDone is closed when the model sync workers exit.
	modelSyncDone chan struct{}

	// modelSyncGeneration prevents workers from a stopped pool from completing
	// tasks retained for a newer pool.
	modelSyncGeneration uint64

	// modelSyncNextEpoch assigns a unique lifecycle to each auth sync task.
	modelSyncNextEpoch uint64

	// modelSyncQueue carries auth IDs that need registry and scheduler resync.
	modelSyncQueue chan string

	// modelSyncPending deduplicates queued or running auth sync tasks.
	modelSyncPending map[string]modelSyncTaskState

	// modelSyncOverflow retains deduplicated tasks while modelSyncQueue is full.
	modelSyncOverflow          []string
	modelSyncOverflowTokens    []uint64
	modelSyncOverflowSet       map[string]struct{}
	modelSyncOverflowToken     map[string]uint64
	modelSyncOverflowNextToken uint64

	// chatGPTWebImportModelsEnabled tracks the last model-validation setting
	// applied by the Service. Management endpoints mutate the shared config
	// before the file watcher reloads it, so pointer comparison is insufficient.
	chatGPTWebImportModelsEnabled atomic.Bool

	// modelSyncMutationLockedObserved is used by concurrency tests to observe
	// the exact point where model synchronization owns the auth mutation lock.
	modelSyncMutationLockedObserved func(*coreauth.Auth)

	// authUpdateMutationLockedObserved is used by concurrency tests to observe
	// the exact point where a watcher update owns the auth mutation lock.
	authUpdateMutationLockedObserved func(*coreauth.Auth)

	// antigravityModelCapabilities caches per-auth model discovery results.
	antigravityModelCapabilities sync.Map

	// chatGPTWebModelCatalog caches the last successful per-auth web catalog.
	chatGPTWebModelCatalog sync.Map

	// chatGPTWebCatalogCommitObserved observes catalog commits after installation validation.
	chatGPTWebCatalogCommitObserved func(*coreauth.Auth)

	// chatGPTWebReloginObserved observes re-login scheduling after model state is synchronized.
	chatGPTWebReloginObserved func(*coreauth.Auth)

	// modelSyncAuthLoadedObserved observes the auth snapshot loaded before mutation serialization.
	modelSyncAuthLoadedObserved func(*coreauth.Auth)

	// authModelTransitionMu protects per-auth model transition lock lifecycle.
	authModelTransitionMu sync.Mutex

	// authModelTransitionLocks serializes registry transitions for each auth ID.
	authModelTransitionLocks map[string]*authModelTransitionLockEntry

	// chatGPTWebModelFetchMu protects per-auth catalog fetch lock lifecycle.
	chatGPTWebModelFetchMu sync.Mutex

	// chatGPTWebModelFetchLocks serializes catalog fetches for each auth ID.
	chatGPTWebModelFetchLocks map[string]*chatGPTWebModelFetchLockEntry

	// usagePersistenceMu protects the periodic persistence loop lifecycle.
	usagePersistenceMu sync.Mutex

	// usagePersistenceCancel stops the periodic usage persistence loop.
	usagePersistenceCancel context.CancelFunc

	// usagePersistenceDone is closed when the periodic usage persistence loop exits.
	usagePersistenceDone chan struct{}

	// usageRestoreMu protects the cancellable one-shot background restore.
	usageRestoreMu sync.Mutex

	usageRestoreCancel  context.CancelFunc
	usageRestoreDone    chan struct{}
	usageRestoreActive  bool
	usageRestoreApplied bool
	// usageRestoreNeedsSidecar remains true after an interrupted or failed
	// restore so shutdown cannot overwrite an unapplied main snapshot.
	usageRestoreNeedsSidecar bool

	// usageStats optionally overrides the shared usage statistics store for tests.
	usageStats *internalusage.RequestStatistics

	// authManager handles legacy authentication operations.
	authManager *sdkAuth.Manager

	// accessManager handles request authentication providers.
	accessManager *sdkaccess.Manager

	// coreManager handles core authentication and execution.
	coreManager *coreauth.Manager

	// proxyPoolManager owns stable per-credential proxy bindings and health checks.
	proxyPoolManager *proxypool.Manager

	// chatGPTWebExecutorMu protects shared login coordinator initialization.
	chatGPTWebExecutorMu sync.Mutex

	// chatGPTWebLoginCoordinator serializes account login operations service-wide.
	chatGPTWebLoginCoordinator *executor.ChatGPTWebLoginCoordinator

	// shutdownOnce starts the shared shutdown task only once.
	shutdownOnce sync.Once
	shutdownDone chan struct{}

	// shutdownErr retains non-executor errors from the one-time shutdown sequence.
	shutdownErr error

	// wsGateway manages websocket Gemini providers.
	wsGateway *wsrelay.Manager

	// maintenanceMu protects auth maintenance queue state.
	maintenanceMu sync.Mutex

	// maintenanceCancel stops the optional auth maintenance worker.
	maintenanceCancel context.CancelFunc

	// maintenanceDone is closed when the maintenance worker exits.
	maintenanceDone chan struct{}

	// maintenanceQueue stores pending auth file deletions.
	maintenanceQueue []authMaintenanceCandidate

	// maintenancePending deduplicates queued auth files by canonical path.
	maintenancePending map[string]struct{}

	// maintenanceGeneration tracks cancellation generations for auth maintenance keys.
	maintenanceGeneration map[string]uint64

	// maintenanceStaged tracks paths whose watcher events belong to maintenance deletion.
	maintenanceStaged map[string]int

	// maintenanceDeleteGenerations retains durable deletion identities across safe retries.
	maintenanceDeleteGenerations map[string]*authfileguard.DeleteGeneration

	// maintenanceDeleteClearing prevents a replacement delete generation from
	// starting while a canceled queued generation clears its durable quarantine.
	maintenanceDeleteClearing map[string]*authfileguard.DeleteGeneration

	// maintenanceWake nudges the maintenance worker to wake early.
	maintenanceWake chan struct{}

	// maintenanceDirtyQueue stores changed auth IDs that need an indexed policy check.
	maintenanceDirtyQueue []string

	// maintenanceDirtySet deduplicates changed auth IDs waiting for policy checks.
	maintenanceDirtySet map[string]struct{}

	// maintenanceDependencyReconcilePending retries retained Codex cleanup after a transient failure.
	maintenanceDependencyReconcilePending bool

	// chatGPTWebDeadAuthDeletedCount tracks successful automatic dead credential deletions for this process.
	chatGPTWebDeadAuthDeletedCount atomic.Uint64
}

// RegisterUsagePlugin registers a usage plugin on the global usage manager.
// This allows external code to monitor API usage and token consumption.
//
// Parameters:
//   - plugin: The usage plugin to register
func (s *Service) RegisterUsagePlugin(plugin sdkusage.Plugin) {
	sdkusage.RegisterPlugin(plugin)
}

// newDefaultAuthManager creates a default authentication manager with all supported providers.
func newDefaultAuthManager() *sdkAuth.Manager {
	return sdkAuth.NewManager(
		sdkAuth.GetTokenStore(),
		sdkAuth.NewCodexAuthenticator(),
		sdkAuth.NewClaudeAuthenticator(),
		sdkAuth.NewXAIAuthenticator(),
	)
}

const (
	usagePersistenceDisabledPollInterval    = 5 * time.Second
	authMaintenanceDisabledPollInterval     = 5 * time.Second
	defaultMaintenanceScanIntervalSeconds   = 30
	defaultMaintenanceDeleteIntervalSeconds = 5
	defaultMaintenanceQuotaStrikeThreshold  = 6
	authMaintenanceStagedIgnoreWindow       = 200 * time.Millisecond
	authMaintenanceCheckpointDeletes        = 64
	authMaintenanceDirtyBatchSize           = 256
	defaultModelSyncWorkers                 = 4
	defaultModelSyncQueueSize               = 256
	authMaintenanceMetadataPrefix           = "auth_maintenance_"
	authMaintenanceActionMetadataKey        = "auth_maintenance_action"
	authMaintenanceReasonMetadataKey        = "auth_maintenance_reason"
	authMaintenanceMarkedAtMetadataKey      = "auth_maintenance_marked_at"
	authMaintenancePendingDeleteMetadataKey = "auth_maintenance_pending_delete"
	authMaintenancePreviousDisabledKey      = "auth_maintenance_previous_disabled"
	authMaintenanceDeleteAction             = "delete"
	authMaintenanceDisableAction            = "disable"
)

var authMaintenanceDeleteRetryDelays = [...]time.Duration{
	time.Second,
	5 * time.Second,
	30 * time.Second,
	time.Minute,
	5 * time.Minute,
}

var (
	readAuthMaintenanceFile   = os.ReadFile
	statAuthMaintenanceFile   = os.Stat
	removeAuthMaintenanceFile = os.Remove
	renameAuthMaintenanceFile = os.Rename
)

type authMaintenanceCandidate struct {
	Key           string
	Path          string
	IDs           []string
	SourceHashes  map[string]string
	Reason        string
	Generation    uint64
	Attempts      int
	NextAttemptAt time.Time
}

type modelSyncTaskState struct {
	epoch              uint64
	installationID     string
	importOnly         bool
	nextEpoch          uint64
	nextInstallationID string
	nextImportOnly     bool
	queued             bool
	running            bool
	dirty              bool
}

type authModelTransitionLockEntry struct {
	semaphore  chan struct{}
	references int
}

type authMaintenanceHook struct {
	service *Service
}

func (h authMaintenanceHook) OnAuthRegistered(ctx context.Context, auth *coreauth.Auth) {
	h.handleAuthChange(ctx, auth)
}

func (h authMaintenanceHook) OnAuthUpdated(ctx context.Context, auth *coreauth.Auth) {
	h.handleAuthChange(ctx, auth)
}

func (h authMaintenanceHook) handleAuthChange(ctx context.Context, auth *coreauth.Auth) {
	if h.service == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	h.service.markAuthMaintenanceChange(auth.ID)
	if ctx != nil && ctx.Value(modelSyncHookSuppressedContextKey{}) != nil {
		if h.service.coreManager != nil {
			current, ok := h.service.coreManager.CurrentAuthInstallation(auth)
			if !ok {
				return
			}
			auth = current
		}
		if nativeChatGPTWeb := isNativeChatGPTWebAuth(auth); nativeChatGPTWeb {
			h.service.ensureExecutorsForAuth(auth)
			if auth.Disabled || auth.Status == coreauth.StatusDisabled || !auth.LifecycleSelectable() ||
				coreauth.ChatGPTWebImportIntent(auth, coreauth.ChatGPTWebImportSessionIntent) {
				h.service.cancelChatGPTWebBackgroundMaintenance(auth, "auth_lifecycle_unavailable")
			} else {
				h.service.syncChatGPTWebAccountInfoRecovery(auth)
				h.service.restoreChatGPTWebImportAccountInfoIntent(auth)
			}
		}
		return
	}
	unlockTransition, errTransition := h.service.lockAuthModelTransitionContext(ctx, auth.ID)
	if errTransition != nil {
		if h.service.coreManager != nil {
			if current, ok := h.service.coreManager.CurrentAuthInstallation(auth); ok {
				if isNativeChatGPTWebAuth(current) {
					h.service.ensureExecutorsForAuth(current)
					if current.Disabled || current.Status == coreauth.StatusDisabled || !current.LifecycleSelectable() ||
						coreauth.ChatGPTWebImportIntent(current, coreauth.ChatGPTWebImportSessionIntent) {
						h.service.cancelChatGPTWebBackgroundMaintenance(current, "auth_lifecycle_unavailable")
					} else {
						h.service.syncChatGPTWebAccountInfoRecovery(current)
						h.service.restoreChatGPTWebImportAccountInfoIntent(current)
					}
					h.service.triggerChatGPTWebRelogin(current)
				}
				if !isNativeChatGPTWebAuth(current) ||
					(current.LifecycleSelectable() && !coreauth.ChatGPTWebImportIntent(current, coreauth.ChatGPTWebImportSessionIntent)) {
					h.service.enqueueModelSyncTaskForInstallation(
						current.ID,
						current.RuntimeInstallationID(),
						true,
					)
				}
			}
		}
		return
	}
	if h.service.coreManager != nil {
		current, ok := h.service.coreManager.CurrentAuthInstallation(auth)
		if !ok {
			unlockTransition()
			return
		}
		auth = current
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	nativeChatGPTWeb := isNativeChatGPTWebAuth(auth)
	registeredProvider := registry.GetGlobalRegistry().GetProviderForClient(auth.ID)
	if nativeChatGPTWeb {
		h.service.ensureExecutorsForAuth(auth)
		if auth.Disabled || auth.Status == coreauth.StatusDisabled || !auth.LifecycleSelectable() ||
			coreauth.ChatGPTWebImportIntent(auth, coreauth.ChatGPTWebImportSessionIntent) {
			h.service.cancelChatGPTWebBackgroundMaintenance(auth, "auth_lifecycle_unavailable")
		} else {
			h.service.syncChatGPTWebAccountInfoRecovery(auth)
			h.service.restoreChatGPTWebImportAccountInfoIntent(auth)
		}
	}
	if provider != "antigravity" {
		h.service.antigravityModelCapabilities.Delete(auth.ID)
	}
	if !nativeChatGPTWeb {
		h.service.chatGPTWebModelCatalog.Delete(auth.ID)
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		h.service.chatGPTWebModelCatalog.Delete(auth.ID)
		GlobalModelRegistry().UnregisterClient(auth.ID)
		if provider == "antigravity" {
			h.service.antigravityModelCapabilities.Delete(auth.ID)
		}
		unlockTransition()
		return
	}
	registryAction := chatGPTWebRegistryStateNone
	antigravityProviderChanged := false
	if nativeChatGPTWeb {
		if coreauth.ChatGPTWebCredentialRefreshed(ctx) {
			h.service.migrateOpaqueChatGPTWebCatalogAfterRefreshLocked(auth)
		}
		resetTransientState := coreauth.ChatGPTWebCredentialReplaced(ctx)
		registryAction = h.service.reconcileChatGPTWebAuthStateLocked(ctx, auth, resetTransientState, resetTransientState)
	} else if provider != "antigravity" {
		providerChanged := registeredProvider != "" && !strings.EqualFold(registeredProvider, provider)
		preserveTransientState := registeredProvider != "" && !providerChanged && authHasTransientState(auth)
		if preserveTransientState {
			h.service.registerModelsForAuthPreservingState(auth)
		} else {
			h.service.registerModelsForAuth(auth)
		}
		unlockTransition()
		if h.service.coreManager != nil {
			if preserveTransientState {
				h.service.coreManager.PruneRegistryModelStatesIfCurrent(ctx, auth)
			} else {
				h.service.coreManager.ReconcileRegistryModelStatesIfCurrent(ctx, auth)
			}
			h.service.coreManager.RefreshSchedulerEntry(auth.ID)
		}
		return
	} else {
		antigravityProviderChanged = registeredProvider != "" && !strings.EqualFold(registeredProvider, provider)
		if antigravityProviderChanged {
			GlobalModelRegistry().UnregisterClient(auth.ID)
		}
	}
	unlockTransition()
	if nativeChatGPTWeb {
		h.service.applyChatGPTWebRegistryState(ctx, auth, registryAction)
	} else if antigravityProviderChanged && h.service.coreManager != nil {
		h.service.coreManager.ReconcileRegistryModelStatesIfCurrent(ctx, auth)
		h.service.coreManager.RefreshSchedulerEntry(auth.ID)
	}
	importPolicy, imported := coreauth.ChatGPTWebImportPolicyFromContext(ctx)
	modelSyncReady := !nativeChatGPTWeb ||
		(auth.LifecycleSelectable() && !coreauth.ChatGPTWebImportIntent(auth, coreauth.ChatGPTWebImportSessionIntent))
	if modelSyncReady && (!imported || importPolicy.ValidateModels) {
		if imported {
			h.service.enqueueImportModelSyncTaskForInstallation(
				auth.ID,
				auth.RuntimeInstallationID(),
				true,
			)
		} else {
			h.service.enqueueModelSyncTaskForInstallation(
				auth.ID,
				auth.RuntimeInstallationID(),
				true,
			)
		}
	}
	if nativeChatGPTWeb {
		h.service.triggerChatGPTWebRelogin(auth)
	}
}

func (s *Service) cancelChatGPTWebBackgroundMaintenance(auth *coreauth.Auth, reason string) {
	if s == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	s.cancelModelSyncTask(auth.ID)
	if s.coreManager == nil {
		return
	}
	registered, ok := s.coreManager.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return
	}
	closer, ok := registered.(coreauth.AuthInstanceExecutionSessionCloser)
	if !ok {
		return
	}
	closer.CloseAuthInstanceExecutionSessions(auth.ID, auth.RuntimeInstanceID(), reason)
}

func (s *Service) restoreChatGPTWebImportAccountInfoIntent(auth *coreauth.Auth) {
	if s == nil || s.coreManager == nil || auth == nil || auth.Disabled ||
		auth.Status == coreauth.StatusDisabled || !auth.LifecycleSelectable() ||
		!coreauth.ChatGPTWebImportIntent(auth, coreauth.ChatGPTWebImportAccountInfoIntent) {
		return
	}
	cfg := s.currentConfig()
	if cfg == nil || !cfg.ChatGPTWeb.AccountInfo.Resolved().AutomaticRefreshEnabled() ||
		!cfg.ChatGPTWeb.Import.Resolved().RefreshAccountInfoAfterUpload {
		return
	}
	registered, ok := s.coreManager.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return
	}
	trigger, ok := registered.(interface {
		TriggerImportAccountInfoRefreshState(string) string
	})
	if ok {
		trigger.TriggerImportAccountInfoRefreshState(auth.ID)
	}
}

func (h authMaintenanceHook) OnResult(ctx context.Context, result coreauth.Result) {
	if h.service != nil {
		h.service.triggerChatGPTWebAccountInfoRefresh(result)
		h.service.handleAuthMaintenanceResult(ctx, result)
	}
}

func (s *Service) triggerChatGPTWebAccountInfoRefresh(result coreauth.Result) {
	if s == nil || s.coreManager == nil || result.Success ||
		!strings.EqualFold(strings.TrimSpace(result.Provider), chatgptwebauth.Provider) ||
		!strings.EqualFold(strings.TrimSpace(result.Model), chatgptwebauth.ImageModel) ||
		result.Error == nil || result.Error.StatusCode() != http.StatusTooManyRequests {
		return
	}
	authID := strings.TrimSpace(result.AuthID)
	if authID == "" {
		return
	}
	registered, ok := s.coreManager.Executor(chatgptwebauth.Provider)
	if !ok || registered == nil {
		return
	}
	trigger, ok := registered.(interface {
		TriggerAutomaticAccountInfoRefresh(string) bool
	})
	if !ok {
		return
	}
	trigger.TriggerAutomaticAccountInfoRefresh(authID)
}

type usagePersistenceSettings struct {
	enabled       bool
	interval      time.Duration
	retentionDays int
	maxBytes      int64
}

func usagePersistenceSettingsForConfig(cfg *config.Config) usagePersistenceSettings {
	if cfg == nil {
		return usagePersistenceSettings{}
	}
	settings := usagePersistenceSettings{
		enabled:       cfg.UsageStatisticsPersistence(),
		retentionDays: max(cfg.UsageStatisticsDetailRetentionDays, 0),
	}
	if cfg.UsageStatisticsPersistIntervalSeconds > 0 {
		seconds := int64(cfg.UsageStatisticsPersistIntervalSeconds)
		maxSeconds := int64((time.Duration(1<<63 - 1)) / time.Second)
		if seconds > maxSeconds {
			settings.interval = time.Duration(1<<63 - 1)
		} else {
			settings.interval = time.Duration(seconds) * time.Second
		}
	}
	if cfg.UsageStatisticsMaxStorageMB > 0 {
		megabytes := int64(cfg.UsageStatisticsMaxStorageMB)
		maxMegabytes := int64(^uint64(0)>>1) >> 20
		if megabytes > maxMegabytes {
			settings.maxBytes = int64(^uint64(0) >> 1)
		} else {
			settings.maxBytes = megabytes << 20
		}
	}
	return settings
}

func (s *Service) currentConfig() *config.Config {
	if s == nil {
		return nil
	}
	s.cfgMu.RLock()
	defer s.cfgMu.RUnlock()
	return s.cfg
}

func (s *Service) usageStatisticsEnabled() bool {
	cfg := s.currentConfig()
	return cfg != nil && cfg.UsageStatisticsEnabled
}

func (s *Service) usagePersistenceSettings() usagePersistenceSettings {
	return usagePersistenceSettingsForConfig(s.currentConfig())
}

func (s *Service) usageStatisticsFilePath() string {
	cfg := s.currentConfig()
	if cfg == nil {
		return ""
	}
	return internalusage.StatisticsFilePath(cfg)
}

func (s *Service) usageStatisticsStore() *internalusage.RequestStatistics {
	if s != nil && s.usageStats != nil {
		return s.usageStats
	}
	return internalusage.GetRequestStatistics()
}

func (s *Service) startUsageRestore() {
	if s == nil || !s.usageStatisticsEnabled() || !s.usagePersistenceSettings().enabled {
		return
	}
	path := s.usageStatisticsFilePath()
	if strings.TrimSpace(path) == "" {
		return
	}

	s.usageRestoreMu.Lock()
	if s.usageRestoreActive || s.usageRestoreApplied {
		s.usageRestoreMu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.usageRestoreCancel = cancel
	s.usageRestoreDone = done
	s.usageRestoreActive = true
	s.usageRestoreApplied = false
	s.usageRestoreMu.Unlock()

	stats := s.usageStatisticsStore()
	expectedGeneration := stats.HistoryGeneration()
	finishStage := s.startupState.BeginStage("usage_snapshot_restore")
	go func() {
		defer close(done)
		loaded, prepared, result, errPrepare := internalusage.PrepareRequestStatistics(ctx, path)
		errorCode := ""
		applied := false
		if errPrepare != nil {
			if !errors.Is(errPrepare, context.Canceled) {
				errorCode = "usage_snapshot_restore_failed"
				log.WithError(errPrepare).Warn("failed to prepare usage statistics snapshot")
			}
		} else if loaded {
			liveResult, appliedRestore := stats.ApplyPreparedRestore(prepared, expectedGeneration)
			applied = appliedRestore
			if appliedRestore {
				result.Added += liveResult.Added
				result.Skipped += liveResult.Skipped
				removed := s.reconcileUsageStatistics("background-restore")
				log.WithFields(log.Fields{
					"added":   result.Added,
					"skipped": result.Skipped,
					"removed": removed,
				}).Info("usage statistics restored in background")
			} else if ctx.Err() == nil {
				// A newer destructive mutation intentionally supersedes the old
				// snapshot, so shutdown may safely persist the current live state.
				applied = true
				errorCode = "usage_snapshot_restore_stale"
				log.Info("usage statistics restore discarded after a newer clear or prune")
			}
		} else {
			applied = true
		}
		finishStage(result.Added, errorCode)

		s.usageRestoreMu.Lock()
		if s.usageRestoreDone == done {
			s.usageRestoreActive = false
			s.usageRestoreApplied = applied
			s.usageRestoreNeedsSidecar = !applied
			s.usageRestoreCancel = nil
		}
		s.usageRestoreMu.Unlock()
	}()
}

func (s *Service) usageRestoreInProgress() bool {
	if s == nil {
		return false
	}
	s.usageRestoreMu.Lock()
	active := s.usageRestoreActive
	s.usageRestoreMu.Unlock()
	return active
}

// stopUsageRestore cancels and joins the background loader. It reports whether
// live history must be preserved in the pending sidecar instead of replacing
// an unapplied main snapshot.
func (s *Service) stopUsageRestore() bool {
	if s == nil {
		return false
	}
	s.usageRestoreMu.Lock()
	cancel := s.usageRestoreCancel
	done := s.usageRestoreDone
	s.usageRestoreMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	s.usageRestoreMu.Lock()
	needsSidecar := s.usageRestoreNeedsSidecar
	s.usageRestoreDone = nil
	s.usageRestoreCancel = nil
	s.usageRestoreActive = false
	s.usageRestoreMu.Unlock()
	return needsSidecar
}

func (s *Service) persistUsageStatistics(reason string) {
	if s == nil {
		return
	}
	settings := s.usagePersistenceSettings()
	if !settings.enabled || s.usageRestoreInProgress() {
		return
	}
	path := s.usageStatisticsFilePath()
	if strings.TrimSpace(path) == "" {
		return
	}
	if settings.interval <= 0 && reason != "shutdown" {
		return
	}
	result, err := internalusage.PersistRequestStatisticsWithPolicy(path, s.usageStatisticsStore(), internalusage.PersistencePolicy{
		DetailRetentionDays: settings.retentionDays,
		MaxBytes:            settings.maxBytes,
	})
	if err != nil {
		log.WithError(err).Warnf("failed to persist usage statistics during %s", reason)
		return
	}
	if !result.Saved {
		return
	}
	if result.Pruned > 0 {
		log.WithFields(log.Fields{
			"pruned":       result.Pruned,
			"size_bytes":   result.SizeBytes,
			"detail_count": result.DetailCount,
		}).Info("usage statistics retention applied")
	}
	if reason == "shutdown" {
		log.Infof("usage statistics persisted to %s during shutdown", path)
		return
	}
	log.Debugf("usage statistics persisted to %s (%s)", path, reason)
}

func (s *Service) nextUsagePersistenceWait() time.Duration {
	settings := s.usagePersistenceSettings()
	if !s.usageStatisticsEnabled() || !settings.enabled {
		return usagePersistenceDisabledPollInterval
	}
	interval := settings.interval
	if interval <= 0 {
		return usagePersistenceDisabledPollInterval
	}
	return interval
}

func (s *Service) startUsagePersistenceLoop() {
	if s == nil {
		return
	}
	settings := s.usagePersistenceSettings()
	if !s.usageStatisticsEnabled() || !settings.enabled || settings.interval <= 0 {
		return
	}
	s.usagePersistenceMu.Lock()
	defer s.usagePersistenceMu.Unlock()
	if s.usagePersistenceCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	s.usagePersistenceCancel = cancel
	s.usagePersistenceDone = done

	go func() {
		defer close(done)
		for {
			wait := s.nextUsagePersistenceWait()
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-timer.C:
			}

			settings := s.usagePersistenceSettings()
			if s.usageStatisticsEnabled() && settings.enabled && settings.interval > 0 {
				s.reconcileUsageStatistics("periodic")
				s.persistUsageStatistics("periodic")
			}
		}
	}()
}

func (s *Service) stopUsagePersistenceLoop() {
	if s == nil {
		return
	}
	s.usagePersistenceMu.Lock()
	cancel := s.usagePersistenceCancel
	done := s.usagePersistenceDone
	s.usagePersistenceCancel = nil
	s.usagePersistenceDone = nil
	s.usagePersistenceMu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *Service) restartUsagePersistenceLoop() {
	if s == nil {
		return
	}
	s.stopUsagePersistenceLoop()
	s.startUsagePersistenceLoop()
}

func (s *Service) applyUsagePersistenceConfigChange(previousCollection bool, previousSettings usagePersistenceSettings, newCfg *config.Config) {
	if s == nil || newCfg == nil {
		return
	}
	currentCollection := newCfg.UsageStatisticsEnabled
	currentSettings := usagePersistenceSettingsForConfig(newCfg)

	if previousSettings.enabled && !currentSettings.enabled {
		s.stopUsageRestore()
		s.stopUsagePersistenceLoop()
	}
	if previousCollection && !currentCollection {
		s.stopUsageRestore()
	}
	if currentCollection && currentSettings.enabled && (!previousCollection || !previousSettings.enabled) {
		s.startUsageRestore()
	}
	if previousCollection != currentCollection || previousSettings != currentSettings {
		s.restartUsagePersistenceLoop()
	}
}

func resolveAuthFilePath(auth *coreauth.Auth, authDir string) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil && strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true") {
		return ""
	}
	path := ""
	if auth.Attributes != nil {
		path = strings.TrimSpace(auth.Attributes["path"])
	}
	if path == "" {
		path = strings.TrimSpace(auth.FileName)
	}
	if path == "" {
		return ""
	}
	if !filepath.IsAbs(path) {
		if strings.TrimSpace(authDir) == "" {
			return ""
		}
		path = filepath.Join(authDir, filepath.Base(path))
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return path
}

func authExistsForUsage(auth *coreauth.Auth, authDir string) bool {
	if auth == nil {
		return false
	}
	if auth.Attributes != nil && strings.EqualFold(strings.TrimSpace(auth.Attributes["runtime_only"]), "true") {
		return true
	}
	_ = authDir
	return true
}

func authUpdateDeletionPath(auth *coreauth.Auth, authDir string) string {
	if auth == nil {
		return ""
	}
	return resolveAuthFilePath(auth, authDir)
}

func sameAuthFilePath(left, right string) bool {
	left = authfileguard.PathIdentity(left)
	right = authfileguard.PathIdentity(right)
	return left != "" && left == right
}

func authDeleteUpdateMatchesCurrentGeneration(update, current *coreauth.Auth) bool {
	if update == nil || update.Attributes == nil {
		return true
	}
	expectedHash := strings.TrimSpace(update.Attributes[coreauth.SourceHashAttributeKey])
	if expectedHash == "" {
		return true
	}
	if current == nil || current.Attributes == nil {
		return false
	}
	return strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey]) == expectedHash
}

func authUpdateIsMaintenanceDelete(auth *coreauth.Auth) bool {
	if auth == nil {
		return false
	}
	if authMaintenancePendingDelete(auth) {
		return true
	}
	if auth.Metadata == nil {
		return false
	}
	action, _ := auth.Metadata[authMaintenanceActionMetadataKey].(string)
	return strings.EqualFold(strings.TrimSpace(action), authMaintenanceDeleteAction)
}

func (s *Service) buildValidUsageAuthIndexes() map[string]struct{} {
	if s == nil || s.coreManager == nil {
		return nil
	}
	cfg := s.currentConfig()
	authDir := ""
	if cfg != nil {
		authDir = strings.TrimSpace(cfg.AuthDir)
	}
	auths := s.coreManager.List()
	indexes := make(map[string]struct{}, len(auths))
	for _, auth := range auths {
		if auth == nil || !authExistsForUsage(auth, authDir) {
			continue
		}
		if index := strings.TrimSpace(auth.EnsureIndex()); index != "" {
			indexes[index] = struct{}{}
		}
	}
	return indexes
}

func (s *Service) reconcileUsageStatistics(reason string) int {
	if s == nil || !s.usageStatisticsEnabled() {
		return 0
	}
	valid := s.buildValidUsageAuthIndexes()
	removed := s.usageStatisticsStore().PruneAuthIndexes(valid)
	if removed > 0 {
		log.Infof("usage statistics reconciled (%s): removed %d stale records", reason, removed)
	}
	return removed
}

func (s *Service) usageAuthIndexesForIDs(ids []string) []string {
	if s == nil || !s.usageStatisticsEnabled() || len(ids) == 0 || s.coreManager == nil {
		return nil
	}
	indexSet := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		auth, ok := s.coreManager.GetByID(id)
		if !ok || auth == nil {
			continue
		}
		if index := strings.TrimSpace(auth.EnsureIndex()); index != "" {
			indexSet[index] = struct{}{}
		}
	}
	if len(indexSet) == 0 {
		return nil
	}
	indexes := make([]string, 0, len(indexSet))
	for index := range indexSet {
		indexes = append(indexes, index)
	}
	return indexes
}

func (s *Service) removeUsageStatisticsForAuthIndexes(indexes []string, reason string) int {
	if s == nil || !s.usageStatisticsEnabled() || len(indexes) == 0 {
		return 0
	}
	removed := s.usageStatisticsStore().RemoveAuthIndexes(indexes)
	if removed > 0 {
		log.Infof("usage statistics updated after %s: removed %d records for %d auth(s)", reason, removed, len(indexes))
		s.persistUsageStatistics("auth-delete")
	}
	return removed
}

func (s *Service) removeUsageStatisticsForAuthIDs(ids []string, reason string) int {
	return s.removeUsageStatisticsForAuthIndexes(s.usageAuthIndexesForIDs(ids), reason)
}

func (s *Service) handleManagementAuthDelete(ctx context.Context, auths []*coreauth.Auth) {
	if s == nil || s.coreManager == nil || len(auths) == 0 {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	usageIndexes := make(map[string]struct{}, len(auths))
	for _, deleted := range auths {
		if deleted == nil || strings.TrimSpace(deleted.ID) == "" {
			continue
		}
		deleted = deleted.Clone()
		lockedCtx, unlockMutation, errLock := s.coreManager.LockAuthMutation(ctx, deleted)
		if errLock != nil {
			log.WithError(errLock).WithField("auth_id", deleted.ID).Warn("failed to lock management delete cleanup")
			continue
		}
		if _, exists := s.coreManager.GetByID(deleted.ID); exists {
			unlockMutation()
			continue
		}
		s.cancelModelSyncTask(deleted.ID)
		executorhelps.CloseProxyTransportCachesForAuth(deleted.ID)
		s.antigravityModelCapabilities.Delete(deleted.ID)
		s.cleanupChatGPTWebModelResourcesAfterDelete(lockedCtx, deleted.ID, deleted.RuntimeInstanceID())
		index := strings.TrimSpace(deleted.EnsureIndex())
		if index != "" {
			usageIndexes[index] = struct{}{}
		}
		unlockMutation()
	}
	if len(usageIndexes) == 0 {
		return
	}
	indexes := make([]string, 0, len(usageIndexes))
	for index := range usageIndexes {
		indexes = append(indexes, index)
	}
	s.removeUsageStatisticsForAuthIndexes(indexes, "management auth delete")
}

func (s *Service) snapshotAuthMaintenanceConfig() (config.AuthMaintenanceConfig, string) {
	if s == nil {
		return config.AuthMaintenanceConfig{
			ScanIntervalSeconds:         defaultMaintenanceScanIntervalSeconds,
			DeleteIntervalSeconds:       defaultMaintenanceDeleteIntervalSeconds,
			QuotaStrikeThreshold:        defaultMaintenanceQuotaStrikeThreshold,
			DisableQuotaStrikeThreshold: defaultMaintenanceQuotaStrikeThreshold,
		}, ""
	}
	cfg := s.currentConfig()
	if cfg == nil {
		return config.AuthMaintenanceConfig{
			ScanIntervalSeconds:         defaultMaintenanceScanIntervalSeconds,
			DeleteIntervalSeconds:       defaultMaintenanceDeleteIntervalSeconds,
			QuotaStrikeThreshold:        defaultMaintenanceQuotaStrikeThreshold,
			DisableQuotaStrikeThreshold: defaultMaintenanceQuotaStrikeThreshold,
		}, ""
	}
	maintenance := cfg.AuthMaintenance
	if maintenance.ScanIntervalSeconds <= 0 {
		maintenance.ScanIntervalSeconds = defaultMaintenanceScanIntervalSeconds
	}
	if maintenance.DeleteIntervalSeconds <= 0 {
		maintenance.DeleteIntervalSeconds = defaultMaintenanceDeleteIntervalSeconds
	}
	if maintenance.QuotaStrikeThreshold <= 0 {
		maintenance.QuotaStrikeThreshold = defaultMaintenanceQuotaStrikeThreshold
	}
	if maintenance.DisableQuotaStrikeThreshold <= 0 {
		maintenance.DisableQuotaStrikeThreshold = defaultMaintenanceQuotaStrikeThreshold
	}
	return maintenance, strings.TrimSpace(cfg.AuthDir)
}

type chatGPTWebDeadAuthDeletePolicy struct {
	enabled    bool
	priorities []int
}

func (s *Service) snapshotChatGPTWebDeadAuthDeletePolicy() chatGPTWebDeadAuthDeletePolicy {
	cfg := s.currentConfig()
	if cfg == nil {
		return chatGPTWebDeadAuthDeletePolicy{}
	}
	return chatGPTWebDeadAuthDeletePolicy{
		enabled:    cfg.ChatGPTWeb.AutoDeleteDeadAuths,
		priorities: append([]int(nil), cfg.ChatGPTWeb.AutoDeleteDeadPriorities...),
	}
}

func (policy chatGPTWebDeadAuthDeletePolicy) matchesPriority(priority int) bool {
	if !policy.enabled {
		return false
	}
	if len(policy.priorities) == 0 {
		return true
	}
	for _, allowed := range policy.priorities {
		if allowed == priority {
			return true
		}
	}
	return false
}

func chatGPTWebDeadAuthDeletePoliciesEqual(first, second chatGPTWebDeadAuthDeletePolicy) bool {
	if first.enabled != second.enabled || len(first.priorities) != len(second.priorities) {
		return false
	}
	for index := range first.priorities {
		if first.priorities[index] != second.priorities[index] {
			return false
		}
	}
	return true
}

func authMaintenanceConfigsEqual(first, second config.AuthMaintenanceConfig) bool {
	return first.Enable == second.Enable &&
		first.ScanIntervalSeconds == second.ScanIntervalSeconds &&
		first.DeleteIntervalSeconds == second.DeleteIntervalSeconds &&
		slices.Equal(first.DeleteStatusCodes, second.DeleteStatusCodes) &&
		slices.Equal(first.DisableStatusCodes, second.DisableStatusCodes) &&
		first.DeleteQuotaExceeded == second.DeleteQuotaExceeded &&
		first.QuotaStrikeThreshold == second.QuotaStrikeThreshold &&
		first.DisableQuotaExceeded == second.DisableQuotaExceeded &&
		first.DisableQuotaStrikeThreshold == second.DisableQuotaStrikeThreshold
}

func (s *Service) warnAuthMaintenanceConfig(cfg config.AuthMaintenanceConfig) {
	if !cfg.Enable {
		return
	}
	if cfg.DeleteQuotaExceeded && cfg.DisableQuotaExceeded {
		log.Warn("auth maintenance: delete-quota-exceeded and disable-quota-exceeded are both enabled; delete policy takes precedence and disable-only handling is skipped")
	}
}

func containsStatusCode(codes []int, want int) bool {
	if want == 0 {
		return false
	}
	for _, code := range codes {
		if code == want {
			return true
		}
	}
	return false
}

func authMaintenancePendingDelete(auth *coreauth.Auth) bool {
	if auth == nil || auth.Metadata == nil {
		return false
	}
	raw, ok := auth.Metadata[authMaintenancePendingDeleteMetadataKey]
	if !ok {
		return false
	}
	switch value := raw.(type) {
	case bool:
		return value
	case string:
		return strings.EqualFold(strings.TrimSpace(value), "true")
	default:
		return false
	}
}

func authMaintenanceReason(auth *coreauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	value, _ := auth.Metadata[authMaintenanceReasonMetadataKey].(string)
	return strings.TrimSpace(value)
}

func authMaintenanceStatusCode(auth *coreauth.Auth, result *coreauth.Result) int {
	statusCode := 0
	if result != nil && result.Error != nil && result.Error.HTTPStatus > 0 {
		statusCode = result.Error.HTTPStatus
	} else if auth != nil {
		if auth.LastError != nil && auth.LastError.HTTPStatus > 0 {
			statusCode = auth.LastError.HTTPStatus
		} else {
			switch strings.ToLower(strings.TrimSpace(auth.StatusMessage)) {
			case "unauthorized":
				statusCode = 401
			case "payment_required":
				statusCode = 402
			case "not_found":
				statusCode = 404
			case "quota exhausted":
				statusCode = 429
			}
		}
	}
	if statusCode == http.StatusTooManyRequests && chatGPTWebImageOnlyMaintenanceResult(auth, result) {
		return 0
	}
	return statusCode
}

func chatGPTWebImageOnlyMaintenanceResult(auth *coreauth.Auth, result *coreauth.Result) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) {
		return false
	}
	if result != nil {
		return chatGPTWebRegisteredImageModel(auth, result.Model)
	}
	if auth.LastError == nil {
		return false
	}
	explicitImageQuota := strings.EqualFold(strings.TrimSpace(auth.LastError.Code), "chatgpt_web_image_quota")
	if auth.LastError.HTTPStatus != http.StatusTooManyRequests {
		return explicitImageQuota
	}
	var (
		imageMatch      bool
		imageMatchAt    time.Time
		nonImageMatch   bool
		nonImageMatchAt time.Time
	)
	for model, state := range auth.ModelStates {
		if state == nil ||
			state.LastError == nil ||
			state.LastError.HTTPStatus != http.StatusTooManyRequests ||
			!authMaintenanceErrorsEqual(state.LastError, auth.LastError) {
			continue
		}
		if chatGPTWebRegisteredImageModel(auth, model) {
			imageMatch = true
			if state.UpdatedAt.After(imageMatchAt) {
				imageMatchAt = state.UpdatedAt
			}
			continue
		}
		nonImageMatch = true
		if state.UpdatedAt.After(nonImageMatchAt) {
			nonImageMatchAt = state.UpdatedAt
		}
	}
	if !imageMatch {
		return explicitImageQuota && !nonImageMatch
	}
	return !nonImageMatch || imageMatchAt.After(nonImageMatchAt)
}

func chatGPTWebRegisteredImageModel(auth *coreauth.Auth, model string) bool {
	if auth == nil {
		return false
	}
	model = strings.TrimSpace(model)
	if strings.EqualFold(model, chatgptwebauth.ImageModel) {
		return true
	}
	for _, info := range registry.GetGlobalRegistry().GetModelsForClient(auth.ID) {
		if info != nil &&
			info.Type == registry.OpenAIImageModelType &&
			strings.EqualFold(strings.TrimSpace(info.ID), model) {
			return true
		}
	}
	return false
}

func authMaintenanceErrorsEqual(first, second *coreauth.Error) bool {
	if first == nil || second == nil {
		return false
	}
	return first.HTTPStatus == second.HTTPStatus &&
		strings.EqualFold(strings.TrimSpace(first.Code), strings.TrimSpace(second.Code)) &&
		strings.TrimSpace(first.Message) == strings.TrimSpace(second.Message)
}

func authEligibleForChatGPTWebDeadDelete(auth *coreauth.Auth, policy chatGPTWebDeadAuthDeletePolicy) (string, bool) {
	if !isNativeChatGPTWebAuth(auth) || auth.LifecycleState() != coreauth.LifecycleStateDead {
		return "", false
	}
	priority := 0
	if auth.Attributes != nil {
		if parsed, errParse := strconv.Atoi(strings.TrimSpace(auth.Attributes["priority"])); errParse == nil {
			priority = parsed
		}
	}
	if !policy.matchesPriority(priority) {
		return "", false
	}
	reason := ""
	if auth.Metadata != nil {
		if value, ok := auth.Metadata["lifecycle_reason"].(string); ok {
			reason = chatgptwebauth.SafeLifecycleReason(value)
		}
	}
	if reason == "" {
		reason = coreauth.LifecycleStateDead
	}
	return "chatgpt_web_dead_" + reason, true
}

func authEligibleForMaintenanceDelete(auth *coreauth.Auth, result *coreauth.Result, cfg config.AuthMaintenanceConfig) (string, bool) {
	if reason := authMaintenanceReason(auth); authMaintenancePendingDelete(auth) {
		if reason == "" {
			reason = "pending_delete"
		}
		return reason, true
	}
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return "", false
	}
	if statusCode := authMaintenanceStatusCode(auth, result); containsStatusCode(cfg.DeleteStatusCodes, statusCode) {
		return fmt.Sprintf("http_%d", statusCode), true
	}
	if cfg.DeleteQuotaExceeded && auth.HasAccountMaintenanceQuotaExceeded() && auth.Quota.StrikeCount >= cfg.QuotaStrikeThreshold {
		return fmt.Sprintf("quota_delete_%d", auth.Quota.StrikeCount), true
	}
	return "", false
}

func authEligibleForMaintenanceDisable(auth *coreauth.Auth, result *coreauth.Result, cfg config.AuthMaintenanceConfig) (string, bool) {
	if auth == nil || auth.Disabled || auth.Status == coreauth.StatusDisabled {
		return "", false
	}
	statusCode := authMaintenanceStatusCode(auth, result)
	if containsStatusCode(cfg.DeleteStatusCodes, statusCode) {
		return "", false
	}
	if containsStatusCode(cfg.DisableStatusCodes, statusCode) {
		return fmt.Sprintf("http_%d", statusCode), true
	}
	if cfg.DeleteQuotaExceeded && cfg.DisableQuotaExceeded {
		return "", false
	}
	if cfg.DisableQuotaExceeded && auth.HasAccountMaintenanceQuotaExceeded() && auth.Quota.StrikeCount >= cfg.DisableQuotaStrikeThreshold {
		return fmt.Sprintf("quota_disable_%d", auth.Quota.StrikeCount), true
	}
	return "", false
}

func (s *Service) ensureAuthMaintenanceQueue() {
	if s == nil {
		return
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if s.maintenancePending == nil {
		s.maintenancePending = make(map[string]struct{})
	}
	if s.maintenanceGeneration == nil {
		s.maintenanceGeneration = make(map[string]uint64)
	}
	if s.maintenanceStaged == nil {
		s.maintenanceStaged = make(map[string]int)
	}
	if s.maintenanceDeleteGenerations == nil {
		s.maintenanceDeleteGenerations = make(map[string]*authfileguard.DeleteGeneration)
	}
	if s.maintenanceDeleteClearing == nil {
		s.maintenanceDeleteClearing = make(map[string]*authfileguard.DeleteGeneration)
	}
	if s.maintenanceWake == nil {
		s.maintenanceWake = make(chan struct{}, 1)
	}
	if s.maintenanceDirtySet == nil {
		s.maintenanceDirtySet = make(map[string]struct{})
	}
}

func (s *Service) wakeAuthMaintenance() {
	if s == nil {
		return
	}
	s.ensureAuthMaintenanceQueue()
	select {
	case s.maintenanceWake <- struct{}{}:
	default:
	}
}

func (s *Service) markAuthMaintenanceChange(authID string) {
	if s == nil {
		return
	}
	cfg := s.currentConfig()
	if cfg == nil || (!cfg.AuthMaintenance.Enable && !cfg.ChatGPTWeb.AutoDeleteDeadAuths) {
		return
	}
	s.markAuthMaintenanceDirty(authID)
}

func (s *Service) markAuthMaintenanceDirty(authID string) bool {
	if s == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	if _, exists := s.maintenanceDirtySet[authID]; exists {
		s.maintenanceMu.Unlock()
		return false
	}
	s.maintenanceDirtySet[authID] = struct{}{}
	s.maintenanceDirtyQueue = append(s.maintenanceDirtyQueue, authID)
	s.maintenanceMu.Unlock()
	s.wakeAuthMaintenance()
	return true
}

func (s *Service) takeAuthMaintenanceDirtyIDs(limit int) []string {
	if s == nil || limit <= 0 {
		return nil
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if len(s.maintenanceDirtyQueue) == 0 {
		return nil
	}
	if limit > len(s.maintenanceDirtyQueue) {
		limit = len(s.maintenanceDirtyQueue)
	}
	ids := append([]string(nil), s.maintenanceDirtyQueue[:limit]...)
	clear(s.maintenanceDirtyQueue[:limit])
	s.maintenanceDirtyQueue = s.maintenanceDirtyQueue[limit:]
	if len(s.maintenanceDirtyQueue) == 0 {
		s.maintenanceDirtyQueue = nil
	}
	for _, id := range ids {
		delete(s.maintenanceDirtySet, id)
	}
	return ids
}

func (s *Service) hasAuthMaintenanceDirtyIDs() bool {
	if s == nil {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return len(s.maintenanceDirtyQueue) > 0
}

func (s *Service) installAuthMaintenanceHook(ctx context.Context) {
	if s == nil || s.coreManager == nil {
		return
	}
	hook := authMaintenanceHook{service: s}
	s.coreManager.AddHook(hook)
	for _, auth := range s.coreManager.ChatGPTWebAuths() {
		if isNativeChatGPTWebAuth(auth) {
			hook.OnAuthUpdated(ctx, auth)
		}
	}
}

func (s *Service) triggerChatGPTWebRelogin(auth *coreauth.Auth) {
	if s == nil || s.coreManager == nil || auth == nil || auth.Disabled || auth.LifecycleState() != coreauth.LifecycleStateReloginPending || !strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
		return
	}
	if _, _, isCompat := openAICompatInfoFromAuth(auth); isCompat {
		return
	}
	if s.chatGPTWebReloginObserved != nil {
		s.chatGPTWebReloginObserved(auth)
	}
	s.ensureExecutorsForAuth(auth)
	registered, ok := s.coreManager.Executor("chatgpt-web")
	if !ok {
		return
	}
	chatGPTWebExecutor, ok := registered.(*executor.ChatGPTWebExecutor)
	if !ok {
		return
	}
	chatGPTWebExecutor.TriggerBackgroundRelogin(auth)
}

func (s *Service) syncChatGPTWebAccountInfoRecovery(auth *coreauth.Auth) {
	if s == nil || s.coreManager == nil || auth == nil {
		return
	}
	registered, ok := s.coreManager.Executor(chatgptwebauth.Provider)
	if !ok {
		return
	}
	chatGPTWebExecutor, ok := registered.(*executor.ChatGPTWebExecutor)
	if !ok {
		return
	}
	chatGPTWebExecutor.SyncAccountInfoRecovery(auth)
}

func (s *Service) enqueueAuthMaintenanceCandidate(candidate authMaintenanceCandidate) bool {
	if s == nil {
		return false
	}
	key := strings.TrimSpace(candidate.Key)
	if key == "" || len(candidate.IDs) == 0 {
		return false
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if _, exists := s.maintenancePending[key]; exists {
		return false
	}
	candidate.Generation = s.maintenanceGeneration[key]
	s.maintenancePending[key] = struct{}{}
	s.maintenanceQueue = append(s.maintenanceQueue, candidate)
	return true
}

func (s *Service) dequeueAuthMaintenanceCandidate() (authMaintenanceCandidate, bool) {
	if s == nil {
		return authMaintenanceCandidate{}, false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if len(s.maintenanceQueue) == 0 {
		return authMaintenanceCandidate{}, false
	}
	candidate := s.maintenanceQueue[0]
	s.maintenanceQueue = append([]authMaintenanceCandidate(nil), s.maintenanceQueue[1:]...)
	delete(s.maintenancePending, strings.TrimSpace(candidate.Key))
	return candidate, true
}

func authMaintenanceCandidateEnabled(candidate authMaintenanceCandidate, genericEnabled, webDeadEnabled bool) bool {
	if genericEnabled {
		return true
	}
	return webDeadEnabled && isChatGPTWebDeadMaintenanceCandidate(candidate)
}

func isChatGPTWebDeadMaintenanceCandidate(candidate authMaintenanceCandidate) bool {
	return strings.HasPrefix(strings.TrimSpace(candidate.Reason), "chatgpt_web_dead_")
}

func (s *Service) dequeueProcessableAuthMaintenanceCandidate(genericEnabled bool) (authMaintenanceCandidate, bool) {
	if s == nil {
		return authMaintenanceCandidate{}, false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	now := time.Now()
	selected := -1
	for index, candidate := range s.maintenanceQueue {
		if !candidate.NextAttemptAt.IsZero() && now.Before(candidate.NextAttemptAt) {
			continue
		}
		if isChatGPTWebDeadMaintenanceCandidate(candidate) {
			selected = index
			break
		}
		if genericEnabled && selected < 0 {
			selected = index
		}
	}
	if selected < 0 {
		return authMaintenanceCandidate{}, false
	}
	candidate := s.maintenanceQueue[selected]
	s.maintenanceQueue = append(s.maintenanceQueue[:selected], s.maintenanceQueue[selected+1:]...)
	delete(s.maintenancePending, strings.TrimSpace(candidate.Key))
	return candidate, true
}

func (s *Service) nextAuthMaintenanceRetryDelay(genericEnabled, webDeadEnabled bool, now time.Time) (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	var earliest time.Time
	for _, candidate := range s.maintenanceQueue {
		if !authMaintenanceCandidateEnabled(candidate, genericEnabled, webDeadEnabled) || candidate.NextAttemptAt.IsZero() || !now.Before(candidate.NextAttemptAt) {
			continue
		}
		if earliest.IsZero() || candidate.NextAttemptAt.Before(earliest) {
			earliest = candidate.NextAttemptAt
		}
	}
	if earliest.IsZero() {
		return 0, false
	}
	return earliest.Sub(now), true
}

func authMaintenanceDeleteRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	index := attempt - 1
	if index >= len(authMaintenanceDeleteRetryDelays) {
		index = len(authMaintenanceDeleteRetryDelays) - 1
	}
	return authMaintenanceDeleteRetryDelays[index]
}

func (s *Service) requeueAuthMaintenanceCandidate(candidate authMaintenanceCandidate) bool {
	candidate.Attempts++
	candidate.NextAttemptAt = time.Now().Add(authMaintenanceDeleteRetryDelay(candidate.Attempts))
	return s.enqueueAuthMaintenanceCandidate(candidate)
}

func (s *Service) setAuthMaintenanceDependencyReconcilePending(pending bool) {
	if s == nil {
		return
	}
	s.maintenanceMu.Lock()
	s.maintenanceDependencyReconcilePending = pending
	s.maintenanceMu.Unlock()
	if pending {
		s.wakeAuthMaintenance()
	}
}

func (s *Service) authMaintenanceDependencyReconcileRequired() bool {
	if s == nil {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.maintenanceDependencyReconcilePending
}

func (s *Service) hasQueuedAuthMaintenanceCandidates() bool {
	if s == nil {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return len(s.maintenanceQueue) > 0
}

func (s *Service) hasEnabledAuthMaintenanceCandidates(genericEnabled, webDeadEnabled bool) bool {
	if s == nil {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	for _, candidate := range s.maintenanceQueue {
		if authMaintenanceCandidateEnabled(candidate, genericEnabled, webDeadEnabled) {
			return true
		}
	}
	return false
}

func (s *Service) hasReadyAuthMaintenanceCandidates(genericEnabled, webDeadEnabled bool, now time.Time) bool {
	if s == nil {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	for _, candidate := range s.maintenanceQueue {
		if !authMaintenanceCandidateEnabled(candidate, genericEnabled, webDeadEnabled) {
			continue
		}
		if candidate.NextAttemptAt.IsZero() || !now.Before(candidate.NextAttemptAt) {
			return true
		}
	}
	return false
}

func (s *Service) runAuthMaintenanceCheckpoint(ctx context.Context, reason string) {
	if s == nil {
		return
	}
	if _, errReconcile := s.reconcileChatGPTWebDependencies(ctx, reason); errReconcile != nil {
		s.setAuthMaintenanceDependencyReconcilePending(true)
	} else {
		s.setAuthMaintenanceDependencyReconcilePending(false)
	}
	if s.reconcileUsageStatistics(reason) > 0 {
		s.persistUsageStatistics("auth-maintenance-checkpoint")
	}
}

func (s *Service) authMaintenanceCandidateQueued(candidate authMaintenanceCandidate) bool {
	if s == nil {
		return false
	}
	key := strings.TrimSpace(candidate.Key)
	if key == "" {
		return false
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	_, exists := s.maintenancePending[key]
	return exists
}

func (s *Service) authMaintenanceCandidateQueuedForEnabledPolicy(candidate authMaintenanceCandidate, genericEnabled, webDeadEnabled bool) bool {
	if s == nil {
		return false
	}
	key := strings.TrimSpace(candidate.Key)
	if key == "" {
		return false
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	for index, queued := range s.maintenanceQueue {
		if strings.TrimSpace(queued.Key) != key {
			continue
		}
		if isChatGPTWebDeadMaintenanceCandidate(candidate) && !isChatGPTWebDeadMaintenanceCandidate(queued) {
			s.maintenanceQueue = append(s.maintenanceQueue[:index], s.maintenanceQueue[index+1:]...)
			delete(s.maintenancePending, key)
			s.maintenanceGeneration[key]++
			return false
		}
		if authMaintenanceCandidateEnabled(queued, genericEnabled, webDeadEnabled) {
			return true
		}
		s.maintenanceQueue = append(s.maintenanceQueue[:index], s.maintenanceQueue[index+1:]...)
		delete(s.maintenancePending, key)
		s.maintenanceGeneration[key]++
		return false
	}
	_, exists := s.maintenancePending[key]
	if !exists && isChatGPTWebDeadMaintenanceCandidate(candidate) {
		s.maintenanceGeneration[key]++
	}
	return exists
}

func (s *Service) cancelAuthMaintenanceCandidate(candidate authMaintenanceCandidate) bool {
	return s.cancelAuthMaintenanceKey(strings.TrimSpace(candidate.Key))
}

func (s *Service) cancelAuthMaintenanceKey(key string) bool {
	if s == nil {
		return false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return false
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()

	removed := false
	var canceled authMaintenanceCandidate
	queued := false
	filtered := s.maintenanceQueue[:0]
	for _, candidate := range s.maintenanceQueue {
		if strings.TrimSpace(candidate.Key) == key {
			removed = true
			canceled = candidate
			queued = true
			continue
		}
		filtered = append(filtered, candidate)
	}
	s.maintenanceQueue = filtered
	if _, exists := s.maintenancePending[key]; exists {
		delete(s.maintenancePending, key)
		removed = true
	}
	s.maintenanceGeneration[key]++
	generation := s.maintenanceDeleteGenerations[key]
	if generation != nil && queued {
		delete(s.maintenanceDeleteGenerations, key)
		s.maintenanceDeleteClearing[key] = generation
	}
	s.maintenanceMu.Unlock()
	// A queued retry is not mutating persistence, so its quarantine can be
	// cleared immediately. An in-flight delete owns its generation until it
	// observes cancellation or finishes, preventing a crash-recovery gap.
	if generation != nil && queued {
		canceled.Key = key
		errClear := s.clearAuthMaintenanceDeleteQuarantine(canceled, generation)
		s.maintenanceMu.Lock()
		if s.maintenanceDeleteClearing[key] == generation {
			delete(s.maintenanceDeleteClearing, key)
			if errClear != nil && s.maintenanceDeleteGenerations[key] == nil {
				s.maintenanceDeleteGenerations[key] = generation
			}
		}
		s.maintenanceMu.Unlock()
		s.wakeAuthMaintenance()
		if errClear != nil {
			log.WithError(errClear).Warnf("failed to clear canceled auth maintenance quarantine for %s", canceled.Path)
		}
	}
	return removed
}

func (s *Service) authMaintenanceCandidateCanceled(candidate authMaintenanceCandidate) bool {
	if s == nil {
		return false
	}
	key := strings.TrimSpace(candidate.Key)
	if key == "" {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.maintenanceGeneration[key] != candidate.Generation
}

func (s *Service) authMaintenanceDeleteGeneration(candidate authMaintenanceCandidate, expectedHash string) (*authfileguard.DeleteGeneration, bool, error) {
	if s == nil {
		return nil, false, errors.New("auth maintenance service is unavailable")
	}
	key := strings.TrimSpace(candidate.Key)
	expectedHash = strings.TrimSpace(expectedHash)
	if key == "" || expectedHash == "" {
		return nil, false, errors.New("auth maintenance deletion identity is incomplete")
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if s.maintenanceDeleteClearing[key] != nil {
		return nil, false, authfileguard.ErrDeleteGenerationUncertain
	}
	if generation := s.maintenanceDeleteGenerations[key]; generation != nil {
		if generation.ExpectedHash() != expectedHash {
			return nil, false, authfileguard.ErrPersistGenerationStale
		}
		return generation, false, nil
	}
	generation := authfileguard.NewDeleteGeneration(expectedHash)
	s.maintenanceDeleteGenerations[key] = generation
	return generation, true, nil
}

func (s *Service) forgetAuthMaintenanceDeleteGeneration(candidate authMaintenanceCandidate, generation *authfileguard.DeleteGeneration) {
	if s == nil || generation == nil {
		return
	}
	key := strings.TrimSpace(candidate.Key)
	s.maintenanceMu.Lock()
	if s.maintenanceDeleteGenerations[key] == generation {
		delete(s.maintenanceDeleteGenerations, key)
	}
	s.maintenanceMu.Unlock()
}

func (s *Service) clearAuthMaintenanceDeleteQuarantine(candidate authMaintenanceCandidate, generation *authfileguard.DeleteGeneration) error {
	if s == nil || generation == nil || strings.TrimSpace(candidate.Path) == "" {
		return nil
	}
	authDir := ""
	if cfg := s.currentConfig(); cfg != nil {
		authDir = strings.TrimSpace(cfg.AuthDir)
	}
	if errClear := watcher.ClearAuthDeleteQuarantine(s.configPath, authDir, candidate.Path, generation); errClear != nil {
		return errClear
	}
	s.forgetAuthMaintenanceDeleteGeneration(candidate, generation)
	return nil
}

func authMaintenanceExpectedSourceHash(contents []byte) string {
	if hash, errHash := coreauth.CanonicalSourceHashFromBytes(contents); errHash == nil && strings.TrimSpace(hash) != "" {
		return hash
	}
	return coreauth.SourceHashFromBytes(contents)
}

func (s *Service) markAuthMaintenanceStagedPath(path string) {
	if s == nil {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	s.maintenanceStaged[path]++
	s.maintenanceMu.Unlock()
}

func (s *Service) unmarkAuthMaintenanceStagedPath(path string) {
	if s == nil {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	if count, ok := s.maintenanceStaged[path]; ok {
		if count <= 1 {
			delete(s.maintenanceStaged, path)
			return
		}
		s.maintenanceStaged[path] = count - 1
	}
}

func (s *Service) releaseAuthMaintenanceStagedPath(path string) {
	if s == nil {
		return
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return
	}
	time.AfterFunc(authMaintenanceStagedIgnoreWindow, func() {
		s.unmarkAuthMaintenanceStagedPath(path)
	})
}

func (s *Service) authMaintenancePathStaged(path string) bool {
	if s == nil {
		return false
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	s.maintenanceMu.Lock()
	defer s.maintenanceMu.Unlock()
	return s.maintenanceStaged[path] > 0
}

func authMaintenancePathsEqual(first, second string) bool {
	first = authfileguard.PathIdentity(first)
	second = authfileguard.PathIdentity(second)
	return first != "" && first == second
}

func (s *Service) authMaintenanceCandidateForAuth(auth *coreauth.Auth, authDir string, reason string) (authMaintenanceCandidate, bool) {
	if s == nil || s.coreManager == nil || auth == nil {
		return authMaintenanceCandidate{}, false
	}
	path := resolveAuthFilePath(auth, authDir)
	if strings.TrimSpace(path) == "" {
		id := strings.TrimSpace(auth.ID)
		if id == "" {
			return authMaintenanceCandidate{}, false
		}
		return authMaintenanceCandidate{
			Key:    id,
			IDs:    []string{id},
			Reason: strings.TrimSpace(reason),
		}, true
	}

	backedAuths := s.coreManager.AuthsForBackingPath(path)
	ids := make([]string, 0, len(backedAuths))
	seen := make(map[string]struct{}, len(backedAuths))
	for _, current := range backedAuths {
		if !authMaintenancePathsEqual(resolveAuthFilePath(current, authDir), path) {
			continue
		}
		id := strings.TrimSpace(current.ID)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		id := strings.TrimSpace(auth.ID)
		if id == "" {
			return authMaintenanceCandidate{}, false
		}
		ids = append(ids, id)
	}
	return authMaintenanceCandidate{
		Key:    path,
		Path:   path,
		IDs:    ids,
		Reason: strings.TrimSpace(reason),
	}, true
}

func (s *Service) authMaintenanceCandidateForID(id, authDir, reason string) (authMaintenanceCandidate, bool) {
	if s == nil || s.coreManager == nil {
		return authMaintenanceCandidate{}, false
	}
	auth, ok := s.coreManager.GetByID(strings.TrimSpace(id))
	if !ok || auth == nil {
		id = strings.TrimSpace(id)
		if id == "" {
			return authMaintenanceCandidate{}, false
		}
		return authMaintenanceCandidate{Key: id, IDs: []string{id}, Reason: strings.TrimSpace(reason)}, true
	}
	return s.authMaintenanceCandidateForAuth(auth, authDir, reason)
}

func (s *Service) snapshotAuthMaintenanceCandidateSourceHashes(candidate authMaintenanceCandidate, authDir string) (authMaintenanceCandidate, bool) {
	if s == nil || s.coreManager == nil || len(candidate.IDs) == 0 {
		return authMaintenanceCandidate{}, false
	}
	sourceHashes := make(map[string]string, len(candidate.IDs))
	for _, id := range candidate.IDs {
		id = strings.TrimSpace(id)
		current, ok := s.coreManager.GetByID(id)
		if !ok || current == nil || current.Attributes == nil {
			return authMaintenanceCandidate{}, false
		}
		if path := strings.TrimSpace(candidate.Path); path != "" && !authMaintenancePathsEqual(resolveAuthFilePath(current, authDir), path) {
			return authMaintenanceCandidate{}, false
		}
		sourceHash := strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey])
		if sourceHash == "" {
			return authMaintenanceCandidate{}, false
		}
		sourceHashes[id] = sourceHash
	}
	candidate.SourceHashes = sourceHashes
	return candidate, true
}

func (s *Service) scanAuthMaintenanceCandidates(cfg config.AuthMaintenanceConfig, authDir string) []authMaintenanceCandidate {
	return s.scanAuthMaintenanceCandidatesWithPolicy(cfg, chatGPTWebDeadAuthDeletePolicy{}, authDir)
}

func (s *Service) scanAuthMaintenanceCandidatesWithPolicy(cfg config.AuthMaintenanceConfig, webPolicy chatGPTWebDeadAuthDeletePolicy, authDir string) []authMaintenanceCandidate {
	if s == nil || s.coreManager == nil || (!cfg.Enable && !webPolicy.enabled) {
		return nil
	}
	snapshot := s.coreManager.List()
	idsByPath := make(map[string][]string)
	for _, current := range snapshot {
		path := resolveAuthFilePath(current, authDir)
		id := ""
		if current != nil {
			id = strings.TrimSpace(current.ID)
		}
		if path == "" || id == "" {
			continue
		}
		idsByPath[path] = append(idsByPath[path], id)
	}
	grouped := make(map[string]authMaintenanceCandidate)
	for _, auth := range snapshot {
		reason, ok := authEligibleForChatGPTWebDeadDelete(auth, webPolicy)
		if !ok && cfg.Enable {
			reason, ok = authEligibleForMaintenanceDelete(auth, nil, cfg)
		}
		if !ok {
			continue
		}
		path := resolveAuthFilePath(auth, authDir)
		if path == "" {
			continue
		}
		candidate := authMaintenanceCandidate{
			Key:    path,
			Path:   path,
			IDs:    append([]string(nil), idsByPath[path]...),
			Reason: strings.TrimSpace(reason),
		}
		group := grouped[candidate.Key]
		if group.Key == "" {
			group = candidate
		}
		if group.Reason == "" {
			group.Reason = candidate.Reason
		}
		seen := make(map[string]struct{}, len(group.IDs))
		for _, id := range group.IDs {
			seen[id] = struct{}{}
		}
		for _, id := range candidate.IDs {
			if _, exists := seen[id]; exists {
				continue
			}
			group.IDs = append(group.IDs, id)
			seen[id] = struct{}{}
		}
		grouped[candidate.Key] = group
	}
	candidates := make([]authMaintenanceCandidate, 0, len(grouped))
	for _, candidate := range grouped {
		candidates = append(candidates, candidate)
	}
	return candidates
}

func (s *Service) reconcilePersistedChatGPTWebDeadMaintenanceCandidates(ctx context.Context, policy chatGPTWebDeadAuthDeletePolicy, authDir string) bool {
	if s == nil || s.coreManager == nil {
		return false
	}
	retry := false
	for _, auth := range s.coreManager.List() {
		if !authMaintenancePendingDelete(auth) {
			continue
		}
		reason := authMaintenanceReason(auth)
		candidate, ok := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
		if !ok || !isChatGPTWebDeadMaintenanceCandidate(candidate) {
			continue
		}
		candidate, ok = s.snapshotAuthMaintenanceCandidateSourceHashes(candidate, authDir)
		if !ok {
			retry = true
			continue
		}
		if _, eligible := authEligibleForChatGPTWebDeadDelete(auth, policy); !eligible {
			if err := s.clearChatGPTWebDeadMaintenanceCandidate(ctx, candidate); err != nil {
				log.WithError(err).Warnf("failed to clear stale ChatGPT Web maintenance state for %s", candidate.Path)
				retry = true
			}
			continue
		}
		if !s.authMaintenanceCandidateQueued(candidate) {
			s.enqueueAuthMaintenanceCandidate(candidate)
		}
	}
	return retry
}

func (s *Service) scanAuthMaintenanceDisableCandidates(cfg config.AuthMaintenanceConfig, authDir string) []authMaintenanceCandidate {
	if s == nil || s.coreManager == nil || !cfg.Enable {
		return nil
	}
	snapshot := s.coreManager.List()
	grouped := make(map[string]authMaintenanceCandidate)
	for _, auth := range snapshot {
		reason, ok := authEligibleForMaintenanceDisable(auth, nil, cfg)
		if !ok {
			continue
		}
		candidate, ok := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
		if !ok {
			continue
		}
		group := grouped[candidate.Key]
		if group.Key == "" {
			group = candidate
		}
		if group.Reason == "" {
			group.Reason = candidate.Reason
		}
		seen := make(map[string]struct{}, len(group.IDs))
		for _, id := range group.IDs {
			seen[id] = struct{}{}
		}
		for _, id := range candidate.IDs {
			if _, exists := seen[id]; exists {
				continue
			}
			group.IDs = append(group.IDs, id)
			seen[id] = struct{}{}
		}
		grouped[candidate.Key] = group
	}
	candidates := make([]authMaintenanceCandidate, 0, len(grouped))
	for _, candidate := range grouped {
		candidates = append(candidates, candidate)
	}
	return candidates
}

func (s *Service) processDirtyAuthMaintenanceIDs(cfg config.AuthMaintenanceConfig, webPolicy chatGPTWebDeadAuthDeletePolicy, authDir string, limit int) int {
	if s == nil || s.coreManager == nil {
		return 0
	}
	ids := s.takeAuthMaintenanceDirtyIDs(limit)
	for _, id := range ids {
		auth, ok := s.coreManager.GetByID(id)
		if !ok || auth == nil {
			continue
		}
		reason, deleteEligible := authEligibleForChatGPTWebDeadDelete(auth, webPolicy)
		if !deleteEligible && cfg.Enable {
			reason, deleteEligible = authEligibleForMaintenanceDelete(auth, nil, cfg)
		}
		if deleteEligible {
			candidate, candidateOK := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
			if !candidateOK {
				continue
			}
			if strings.TrimSpace(candidate.Path) == "" {
				s.disableAuthMaintenanceCandidate(context.Background(), candidate, false)
				continue
			}
			if s.authMaintenanceCandidateQueuedForEnabledPolicy(candidate, cfg.Enable, webPolicy.enabled) {
				continue
			}
			if !s.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
				continue
			}
			if isChatGPTWebDeadMaintenanceCandidate(candidate) {
				candidate, candidateOK = s.snapshotAuthMaintenanceCandidateSourceHashes(candidate, authDir)
				if !candidateOK {
					continue
				}
			}
			if s.enqueueAuthMaintenanceCandidate(candidate) {
				log.Debugf("auth maintenance queued %s (%s)", candidate.Path, candidate.Reason)
			}
			continue
		}
		if !cfg.Enable {
			continue
		}
		if reason, disableEligible := authEligibleForMaintenanceDisable(auth, nil, cfg); disableEligible {
			candidate, candidateOK := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
			if candidateOK {
				s.disableAuthMaintenanceCandidate(context.Background(), candidate, false)
			}
		}
	}
	return len(ids)
}

func (s *Service) startAuthMaintenance(parent context.Context) {
	if s == nil {
		return
	}
	s.ensureAuthMaintenanceQueue()
	s.maintenanceMu.Lock()
	if s.maintenanceCancel != nil {
		s.maintenanceMu.Unlock()
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	s.maintenanceCancel = cancel
	s.maintenanceDone = done
	s.maintenanceMu.Unlock()

	go func() {
		defer close(done)
		var lastGenericDeleteAt time.Time
		deletesSinceCheckpoint := 0
		var lastMaintenanceConfig config.AuthMaintenanceConfig
		var lastMaintenanceAuthDir string
		var hasLastMaintenanceConfig bool
		var lastWebPolicy chatGPTWebDeadAuthDeletePolicy
		var hasLastWebPolicy bool
		var nextFullScanAt time.Time
		reconcilePersistedWebState := true
		var nextPersistedWebReconcileAt time.Time
		for {
			cfg, authDir := s.snapshotAuthMaintenanceConfig()
			webPolicy := s.snapshotChatGPTWebDeadAuthDeletePolicy()
			s.warnAuthMaintenanceConfig(cfg)
			enabled := cfg.Enable || webPolicy.enabled
			if !hasLastMaintenanceConfig || lastMaintenanceAuthDir != authDir || !authMaintenanceConfigsEqual(lastMaintenanceConfig, cfg) {
				lastMaintenanceConfig = cfg
				lastMaintenanceAuthDir = authDir
				hasLastMaintenanceConfig = true
				nextFullScanAt = time.Time{}
			}
			if !hasLastWebPolicy || !chatGPTWebDeadAuthDeletePoliciesEqual(lastWebPolicy, webPolicy) {
				lastWebPolicy = webPolicy
				hasLastWebPolicy = true
				nextFullScanAt = time.Time{}
				reconcilePersistedWebState = true
				nextPersistedWebReconcileAt = time.Time{}
			}
			if reconcilePersistedWebState && (nextPersistedWebReconcileAt.IsZero() || !time.Now().Before(nextPersistedWebReconcileAt)) {
				if s.reconcilePersistedChatGPTWebDeadMaintenanceCandidates(ctx, webPolicy, authDir) {
					nextPersistedWebReconcileAt = time.Now().Add(authMaintenanceDisabledPollInterval)
				} else {
					reconcilePersistedWebState = false
					nextPersistedWebReconcileAt = time.Time{}
				}
			}

			if candidate, ok := s.dequeueProcessableAuthMaintenanceCandidate(cfg.Enable); ok {
				if s.authMaintenanceCandidateCanceled(candidate) {
					continue
				}
				webDeadCandidate := isChatGPTWebDeadMaintenanceCandidate(candidate)
				if !webDeadCandidate {
					deleteInterval := time.Duration(cfg.DeleteIntervalSeconds) * time.Second
					if deleteInterval <= 0 {
						deleteInterval = time.Duration(defaultMaintenanceDeleteIntervalSeconds) * time.Second
					}
					if !lastGenericDeleteAt.IsZero() {
						wait := deleteInterval - time.Since(lastGenericDeleteAt)
						if wait > 0 {
							timer := time.NewTimer(wait)
							select {
							case <-ctx.Done():
								if !timer.Stop() {
									select {
									case <-timer.C:
									default:
									}
								}
								return
							case <-timer.C:
							}
						}
					}
				}
				if s.authMaintenanceCandidateCanceled(candidate) {
					continue
				}
				deleted, err := s.deleteAuthMaintenanceCandidate(ctx, candidate)
				if !webDeadCandidate {
					lastGenericDeleteAt = time.Now()
				}
				if err != nil {
					outcome, explicitOutcome := coreauth.DeleteOutcomeFromError(err)
					if deleted && explicitOutcome && outcome == coreauth.DeleteOutcomeCommitted {
						log.WithError(err).Warnf("auth maintenance delete completed with cleanup warning for %s", candidate.Path)
					} else if explicitOutcome && outcome == coreauth.DeleteOutcomeUncertain {
						log.WithError(err).Errorf("auth maintenance delete outcome is uncertain for %s; leaving quarantine for persistent recovery", candidate.Path)
						continue
					} else if ctx.Err() != nil {
						return
					} else if s.requeueAuthMaintenanceCandidate(candidate) {
						log.WithError(err).Warnf("auth maintenance delete failed for %s; retry %d scheduled", candidate.Path, candidate.Attempts+1)
					}
				}
				if deleted {
					if webDeadCandidate {
						s.chatGPTWebDeadAuthDeletedCount.Add(1)
					}
					deletesSinceCheckpoint++
					if deletesSinceCheckpoint >= authMaintenanceCheckpointDeletes {
						s.runAuthMaintenanceCheckpoint(ctx, "auth maintenance delete checkpoint")
						deletesSinceCheckpoint = 0
					}
					continue
				}
				if err == nil {
					continue
				}
			}

			processedDirty := s.processDirtyAuthMaintenanceIDs(cfg, webPolicy, authDir, authMaintenanceDirtyBatchSize)
			if processedDirty == authMaintenanceDirtyBatchSize && s.hasAuthMaintenanceDirtyIDs() {
				continue
			}

			now := time.Now()
			fullScanDue := enabled && (nextFullScanAt.IsZero() || !now.Before(nextFullScanAt))
			if fullScanDue {
				scanInterval := time.Duration(cfg.ScanIntervalSeconds) * time.Second
				if scanInterval <= 0 {
					scanInterval = time.Duration(defaultMaintenanceScanIntervalSeconds) * time.Second
				}
				nextFullScanAt = now.Add(scanInterval)
				for _, candidate := range s.scanAuthMaintenanceCandidatesWithPolicy(cfg, webPolicy, authDir) {
					if s.authMaintenanceCandidateQueuedForEnabledPolicy(candidate, cfg.Enable, webPolicy.enabled) {
						continue
					}
					if !s.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
						continue
					}
					if isChatGPTWebDeadMaintenanceCandidate(candidate) {
						snapshot, snapshotOK := s.snapshotAuthMaintenanceCandidateSourceHashes(candidate, authDir)
						if !snapshotOK {
							continue
						}
						candidate = snapshot
					}
					if s.enqueueAuthMaintenanceCandidate(candidate) {
						log.Debugf("auth maintenance queued %s (%s)", candidate.Path, candidate.Reason)
					}
				}
				if cfg.Enable {
					for _, candidate := range s.scanAuthMaintenanceDisableCandidates(cfg, authDir) {
						s.disableAuthMaintenanceCandidate(context.Background(), candidate, false)
					}
				}
			}
			if s.hasReadyAuthMaintenanceCandidates(cfg.Enable, webPolicy.enabled, time.Now()) {
				continue
			}
			if deletesSinceCheckpoint > 0 && !s.hasEnabledAuthMaintenanceCandidates(cfg.Enable, webPolicy.enabled) {
				s.runAuthMaintenanceCheckpoint(ctx, "auth maintenance queue drained")
				deletesSinceCheckpoint = 0
			} else if s.authMaintenanceDependencyReconcileRequired() {
				s.runAuthMaintenanceCheckpoint(ctx, "auth maintenance retry")
			}

			wait := authMaintenanceDisabledPollInterval
			if enabled {
				if nextFullScanAt.IsZero() {
					wait = 0
				} else {
					wait = time.Until(nextFullScanAt)
				}
			}
			if retryWait, hasRetry := s.nextAuthMaintenanceRetryDelay(cfg.Enable, webPolicy.enabled, time.Now()); hasRetry && retryWait < wait {
				wait = retryWait
			}
			if wait < 0 {
				wait = 0
			}
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			case <-s.maintenanceWake:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
			case <-timer.C:
			}
		}
	}()
}

func (s *Service) stopAuthMaintenance() {
	if s == nil {
		return
	}
	s.maintenanceMu.Lock()
	cancel := s.maintenanceCancel
	done := s.maintenanceDone
	s.maintenanceCancel = nil
	s.maintenanceDone = nil
	s.maintenanceMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (s *Service) handleAuthMaintenanceResult(_ context.Context, result coreauth.Result) {
	if s == nil || s.coreManager == nil || result.Success {
		return
	}
	cfg, authDir := s.snapshotAuthMaintenanceConfig()
	webPolicy := s.snapshotChatGPTWebDeadAuthDeletePolicy()
	if !cfg.Enable && !webPolicy.enabled {
		return
	}
	authID := strings.TrimSpace(result.AuthID)
	if authID == "" {
		return
	}
	auth, ok := s.coreManager.GetByID(authID)
	if !ok || auth == nil {
		return
	}
	reason, deleteEligible := authEligibleForChatGPTWebDeadDelete(auth, webPolicy)
	if !deleteEligible && cfg.Enable {
		reason, deleteEligible = authEligibleForMaintenanceDelete(auth, &result, cfg)
	}
	if deleteEligible {
		candidate, candidateOK := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
		if !candidateOK {
			return
		}
		if strings.TrimSpace(candidate.Path) == "" {
			s.disableAuthMaintenanceCandidate(context.Background(), candidate, false)
			return
		}
		if s.authMaintenanceCandidateQueuedForEnabledPolicy(candidate, cfg.Enable, webPolicy.enabled) {
			return
		}
		if !s.disableAuthMaintenanceCandidate(context.Background(), candidate, true) {
			return
		}
		if isChatGPTWebDeadMaintenanceCandidate(candidate) {
			candidate, candidateOK = s.snapshotAuthMaintenanceCandidateSourceHashes(candidate, authDir)
			if !candidateOK {
				return
			}
		}
		if s.enqueueAuthMaintenanceCandidate(candidate) {
			s.wakeAuthMaintenance()
		}
		return
	}
	if reason, ok := authEligibleForMaintenanceDisable(auth, &result, cfg); ok {
		candidate, candidateOK := s.authMaintenanceCandidateForAuth(auth, authDir, reason)
		if !candidateOK {
			return
		}
		s.disableAuthMaintenanceCandidate(context.Background(), candidate, false)
	}
}

func (s *Service) disableAuthMaintenanceCandidate(ctx context.Context, candidate authMaintenanceCandidate, pendingDelete bool) bool {
	if s == nil {
		return false
	}
	success := true
	for _, id := range candidate.IDs {
		if !s.applyCoreAuthRemovalWithReason(ctx, id, candidate.Reason, pendingDelete) {
			success = false
		}
	}
	return success
}

func (s *Service) deleteAuthMaintenanceCandidate(ctx context.Context, candidate authMaintenanceCandidate) (bool, error) {
	if s == nil {
		return false, nil
	}
	webDeadCandidate := isChatGPTWebDeadMaintenanceCandidate(candidate)
	if s.coreManager == nil || (!webDeadCandidate && !s.authMaintenanceCandidateContainsNativeChatGPTWeb(candidate)) {
		return s.deleteAuthMaintenanceCandidateUnchecked(ctx, candidate)
	}
	var deleted bool
	err := s.coreManager.WithChatGPTWebDependencyMutation(ctx, func(lockedCtx context.Context) error {
		if webDeadCandidate {
			_, authDir := s.snapshotAuthMaintenanceConfig()
			policy := s.snapshotChatGPTWebDeadAuthDeletePolicy()
			if !s.chatGPTWebDeadMaintenanceCandidateStillEligible(candidate, policy, authDir) {
				return s.clearChatGPTWebDeadMaintenanceCandidate(lockedCtx, candidate)
			}
		}
		var errDelete error
		deleted, errDelete = s.deleteAuthMaintenanceCandidateUnchecked(lockedCtx, candidate)
		return errDelete
	})
	return deleted, err
}

func (s *Service) authMaintenanceCandidateContainsNativeChatGPTWeb(candidate authMaintenanceCandidate) bool {
	if s == nil || s.coreManager == nil {
		return false
	}
	for _, id := range candidate.IDs {
		auth, ok := s.coreManager.GetByID(strings.TrimSpace(id))
		if ok && isNativeChatGPTWebAuth(auth) {
			return true
		}
	}
	return false
}

func (s *Service) chatGPTWebDeadMaintenanceCandidateStillEligible(candidate authMaintenanceCandidate, policy chatGPTWebDeadAuthDeletePolicy, authDir string) bool {
	if s == nil || s.coreManager == nil || !policy.enabled || !isChatGPTWebDeadMaintenanceCandidate(candidate) {
		return false
	}
	path := strings.TrimSpace(candidate.Path)
	if path == "" {
		return false
	}
	expectedIDs := make(map[string]struct{}, len(candidate.IDs))
	for _, id := range candidate.IDs {
		if id = strings.TrimSpace(id); id != "" {
			expectedIDs[id] = struct{}{}
		}
	}
	if len(expectedIDs) == 0 || len(candidate.SourceHashes) != len(expectedIDs) {
		return false
	}
	contents, errRead := readAuthMaintenanceFile(path)
	if errRead != nil {
		return false
	}
	for id := range expectedIDs {
		if !coreauth.SourceHashMatchesBytes(candidate.SourceHashes[id], contents) {
			return false
		}
	}
	matched := make(map[string]struct{}, len(expectedIDs))
	for _, auth := range s.coreManager.AuthsForBackingPath(path) {
		if !authMaintenancePathsEqual(resolveAuthFilePath(auth, authDir), path) {
			continue
		}
		id := ""
		if auth != nil {
			id = strings.TrimSpace(auth.ID)
		}
		if _, ok := expectedIDs[id]; !ok {
			return false
		}
		expectedSourceHash := strings.TrimSpace(candidate.SourceHashes[id])
		if expectedSourceHash == "" || auth.Attributes == nil || strings.TrimSpace(auth.Attributes[coreauth.SourceHashAttributeKey]) != expectedSourceHash {
			return false
		}
		if _, ok := authEligibleForChatGPTWebDeadDelete(auth, policy); !ok {
			return false
		}
		matched[id] = struct{}{}
	}
	return len(matched) == len(expectedIDs)
}

func (s *Service) clearChatGPTWebDeadMaintenanceCandidate(ctx context.Context, candidate authMaintenanceCandidate) error {
	if s == nil || s.coreManager == nil {
		return nil
	}
	return s.coreManager.WithChatGPTWebDependencyMutation(ctx, func(lockedCtx context.Context) error {
		for _, id := range candidate.IDs {
			current, ok := s.coreManager.GetByID(strings.TrimSpace(id))
			if !ok || current == nil || !authMaintenancePendingDelete(current) {
				continue
			}
			expectedSourceHash := strings.TrimSpace(candidate.SourceHashes[current.ID])
			if expectedSourceHash == "" || current.Attributes == nil || strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey]) != expectedSourceHash {
				continue
			}
			if reason := authMaintenanceReason(current); reason != "" && reason != strings.TrimSpace(candidate.Reason) {
				continue
			}
			previousDisabled, _ := current.Metadata[authMaintenancePreviousDisabledKey].(bool)
			delete(current.Metadata, authMaintenanceActionMetadataKey)
			delete(current.Metadata, authMaintenanceReasonMetadataKey)
			delete(current.Metadata, authMaintenanceMarkedAtMetadataKey)
			delete(current.Metadata, authMaintenancePendingDeleteMetadataKey)
			delete(current.Metadata, authMaintenancePreviousDisabledKey)
			current.Disabled = previousDisabled
			if previousDisabled {
				current.Metadata["disabled"] = true
			} else {
				delete(current.Metadata, "disabled")
			}
			current.Unavailable = false
			coreauth.ApplyLifecycleRuntimeState(current)
			if _, err := s.coreManager.Update(lockedCtx, current); err != nil {
				return fmt.Errorf("clear stale ChatGPT Web maintenance state for %s: %w", current.ID, err)
			}
		}
		return nil
	})
}

func (s *Service) deleteAuthMaintenanceCandidateUnchecked(ctx context.Context, candidate authMaintenanceCandidate) (bool, error) {
	if s == nil {
		return false, nil
	}
	path := strings.TrimSpace(candidate.Path)
	if path == "" {
		return false, nil
	}
	s.ensureAuthMaintenanceQueue()
	if s.authMaintenanceCandidateCanceled(candidate) {
		return false, nil
	}

	info, err := statAuthMaintenanceFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("stat auth file before maintenance delete: %w", err)
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("auth maintenance target is not a regular file")
	}
	contents, err := readAuthMaintenanceFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read auth file before maintenance delete: %w", err)
	}
	unchanged, err := authMaintenanceFileMatchesSnapshot(path, contents)
	if err != nil {
		return false, err
	}
	if !unchanged {
		return false, nil
	}
	if s.authMaintenanceCandidateCanceled(candidate) {
		return false, nil
	}
	if s.coreManager == nil || !s.coreManager.SupportsSourceConditionalDelete() {
		return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeRolledBack, errors.New("auth maintenance requires source-conditional persistence"))
	}
	expectedHash := authMaintenanceExpectedSourceHash(contents)
	if isChatGPTWebDeadMaintenanceCandidate(candidate) {
		if len(candidate.SourceHashes) != len(candidate.IDs) {
			return false, nil
		}
		for _, id := range candidate.IDs {
			if !coreauth.SourceHashMatchesBytes(candidate.SourceHashes[strings.TrimSpace(id)], contents) {
				return false, nil
			}
		}
	}
	generation, generationCreated, errGeneration := s.authMaintenanceDeleteGeneration(candidate, expectedHash)
	if errGeneration != nil {
		return false, errGeneration
	}
	clearQuarantine := func() error {
		return s.clearAuthMaintenanceDeleteQuarantine(candidate, generation)
	}
	if s.authMaintenanceCandidateCanceled(candidate) {
		if errClear := clearQuarantine(); errClear != nil {
			return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("clear canceled auth maintenance quarantine: %w", errClear))
		}
		return false, nil
	}
	authDir := ""
	if cfg := s.currentConfig(); cfg != nil {
		authDir = strings.TrimSpace(cfg.AuthDir)
	}
	if errPersist := watcher.PersistAuthDeleteQuarantine(s.configPath, authDir, path, generation); errPersist != nil {
		if errClear := clearQuarantine(); errClear != nil {
			return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, errors.Join(errPersist, fmt.Errorf("clear failed auth maintenance quarantine: %w", errClear)))
		}
		return false, fmt.Errorf("persist auth maintenance delete quarantine: %w", errPersist)
	}

	s.markAuthMaintenanceStagedPath(path)
	defer s.releaseAuthMaintenanceStagedPath(path)
	if s.authMaintenanceCandidateCanceled(candidate) {
		if errClear := clearQuarantine(); errClear != nil {
			return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("clear canceled auth maintenance quarantine: %w", errClear))
		}
		return false, nil
	}

	deleteCtx := authfileguard.WithDeleteGeneration(ctx, generation)
	deleteAttempt := candidate.Attempts
	if generationCreated {
		deleteAttempt = 0
		deleteCtx = authfileguard.WithDeleteIdentityBinding(deleteCtx)
	}
	deleteCtx = authfileguard.WithDeleteAttempt(deleteCtx, deleteAttempt)
	indexes := s.usageAuthIndexesForIDs(candidate.IDs)
	persisted := false
	var uncertainDeleteErr error
	for _, id := range candidate.IDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		current, exists := s.coreManager.GetByID(id)
		if !exists || current == nil || !authMaintenancePathsEqual(resolveAuthFilePath(current, authDir), path) {
			continue
		}
		if expectedSourceHash := strings.TrimSpace(candidate.SourceHashes[id]); expectedSourceHash != "" &&
			(current.Attributes == nil || strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey]) != expectedSourceHash) {
			continue
		}
		currentCtx := coreauth.WithSkipPersist(deleteCtx)
		if !persisted {
			currentCtx = deleteCtx
		}
		if errDelete := s.deleteCoreAuth(currentCtx, id); errDelete != nil {
			if errors.Is(errDelete, authfileguard.ErrPersistGenerationStale) {
				if errClear := clearQuarantine(); errClear != nil {
					return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("clear stale auth maintenance quarantine: %w", errClear))
				}
				return false, nil
			}
			outcome, explicitOutcome := coreauth.DeleteOutcomeFromError(errDelete)
			if !explicitOutcome || outcome == coreauth.DeleteOutcomeRolledBack {
				if persisted {
					return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, errors.Join(uncertainDeleteErr, errDelete))
				}
				if errClear := clearQuarantine(); errClear != nil {
					return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, errors.Join(errDelete, fmt.Errorf("clear rolled-back auth maintenance quarantine: %w", errClear)))
				}
				return false, errDelete
			}
			persisted = true
			uncertainDeleteErr = errors.Join(uncertainDeleteErr, errDelete)
			continue
		}
		persisted = true
	}
	if !persisted {
		if errClear := clearQuarantine(); errClear != nil {
			return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, fmt.Errorf("clear unused auth maintenance quarantine: %w", errClear))
		}
		return false, nil
	}
	s.removeUsageStatisticsForAuthIndexes(indexes, "auth maintenance delete")
	if uncertainDeleteErr != nil {
		return false, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeUncertain, uncertainDeleteErr)
	}
	if errClear := clearQuarantine(); errClear != nil {
		return true, coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeCommitted, fmt.Errorf("clear completed auth maintenance quarantine: %w", errClear))
	}
	return true, nil
}

func authMaintenanceFileMatchesSnapshot(path string, contents []byte) (bool, error) {
	if strings.TrimSpace(path) == "" {
		return false, nil
	}
	current, err := readAuthMaintenanceFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("read auth file before maintenance delete confirmation: %w", err)
	}
	return bytes.Equal(current, contents), nil
}

func legacyAuthMaintenanceOriginalPath(path string) (string, bool) {
	const marker = ".auth-maintenance."
	index := strings.LastIndex(path, marker)
	if index <= 0 || index+len(marker) >= len(path) {
		return "", false
	}
	for _, character := range path[index+len(marker):] {
		if character < '0' || character > '9' {
			return "", false
		}
	}
	return path[:index], true
}

func validLegacyAuthMaintenanceFile(contents []byte) bool {
	var object map[string]any
	return json.Unmarshal(contents, &object) == nil && object != nil
}

func migrateLegacyAuthMaintenanceFiles(authDir string) error {
	authDir = strings.TrimSpace(authDir)
	if authDir == "" {
		return nil
	}
	var migrationErrors []error
	errWalk := filepath.WalkDir(authDir, func(path string, entry fs.DirEntry, errWalkEntry error) error {
		if errWalkEntry != nil {
			migrationErrors = append(migrationErrors, fmt.Errorf("inspect legacy auth maintenance path %s: %w", path, errWalkEntry))
			return nil
		}
		if entry.IsDir() {
			if entry.Name() == ".cliproxy-delete-quarantine" {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		originalPath, legacy := legacyAuthMaintenanceOriginalPath(path)
		if !legacy {
			return nil
		}
		stagedContents, errRead := readAuthMaintenanceFile(path)
		if errRead != nil {
			migrationErrors = append(migrationErrors, fmt.Errorf("read legacy auth maintenance file %s: %w", path, errRead))
			authfileguard.MarkQuarantined(path)
			return nil
		}
		if !validLegacyAuthMaintenanceFile(stagedContents) {
			migrationErrors = append(migrationErrors, fmt.Errorf("legacy auth maintenance file %s is not a valid JSON credential", path))
			authfileguard.MarkQuarantined(path)
			return nil
		}
		originalContents, errOriginal := readAuthMaintenanceFile(originalPath)
		switch {
		case os.IsNotExist(errOriginal):
			if errRename := renameAuthMaintenanceFile(path, originalPath); errRename != nil {
				migrationErrors = append(migrationErrors, fmt.Errorf("restore legacy auth maintenance file %s: %w", path, errRename))
				authfileguard.MarkQuarantined(path)
			}
		case errOriginal != nil:
			migrationErrors = append(migrationErrors, fmt.Errorf("read legacy auth maintenance target %s: %w", originalPath, errOriginal))
			authfileguard.MarkQuarantined(path)
			authfileguard.MarkQuarantined(originalPath)
		case bytes.Equal(originalContents, stagedContents):
			if errRemove := removeAuthMaintenanceFile(path); errRemove != nil && !os.IsNotExist(errRemove) {
				migrationErrors = append(migrationErrors, fmt.Errorf("remove duplicate legacy auth maintenance file %s: %w", path, errRemove))
			}
		default:
			authfileguard.MarkQuarantined(path)
			authfileguard.MarkQuarantined(originalPath)
			migrationErrors = append(migrationErrors, fmt.Errorf("legacy auth maintenance file %s conflicts with %s", path, originalPath))
		}
		return nil
	})
	if errWalk != nil {
		migrationErrors = append(migrationErrors, fmt.Errorf("scan legacy auth maintenance files: %w", errWalk))
	}
	return errors.Join(migrationErrors...)
}

func (s *Service) ensureAuthUpdateQueue(ctx context.Context) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.authQueueMu.Lock()
	defer s.authQueueMu.Unlock()
	if s.authUpdates == nil {
		s.authUpdates = make(chan watcher.AuthUpdate, 256)
	}
	if s.authQueueDone != nil {
		select {
		case <-s.authQueueDone:
			s.authQueueStop = nil
			s.authQueueDone = nil
		default:
			return
		}
	}
	queueCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.authQueueStop = cancel
	s.authQueueDone = done
	go func() {
		defer close(done)
		s.consumeAuthUpdates(queueCtx)
	}()
}

func (s *Service) consumeAuthUpdates(ctx context.Context) {
	ctx = coreauth.WithSkipPersist(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case update, ok := <-s.authUpdates:
			if !ok {
				return
			}
			s.handleAuthUpdate(ctx, update)
		labelDrain:
			for {
				select {
				case <-ctx.Done():
					return
				case nextUpdate, okNext := <-s.authUpdates:
					if !okNext {
						return
					}
					s.handleAuthUpdate(ctx, nextUpdate)
				default:
					break labelDrain
				}
			}
		}
	}
}

func (s *Service) emitAuthUpdate(ctx context.Context, update watcher.AuthUpdate) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if s.watcher != nil {
		result := s.watcher.dispatchRuntimeAuthUpdateResult(update)
		if result.Enqueued || result.Consumed {
			return
		}
		if result.Fallback != nil {
			update = *result.Fallback
		}
	}
	if s.authUpdates != nil {
		select {
		case s.authUpdates <- update:
			return
		default:
			log.Debugf("auth update queue saturated, applying inline action=%v id=%s", update.Action, update.ID)
		}
	}
	s.handleAuthUpdate(ctx, update)
}

func (s *Service) observeAuthUpdateQueued(update watcher.AuthUpdate) {
	if s == nil || s.coreManager == nil {
		return
	}
	switch update.Action {
	case watcher.AuthUpdateActionAdd, watcher.AuthUpdateActionModify, watcher.AuthUpdateActionDelete:
		s.coreManager.MarkChatGPTWebDependencyIndexDirty()
	}
}

func (s *Service) handleAuthUpdate(ctx context.Context, update watcher.AuthUpdate) {
	if s == nil {
		return
	}
	if update.Action == watcher.AuthUpdateActionReconcileChatGPTWebDependencies {
		_, _ = s.reconcileChatGPTWebDependencies(ctx, "watcher")
		return
	}
	if update.Action == watcher.AuthUpdateActionBarrier {
		if update.Applied != nil {
			close(update.Applied)
		}
		return
	}
	s.observeAuthUpdateQueued(update)
	s.cfgMu.RLock()
	cfg := s.cfg
	s.cfgMu.RUnlock()
	if cfg == nil || s.coreManager == nil {
		return
	}
	switch update.Action {
	case watcher.AuthUpdateActionAdd, watcher.AuthUpdateActionModify:
		if update.Auth == nil || update.Auth.ID == "" {
			return
		}
		if !authMaintenancePendingDelete(update.Auth) {
			if candidate, ok := s.authMaintenanceCandidateForAuth(update.Auth, strings.TrimSpace(cfg.AuthDir), ""); ok {
				s.cancelAuthMaintenanceCandidate(candidate)
			}
		}
		s.applyCoreAuthAddOrUpdate(ctx, update.Auth)
	case watcher.AuthUpdateActionDelete:
		id := update.ID
		if id == "" && update.Auth != nil {
			id = update.Auth.ID
		}
		if id == "" {
			return
		}
		deletedPath := authUpdateDeletionPath(update.Auth, strings.TrimSpace(cfg.AuthDir))
		if deletedPath != "" && s.authMaintenancePathStaged(deletedPath) {
			return
		}
		if deletedPath != "" {
			matchesDelete := func(current *coreauth.Auth) bool {
				if current == nil || !sameAuthFilePath(resolveAuthFilePath(current, strings.TrimSpace(cfg.AuthDir)), deletedPath) {
					return false
				}
				if !authDeleteUpdateMatchesCurrentGeneration(update.Auth, current) {
					return false
				}
				return !authUpdateIsMaintenanceDelete(update.Auth) || authMaintenancePendingDelete(current)
			}
			modelSyncEpoch := s.modelSyncTaskEpoch(id)
			current, ok := s.coreManager.GetByID(id)
			if !ok {
				s.cancelModelSyncTaskIfEpoch(id, modelSyncEpoch)
				return
			}
			if !matchesDelete(current) {
				return
			}
			indexes := s.usageAuthIndexesForIDs([]string{id})
			ctx = coreauth.WithSkipPersist(ctx)
			deleted, errDelete := s.coreManager.DeleteIf(ctx, id, matchesDelete)
			if errDelete != nil || !deleted {
				return
			}
			s.cancelModelSyncTaskIfEpoch(id, modelSyncEpoch)
			executorhelps.CloseProxyTransportCachesForAuth(id)
			s.cleanupChatGPTWebModelResourcesAfterDelete(ctx, id, current.RuntimeInstanceID())
			s.removeUsageStatisticsForAuthIndexes(indexes, "auth delete")
			return
		}
		candidate, ok := s.authMaintenanceCandidateForID(id, strings.TrimSpace(cfg.AuthDir), "file_removed")
		if !ok {
			_ = s.deleteCoreAuth(coreauth.WithSkipPersist(ctx), id)
			return
		}
		indexes := s.usageAuthIndexesForIDs(candidate.IDs)
		ctx = coreauth.WithSkipPersist(ctx)
		for _, candidateID := range candidate.IDs {
			if errDelete := s.deleteCoreAuth(ctx, candidateID); errDelete != nil {
				return
			}
		}
		s.removeUsageStatisticsForAuthIndexes(indexes, "auth delete")
	default:
		log.Debugf("received unknown auth update action: %v", update.Action)
	}
}

func (s *Service) reconcileChatGPTWebDependencies(ctx context.Context, reason string) ([]string, error) {
	if s == nil || s.coreManager == nil {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	indexesByID := make(map[string]string)
	modelSyncEpochs := make(map[string]uint64)
	for _, auth := range s.coreManager.List() {
		if auth == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(auth) {
			continue
		}
		indexesByID[auth.ID] = auth.EnsureIndex()
		modelSyncEpochs[auth.ID] = s.modelSyncTaskEpoch(auth.ID)
	}
	deletedIDs, errReconcile := s.coreManager.ReconcileChatGPTWebDependencies(ctx)
	if errReconcile != nil {
		log.WithError(errReconcile).WithField("reason", reason).Warn("failed to reconcile ChatGPT Web credential dependencies")
	}
	if len(deletedIDs) == 0 {
		return nil, errReconcile
	}
	indexes := make([]string, 0, len(deletedIDs))
	for _, id := range deletedIDs {
		s.cancelModelSyncTaskIfEpoch(id, modelSyncEpochs[id])
		executorhelps.CloseProxyTransportCachesForAuth(id)
		s.cleanupChatGPTWebModelResourcesAfterDelete(ctx, id, "")
		if index := strings.TrimSpace(indexesByID[id]); index != "" {
			indexes = append(indexes, index)
		}
	}
	s.removeUsageStatisticsForAuthIndexes(indexes, "chatgpt web dependency reconcile")
	if s.reconcileUsageStatistics("chatgpt web dependency reconcile") > 0 {
		s.persistUsageStatistics("chatgpt-web-dependency-reconcile")
	}
	log.WithFields(log.Fields{"reason": reason, "deleted": len(deletedIDs)}).Info("removed retained Codex credentials without Web dependents")
	return deletedIDs, errReconcile
}

func (s *Service) ensureWebsocketGateway() {
	if s == nil {
		return
	}
	if s.wsGateway != nil {
		return
	}
	opts := wsrelay.Options{
		Path:           "/v1/ws",
		OnConnected:    s.wsOnConnected,
		OnDisconnected: s.wsOnDisconnected,
		LogDebugf:      log.Debugf,
		LogInfof:       log.Infof,
		LogWarnf:       log.Warnf,
	}
	s.wsGateway = wsrelay.NewManager(opts)
}

func (s *Service) wsOnConnected(channelID string) {
	if s == nil || channelID == "" {
		return
	}
	if !strings.HasPrefix(strings.ToLower(channelID), "aistudio-") {
		return
	}
	if s.coreManager != nil {
		if existing, ok := s.coreManager.GetByID(channelID); ok && existing != nil {
			if !existing.Disabled && existing.Status == coreauth.StatusActive {
				return
			}
		}
	}
	now := time.Now().UTC()
	auth := &coreauth.Auth{
		ID:         channelID,  // keep channel identifier as ID
		Provider:   "aistudio", // logical provider for switch routing
		Label:      channelID,  // display original channel id
		Status:     coreauth.StatusActive,
		CreatedAt:  now,
		UpdatedAt:  now,
		Attributes: map[string]string{"runtime_only": "true"},
		Metadata:   map[string]any{"email": channelID}, // metadata drives logging and usage tracking
	}
	log.Infof("websocket provider connected: %s", channelID)
	s.emitAuthUpdate(context.Background(), watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionAdd,
		ID:     auth.ID,
		Auth:   auth,
	})
}

func (s *Service) wsOnDisconnected(channelID string, reason error) {
	if s == nil || channelID == "" {
		return
	}
	if reason != nil {
		if strings.Contains(reason.Error(), "replaced by new connection") {
			log.Infof("websocket provider replaced: %s", channelID)
			return
		}
		log.Warnf("websocket provider disconnected: %s (%v)", channelID, reason)
	} else {
		log.Infof("websocket provider disconnected: %s", channelID)
	}
	ctx := context.Background()
	s.emitAuthUpdate(ctx, watcher.AuthUpdate{
		Action: watcher.AuthUpdateActionDelete,
		ID:     channelID,
	})
}

func (s *Service) applyCoreAuthAddOrUpdate(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || s.coreManager == nil || auth == nil {
		return
	}
	auth = auth.Clone()
	if auth.ID == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.cfgMu.RLock()
	authDir := ""
	if s.cfg != nil {
		authDir = strings.TrimSpace(s.cfg.AuthDir)
	}
	s.cfgMu.RUnlock()
	path := resolveAuthFilePath(auth, authDir)
	lockedCtx, unlockAuthMutation, errMutationLock := s.coreManager.LockAuthMutation(ctx, auth)
	if errMutationLock != nil {
		log.Errorf("failed to lock auth update %s: %v", auth.ID, errMutationLock)
		return
	}
	if s.authUpdateMutationLockedObserved != nil {
		s.authUpdateMutationLockedObserved(auth.Clone())
	}
	mutationLocked := true
	defer func() {
		if mutationLocked {
			unlockAuthMutation()
		}
	}()
	ctx = lockedCtx
	unlockPath := func() {}
	pathLocked := false
	if path != "" {
		var errPathLock error
		unlockPath, errPathLock = authfileguard.LockContext(ctx, path)
		if errPathLock != nil {
			log.Errorf("failed to lock auth file update %s: %v", auth.ID, errPathLock)
			return
		}
		pathLocked = true
		ctx = coreauth.WithSkipPersist(ctx)
	}
	defer func() {
		if pathLocked {
			unlockPath()
		}
	}()
	if !authFileUpdateStillCurrentAtPath(auth, path) {
		log.Debugf("ignoring stale auth file update for %s", auth.ID)
		return
	}
	unlockTransition, errTransition := s.lockAuthModelTransitionContext(ctx, auth.ID)
	if errTransition != nil {
		log.Errorf("failed to lock auth model transition %s: %v", auth.ID, errTransition)
		return
	}
	transitionLocked := true
	defer func() {
		if transitionLocked {
			unlockTransition()
		}
	}()
	s.ensureExecutorsForAuth(auth)

	// IMPORTANT: Update coreManager FIRST, before model registration.
	// This ensures that configuration changes (proxy_url, prefix, etc.) take effect
	// immediately for API calls, rather than waiting for model registration to complete.
	op := "register"
	var err error
	var installed *coreauth.Auth
	existing, existed := s.coreManager.GetByID(auth.ID)
	touchesChatGPTWeb := isNativeChatGPTWebAuth(auth) ||
		(existed && isNativeChatGPTWebAuth(existing))
	managerCtx := context.WithValue(ctx, modelSyncHookSuppressedContextKey{}, true)
	if existed {
		auth.CreatedAt = existing.CreatedAt
		if !existing.Disabled && existing.Status != coreauth.StatusDisabled && !auth.Disabled && auth.Status != coreauth.StatusDisabled {
			auth.LastRefreshedAt = existing.LastRefreshedAt
			auth.NextRefreshAfter = existing.NextRefreshAfter
			if !touchesChatGPTWeb && len(auth.ModelStates) == 0 && len(existing.ModelStates) > 0 {
				auth.ModelStates = existing.ModelStates
			}
		}
		preserveChatGPTWebReplacementRuntimeState(existing, auth)
		op = "update"
		installed, err = s.coreManager.Update(managerCtx, auth)
	} else {
		installed, err = s.coreManager.Register(managerCtx, auth)
	}
	if pathLocked {
		unlockPath()
		pathLocked = false
	}
	unlockAuthMutation()
	mutationLocked = false
	if err != nil {
		log.Errorf("failed to %s auth %s: %v", op, auth.ID, err)
		current, ok := s.coreManager.GetByID(auth.ID)
		if !ok || current.Disabled {
			GlobalModelRegistry().UnregisterClient(auth.ID)
			return
		}
		auth = current
	} else {
		auth = installed
		executorhelps.CloseProxyTransportCachesForAuth(auth.ID)
	}
	registryAction := chatGPTWebRegistryStateNone
	currentInstallation := false
	if touchesChatGPTWeb {
		var current *coreauth.Auth
		current, currentInstallation = s.coreManager.CurrentAuthInstallation(auth)
		if currentInstallation {
			auth = current
			registryAction = s.reconcileChatGPTWebAuthStateLocked(ctx, auth, true, false)
		}
	}
	unlockTransition()
	transitionLocked = false
	if currentInstallation {
		s.applyChatGPTWebRegistryState(ctx, auth, registryAction)
	}

	// Register models after auth is updated in coreManager.
	// When the background model sync pool is running, keep this work off the
	// auth update hot path so watcher bursts do not block on registry sync.
	if !s.enqueueModelSync(auth.ID) {
		s.syncAuthModelsInline(ctx, auth.ID)
	}
	if isNativeChatGPTWebAuth(auth) {
		s.triggerChatGPTWebRelogin(auth)
	}
}

func authFileUpdateStillCurrent(auth *coreauth.Auth, authDir string) bool {
	path := resolveAuthFilePath(auth, authDir)
	return authFileUpdateStillCurrentAtPath(auth, path)
}

func authFileUpdateStillCurrentAtPath(auth *coreauth.Auth, path string) bool {
	if path == "" {
		return true
	}
	data, errRead := sdkAuth.ReadAuthFileSnapshot(path)
	if errRead != nil || len(data) == 0 || coreauth.IsRetiredGeminiCLIAuthFileData(data) {
		return false
	}
	expectedHash := ""
	if auth.Attributes != nil {
		expectedHash = strings.TrimSpace(auth.Attributes[coreauth.SourceHashAttributeKey])
	}
	if expectedHash == "" {
		return true
	}
	current := &coreauth.Auth{}
	if errSync := coreauth.SyncPersistedMetadataAndSourceHash(current, data); errSync != nil || current.Attributes == nil {
		return false
	}
	return strings.TrimSpace(current.Attributes[coreauth.SourceHashAttributeKey]) == expectedHash
}

func (s *Service) applyCoreAuthRemoval(ctx context.Context, id string) {
	s.applyCoreAuthRemovalWithReason(ctx, id, "", false)
}

func (s *Service) deleteCoreAuth(ctx context.Context, id string) error {
	if s == nil || s.coreManager == nil {
		return nil
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	removedRuntimeInstanceID := ""
	modelSyncEpoch := s.modelSyncTaskEpoch(id)
	if current, ok := s.coreManager.GetByID(id); ok && current != nil {
		removedRuntimeInstanceID = current.RuntimeInstanceID()
	}
	errDelete := s.coreManager.Delete(ctx, id)
	_, stillInstalled := s.coreManager.GetByID(id)
	if errDelete != nil && stillInstalled {
		log.Errorf("failed to delete auth %s: %v", id, errDelete)
		return errDelete
	}
	s.cancelModelSyncTaskIfEpoch(id, modelSyncEpoch)
	executorhelps.CloseProxyTransportCachesForAuth(id)
	s.antigravityModelCapabilities.Delete(id)
	s.cleanupChatGPTWebModelResourcesAfterDelete(ctx, id, removedRuntimeInstanceID)
	if errDelete != nil {
		log.WithError(errDelete).Warnf("auth %s was quarantined after an uncertain persistent deletion", id)
	}
	return errDelete
}

func (s *Service) applyCoreAuthRemovalWithReason(ctx context.Context, id string, reason string, pendingDelete bool) bool {
	if s == nil || strings.TrimSpace(id) == "" {
		return false
	}
	if s.coreManager == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id = strings.TrimSpace(id)
	existing, ok := s.coreManager.GetByID(id)
	if !ok || existing == nil {
		executorhelps.CloseProxyTransportCachesForAuth(id)
		s.antigravityModelCapabilities.Delete(id)
		s.cleanupChatGPTWebModelResourcesAfterDelete(ctx, id, "")
		return true
	}

	now := time.Now().UTC()
	previousDisabled := existing.Disabled
	existing.Disabled = true
	existing.Unavailable = true
	existing.Status = coreauth.StatusDisabled
	if pendingDelete {
		if strings.TrimSpace(reason) == "" {
			reason = "pending_delete"
		}
		existing.StatusMessage = fmt.Sprintf("disabled by auth maintenance (%s)", reason)
	} else if strings.TrimSpace(reason) != "" {
		existing.StatusMessage = fmt.Sprintf("disabled by auth maintenance (%s)", reason)
	} else if existing.StatusMessage == "" {
		existing.StatusMessage = "disabled"
	}
	existing.UpdatedAt = now
	if existing.Metadata == nil {
		existing.Metadata = make(map[string]any)
	}
	existing.Metadata["disabled"] = true
	if pendingDelete {
		if _, exists := existing.Metadata[authMaintenancePreviousDisabledKey]; !exists {
			existing.Metadata[authMaintenancePreviousDisabledKey] = previousDisabled
		}
		existing.Metadata[authMaintenanceActionMetadataKey] = authMaintenanceDeleteAction
		existing.Metadata[authMaintenancePendingDeleteMetadataKey] = true
	} else {
		delete(existing.Metadata, authMaintenancePendingDeleteMetadataKey)
	}
	if strings.TrimSpace(reason) != "" {
		if !pendingDelete {
			existing.Metadata[authMaintenanceActionMetadataKey] = authMaintenanceDisableAction
		}
		existing.Metadata[authMaintenanceReasonMetadataKey] = strings.TrimSpace(reason)
		existing.Metadata[authMaintenanceMarkedAtMetadataKey] = now.Format(time.RFC3339Nano)
	} else {
		if pendingDelete {
			existing.Metadata[authMaintenanceMarkedAtMetadataKey] = now.Format(time.RFC3339Nano)
		} else {
			delete(existing.Metadata, authMaintenanceActionMetadataKey)
			delete(existing.Metadata, authMaintenanceMarkedAtMetadataKey)
		}
		delete(existing.Metadata, authMaintenanceReasonMetadataKey)
	}

	if _, err := s.coreManager.Update(ctx, existing); err != nil {
		log.Errorf("failed to disable auth %s: %v", id, err)
		return false
	}
	executorhelps.CloseProxyTransportCachesForAuth(id)
	s.antigravityModelCapabilities.Delete(id)
	s.chatGPTWebModelCatalog.Delete(id)
	GlobalModelRegistry().UnregisterClient(id)
	if strings.EqualFold(strings.TrimSpace(existing.Provider), "codex") {
		s.ensureExecutorsForAuth(existing)
	}
	return true
}

func (s *Service) applyRetryConfig(cfg *config.Config) {
	if s == nil || s.coreManager == nil || cfg == nil {
		return
	}
	maxInterval := time.Duration(cfg.MaxRetryInterval) * time.Second
	s.coreManager.SetRetryConfig(cfg.RequestRetry, maxInterval, cfg.MaxRetryCredentials)
}

func (s *Service) enqueueModelSync(authID string) bool {
	return s.enqueueModelSyncTask(authID, false)
}

func (s *Service) enqueueModelSyncTask(authID string, retainWhenStopped bool) bool {
	installationID := ""
	if s != nil && s.coreManager != nil {
		if current, ok := s.coreManager.GetByID(strings.TrimSpace(authID)); ok && current != nil {
			installationID = current.RuntimeInstallationID()
		}
	}
	return s.enqueueModelSyncTaskForInstallation(authID, installationID, retainWhenStopped)
}

func (s *Service) enqueueModelSyncTaskForInstallation(
	authID string,
	installationID string,
	retainWhenStopped bool,
) bool {
	return s.enqueueModelSyncTaskForInstallationKind(authID, installationID, retainWhenStopped, false)
}

func (s *Service) enqueueImportModelSyncTaskForInstallation(
	authID string,
	installationID string,
	retainWhenStopped bool,
) bool {
	return s.enqueueModelSyncTaskForInstallationKind(authID, installationID, retainWhenStopped, true)
}

func (s *Service) enqueueModelSyncTaskForInstallationKind(
	authID string,
	installationID string,
	retainWhenStopped bool,
	importOnly bool,
) bool {
	if s == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	installationID = strings.TrimSpace(installationID)
	if authID == "" {
		return false
	}
	s.modelSyncMu.Lock()
	if s.modelSyncPending == nil {
		s.modelSyncPending = make(map[string]modelSyncTaskState)
	}
	if s.modelSyncOverflowSet == nil {
		s.modelSyncOverflowSet = make(map[string]struct{})
	}
	if s.modelSyncQueue == nil || s.modelSyncCancel == nil {
		if !retainWhenStopped {
			s.modelSyncMu.Unlock()
			return false
		}
		if state, exists := s.modelSyncPending[authID]; exists {
			if installationID != "" && installationID != state.installationID {
				state = modelSyncTaskState{
					epoch:          s.nextModelSyncEpochLocked(),
					installationID: installationID,
					importOnly:     importOnly,
				}
			} else if !importOnly {
				state.importOnly = false
			}
			state.running = false
			state.queued = true
			state.dirty = false
			s.modelSyncPending[authID] = state
			s.enqueueModelSyncOverflowLocked(authID)
		} else {
			state := modelSyncTaskState{
				epoch:          s.nextModelSyncEpochLocked(),
				installationID: installationID,
				importOnly:     importOnly,
				queued:         true,
			}
			s.modelSyncPending[authID] = state
			s.enqueueModelSyncOverflowLocked(authID)
		}
		s.modelSyncMu.Unlock()
		return true
	}
	if state, exists := s.modelSyncPending[authID]; exists {
		if installationID != "" && installationID != state.installationID {
			if state.running {
				if installationID != state.nextInstallationID {
					state.nextEpoch = s.nextModelSyncEpochLocked()
					state.nextInstallationID = installationID
					state.nextImportOnly = importOnly
				} else if !importOnly {
					state.nextImportOnly = false
				}
				state.dirty = true
				s.modelSyncPending[authID] = state
				s.modelSyncMu.Unlock()
				return true
			}
			alreadyDispatched := state.queued
			s.modelSyncPending[authID] = modelSyncTaskState{
				epoch:          s.nextModelSyncEpochLocked(),
				installationID: installationID,
				importOnly:     importOnly,
				queued:         true,
			}
			if !alreadyDispatched {
				s.promoteModelSyncOverflowLocked()
				select {
				case s.modelSyncQueue <- authID:
				default:
					s.enqueueModelSyncOverflowLocked(authID)
				}
			}
			s.modelSyncMu.Unlock()
			return true
		}
		if state.running {
			if !importOnly {
				state.importOnly = false
				state.dirty = true
			}
			s.modelSyncPending[authID] = state
		} else if !importOnly && state.importOnly {
			state.importOnly = false
			s.modelSyncPending[authID] = state
		}
		s.modelSyncMu.Unlock()
		return true
	}
	s.modelSyncPending[authID] = modelSyncTaskState{
		epoch:          s.nextModelSyncEpochLocked(),
		installationID: installationID,
		importOnly:     importOnly,
		queued:         true,
	}
	s.promoteModelSyncOverflowLocked()
	select {
	case s.modelSyncQueue <- authID:
		s.modelSyncMu.Unlock()
		return true
	default:
		s.enqueueModelSyncOverflowLocked(authID)
		s.modelSyncMu.Unlock()
		return true
	}
}

func (s *Service) promoteModelSyncOverflowLocked() {
	s.ensureModelSyncOverflowTokensLocked()
	for len(s.modelSyncOverflow) > 0 {
		authID := s.modelSyncOverflow[0]
		token := s.modelSyncOverflowTokens[0]
		currentToken, current := s.modelSyncOverflowToken[authID]
		if !current || currentToken != token {
			s.popModelSyncOverflowLocked()
			continue
		}
		state, pending := s.modelSyncPending[authID]
		if !pending || !state.queued {
			s.popModelSyncOverflowLocked()
			continue
		}
		select {
		case s.modelSyncQueue <- authID:
			s.popModelSyncOverflowLocked()
		default:
			return
		}
	}
}

func (s *Service) enqueueModelSyncOverflowLocked(authID string) {
	if authID == "" {
		return
	}
	if s.modelSyncOverflowSet == nil {
		s.modelSyncOverflowSet = make(map[string]struct{})
	}
	if s.modelSyncOverflowToken == nil {
		s.modelSyncOverflowToken = make(map[string]uint64)
	}
	if _, exists := s.modelSyncOverflowSet[authID]; exists {
		return
	}
	s.modelSyncOverflowNextToken++
	if s.modelSyncOverflowNextToken == 0 {
		s.modelSyncOverflowNextToken++
	}
	token := s.modelSyncOverflowNextToken
	s.modelSyncOverflowSet[authID] = struct{}{}
	s.modelSyncOverflowToken[authID] = token
	s.modelSyncOverflow = append(s.modelSyncOverflow, authID)
	s.modelSyncOverflowTokens = append(s.modelSyncOverflowTokens, token)
}

func (s *Service) popModelSyncOverflowLocked() {
	if len(s.modelSyncOverflow) == 0 {
		return
	}
	authID := s.modelSyncOverflow[0]
	token := uint64(0)
	if len(s.modelSyncOverflowTokens) > 0 {
		token = s.modelSyncOverflowTokens[0]
		s.modelSyncOverflowTokens = s.modelSyncOverflowTokens[1:]
	}
	s.modelSyncOverflow = s.modelSyncOverflow[1:]
	if current, ok := s.modelSyncOverflowToken[authID]; ok && current == token {
		delete(s.modelSyncOverflowToken, authID)
		delete(s.modelSyncOverflowSet, authID)
	}
}

func (s *Service) invalidateModelSyncOverflowLocked(authID string) {
	delete(s.modelSyncOverflowSet, authID)
	delete(s.modelSyncOverflowToken, authID)
}

func (s *Service) ensureModelSyncOverflowTokensLocked() {
	if len(s.modelSyncOverflowTokens) == len(s.modelSyncOverflow) {
		return
	}
	s.modelSyncOverflowTokens = s.modelSyncOverflowTokens[:0]
	if s.modelSyncOverflowToken == nil {
		s.modelSyncOverflowToken = make(map[string]uint64)
	} else {
		clear(s.modelSyncOverflowToken)
	}
	for _, authID := range s.modelSyncOverflow {
		s.modelSyncOverflowNextToken++
		if s.modelSyncOverflowNextToken == 0 {
			s.modelSyncOverflowNextToken++
		}
		token := s.modelSyncOverflowNextToken
		s.modelSyncOverflowTokens = append(s.modelSyncOverflowTokens, token)
		if _, active := s.modelSyncOverflowSet[authID]; active {
			s.modelSyncOverflowToken[authID] = token
		}
	}
}

func (s *Service) nextModelSyncEpochLocked() uint64 {
	s.modelSyncNextEpoch++
	if s.modelSyncNextEpoch == 0 {
		s.modelSyncNextEpoch++
	}
	return s.modelSyncNextEpoch
}

func (s *Service) cancelModelSyncTask(authID string) {
	s.cancelModelSyncTaskIfEpoch(authID, s.modelSyncTaskEpoch(authID))
}

func (s *Service) modelSyncTaskEpoch(authID string) uint64 {
	if s == nil {
		return 0
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return 0
	}
	s.modelSyncMu.Lock()
	defer s.modelSyncMu.Unlock()
	state := s.modelSyncPending[authID]
	if state.nextEpoch != 0 {
		return state.nextEpoch
	}
	return state.epoch
}

func (s *Service) cancelModelSyncTaskIfEpoch(authID string, epoch uint64) {
	if s == nil || epoch == 0 {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	s.modelSyncMu.Lock()
	state, pending := s.modelSyncPending[authID]
	if !pending {
		s.modelSyncMu.Unlock()
		return
	}
	if state.nextEpoch == epoch {
		state.nextEpoch = 0
		state.nextInstallationID = ""
		s.modelSyncPending[authID] = state
		s.modelSyncMu.Unlock()
		return
	}
	if state.epoch != epoch || state.nextEpoch != 0 {
		s.modelSyncMu.Unlock()
		return
	}
	delete(s.modelSyncPending, authID)
	s.invalidateModelSyncOverflowLocked(authID)
	s.promoteModelSyncOverflowLocked()
	s.modelSyncMu.Unlock()
}

func (s *Service) cancelQueuedImportModelSyncTasks() {
	if s == nil {
		return
	}
	s.modelSyncMu.Lock()
	for authID, state := range s.modelSyncPending {
		if state.queued && state.importOnly {
			delete(s.modelSyncPending, authID)
			s.invalidateModelSyncOverflowLocked(authID)
			continue
		}
		if state.nextEpoch != 0 && state.nextImportOnly {
			state.nextEpoch = 0
			state.nextInstallationID = ""
			state.nextImportOnly = false
			state.dirty = false
			s.modelSyncPending[authID] = state
		}
	}
	s.promoteModelSyncOverflowLocked()
	s.modelSyncMu.Unlock()
}

func (s *Service) applyChatGPTWebImportModelValidationConfig(ctx context.Context, enabled bool) {
	if s == nil {
		return
	}
	previous := s.chatGPTWebImportModelsEnabled.Swap(enabled)
	if previous == enabled {
		return
	}
	if enabled {
		s.restoreChatGPTWebImportModelIntents(ctx)
		return
	}
	s.cancelQueuedImportModelSyncTasks()
}

func (s *Service) beginModelSyncTask(authID string, generation uint64) (uint64, bool) {
	epoch, _, ok := s.beginModelSyncTaskWithKind(authID, generation)
	return epoch, ok
}

func (s *Service) beginModelSyncTaskWithKind(authID string, generation uint64) (uint64, bool, bool) {
	if s == nil {
		return 0, false, false
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return 0, false, false
	}
	s.modelSyncMu.Lock()
	defer s.modelSyncMu.Unlock()
	if generation != s.modelSyncGeneration {
		return 0, false, false
	}
	state, exists := s.modelSyncPending[authID]
	if !exists || !state.queued {
		s.promoteModelSyncOverflowLocked()
		return 0, false, false
	}
	state.queued = false
	state.running = true
	state.dirty = false
	s.modelSyncPending[authID] = state
	return state.epoch, state.importOnly, true
}

func (s *Service) completeModelSyncTask(
	authID string,
	epoch uint64,
	generation uint64,
	allowImmediateRetry bool,
) bool {
	if s == nil {
		return false
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return false
	}
	s.modelSyncMu.Lock()
	defer s.modelSyncMu.Unlock()
	if generation != s.modelSyncGeneration {
		return false
	}
	state, exists := s.modelSyncPending[authID]
	if !exists || state.epoch != epoch || !state.running {
		return false
	}
	if state.nextEpoch != 0 {
		state.epoch = state.nextEpoch
		state.installationID = state.nextInstallationID
		state.importOnly = state.nextImportOnly
		state.nextEpoch = 0
		state.nextInstallationID = ""
		state.nextImportOnly = false
		state.running = false
		state.queued = true
		state.dirty = false
		s.modelSyncPending[authID] = state
		s.promoteModelSyncOverflowLocked()
		select {
		case s.modelSyncQueue <- authID:
		default:
			s.enqueueModelSyncOverflowLocked(authID)
		}
		return false
	}
	if state.dirty && allowImmediateRetry {
		state.dirty = false
		s.modelSyncPending[authID] = state
		return true
	}
	if state.dirty {
		state.dirty = false
		state.running = false
		state.queued = true
		s.modelSyncPending[authID] = state
		s.promoteModelSyncOverflowLocked()
		select {
		case s.modelSyncQueue <- authID:
		default:
			s.enqueueModelSyncOverflowLocked(authID)
		}
		return false
	}
	delete(s.modelSyncPending, authID)
	s.promoteModelSyncOverflowLocked()
	return false
}

func (s *Service) lockAuthModelTransition(authID string) func() {
	unlock, err := s.lockAuthModelTransitionContext(context.Background(), authID)
	if err != nil || unlock == nil {
		return func() {}
	}
	return unlock
}

func (s *Service) lockAuthModelTransitionContext(ctx context.Context, authID string) (func(), error) {
	if s == nil {
		return func() {}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return func() {}, nil
	}
	s.authModelTransitionMu.Lock()
	if s.authModelTransitionLocks == nil {
		s.authModelTransitionLocks = make(map[string]*authModelTransitionLockEntry)
	}
	entry := s.authModelTransitionLocks[authID]
	if entry == nil {
		entry = &authModelTransitionLockEntry{semaphore: make(chan struct{}, 1)}
		entry.semaphore <- struct{}{}
		s.authModelTransitionLocks[authID] = entry
	}
	entry.references++
	s.authModelTransitionMu.Unlock()

	select {
	case <-ctx.Done():
		s.releaseAuthModelTransitionReference(authID, entry)
		return nil, ctx.Err()
	case <-entry.semaphore:
	}
	if errContext := ctx.Err(); errContext != nil {
		entry.semaphore <- struct{}{}
		s.releaseAuthModelTransitionReference(authID, entry)
		return nil, errContext
	}

	var once sync.Once
	return func() {
		once.Do(func() {
			entry.semaphore <- struct{}{}
			s.releaseAuthModelTransitionReference(authID, entry)
		})
	}, nil
}

func (s *Service) releaseAuthModelTransitionReference(authID string, entry *authModelTransitionLockEntry) {
	if s == nil || entry == nil {
		return
	}
	s.authModelTransitionMu.Lock()
	defer s.authModelTransitionMu.Unlock()
	entry.references--
	if entry.references == 0 && s.authModelTransitionLocks[authID] == entry {
		delete(s.authModelTransitionLocks, authID)
	}
}

func (s *Service) stopAuthUpdateQueue(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.authQueueMu.Lock()
	cancel := s.authQueueStop
	done := s.authQueueDone
	if cancel != nil {
		cancel()
	}
	s.authQueueMu.Unlock()
	if done == nil {
		return nil
	}
	if s.authQueueWaitObserved != nil {
		s.authQueueWaitObserved()
	}
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.authQueueMu.Lock()
	if s.authQueueDone == done {
		s.authQueueStop = nil
		s.authQueueDone = nil
	}
	s.authQueueMu.Unlock()
	return nil
}

func (s *Service) syncAuthModels(ctx context.Context, authID string) {
	if s == nil || s.coreManager == nil {
		return
	}
	authID = strings.TrimSpace(authID)
	if authID == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	current, ok := s.coreManager.GetByID(authID)
	if !ok || current == nil {
		unlockTransition, errTransition := s.lockAuthModelTransitionContext(ctx, authID)
		if errTransition != nil {
			return
		}
		if replacement, exists := s.coreManager.GetByID(authID); exists && replacement != nil {
			unlockTransition()
			s.enqueueModelSyncTaskForInstallation(
				replacement.ID,
				replacement.RuntimeInstallationID(),
				true,
			)
			return
		}
		s.antigravityModelCapabilities.Delete(authID)
		s.chatGPTWebModelCatalog.Delete(authID)
		GlobalModelRegistry().UnregisterClient(authID)
		unlockTransition()
		return
	}
	if s.modelSyncAuthLoadedObserved != nil {
		s.modelSyncAuthLoadedObserved(current.Clone())
	}
	if isNativeChatGPTWebAuth(current) {
		s.syncChatGPTWebModels(ctx, current)
		return
	}

	lockedCtx, unlockMutation, errMutation := s.coreManager.LockAuthMutation(ctx, current)
	if errMutation != nil {
		return
	}
	if s.modelSyncMutationLockedObserved != nil {
		s.modelSyncMutationLockedObserved(current.Clone())
	}
	unlockTransition, errTransition := s.lockAuthModelTransitionContext(lockedCtx, authID)
	if errTransition != nil {
		unlockMutation()
		return
	}
	current, ok = s.coreManager.CurrentAuthInstallation(current)
	if !ok {
		replacement, exists := s.coreManager.GetByID(authID)
		unlockTransition()
		unlockMutation()
		if exists && replacement != nil {
			s.enqueueModelSyncTaskForInstallation(
				replacement.ID,
				replacement.RuntimeInstallationID(),
				true,
			)
		}
		return
	}
	provider := strings.ToLower(strings.TrimSpace(current.Provider))
	if isNativeChatGPTWebAuth(current) {
		unlockTransition()
		unlockMutation()
		s.syncChatGPTWebModels(ctx, current)
		return
	}
	if provider != "antigravity" || current.Disabled || current.Status == coreauth.StatusDisabled {
		s.antigravityModelCapabilities.Delete(authID)
	}
	s.chatGPTWebModelCatalog.Delete(authID)
	preserveTransientState := provider != "antigravity" &&
		registry.GetGlobalRegistry().GetProviderForClient(current.ID) != "" &&
		authHasTransientState(current)
	if preserveTransientState {
		s.registerModelsForAuthPreservingState(current)
	} else {
		s.registerModelsForAuth(current)
	}
	if preserveTransientState {
		s.coreManager.PruneRegistryModelStatesIfCurrent(lockedCtx, current)
	} else {
		s.coreManager.ReconcileRegistryModelStatesIfCurrent(lockedCtx, current)
	}
	s.coreManager.RefreshSchedulerEntry(current.ID)
	unlockTransition()
	unlockMutation()

	if provider != "antigravity" || current.Disabled || current.Status == coreauth.StatusDisabled {
		return
	}

	hints, source, okFetch := s.fetchAntigravityModelCapabilityHintsWithSource(ctx, current)
	if !okFetch {
		return
	}
	lockedCtx, unlockMutation, errMutation = s.coreManager.LockAuthMutation(ctx, source)
	if errMutation != nil {
		return
	}
	unlockTransition, errTransition = s.lockAuthModelTransitionContext(lockedCtx, authID)
	if errTransition != nil {
		unlockMutation()
		return
	}
	latest, currentSource := s.currentAuthForAntigravityCapability(source)
	if !currentSource {
		unlockTransition()
		unlockMutation()
		return
	}
	entry := &antigravityModelCapabilityCacheEntry{
		RuntimeInstanceID: latest.RuntimeInstanceID(),
		Hints:             hints,
	}
	s.antigravityModelCapabilities.Store(latest.ID, entry)
	s.registerModelsForAuth(latest)
	s.coreManager.ReconcileRegistryModelStatesIfCurrent(lockedCtx, latest)
	s.coreManager.RefreshSchedulerEntry(latest.ID)
	unlockTransition()
	unlockMutation()
}

func (s *Service) currentAuthForAntigravityCapability(source *coreauth.Auth) (*coreauth.Auth, bool) {
	if s == nil || s.coreManager == nil || source == nil || strings.TrimSpace(source.ID) == "" {
		return nil, false
	}
	current, ok := s.coreManager.GetByID(source.ID)
	if !ok || current == nil {
		return nil, false
	}
	if current.Disabled || current.Status == coreauth.StatusDisabled || !strings.EqualFold(strings.TrimSpace(current.Provider), "antigravity") {
		return current, false
	}
	expectedInstanceID := strings.TrimSpace(source.RuntimeInstanceID())
	return current, expectedInstanceID != "" && expectedInstanceID == strings.TrimSpace(current.RuntimeInstanceID())
}

func (s *Service) syncAuthModelsInline(ctx context.Context, authID string) {
	if s == nil {
		return
	}
	s.syncAuthModels(ctx, authID)
}

func (s *Service) syncAuthModelsForGeneration(ctx context.Context, authID string, generation uint64) {
	if s == nil {
		return
	}
	epoch, importOnly, ok := s.beginModelSyncTaskWithKind(authID, generation)
	if !ok {
		return
	}
	if importOnly {
		cfg := s.currentConfig()
		if cfg == nil || !cfg.ChatGPTWeb.Import.Resolved().ValidateModelsAfterUpload {
			s.completeModelSyncTask(authID, epoch, generation, false)
			return
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		s.syncAuthModels(ctx, authID)
		if !s.completeModelSyncTask(authID, epoch, generation, attempt == 0) {
			return
		}
	}
}

func (s *Service) handleManagementAuthStatusChange(ctx context.Context, auth *coreauth.Auth) {
	if s == nil || auth == nil {
		return
	}
	if strings.TrimSpace(auth.ID) == "" {
		return
	}
	if auth.Disabled || auth.Status == coreauth.StatusDisabled {
		s.antigravityModelCapabilities.Delete(auth.ID)
		if isNativeChatGPTWebAuth(auth) {
			s.chatGPTWebModelCatalog.Delete(auth.ID)
			GlobalModelRegistry().UnregisterClient(auth.ID)
			if s.coreManager != nil {
				s.coreManager.RefreshSchedulerEntry(auth.ID)
			}
		}
		return
	}
	authDir := ""
	if cfg := s.currentConfig(); cfg != nil {
		authDir = strings.TrimSpace(cfg.AuthDir)
	}
	if candidate, ok := s.authMaintenanceCandidateForAuth(auth, authDir, ""); ok {
		s.cancelAuthMaintenanceCandidate(candidate)
	}
	s.ensureExecutorsForAuth(auth)
	if provider := strings.ToLower(strings.TrimSpace(auth.Provider)); provider == "antigravity" || isNativeChatGPTWebAuth(auth) {
		// The manager update hook already queued or completed provider model sync.
		return
	}
	s.syncAuthModels(ctx, auth.ID)
}

func (s *Service) refreshChatGPTWebModelCatalogs(ctx context.Context) {
	if s == nil || s.coreManager == nil {
		return
	}
	for _, auth := range s.coreManager.ChatGPTWebAuths() {
		if !isNativeChatGPTWebAuth(auth) || auth.Disabled || auth.Status == coreauth.StatusDisabled || !auth.LifecycleSelectable() {
			continue
		}
		if !s.enqueueModelSync(auth.ID) {
			s.syncAuthModelsInline(ctx, auth.ID)
		}
	}
}

func (s *Service) startModelSyncLoop(parent context.Context) {
	if s == nil {
		return
	}
	s.modelSyncMu.Lock()
	if s.modelSyncDone != nil {
		select {
		case <-s.modelSyncDone:
			if s.modelSyncCancel != nil {
				s.modelSyncCancel()
			}
			s.modelSyncCancel = nil
			s.modelSyncDone = nil
			s.modelSyncQueue = nil
		default:
		}
	}
	if s.modelSyncCancel != nil {
		s.modelSyncMu.Unlock()
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	queue := make(chan string, defaultModelSyncQueueSize)
	done := make(chan struct{})
	s.modelSyncGeneration++
	generation := s.modelSyncGeneration
	if s.modelSyncPending == nil {
		s.modelSyncPending = make(map[string]modelSyncTaskState)
	}
	s.modelSyncOverflow = s.modelSyncOverflow[:0]
	s.modelSyncOverflowTokens = s.modelSyncOverflowTokens[:0]
	clear(s.modelSyncOverflowSet)
	clear(s.modelSyncOverflowToken)
	pendingIDs := make([]string, 0, len(s.modelSyncPending))
	for authID, state := range s.modelSyncPending {
		if state.nextEpoch != 0 {
			state.epoch = state.nextEpoch
			state.installationID = state.nextInstallationID
			state.importOnly = state.nextImportOnly
			state.nextEpoch = 0
			state.nextInstallationID = ""
			state.nextImportOnly = false
		}
		if state.epoch == 0 {
			state.epoch = s.nextModelSyncEpochLocked()
		}
		state.queued = true
		state.running = false
		state.dirty = false
		s.modelSyncPending[authID] = state
		pendingIDs = append(pendingIDs, authID)
	}
	sort.Strings(pendingIDs)
	for _, authID := range pendingIDs {
		s.enqueueModelSyncOverflowLocked(authID)
	}
	s.modelSyncCancel = cancel
	s.modelSyncDone = done
	s.modelSyncQueue = queue
	s.promoteModelSyncOverflowLocked()
	s.modelSyncMu.Unlock()

	go func() {
		defer close(done)
		var workers sync.WaitGroup
		for i := 0; i < defaultModelSyncWorkers; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case authID := <-queue:
						s.syncAuthModelsForGeneration(ctx, authID, generation)
					}
				}
			}()
		}
		workers.Wait()
	}()
}

func (s *Service) stopModelSyncLoop() {
	if s == nil {
		return
	}
	s.modelSyncMu.Lock()
	cancel := s.modelSyncCancel
	done := s.modelSyncDone
	if cancel != nil {
		s.modelSyncGeneration++
	}
	s.modelSyncMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	s.modelSyncMu.Lock()
	if s.modelSyncDone == done {
		s.modelSyncCancel = nil
		s.modelSyncDone = nil
		s.modelSyncQueue = nil
	}
	s.modelSyncMu.Unlock()
}

func openAICompatInfoFromAuth(a *coreauth.Auth) (providerKey string, compatName string, ok bool) {
	if a == nil {
		return "", "", false
	}
	if len(a.Attributes) > 0 {
		providerKey = strings.TrimSpace(a.Attributes["provider_key"])
		compatName = strings.TrimSpace(a.Attributes["compat_name"])
		if compatName != "" {
			if providerKey == "" {
				providerKey = compatName
			}
			return strings.ToLower(providerKey), compatName, true
		}
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "openai-compatibility") {
		return "openai-compatibility", strings.TrimSpace(a.Label), true
	}
	return "", "", false
}

func (s *Service) ensureExecutorsForAuth(a *coreauth.Auth) {
	s.ensureExecutorsForAuthWithMode(a, false)
}

func (s *Service) ensureExecutorsForAuthWithMode(a *coreauth.Auth, forceReplace bool) {
	if s == nil || s.coreManager == nil || a == nil {
		return
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "codex") {
		if !forceReplace {
			existingExecutor, hasExecutor := s.coreManager.Executor("codex")
			if hasExecutor {
				_, isCodexAutoExecutor := existingExecutor.(*executor.CodexAutoExecutor)
				if isCodexAutoExecutor {
					return
				}
			}
		}
		s.coreManager.RegisterExecutor(executor.NewCodexAutoExecutor(s.cfg))
		return
	}
	// Skip disabled auth entries when (re)binding executors.
	// Disabled auths can linger during config reloads (e.g., removed OpenAI-compat entries)
	// and must not override active provider executors.
	if a.Disabled {
		return
	}
	if compatProviderKey, _, isCompat := openAICompatInfoFromAuth(a); isCompat {
		if compatProviderKey == "" {
			compatProviderKey = strings.ToLower(strings.TrimSpace(a.Provider))
		}
		if compatProviderKey == "" {
			compatProviderKey = "openai-compatibility"
		}
		s.coreManager.RegisterExecutor(executor.NewOpenAICompatExecutor(compatProviderKey, s.cfg))
		return
	}
	if strings.EqualFold(strings.TrimSpace(a.Provider), "chatgpt-web") {
		s.ensureChatGPTWebExecutor(forceReplace)
		return
	}
	switch strings.ToLower(a.Provider) {
	case "gemini":
		s.coreManager.RegisterExecutor(executor.NewGeminiExecutor(s.cfg))
	case "gemini-interactions":
		s.coreManager.RegisterExecutor(executor.NewGeminiInteractionsExecutor(s.cfg))
	case "vertex":
		s.coreManager.RegisterExecutor(executor.NewGeminiVertexExecutor(s.cfg))
	case "aistudio":
		if s.wsGateway != nil {
			s.coreManager.RegisterExecutor(executor.NewAIStudioExecutor(s.cfg, a.ID, s.wsGateway))
		}
		return
	case "antigravity":
		s.coreManager.RegisterExecutor(executor.NewAntigravityExecutor(s.cfg))
	case "claude":
		s.coreManager.RegisterExecutor(executor.NewClaudeExecutor(s.cfg))
	case "kimi":
		s.coreManager.RegisterExecutor(executor.NewKimiExecutor(s.cfg))
	case "xai":
		s.coreManager.RegisterExecutor(executor.NewXAIAutoExecutor(s.cfg))
	default:
		providerKey := strings.ToLower(strings.TrimSpace(a.Provider))
		if providerKey == "" {
			providerKey = "openai-compatibility"
		}
		s.coreManager.RegisterExecutor(executor.NewOpenAICompatExecutor(providerKey, s.cfg))
	}
}

func (s *Service) ensureChatGPTWebExecutor(forceReplace bool) {
	if s == nil || s.coreManager == nil {
		return
	}
	cfg := s.currentConfig()
	s.chatGPTWebExecutorMu.Lock()
	defer s.chatGPTWebExecutorMu.Unlock()
	if s.chatGPTWebLoginCoordinator == nil {
		s.chatGPTWebLoginCoordinator = executor.NewChatGPTWebLoginCoordinator()
	}
	coordinator := s.chatGPTWebLoginCoordinator
	existingExecutor, hasExecutor := s.coreManager.Executor("chatgpt-web")
	if hasExecutor {
		if chatGPTWebExecutor, isChatGPTWebExecutor := existingExecutor.(*executor.ChatGPTWebExecutor); isChatGPTWebExecutor {
			if forceReplace {
				chatGPTWebExecutor.UpdateConfig(cfg)
			}
			return
		}
	}
	s.coreManager.RegisterExecutor(executor.NewChatGPTWebExecutorWithLoginCoordinator(cfg, s.coreManager, coordinator))
}

func (s *Service) registerResolvedModelsForAuth(a *coreauth.Auth, providerKey string, models []*ModelInfo) {
	s.registerResolvedModelsForAuthWithState(a, providerKey, models, false)
}

func (s *Service) registerResolvedModelsForAuthPreservingState(a *coreauth.Auth, providerKey string, models []*ModelInfo) {
	s.registerResolvedModelsForAuthWithState(a, providerKey, models, true)
}

func (s *Service) registerResolvedModelsForAuthWithState(a *coreauth.Auth, providerKey string, models []*ModelInfo, preserveTransientState bool) {
	if a == nil || a.ID == "" {
		return
	}
	if len(models) == 0 {
		GlobalModelRegistry().UnregisterClient(a.ID)
		return
	}
	if preserveTransientState {
		registry.GetGlobalRegistry().RegisterClientPreservingState(a.ID, providerKey, models)
		return
	}
	GlobalModelRegistry().RegisterClient(a.ID, providerKey, models)
}

// rebindExecutors refreshes provider executors so they observe the latest configuration.
func (s *Service) rebindExecutors() {
	if s == nil || s.coreManager == nil {
		return
	}
	s.rebindExecutorsForAuths(s.coreManager.List())
}

func (s *Service) rebindExecutorsForAuths(auths []*coreauth.Auth) {
	reboundCodex := false
	s.ensureChatGPTWebExecutor(true)
	for _, auth := range auths {
		if auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			if reboundCodex {
				continue
			}
			reboundCodex = true
		}
		if auth != nil && strings.EqualFold(strings.TrimSpace(auth.Provider), "chatgpt-web") {
			if _, _, isCompat := openAICompatInfoFromAuth(auth); !isCompat {
				continue
			}
		}
		s.ensureExecutorsForAuthWithMode(auth, true)
	}
	for _, auth := range auths {
		s.triggerChatGPTWebRelogin(auth)
	}
}

func normalizeRuntimeRoutingStrategy(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "fill-first", "fillfirst", "ff":
		return "fill-first"
	case "random", "rand", "r":
		return "random"
	default:
		return "round-robin"
	}
}

func runtimeRestartFields(previous, requested *config.Config) []string {
	if previous == nil || requested == nil {
		return nil
	}
	fields := make([]string, 0, 6)
	if previous.Host != requested.Host {
		fields = append(fields, "host")
	}
	if previous.Port != requested.Port {
		fields = append(fields, "port")
	}
	if previous.TLS != requested.TLS {
		fields = append(fields, "tls")
	}
	if previous.AuthDir != requested.AuthDir {
		fields = append(fields, "auth-dir")
	}
	if previous.CommercialMode != requested.CommercialMode {
		fields = append(fields, "commercial-mode")
	}
	if previous.RemoteManagement.AccessPath != requested.RemoteManagement.AccessPath {
		fields = append(fields, "remote-management.access-path")
	}
	return fields
}

func preserveRuntimeStartupFields(runtimeCfg, previous *config.Config) {
	if runtimeCfg == nil || previous == nil {
		return
	}
	runtimeCfg.Host = previous.Host
	runtimeCfg.Port = previous.Port
	runtimeCfg.TLS = previous.TLS
	runtimeCfg.AuthDir = previous.AuthDir
	runtimeCfg.CommercialMode = previous.CommercialMode
	runtimeCfg.RemoteManagement.AccessPath = previous.RemoteManagement.AccessPath
}

func runtimeConfigHash(cfg *config.Config) ([sha256.Size]byte, error) {
	if cfg == nil {
		return [sha256.Size]byte{}, nil
	}
	payload, errMarshal := yaml.Marshal(cfg)
	if errMarshal != nil {
		return [sha256.Size]byte{}, errMarshal
	}
	return sha256.Sum256(payload), nil
}

// ApplyRuntimeConfig atomically serializes configuration changes from both
// management writes and the file watcher. Startup-only fields remain persisted
// but are held at their current runtime values until process restart.
func (s *Service) ApplyRuntimeConfig(ctx context.Context, requested *config.Config) (config.RuntimeApplyResult, error) {
	result := config.RuntimeApplyResult{}
	if s == nil || requested == nil {
		return result, errors.New("runtime configuration is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	requestedSnapshot, errClone := config.Clone(requested)
	if errClone != nil {
		return result, errClone
	}
	s.runtimeConfigApplyMu.Lock()
	defer s.runtimeConfigApplyMu.Unlock()

	previous, errPrevious := config.Clone(s.currentConfig())
	if errPrevious != nil {
		return result, errPrevious
	}
	runtimeCfg, errRuntime := config.Clone(requestedSnapshot)
	if errRuntime != nil {
		return result, errRuntime
	}
	result.RestartFields = runtimeRestartFields(previous, requestedSnapshot)
	result.RestartRequired = len(result.RestartFields) > 0
	preserveRuntimeStartupFields(runtimeCfg, previous)
	digest, errDigest := runtimeConfigHash(runtimeCfg)
	if errDigest != nil {
		return result, fmt.Errorf("hash runtime configuration: %w", errDigest)
	}
	if !s.runtimeConfigHashed && previous != nil {
		if previousDigest, errPreviousDigest := runtimeConfigHash(previous); errPreviousDigest == nil {
			s.runtimeConfigDigest = previousDigest
			s.runtimeConfigHashed = true
		}
	}
	if s.runtimeConfigHashed && digest == s.runtimeConfigDigest {
		if s.server != nil {
			if errManagement := s.server.SetManagementConfig(requestedSnapshot); errManagement != nil {
				return result, errManagement
			}
		}
		if s.watcher != nil {
			s.watcher.SetConfig(runtimeCfg)
		}
		result.Applied = true
		result.Deduplicated = true
		return result, nil
	}

	if errApply := s.applyRuntimeConfigState(ctx, previous, runtimeCfg); errApply != nil {
		var errRollback error
		if previous != nil {
			errRollback = s.applyRuntimeConfigState(context.WithoutCancel(ctx), runtimeCfg, previous)
			if s.watcher != nil {
				s.watcher.SetConfig(previous)
			}
		}
		if errRollback != nil {
			return result, errors.Join(errApply, fmt.Errorf("rollback runtime configuration: %w", errRollback))
		}
		return result, errApply
	}
	if s.server != nil {
		if errManagement := s.server.SetManagementConfig(requestedSnapshot); errManagement != nil {
			errRollback := s.applyRuntimeConfigState(context.WithoutCancel(ctx), runtimeCfg, previous)
			if previous != nil && s.watcher != nil {
				s.watcher.SetConfig(previous)
			}
			if errRollback != nil {
				return result, errors.Join(errManagement, fmt.Errorf("rollback runtime configuration: %w", errRollback))
			}
			return result, errManagement
		}
	}
	if s.watcher != nil {
		s.watcher.SetConfig(runtimeCfg)
	}
	s.runtimeConfigDigest = digest
	s.runtimeConfigHashed = true
	result.Applied = true
	return result, nil
}

func (s *Service) applyRuntimeConfigState(ctx context.Context, previousCfg, nextCfg *config.Config) error {
	if s == nil || nextCfg == nil {
		return errors.New("runtime configuration is unavailable")
	}
	previousUsageEnabled := previousCfg != nil && previousCfg.UsageStatisticsEnabled
	previousUsageSettings := usagePersistenceSettingsForConfig(previousCfg)

	if s.coreManager != nil {
		var selector coreauth.Selector
		switch normalizeRuntimeRoutingStrategy(nextCfg.Routing.Strategy) {
		case "fill-first":
			selector = &coreauth.FillFirstSelector{Range: normalizedRoutingFillFirstRange(nextCfg)}
		case "random":
			selector = &coreauth.RandomSelector{}
		default:
			selector = &coreauth.RoundRobinSelector{}
		}
		if nextCfg.Routing.ClaudeCodeSessionAffinity || nextCfg.Routing.SessionAffinity {
			ttl := time.Hour
			if ttlText := strings.TrimSpace(nextCfg.Routing.SessionAffinityTTL); ttlText != "" {
				if parsed, errParse := time.ParseDuration(ttlText); errParse == nil && parsed > 0 {
					ttl = parsed
				}
			}
			failover := routingSessionAffinityFailoverEnabled(nextCfg)
			selector = coreauth.NewSessionAffinitySelectorWithConfig(coreauth.SessionAffinityConfig{
				Fallback: selector,
				TTL:      ttl,
				Failover: &failover,
			})
		}
		s.coreManager.SetSelector(selector)
	}

	s.applyRetryConfig(nextCfg)
	s.applyPprofConfig(nextCfg)
	executorhelps.CloseUnscopedProxyTransportCaches()
	if s.proxyPoolManager != nil {
		if errProxyConfig := s.proxyPoolManager.UpdateConfig(nextCfg); errProxyConfig != nil {
			return fmt.Errorf("update proxy pool runtime: %w", errProxyConfig)
		}
	}
	if s.server != nil {
		if errServer := s.server.UpdateClients(nextCfg); errServer != nil {
			return fmt.Errorf("update API server runtime: %w", errServer)
		}
	}
	s.cfgMu.Lock()
	s.cfg = nextCfg
	s.cfgMu.Unlock()
	if s.coreManager != nil {
		s.coreManager.SetConfig(nextCfg)
		s.coreManager.SetOAuthModelAlias(nextCfg.OAuthModelAlias)
	}
	s.rebindExecutors()
	authModelExclusionsChanged := authModelExclusionsSignature(previousCfg) != authModelExclusionsSignature(nextCfg)
	if s.coreManager != nil && !authModelExclusionsChanged && shouldRefreshChatGPTWebRegistrations(previousCfg, nextCfg) {
		for _, auth := range s.coreManager.ChatGPTWebAuths() {
			s.refreshChatGPTWebModelRegistration(ctx, auth)
		}
	}
	s.applyChatGPTWebImportModelValidationConfig(ctx, nextCfg.ChatGPTWeb.Import.Resolved().ValidateModelsAfterUpload)
	if s.coreManager != nil && authModelExclusionsChanged {
		for _, auth := range s.coreManager.ListMetadataSummaries("type", "provider_key", "compat_name") {
			if isNativeChatGPTWebAuth(auth) {
				s.refreshChatGPTWebModelRegistration(ctx, auth)
				continue
			}
			s.refreshModelRegistrationForAuth(auth)
		}
	} else if s.coreManager != nil && shouldRefreshCodexRegistrations(previousCfg, nextCfg) {
		for _, auth := range s.coreManager.AuthsForProviders("codex") {
			s.refreshModelRegistrationForAuth(auth)
		}
	}
	s.applyUsagePersistenceConfigChange(previousUsageEnabled, previousUsageSettings, nextCfg)
	s.warnAuthMaintenanceConfig(nextCfg.AuthMaintenance)
	s.wakeAuthMaintenance()
	return nil
}

func (s *Service) prepareAPIServer() error {
	if s.startupState == nil {
		s.startupState = api.NewStartupState()
	}
	if s.authManager == nil {
		s.authManager = newDefaultAuthManager()
	}

	// Management login must be available before the first ChatGPT Web
	// credential exists, including on a fresh installation.
	s.ensureChatGPTWebExecutor(false)

	serverOpts := append([]api.ServerOption(nil), s.serverOptions...)
	serverOpts = append(serverOpts, api.WithAuthStatusHook(s.handleManagementAuthStatusChange))
	serverOpts = append(serverOpts, api.WithAuthDeleteHook(s.handleManagementAuthDelete))
	serverOpts = append(serverOpts, api.WithChatGPTWebDependencyReconcileHook(s.reconcileChatGPTWebDependencies))
	serverOpts = append(serverOpts, api.WithChatGPTWebDeadAuthDeleteCountProvider(s.chatGPTWebDeadAuthDeletedCount.Load))
	serverOpts = append(serverOpts, api.WithProxyPoolManager(s.proxyPoolManager))
	serverOpts = append(serverOpts, api.WithRuntimeConfigApply(s.ApplyRuntimeConfig))
	serverOpts = append(serverOpts, api.WithStartupState(s.startupState))
	s.server = api.NewServer(s.cfg, s.coreManager, s.accessManager, s.configPath, serverOpts...)

	s.ensureWebsocketGateway()
	if s.server != nil && s.wsGateway != nil {
		s.server.AttachWebsocketRoute(s.wsGateway.Path(), s.wsGateway.Handler())
		s.server.SetWebsocketAuthChangeHandler(func(oldEnabled, newEnabled bool) {
			if oldEnabled == newEnabled {
				return
			}
			if !oldEnabled && newEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if errStop := s.wsGateway.Stop(ctx); errStop != nil {
					log.Warnf("failed to reset websocket connections after ws-auth change %t -> %t: %v", oldEnabled, newEnabled, errStop)
					return
				}
				log.Debugf("ws-auth enabled; existing websocket sessions terminated to enforce authentication")
				return
			}
			log.Debugf("ws-auth disabled; existing websocket sessions remain connected")
		})
	}

	if errBeforeStart := s.applyBeforeStartConfig(); errBeforeStart != nil {
		return errBeforeStart
	}
	s.configureModelRefreshCallback()
	return nil
}

func (s *Service) configureModelRefreshCallback() {
	registry.SetModelRefreshCallback(func(changedProviders []string) {
		if s == nil || s.coreManager == nil || len(changedProviders) == 0 {
			return
		}

		providerSet := make(map[string]bool, len(changedProviders))
		for _, p := range changedProviders {
			providerSet[strings.ToLower(strings.TrimSpace(p))] = true
		}

		auths := s.coreManager.AuthsForProviders(changedProviders...)
		refreshed := 0
		for _, item := range auths {
			if item == nil || item.ID == "" {
				continue
			}
			auth, ok := s.coreManager.GetByID(item.ID)
			if !ok || auth == nil || auth.Disabled {
				continue
			}
			provider := strings.ToLower(strings.TrimSpace(auth.Provider))
			if !providerSet[provider] {
				continue
			}
			if s.refreshModelRegistrationForAuth(auth) {
				refreshed++
			}
		}

		if refreshed > 0 {
			log.Infof("re-registered models for %d auth(s) due to model catalog changes: %v", refreshed, changedProviders)
		}
	})
}

func (s *Service) startAPIListener() error {
	if s.server == nil {
		return errors.New("cliproxy: API server unavailable")
	}
	finishListener := s.startupState.BeginStage("listener_start")
	s.serverErr = make(chan error, 1)
	go func() {
		if errStart := s.server.Start(); errStart != nil {
			s.serverErr <- errStart
		} else {
			s.serverErr <- nil
		}
	}()

	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case errStart := <-s.serverErr:
		finishListener(0, "listener_start_failed")
		if errStart == nil {
			return errors.New("cliproxy: API server stopped during startup")
		}
		return errStart
	case <-timer.C:
	}
	finishListener(1, "")
	s.startupState.SetPhase(api.StartupPhaseListenerReady)
	fmt.Printf("API server started successfully on: %s:%d\n", s.cfg.Host, s.cfg.Port)

	s.applyPprofConfig(s.cfg)
	if s.hooks.OnAfterStart != nil {
		s.hooks.OnAfterStart(s)
	}
	return nil
}

// Run starts the service and blocks until the context is cancelled or the server stops.
// It initializes all components including authentication, file watching, HTTP server,
// and starts processing requests. The method blocks until the context is cancelled.
//
// Parameters:
//   - ctx: The context for controlling the service lifecycle
//
// Returns:
//   - error: An error if the service fails to start or run
func (s *Service) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("cliproxy: service is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.chatGPTWebImportModelsEnabled.Store(
		s.cfg != nil && s.cfg.ChatGPTWeb.Import.Resolved().ValidateModelsAfterUpload,
	)

	sdkusage.StartDefault(ctx)

	defer func() {
		if err := s.shutdownOnRunExit(30 * time.Second); err != nil {
			log.Errorf("service shutdown returned error: %v", err)
		}
	}()

	if err := s.ensureAuthDir(); err != nil {
		return err
	}
	if errMigration := migrateLegacyAuthMaintenanceFiles(s.cfg.AuthDir); errMigration != nil {
		log.WithError(errMigration).Warn("legacy auth maintenance files require attention after migration")
	}

	s.applyRetryConfig(s.cfg)
	if errQuarantine := watcher.LoadAuthDeleteQuarantine(s.configPath, s.cfg.AuthDir); errQuarantine != nil {
		return fmt.Errorf("cliproxy: load auth deletion quarantine: %w", errQuarantine)
	}
	if errPrepare := s.prepareAPIServer(); errPrepare != nil {
		return errPrepare
	}
	if errStart := s.startAPIListener(); errStart != nil {
		return errStart
	}
	defer func() {
		if !s.startupState.Ready() {
			s.startupState.MarkFailed()
		}
	}()

	s.startupState.SetPhase(api.StartupPhaseAuthLoading)
	finishAuthLoad := s.startupState.BeginStage("auth_store_load")
	authLoadErrorCode := ""
	if s.coreManager != nil {
		if errLoad := s.coreManager.Load(ctx); errLoad != nil {
			log.Warnf("failed to load auth store: %v", errLoad)
			authLoadErrorCode = "auth_store_load_failed"
		}
	}
	authCount := int64(0)
	if s.coreManager != nil {
		authCount = int64(s.coreManager.Count())
	}
	finishAuthLoad(authCount, authLoadErrorCode)

	s.startupState.SetPhase(api.StartupPhaseRoutingBootstrap)
	finishDependencies := s.startupState.BeginStage("credential_dependency_reconcile")
	_, _ = s.reconcileChatGPTWebDependencies(ctx, "startup")
	finishDependencies(authCount, "")

	tokenResult, err := s.tokenProvider.Load(ctx, s.cfg)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if tokenResult == nil {
		tokenResult = &TokenClientResult{}
	}
	_ = tokenResult

	apiKeyResult, err := s.apiKeyProvider.Load(ctx, s.cfg)
	if err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	if apiKeyResult == nil {
		apiKeyResult = &APIKeyClientResult{}
	}
	_ = apiKeyResult
	// legacy clients removed; no caches to refresh

	finishRouting := s.startupState.BeginStage("routing_bootstrap")
	s.startModelSyncLoop(ctx)
	s.restoreChatGPTWebImportModelIntents(ctx)
	s.installAuthMaintenanceHook(ctx)
	if cfg, _ := s.snapshotAuthMaintenanceConfig(); cfg.Enable {
		s.warnAuthMaintenanceConfig(cfg)
	}

	var watcherWrapper *WatcherWrapper
	applyReloadedConfig := func(newCfg *config.Config) (config.RuntimeApplyResult, error) {
		if newCfg == nil {
			s.cfgMu.RLock()
			newCfg = s.cfg
			s.cfgMu.RUnlock()
		}
		if newCfg == nil {
			return config.RuntimeApplyResult{}, errors.New("reloaded configuration is unavailable")
		}
		result, errApply := s.ApplyRuntimeConfig(ctx, newCfg)
		if errApply != nil {
			return result, errApply
		}
		if result.RestartRequired {
			log.WithField("fields", result.RestartFields).Warn("configuration saved with startup-only changes; restart required")
		}
		return result, nil
	}
	reloadCallback := func(newCfg *config.Config) {
		if _, errApply := applyReloadedConfig(newCfg); errApply != nil {
			log.WithError(errApply).Error("failed to apply reloaded runtime configuration; previous runtime retained")
		}
	}

	watcherWrapper, err = s.watcherFactory(s.configPath, s.cfg.AuthDir, reloadCallback)
	if err != nil {
		finishRouting(authCount, "watcher_create_failed")
		s.startupState.MarkFailed()
		return fmt.Errorf("cliproxy: failed to create watcher: %w", err)
	}
	s.watcher = watcherWrapper
	watcherWrapper.SetConfigApply(func(newCfg *config.Config) (*config.Config, error) {
		if _, errApply := applyReloadedConfig(newCfg); errApply != nil {
			return nil, errApply
		}
		return config.Clone(s.currentConfig())
	})
	s.ensureAuthUpdateQueue(ctx)
	if s.authUpdates != nil {
		watcherWrapper.SetAuthUpdateObserver(s.observeAuthUpdateQueued)
		watcherWrapper.SetAuthUpdateQueue(s.authUpdates)
	}
	watcherWrapper.SetConfig(s.cfg)
	if s.coreManager != nil {
		watcherWrapper.SeedCurrentFileAuths(s.coreManager.List())
	}

	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	s.watcherCancel = watcherCancel
	if err = watcherWrapper.Start(watcherCtx); err != nil {
		finishRouting(authCount, "watcher_start_failed")
		s.startupState.MarkFailed()
		return fmt.Errorf("cliproxy: failed to start watcher: %w", err)
	}
	if err = watcherWrapper.WaitForAuthUpdates(ctx); err != nil {
		finishRouting(authCount, "watcher_initial_sync_failed")
		s.startupState.MarkFailed()
		return fmt.Errorf("cliproxy: wait for initial auth updates: %w", err)
	}
	log.Info("file watcher started for config and auth directory changes")
	// Start proxy health checks only after file-backed auths are visible, so
	// restored bindings are not pruned as stale.
	if s.proxyPoolManager != nil {
		s.proxyPoolManager.Start(ctx)
	}
	s.startAuthMaintenance(context.Background())

	// Prefer core auth manager auto refresh if available.
	if s.coreManager != nil {
		interval := 15 * time.Minute
		s.coreManager.StartAutoRefresh(context.Background(), interval)
		log.Infof("core auth auto-refresh started (interval=%s)", interval)
	}
	finishRouting(authCount, "")
	s.startupState.MarkReady()
	s.startUsageRestore()
	s.startUsagePersistenceLoop()

	select {
	case <-ctx.Done():
		log.Debug("service context cancelled, shutting down...")
		return ctx.Err()
	case err = <-s.serverErr:
		return err
	}
}

func (s *Service) shutdownOnRunExit(timeout time.Duration) error {
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), timeout)
	defer shutdownCancel()
	return s.Shutdown(shutdownCtx)
}

func (s *Service) applyBeforeStartConfig() error {
	if s.hooks.OnBeforeStart != nil {
		s.hooks.OnBeforeStart(s.cfg)
	}
	s.ensureChatGPTWebExecutor(true)
	if s.proxyPoolManager != nil {
		if errProxyConfig := s.proxyPoolManager.UpdateConfig(s.cfg); errProxyConfig != nil {
			return fmt.Errorf("cliproxy: apply proxy configuration after before-start hook: %w", errProxyConfig)
		}
	}
	return nil
}

// Shutdown gracefully stops background workers and the HTTP server.
// It ensures all resources are properly cleaned up and connections are closed.
// The shutdown is idempotent and can be called multiple times safely.
//
// Parameters:
//   - ctx: The context for controlling the shutdown timeout
//
// Returns:
//   - error: An error if shutdown fails
func (s *Service) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.shutdownOnce.Do(func() {
		s.shutdownDone = make(chan struct{})
		go s.runShutdown()
	})
	select {
	case <-s.shutdownDone:
	case <-ctx.Done():
		select {
		case <-s.shutdownDone:
		default:
			return ctx.Err()
		}
	}
	return s.shutdownErr
}

func (s *Service) runShutdown() {
	defer close(s.shutdownDone)
	var shutdownErr error

	if s.watcherCancel != nil {
		s.watcherCancel()
	}
	if s.server != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		errStop := s.server.Stop(shutdownCtx)
		cancel()
		if errStop != nil {
			log.Errorf("error stopping API server: %v", errStop)
			shutdownErr = errors.Join(shutdownErr, errStop)
		}
	}
	if s.coreManager != nil {
		s.coreManager.StopAutoRefresh()
		s.coreManager.BeginCloseExecutors()
	}
	if s.proxyPoolManager != nil {
		s.proxyPoolManager.Stop()
	}
	if s.watcher != nil {
		if errStop := s.watcher.Stop(); errStop != nil {
			log.Errorf("failed to stop file watcher: %v", errStop)
			shutdownErr = errors.Join(shutdownErr, errStop)
		}
	}
	if s.wsGateway != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		errStop := s.wsGateway.Stop(shutdownCtx)
		cancel()
		if errStop != nil {
			log.Errorf("failed to stop websocket gateway: %v", errStop)
			shutdownErr = errors.Join(shutdownErr, errStop)
		}
	}
	authQueueShutdownCtx, cancelAuthQueueShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	errStopAuthQueue := s.stopAuthUpdateQueue(authQueueShutdownCtx)
	cancelAuthQueueShutdown()
	if errStopAuthQueue != nil {
		log.WithError(errStopAuthQueue).Error("failed to stop authentication update queue")
		shutdownErr = errors.Join(shutdownErr, errStopAuthQueue)
	} else if s.authQueueStoppedObserved != nil {
		s.authQueueStoppedObserved()
	}
	s.stopModelSyncLoop()
	s.stopAuthMaintenance()
	s.stopUsagePersistenceLoop()
	needsUsageSidecar := s.stopUsageRestore()
	if s.usagePersistenceSettings().enabled {
		if needsUsageSidecar {
			if errPending := internalusage.PersistPendingRequestStatistics(s.usageStatisticsFilePath(), s.usageStatisticsStore()); errPending != nil {
				log.WithError(errPending).Error("failed to preserve pending usage statistics during shutdown")
				shutdownErr = errors.Join(shutdownErr, errPending)
			}
		} else {
			s.reconcileUsageStatistics("shutdown")
			s.persistUsageStatistics("shutdown")
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	errShutdownPprof := s.shutdownPprof(shutdownCtx)
	cancel()
	if errShutdownPprof != nil {
		log.Errorf("failed to stop pprof server: %v", errShutdownPprof)
		shutdownErr = errors.Join(shutdownErr, errShutdownPprof)
	}
	if s.coreManager != nil {
		errClose := s.coreManager.CloseExecutors()
		if errClose != nil {
			log.Errorf("failed to close provider executors: %v", errClose)
			shutdownErr = errors.Join(shutdownErr, errClose)
		}
	}

	sdkusage.StopDefault()
	executorhelps.CloseAllProxyTransportCaches()
	s.shutdownErr = shutdownErr
}

func (s *Service) ensureAuthDir() error {
	info, err := os.Stat(s.cfg.AuthDir)
	if err != nil {
		if os.IsNotExist(err) {
			if mkErr := os.MkdirAll(s.cfg.AuthDir, 0o755); mkErr != nil {
				return fmt.Errorf("cliproxy: failed to create auth directory %s: %w", s.cfg.AuthDir, mkErr)
			}
			log.Infof("created missing auth directory: %s", s.cfg.AuthDir)
			return nil
		}
		return fmt.Errorf("cliproxy: error checking auth directory %s: %w", s.cfg.AuthDir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("cliproxy: auth path exists but is not a directory: %s", s.cfg.AuthDir)
	}
	return nil
}

// registerModelsForAuth (re)binds provider models in the global registry using the core auth ID as client identifier.
func (s *Service) registerModelsForAuth(a *coreauth.Auth) {
	s.registerModelsForAuthWithState(a, false)
}

func (s *Service) registerModelsForAuthPreservingState(a *coreauth.Auth) {
	s.registerModelsForAuthWithState(a, true)
}

func (s *Service) registerModelsForAuthWithState(a *coreauth.Auth, preserveTransientState bool) {
	if a == nil || a.ID == "" {
		return
	}
	if a.Disabled {
		GlobalModelRegistry().UnregisterClient(a.ID)
		return
	}
	authKind := strings.ToLower(strings.TrimSpace(a.Attributes["auth_kind"]))
	if authKind == "" {
		if kind, _ := a.AccountInfo(); strings.EqualFold(kind, "api_key") {
			authKind = "apikey"
		}
	}
	// Unregister legacy client ID (if present) to avoid double counting
	if a.Runtime != nil {
		if idGetter, ok := a.Runtime.(interface{ GetClientID() string }); ok {
			if rid := idGetter.GetClientID(); rid != "" && rid != a.ID {
				GlobalModelRegistry().UnregisterClient(rid)
			}
		}
	}
	provider := strings.ToLower(strings.TrimSpace(a.Provider))
	compatProviderKey, compatDisplayName, compatDetected := openAICompatInfoFromAuth(a)
	if compatDetected {
		provider = "openai-compatibility"
	}
	excluded := s.oauthExcludedModels(provider, authKind)
	// The synthesizer pre-merges per-account and global exclusions into the "excluded_models" attribute.
	// If this attribute is present, it represents the complete list of exclusions and overrides the global config.
	if a.Attributes != nil {
		if val, ok := a.Attributes["excluded_models"]; ok && strings.TrimSpace(val) != "" {
			excluded = strings.Split(val, ",")
		}
	}
	var models []*ModelInfo
	switch provider {
	case "gemini":
		models = registry.GetGeminiModels()
		if entry := s.resolveConfigGeminiKey(a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildGeminiConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "gemini-interactions":
		models = registry.GetGeminiModels()
		if entry := s.resolveConfigInteractionsKey(a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildGeminiConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "vertex":
		// Vertex AI Gemini supports the same model identifiers as Gemini.
		models = registry.GetGeminiVertexModels()
		if entry := s.resolveConfigVertexCompatKey(a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildVertexCompatConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "aistudio":
		models = registry.GetAIStudioModels()
		models = applyExcludedModels(models, excluded)
	case "antigravity":
		models = registry.GetAntigravityModels()
		hints := antigravityModelCapabilityHints{}
		if cached, ok := s.antigravityModelCapabilities.Load(a.ID); ok {
			if entry, okEntry := cached.(*antigravityModelCapabilityCacheEntry); okEntry && entry != nil &&
				entry.RuntimeInstanceID == a.RuntimeInstanceID() {
				hints = entry.Hints
			} else if okEntry && entry != nil {
				s.antigravityModelCapabilities.CompareAndDelete(a.ID, entry)
			}
		}
		models = applyAntigravityFetchedModelCapabilities(models, hints)
		models = applyExcludedModels(models, excluded)
	case "claude":
		models = registry.GetClaudeModels()
		if entry := s.resolveConfigClaudeKey(a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildClaudeConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		models = applyExcludedModels(models, excluded)
	case "codex":
		codexPlanType := codexPlanTypeForRegistration(a)
		models = codexModelsForPlan(codexPlanType)
		if entry := s.resolveConfigCodexKey(a); entry != nil {
			if len(entry.Models) > 0 {
				models = buildCodexConfigModels(entry)
			}
			if authKind == "apikey" {
				excluded = entry.ExcludedModels
			}
		}
		if authKind != "apikey" {
			models = removeCodexCustomModelOverrides(models, s.cfg)
			models = upsertModelInfos(models, codexCustomModelInfosForPlan(s.cfg, codexPlanType))
		}
		allowImageModel := codexPlanAllowsImageModel(codexPlanType)
		if strings.EqualFold(codexPlanType, "free") && freePlanImageModelEnabled(s.cfg) {
			allowImageModel = true
		}
		if allowImageModel {
			models = upsertModelInfos(models, codexDynamicImageModelInfos(s.cfg))
		}
		models = applyExcludedModels(models, excluded)
	case "kimi":
		models = registry.GetKimiModels()
		models = applyExcludedModels(models, excluded)
	case "xai":
		models = registry.GetXAIModels()
		models = applyExcludedModels(models, excluded)
	case "chatgpt-web":
		if !a.LifecycleSelectable() {
			models = nil
			break
		}
		models = s.chatGPTWebModelsForAuth(a)
		models = applyExcludedModels(models, excluded)
	default:
		// Handle OpenAI-compatibility providers by name using config
		if s.cfg != nil {
			providerKey := provider
			compatName := strings.TrimSpace(a.Provider)
			isCompatAuth := false
			if compatDetected {
				if compatProviderKey != "" {
					providerKey = compatProviderKey
				}
				if compatDisplayName != "" {
					compatName = compatDisplayName
				}
				isCompatAuth = true
			}
			if strings.EqualFold(providerKey, "openai-compatibility") {
				isCompatAuth = true
				if a.Attributes != nil {
					if v := strings.TrimSpace(a.Attributes["compat_name"]); v != "" {
						compatName = v
					}
					if v := strings.TrimSpace(a.Attributes["provider_key"]); v != "" {
						providerKey = strings.ToLower(v)
						isCompatAuth = true
					}
				}
				if providerKey == "openai-compatibility" && compatName != "" {
					providerKey = strings.ToLower(compatName)
				}
			} else if a.Attributes != nil {
				if v := strings.TrimSpace(a.Attributes["compat_name"]); v != "" {
					compatName = v
					isCompatAuth = true
				}
				if v := strings.TrimSpace(a.Attributes["provider_key"]); v != "" {
					providerKey = strings.ToLower(v)
					isCompatAuth = true
				}
			}
			for i := range s.cfg.OpenAICompatibility {
				compat := &s.cfg.OpenAICompatibility[i]
				if compat.Disabled {
					continue
				}
				if strings.EqualFold(compat.Name, compatName) {
					isCompatAuth = true
					// Convert compatibility models to registry models
					ms := make([]*ModelInfo, 0, len(compat.Models))
					for j := range compat.Models {
						m := compat.Models[j]
						// Use alias as model ID, fallback to name if alias is empty
						modelID := m.Alias
						if modelID == "" {
							modelID = m.Name
						}
						thinking := m.Thinking
						if thinking == nil {
							thinking = &registry.ThinkingSupport{Levels: []string{"low", "medium", "high"}}
						}
						ms = append(ms, &ModelInfo{
							ID:          modelID,
							UpstreamID:  strings.TrimSpace(m.Name),
							Object:      "model",
							Created:     time.Now().Unix(),
							OwnedBy:     compat.Name,
							Type:        "openai-compatibility",
							DisplayName: modelID,
							UserDefined: false,
							Thinking:    thinking,
						})
					}
					// Register and return
					if len(ms) > 0 {
						if providerKey == "" {
							providerKey = "openai-compatibility"
						}
						ms = applyAuthModelExclusions(s.cfg, a, providerKey, ms)
						resolved := applyModelPrefixes(ms, a.Prefix, s.cfg.ForceModelPrefix)
						if preserveTransientState {
							s.registerResolvedModelsForAuthPreservingState(a, providerKey, resolved)
						} else {
							s.registerResolvedModelsForAuth(a, providerKey, resolved)
						}
					} else {
						// Ensure stale registrations are cleared when model list becomes empty.
						GlobalModelRegistry().UnregisterClient(a.ID)
					}
					return
				}
			}
			if isCompatAuth {
				// No matching provider found or models removed entirely; drop any prior registration.
				GlobalModelRegistry().UnregisterClient(a.ID)
				return
			}
		}
	}
	models = applyOAuthModelAlias(s.cfg, provider, authKind, models)
	if len(models) > 0 {
		key := provider
		if key == "" {
			key = strings.ToLower(strings.TrimSpace(a.Provider))
		}
		models = applyAuthModelExclusions(s.cfg, a, key, models)
		resolved := applyModelPrefixes(models, a.Prefix, s.cfg != nil && s.cfg.ForceModelPrefix)
		if preserveTransientState {
			s.registerResolvedModelsForAuthPreservingState(a, key, resolved)
		} else {
			s.registerResolvedModelsForAuth(a, key, resolved)
		}
		return
	}

	GlobalModelRegistry().UnregisterClient(a.ID)
}

// refreshModelRegistrationForAuth re-applies the latest model registration for
// one auth and reconciles any concurrent auth changes that race with the
// refresh. Callers are expected to pre-filter provider membership.
//
// Re-registration is deliberate: registry cooldown/suspension state is treated
// as part of the previous registration snapshot and is cleared when the auth is
// rebound to the refreshed model catalog.
func (s *Service) refreshModelRegistrationForAuth(current *coreauth.Auth) bool {
	if s == nil || s.coreManager == nil || current == nil || current.ID == "" {
		return false
	}

	if !current.Disabled {
		s.ensureExecutorsForAuth(current)
	}
	s.registerModelsForAuth(current)
	s.coreManager.ReconcileRegistryModelStates(context.Background(), current.ID)

	latest, ok := s.latestAuthForModelRegistration(current.ID)
	if !ok || latest.Disabled {
		GlobalModelRegistry().UnregisterClient(current.ID)
		s.coreManager.RefreshSchedulerEntry(current.ID)
		return false
	}

	// Re-apply the latest auth snapshot so concurrent auth updates cannot leave
	// stale model registrations behind. This may duplicate registration work when
	// no auth fields changed, but keeps the refresh path simple and correct.
	s.ensureExecutorsForAuth(latest)
	s.registerModelsForAuth(latest)
	s.coreManager.ReconcileRegistryModelStates(context.Background(), latest.ID)
	s.coreManager.RefreshSchedulerEntry(current.ID)
	return true
}

// latestAuthForModelRegistration returns the latest auth snapshot regardless of
// provider membership. Callers use this after a registration attempt to restore
// whichever state currently owns the client ID in the global registry.
func (s *Service) latestAuthForModelRegistration(authID string) (*coreauth.Auth, bool) {
	if s == nil || s.coreManager == nil || authID == "" {
		return nil, false
	}
	auth, ok := s.coreManager.GetByID(authID)
	if !ok || auth == nil || auth.ID == "" {
		return nil, false
	}
	return auth, true
}

func (s *Service) resolveConfigClaudeKey(auth *coreauth.Auth) *config.ClaudeKey {
	if auth == nil || s.cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range s.cfg.ClaudeKey {
		entry := &s.cfg.ClaudeKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && attrBase != "" {
			if strings.EqualFold(cfgKey, attrKey) && strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range s.cfg.ClaudeKey {
			entry := &s.cfg.ClaudeKey[i]
			if strings.EqualFold(strings.TrimSpace(entry.APIKey), attrKey) {
				return entry
			}
		}
	}
	return nil
}

func (s *Service) resolveConfigGeminiKey(auth *coreauth.Auth) *config.GeminiKey {
	if s == nil || s.cfg == nil {
		return nil
	}
	return resolveConfigGeminiKeyEntry(auth, s.cfg.GeminiKey)
}

func (s *Service) resolveConfigInteractionsKey(auth *coreauth.Auth) *config.GeminiKey {
	if s == nil || s.cfg == nil {
		return nil
	}
	return resolveConfigGeminiKeyEntryExact(auth, s.cfg.InteractionsKey)
}

func resolveConfigGeminiKeyEntryExact(auth *coreauth.Auth, entries []config.GeminiKey) *config.GeminiKey {
	if auth == nil {
		return nil
	}
	attrKey, attrBase := "", ""
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range entries {
		entry := &entries[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		switch {
		case attrKey != "" && attrBase != "" && cfgKey == attrKey && cfgBase == attrBase:
			return entry
		case attrKey != "" && attrBase == "" && cfgKey == attrKey && cfgBase == "":
			return entry
		case attrKey == "" && attrBase != "" && cfgBase == attrBase:
			return entry
		}
	}
	return nil
}

func resolveConfigGeminiKeyEntry(auth *coreauth.Auth, entries []config.GeminiKey) *config.GeminiKey {
	if auth == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range entries {
		entry := &entries[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	return nil
}

func (s *Service) resolveConfigVertexCompatKey(auth *coreauth.Auth) *config.VertexCompatKey {
	if auth == nil || s.cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range s.cfg.VertexCompatAPIKey {
		entry := &s.cfg.VertexCompatAPIKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	if attrKey != "" {
		for i := range s.cfg.VertexCompatAPIKey {
			entry := &s.cfg.VertexCompatAPIKey[i]
			if strings.EqualFold(strings.TrimSpace(entry.APIKey), attrKey) {
				return entry
			}
		}
	}
	return nil
}

func (s *Service) resolveConfigCodexKey(auth *coreauth.Auth) *config.CodexKey {
	if auth == nil || s.cfg == nil {
		return nil
	}
	var attrKey, attrBase string
	if auth.Attributes != nil {
		attrKey = strings.TrimSpace(auth.Attributes["api_key"])
		attrBase = strings.TrimSpace(auth.Attributes["base_url"])
	}
	for i := range s.cfg.CodexKey {
		entry := &s.cfg.CodexKey[i]
		cfgKey := strings.TrimSpace(entry.APIKey)
		cfgBase := strings.TrimSpace(entry.BaseURL)
		if attrKey != "" && strings.EqualFold(cfgKey, attrKey) {
			if cfgBase == "" || strings.EqualFold(cfgBase, attrBase) {
				return entry
			}
			continue
		}
		if attrKey == "" && attrBase != "" && strings.EqualFold(cfgBase, attrBase) {
			return entry
		}
	}
	return nil
}

func (s *Service) oauthExcludedModels(provider, authKind string) []string {
	cfg := s.cfg
	if cfg == nil {
		return nil
	}
	authKindKey := strings.ToLower(strings.TrimSpace(authKind))
	providerKey := strings.ToLower(strings.TrimSpace(provider))
	if authKindKey == "apikey" {
		return nil
	}
	return cfg.OAuthExcludedModels[providerKey]
}

func codexPlanTypeForRegistration(auth *coreauth.Auth) string {
	planType := ""
	if auth != nil && auth.Attributes != nil {
		planType = strings.ToLower(strings.TrimSpace(auth.Attributes["plan_type"]))
	}
	if planType == "" && auth != nil {
		planType = strings.ToLower(strings.TrimSpace(internalcodex.EffectivePlanType(auth.Metadata)))
	}
	switch planType {
	case "plus", "free", "team", "business", "go":
		return planType
	case "pro":
		return "pro"
	default:
		return "pro"
	}
}

func codexModelsForPlan(planType string) []*ModelInfo {
	switch strings.ToLower(strings.TrimSpace(planType)) {
	case "plus":
		return registry.GetCodexPlusModels()
	case "free":
		return registry.GetCodexFreeModels()
	case "team", "business", "go":
		return registry.GetCodexTeamModels()
	default:
		return registry.GetCodexProModels()
	}
}

func codexPlanAllowsImageModel(planType string) bool {
	switch strings.ToLower(strings.TrimSpace(planType)) {
	case "plus", "pro", "team", "business", "go":
		return true
	case "free":
		return false
	default:
		return false
	}
}

func configuredImagesImageModel(cfg *config.Config) string {
	if cfg == nil {
		return "gpt-image-2"
	}
	modelID := strings.TrimSpace(cfg.Images.ImageModel)
	if modelID == "" {
		return "gpt-image-2"
	}
	return modelID
}

func shouldRefreshCodexRegistrations(previousCfg, nextCfg *config.Config) bool {
	if configuredImagesImageModel(previousCfg) != configuredImagesImageModel(nextCfg) {
		return true
	}
	if configuredNativeImageModelsSignature(previousCfg) != configuredNativeImageModelsSignature(nextCfg) {
		return true
	}
	if freePlanImageModelEnabled(previousCfg) != freePlanImageModelEnabled(nextCfg) {
		return true
	}
	return codexCustomModelsSignature(previousCfg) != codexCustomModelsSignature(nextCfg)
}

func shouldRefreshChatGPTWebRegistrations(previousCfg, nextCfg *config.Config) bool {
	return configuredImagesImageModel(previousCfg) != configuredImagesImageModel(nextCfg)
}

func freePlanImageModelEnabled(cfg *config.Config) bool {
	if cfg == nil {
		return false
	}
	return cfg.Images.EnableFreePlanImageModel
}

func codexDynamicImageModelInfo(cfg *config.Config) *ModelInfo {
	modelID := configuredImagesImageModel(cfg)
	if modelID == "" {
		return nil
	}
	return codexImageModelInfo(modelID)
}

func codexDynamicImageModelInfos(cfg *config.Config) []*ModelInfo {
	modelIDs := configuredCodexImageModels(cfg)
	out := make([]*ModelInfo, 0, len(modelIDs))
	for _, modelID := range modelIDs {
		if info := codexImageModelInfo(modelID); info != nil {
			out = append(out, info)
		}
	}
	return out
}

func configuredCodexImageModels(cfg *config.Config) []string {
	models := []string{configuredImagesImageModel(cfg)}
	models = append(models, enabledNativeImageEndpointModels(cfg, "generations")...)
	models = append(models, enabledNativeImageEndpointModels(cfg, "edits")...)
	return normalizeModelIDs(models)
}

func enabledNativeImageEndpointModels(cfg *config.Config, endpoint string) []string {
	if cfg == nil {
		return nil
	}
	var endpointCfg config.NativeImageEndpointConfig
	switch endpoint {
	case "generations":
		endpointCfg = cfg.Images.Native.Generations
	case "edits":
		endpointCfg = cfg.Images.Native.Edits
	default:
		return nil
	}
	if !endpointCfg.Enabled {
		return nil
	}
	if len(endpointCfg.Models) == 0 {
		return []string{"gpt-image-2", "gpt-image-1.5"}
	}
	return endpointCfg.Models
}

func normalizeModelIDs(models []string) []string {
	if len(models) == 0 {
		return nil
	}
	out := make([]string, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, modelID := range models {
		modelID = strings.TrimSpace(modelID)
		if modelID == "" {
			continue
		}
		key := strings.ToLower(modelID)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, modelID)
	}
	return out
}

func configuredNativeImageModelsSignature(cfg *config.Config) string {
	if cfg == nil {
		return ""
	}
	return strings.Join(normalizeModelIDs(append(
		enabledNativeImageEndpointModels(cfg, "generations"),
		enabledNativeImageEndpointModels(cfg, "edits")...,
	)), "\x00")
}

func authModelExclusionsSignature(cfg *config.Config) string {
	if cfg == nil || len(cfg.AuthModelExclusions) == 0 {
		return ""
	}
	var b strings.Builder
	for _, rule := range cfg.AuthModelExclusions {
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString(strings.Join(rule.Models, ","))
		b.WriteByte(':')
		b.WriteString(strings.Join(rule.Providers, ","))
		b.WriteByte(':')
		for i, priority := range rule.Priorities {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strconv.Itoa(priority))
		}
		b.WriteByte(':')
		b.WriteString(strings.Join(rule.KeywordContains, ","))
		b.WriteByte(':')
		if rule.DisableImageGeneration {
			b.WriteString("disable-image-generation")
		}
	}
	return b.String()
}

func codexImageModelInfo(modelID string) *ModelInfo {
	modelID = strings.TrimSpace(modelID)
	if modelID == "" {
		return nil
	}
	return &ModelInfo{
		ID:          modelID,
		Object:      "model",
		Created:     1704067200, // 2024-01-01
		OwnedBy:     "openai",
		Type:        "openai",
		DisplayName: modelID,
		Version:     modelID,
	}
}

func codexCustomModelInfosForPlan(cfg *config.Config, planType string) []*ModelInfo {
	if cfg == nil || len(cfg.CodexCustomModels) == 0 {
		return nil
	}
	planType = strings.ToLower(strings.TrimSpace(planType))
	out := make([]*ModelInfo, 0, len(cfg.CodexCustomModels))
	for i := range cfg.CodexCustomModels {
		entry := cfg.CodexCustomModels[i]
		if !codexCustomModelAllowsPlan(entry, planType) {
			continue
		}
		info := codexCustomModelInfo(entry)
		if info != nil {
			out = append(out, info)
		}
	}
	return out
}

func removeCodexCustomModelOverrides(models []*ModelInfo, cfg *config.Config) []*ModelInfo {
	if len(models) == 0 || cfg == nil || len(cfg.CodexCustomModels) == 0 {
		return models
	}
	overrides := make(map[string]struct{}, len(cfg.CodexCustomModels))
	for _, entry := range cfg.CodexCustomModels {
		id := strings.ToLower(strings.TrimSpace(entry.ID))
		if id != "" {
			overrides[id] = struct{}{}
		}
	}
	if len(overrides) == 0 {
		return models
	}
	out := make([]*ModelInfo, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		id := strings.ToLower(strings.TrimSpace(model.ID))
		if _, overridden := overrides[id]; overridden {
			continue
		}
		out = append(out, model)
	}
	return out
}

func codexCustomModelAllowsPlan(entry config.CodexCustomModel, planType string) bool {
	planType = strings.ToLower(strings.TrimSpace(planType))
	for _, group := range entry.Groups {
		if strings.EqualFold(strings.TrimSpace(group), planType) {
			return true
		}
	}
	return false
}

func codexCustomModelInfo(entry config.CodexCustomModel) *ModelInfo {
	modelID := strings.TrimSpace(entry.ID)
	if modelID == "" {
		return nil
	}
	displayName := strings.TrimSpace(entry.DisplayName)
	if displayName == "" {
		displayName = modelID
	}
	return &ModelInfo{
		ID:                  modelID,
		Object:              "model",
		Created:             1704067200, // 2024-01-01
		OwnedBy:             "openai",
		Type:                "openai",
		DisplayName:         displayName,
		Version:             modelID,
		SupportedParameters: []string{"tools"},
		Thinking:            &registry.ThinkingSupport{Levels: []string{"low", "medium", "high", "xhigh"}},
	}
}

func codexCustomModelsSignature(cfg *config.Config) string {
	if cfg == nil || len(cfg.CodexCustomModels) == 0 {
		return ""
	}
	var b strings.Builder
	for _, entry := range cfg.CodexCustomModels {
		id := strings.TrimSpace(entry.ID)
		if id == "" {
			continue
		}
		if b.Len() > 0 {
			b.WriteByte('|')
		}
		b.WriteString(strings.ToLower(id))
		b.WriteByte(':')
		b.WriteString(strings.TrimSpace(entry.DisplayName))
		b.WriteByte(':')
		for i, group := range entry.Groups {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(strings.ToLower(strings.TrimSpace(group)))
		}
	}
	return b.String()
}

func upsertModelInfos(models []*ModelInfo, extras []*ModelInfo) []*ModelInfo {
	for _, extra := range extras {
		models = upsertModelInfo(models, extra)
	}
	return models
}

func upsertModelInfo(models []*ModelInfo, extra *ModelInfo) []*ModelInfo {
	if extra == nil {
		return models
	}
	extraID := strings.TrimSpace(extra.ID)
	if extraID == "" {
		return models
	}
	out := make([]*ModelInfo, 0, len(models)+1)
	for _, model := range models {
		if model == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(model.ID), extraID) {
			continue
		}
		out = append(out, model)
	}
	out = append(out, extra)
	return out
}

func applyExcludedModels(models []*ModelInfo, excluded []string) []*ModelInfo {
	if len(models) == 0 || len(excluded) == 0 {
		return models
	}

	patterns := make([]string, 0, len(excluded))
	for _, item := range excluded {
		if trimmed := strings.TrimSpace(item); trimmed != "" {
			patterns = append(patterns, strings.ToLower(trimmed))
		}
	}
	if len(patterns) == 0 {
		return models
	}

	filtered := make([]*ModelInfo, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		modelID := strings.ToLower(strings.TrimSpace(model.ID))
		blocked := false
		for _, pattern := range patterns {
			if matchWildcard(pattern, modelID) {
				blocked = true
				break
			}
		}
		if !blocked {
			filtered = append(filtered, model)
		}
	}
	return filtered
}

func applyAuthModelExclusions(cfg *config.Config, auth *coreauth.Auth, provider string, models []*ModelInfo) []*ModelInfo {
	if cfg == nil || auth == nil || len(models) == 0 || len(cfg.AuthModelExclusions) == 0 {
		return models
	}
	blocked := make(map[string]struct{})
	if coreauth.AuthDisablesImageGeneration(cfg, auth, provider) {
		for _, modelID := range configuredCodexImageModels(cfg) {
			modelID = strings.ToLower(strings.TrimSpace(modelID))
			if modelID != "" {
				blocked[modelID] = struct{}{}
			}
		}
	}
	allowOnly := false
	allowed := make(map[string]struct{})
	for _, rule := range cfg.AuthModelExclusions {
		if !coreauth.AuthModelExclusionRuleMatches(rule, auth, provider) {
			continue
		}
		allMode, ruleBlocked, ruleAllowed := parseAuthModelExclusionModels(rule.Models)
		if allMode {
			allowOnly = true
			for _, modelID := range ruleAllowed {
				allowed[modelID] = struct{}{}
			}
			continue
		}
		for _, modelID := range ruleBlocked {
			if modelID != "" {
				blocked[modelID] = struct{}{}
			}
		}
	}
	if !allowOnly && len(blocked) == 0 {
		return models
	}
	out := make([]*ModelInfo, 0, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		modelID := strings.ToLower(strings.TrimSpace(model.ID))
		if allowOnly {
			if _, keep := allowed[modelID]; !keep {
				continue
			}
		}
		if _, skip := blocked[modelID]; skip {
			continue
		}
		out = append(out, model)
	}
	return out
}

func parseAuthModelExclusionModels(models []string) (bool, []string, []string) {
	if len(models) == 0 {
		return false, nil, nil
	}
	firstModelIndex := -1
	for i, raw := range models {
		if strings.TrimSpace(raw) != "" {
			firstModelIndex = i
			break
		}
	}
	if firstModelIndex < 0 {
		return false, nil, nil
	}
	allMode := strings.ToLower(strings.TrimSpace(models[firstModelIndex])) == "-all"
	blocked := make([]string, 0, len(models))
	allowed := make([]string, 0, len(models))
	for i, raw := range models {
		modelID := strings.ToLower(strings.TrimSpace(raw))
		if modelID == "" {
			continue
		}
		if i == firstModelIndex && allMode {
			continue
		}
		if allMode {
			if strings.HasPrefix(modelID, "+") {
				modelID = strings.TrimSpace(strings.TrimPrefix(modelID, "+"))
				if modelID != "" {
					allowed = append(allowed, modelID)
				}
			}
			continue
		}
		if strings.HasPrefix(modelID, "+") {
			modelID = strings.TrimSpace(strings.TrimPrefix(modelID, "+"))
		}
		if modelID != "" {
			blocked = append(blocked, modelID)
		}
	}
	return allMode, blocked, allowed
}

func applyModelPrefixes(models []*ModelInfo, prefix string, forceModelPrefix bool) []*ModelInfo {
	trimmedPrefix := strings.TrimSpace(prefix)
	if trimmedPrefix == "" || len(models) == 0 {
		return models
	}

	out := make([]*ModelInfo, 0, len(models)*2)
	seen := make(map[string]struct{}, len(models)*2)

	addModel := func(model *ModelInfo) {
		if model == nil {
			return
		}
		id := strings.TrimSpace(model.ID)
		if id == "" {
			return
		}
		if _, exists := seen[id]; exists {
			return
		}
		seen[id] = struct{}{}
		out = append(out, model)
	}

	for _, model := range models {
		if model == nil {
			continue
		}
		baseID := strings.TrimSpace(model.ID)
		if baseID == "" {
			continue
		}
		if !forceModelPrefix || trimmedPrefix == baseID {
			addModel(model)
		}
		clone := *model
		clone.ID = trimmedPrefix + "/" + baseID
		clone.UpstreamID = modelInfoUpstreamID(model)
		addModel(&clone)
	}
	return out
}

func modelInfoUpstreamID(model *ModelInfo) string {
	if model == nil {
		return ""
	}
	if upstreamID := strings.TrimSpace(model.UpstreamID); upstreamID != "" {
		return upstreamID
	}
	return strings.TrimSpace(model.ID)
}

// matchWildcard performs case-insensitive wildcard matching where '*' matches any substring.
func matchWildcard(pattern, value string) bool {
	if pattern == "" {
		return false
	}

	// Fast path for exact match (no wildcard present).
	if !strings.Contains(pattern, "*") {
		return pattern == value
	}

	parts := strings.Split(pattern, "*")
	// Handle prefix.
	if prefix := parts[0]; prefix != "" {
		if !strings.HasPrefix(value, prefix) {
			return false
		}
		value = value[len(prefix):]
	}

	// Handle suffix.
	if suffix := parts[len(parts)-1]; suffix != "" {
		if !strings.HasSuffix(value, suffix) {
			return false
		}
		value = value[:len(value)-len(suffix)]
	}

	// Handle middle segments in order.
	for i := 1; i < len(parts)-1; i++ {
		segment := parts[i]
		if segment == "" {
			continue
		}
		idx := strings.Index(value, segment)
		if idx < 0 {
			return false
		}
		value = value[idx+len(segment):]
	}

	return true
}

type modelEntry interface {
	GetName() string
	GetAlias() string
}

func buildConfigModels[T modelEntry](models []T, ownedBy, modelType string) []*ModelInfo {
	if len(models) == 0 {
		return nil
	}
	now := time.Now().Unix()
	out := make([]*ModelInfo, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for i := range models {
		model := models[i]
		name := strings.TrimSpace(model.GetName())
		alias := strings.TrimSpace(model.GetAlias())
		if alias == "" {
			alias = name
		}
		if alias == "" {
			continue
		}
		key := strings.ToLower(alias)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		display := name
		if display == "" {
			display = alias
		}
		info := &ModelInfo{
			ID:          alias,
			UpstreamID:  name,
			Object:      "model",
			Created:     now,
			OwnedBy:     ownedBy,
			Type:        modelType,
			DisplayName: display,
			UserDefined: true,
		}
		if name != "" {
			if upstream := registry.LookupStaticModelInfo(name); upstream != nil && upstream.Thinking != nil {
				info.Thinking = upstream.Thinking
			}
		}
		out = append(out, info)
	}
	return out
}

func buildVertexCompatConfigModels(entry *config.VertexCompatKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "google", "vertex")
}

func buildGeminiConfigModels(entry *config.GeminiKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "google", "gemini")
}

func buildClaudeConfigModels(entry *config.ClaudeKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "anthropic", "claude")
}

func buildCodexConfigModels(entry *config.CodexKey) []*ModelInfo {
	if entry == nil {
		return nil
	}
	return buildConfigModels(entry.Models, "openai", "openai")
}

func rewriteModelInfoName(name, oldID, newID string) string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return name
	}
	oldID = strings.TrimSpace(oldID)
	newID = strings.TrimSpace(newID)
	if oldID == "" || newID == "" {
		return name
	}
	if strings.EqualFold(oldID, newID) {
		return name
	}
	if strings.EqualFold(trimmed, oldID) {
		return newID
	}
	if strings.HasSuffix(trimmed, "/"+oldID) {
		prefix := strings.TrimSuffix(trimmed, oldID)
		return prefix + newID
	}
	if trimmed == "models/"+oldID {
		return "models/" + newID
	}
	return name
}

func applyOAuthModelAlias(cfg *config.Config, provider, authKind string, models []*ModelInfo) []*ModelInfo {
	if cfg == nil || len(models) == 0 {
		return models
	}
	channel := coreauth.OAuthModelAliasChannel(provider, authKind)
	if channel == "" || len(cfg.OAuthModelAlias) == 0 {
		return models
	}
	aliases := cfg.OAuthModelAlias[channel]
	if len(aliases) == 0 {
		return models
	}

	type aliasEntry struct {
		alias string
		fork  bool
	}

	forward := make(map[string][]aliasEntry, len(aliases))
	for i := range aliases {
		name := strings.TrimSpace(aliases[i].Name)
		alias := strings.TrimSpace(aliases[i].Alias)
		if name == "" || alias == "" {
			continue
		}
		if strings.EqualFold(name, alias) {
			continue
		}
		key := strings.ToLower(name)
		forward[key] = append(forward[key], aliasEntry{alias: alias, fork: aliases[i].Fork})
	}
	if len(forward) == 0 {
		return models
	}

	out := make([]*ModelInfo, 0, len(models))
	seen := make(map[string]struct{}, len(models))
	for _, model := range models {
		if model == nil {
			continue
		}
		id := strings.TrimSpace(model.ID)
		if id == "" {
			continue
		}
		key := strings.ToLower(id)
		entries := forward[key]
		if len(entries) == 0 {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, model)
			continue
		}

		keepOriginal := false
		for _, entry := range entries {
			if entry.fork {
				keepOriginal = true
				break
			}
		}
		if keepOriginal {
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				out = append(out, model)
			}
		}

		addedAlias := false
		for _, entry := range entries {
			mappedID := strings.TrimSpace(entry.alias)
			if mappedID == "" {
				continue
			}
			if strings.EqualFold(mappedID, id) {
				continue
			}
			aliasKey := strings.ToLower(mappedID)
			if _, exists := seen[aliasKey]; exists {
				continue
			}
			seen[aliasKey] = struct{}{}
			clone := *model
			clone.ID = mappedID
			clone.UpstreamID = modelInfoUpstreamID(model)
			if clone.Name != "" {
				clone.Name = rewriteModelInfoName(clone.Name, id, mappedID)
			}
			out = append(out, &clone)
			addedAlias = true
		}

		if !keepOriginal && !addedAlias {
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			out = append(out, model)
		}
	}
	return out
}
