package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/sync/singleflight"
)

type chatGPTWebAuthService interface {
	Login(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	LoginAcquisitionTimeout(chatgptwebauth.LoginInput) time.Duration
	Refresh(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	RefreshSession(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
}

type chatGPTWebLoginGate struct {
	signal chan struct{}
	refs   int
}

// ChatGPTWebLoginCoordinator serializes account login transactions across
// executor replacements owned by one service.
type ChatGPTWebLoginCoordinator struct {
	mu    sync.Mutex
	gates map[string]*chatGPTWebLoginGate
}

// NewChatGPTWebLoginCoordinator creates an account login coordinator.
func NewChatGPTWebLoginCoordinator() *ChatGPTWebLoginCoordinator {
	return &ChatGPTWebLoginCoordinator{gates: make(map[string]*chatGPTWebLoginGate)}
}

const (
	chatGPTWebBackgroundReloginConcurrency = config.DefaultChatGPTWebAutoReloginWorkers
	chatGPTWebBackgroundReloginLogInterval = 3
	chatGPTWebBackgroundReloginMaxBackoff  = 5 * time.Minute
)

var chatGPTWebBackgroundReloginSlots = make(chan struct{}, chatGPTWebBackgroundReloginConcurrency)

type chatGPTWebReloginQueueWorkerContextKey struct{}

var (
	errChatGPTWebReloginOwnershipChanged = errors.New("chatgpt web re-login ownership changed")
	errChatGPTWebReloginAcquisitionLimit = errors.New("chatgpt web re-login acquisition deadline exceeded")
)

// ChatGPTWebExecutor manages ChatGPT Web credential refresh and re-login.
// Request protocol support is added separately from the credential lifecycle.
type ChatGPTWebExecutor struct {
	cfg                       atomic.Pointer[config.Config]
	configUpdateMu            sync.Mutex
	manager                   *cliproxyauth.Manager
	authService               chatGPTWebAuthService
	runtimeBaseURL            string
	runtimeRand               io.Reader
	imageInitialWait          time.Duration
	imagePollInterval         time.Duration
	imageSettleWait           time.Duration
	imageMaxPolls             int
	searchPollInterval        time.Duration
	searchMaxPolls            int
	streamInitialWait         time.Duration
	streamHeartbeat           time.Duration
	accountInfoTimeout        time.Duration
	now                       func() time.Time
	reloginBackoff            func(int) time.Duration
	reloginSlotAcquired       func()
	reloginSlots              chan struct{}
	refreshGroup              singleflight.Group
	refreshWG                 sync.WaitGroup
	reloginMu                 sync.Mutex
	reloginFlights            map[string]*chatGPTWebReloginFlight
	reloginWG                 sync.WaitGroup
	loginCoordinator          *ChatGPTWebLoginCoordinator
	loginWG                   sync.WaitGroup
	sentinelRuntime           helps.ChatGPTWebSentinelRuntime
	personaOutcomeMu          sync.Mutex
	personaOutcomes           map[string]chatgptwebauth.PersonaOutcomeSnapshot
	usageCache                *helps.ChatGPTWebUsageCache
	accountInfo               *chatGPTWebAccountInfoRuntime
	sentinelSDKFetcherFactory func(*chatgptwebauth.Client, *chatgptwebauth.Credential) chatgptwebauth.SentinelSDKFetcher
	backgroundMu              sync.Mutex
	backgroundQueue           *chatGPTWebReloginQueue
	reloginReconcilePending   bool
	reloginReconcileRequested bool
	reloginReconcileFull      bool
	lifecycleCtx              context.Context
	lifecycleCancel           context.CancelFunc
	closed                    bool
}

// NewChatGPTWebExecutor creates a lifecycle-aware ChatGPT Web executor.
func NewChatGPTWebExecutor(cfg *config.Config, manager *cliproxyauth.Manager) *ChatGPTWebExecutor {
	return NewChatGPTWebExecutorWithLoginCoordinator(cfg, manager, nil)
}

// NewChatGPTWebExecutorWithLoginCoordinator creates an executor that shares
// account login serialization with other executor generations.
func NewChatGPTWebExecutorWithLoginCoordinator(cfg *config.Config, manager *cliproxyauth.Manager, coordinator *ChatGPTWebLoginCoordinator) *ChatGPTWebExecutor {
	if coordinator == nil {
		coordinator = NewChatGPTWebLoginCoordinator()
	}
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	executor := &ChatGPTWebExecutor{
		manager:            manager,
		authService:        chatgptwebauth.NewService(chatgptwebauth.Options{}),
		runtimeBaseURL:     "https://chatgpt.com",
		runtimeRand:        rand.Reader,
		imageInitialWait:   10 * time.Second,
		imagePollInterval:  10 * time.Second,
		imageSettleWait:    5 * time.Second,
		imageMaxPolls:      chatGPTWebImageMaxPollAttempts,
		searchPollInterval: 750 * time.Millisecond,
		searchMaxPolls:     chatGPTWebSearchMaxPollAttempts,
		streamInitialWait:  time.Second,
		streamHeartbeat:    15 * time.Second,
		accountInfoTimeout: chatgptwebauth.DefaultAcquisitionTimeout,
		now:                time.Now,
		reloginBackoff:     chatGPTWebBackgroundReloginBackoff,
		reloginSlots:       chatGPTWebBackgroundReloginSlots,
		reloginFlights:     make(map[string]*chatGPTWebReloginFlight),
		loginCoordinator:   coordinator,
		sentinelRuntime:    helps.NewChatGPTWebSentinelRuntime(chatGPTWebSentinelRuntimeConfig(cfg)),
		personaOutcomes:    make(map[string]chatgptwebauth.PersonaOutcomeSnapshot),
		usageCache:         helps.NewChatGPTWebUsageCache(),
		lifecycleCtx:       lifecycleCtx,
		lifecycleCancel:    lifecycleCancel,
	}
	reloginPolicy := config.ChatGPTWebConfig{}.ResolvedAutoRelogin()
	if cfg != nil {
		reloginPolicy = cfg.ChatGPTWeb.ResolvedAutoRelogin()
	}
	executor.backgroundQueue = newChatGPTWebReloginQueue(executor, lifecycleCtx, reloginPolicy.Workers, reloginPolicy.QueueSize)
	executor.UpdateConfig(cfg)
	executor.accountInfo = newChatGPTWebAccountInfoRuntime(executor, executor.configSnapshot())
	executor.accountInfo.start()
	return executor
}

// Close cancels provider-owned acquisition work and waits for background
// re-login workers to exit.
func (e *ChatGPTWebExecutor) Close() error {
	if e == nil {
		return nil
	}
	e.backgroundMu.Lock()
	if !e.closed {
		e.closed = true
		if e.lifecycleCancel != nil {
			e.lifecycleCancel()
		}
	}
	e.backgroundMu.Unlock()
	if e.accountInfo != nil {
		e.accountInfo.close()
	}
	e.refreshWG.Wait()
	if e.backgroundQueue != nil {
		e.backgroundQueue.close()
	}
	e.reloginWG.Wait()
	e.loginWG.Wait()
	if e.sentinelRuntime != nil {
		e.sentinelRuntime.Close()
	}
	if e.usageCache != nil {
		e.usageCache.Close()
	}
	return nil
}

// CloseAuthInstanceExecutionSessions clears account-info work owned by a
// removed or replaced credential runtime.
func (e *ChatGPTWebExecutor) CloseAuthInstanceExecutionSessions(authID string, authInstanceID string, reason string) {
	if e == nil {
		return
	}
	removeBackground := false
	switch strings.TrimSpace(reason) {
	case "auth_runtime_replaced", "auth_refreshed", "auth_delete_rolled_back":
		authInstanceID = strings.TrimSpace(authInstanceID)
		if authInstanceID == "" {
			return
		}
		removeBackground = true
	case "auth_removed", "auth_delete_uncertain", "auth_retired", "auth_reloaded",
		"auth_replaced", "auth_identity_changed", "auth_lifecycle_unavailable":
		removeBackground = true
	}
	if removeBackground && e.backgroundQueue != nil {
		e.backgroundQueue.removeAuthInstance(authID, authInstanceID)
	}
	if e.accountInfo == nil {
		return
	}
	switch strings.TrimSpace(reason) {
	case "auth_runtime_replaced", "auth_refreshed", "auth_delete_rolled_back",
		"auth_removed", "auth_delete_uncertain", "auth_retired", "auth_reloaded",
		"auth_replaced", "auth_identity_changed", "auth_lifecycle_unavailable":
		e.accountInfo.removeAuthInstance(authID, authInstanceID)
	}
}

// Identifier returns the provider identifier.
func (e *ChatGPTWebExecutor) Identifier() string { return chatgptwebauth.Provider }

// UpdateConfig replaces the immutable runtime configuration snapshot without
// interrupting in-flight login or request operations.
func (e *ChatGPTWebExecutor) UpdateConfig(cfg *config.Config) {
	if e == nil {
		return
	}
	e.configUpdateMu.Lock()
	defer e.configUpdateMu.Unlock()
	if cfg == nil {
		e.cfg.Store(nil)
		configureChatGPTWebImageAdmissions(config.ChatGPTWebImageConfig{}.Resolved())
		if e.backgroundQueue != nil {
			policy := config.ChatGPTWebConfig{}.ResolvedAutoRelogin()
			e.backgroundQueue.setConfig(false, policy.Workers, policy.QueueSize)
		}
		if e.sentinelRuntime != nil {
			e.sentinelRuntime.UpdateConfig(chatgptwebauth.SentinelRuntimeConfig{})
		}
		if e.accountInfo != nil {
			e.accountInfo.updateConfig(nil)
		}
		return
	}
	snapshot, errClone := cloneChatGPTWebExecutorConfig(cfg)
	if errClone != nil {
		log.WithError(errClone).Error("chatgpt web executor: retain previous configuration after snapshot failure")
		return
	}
	if errValidate := snapshot.Images.ChatGPTWeb.Validate(); errValidate != nil {
		log.WithError(errValidate).Error("chatgpt web executor: retain previous configuration after image policy validation failure")
		return
	}
	e.cfg.Store(snapshot)
	configureChatGPTWebImageAdmissions(snapshot.Images.ChatGPTWeb.Resolved())
	if e.backgroundQueue != nil {
		policy := snapshot.ChatGPTWeb.ResolvedAutoRelogin()
		e.backgroundQueue.setConfig(snapshot.ChatGPTWeb.AutoRelogin, policy.Workers, policy.QueueSize)
		// Constructor initialization runs before account-info and service hooks are
		// ready. Startup reconciliation is scheduled explicitly after bootstrap;
		// subsequent hot updates can reconcile immediately.
		if snapshot.ChatGPTWeb.AutoRelogin && e.accountInfo != nil {
			e.scheduleBackgroundReloginReconcile(false)
		}
	}
	if e.usageCache != nil {
		usageCache := snapshot.ChatGPTWeb.UsageCache.Resolved()
		retention := time.Duration(usageCache.OrphanRetentionMinutes) * time.Minute
		if errPrepare := e.usageCache.Prepare(usageCache.Path, retention); errPrepare != nil {
			log.WithError(errPrepare).Warn("chatgpt web executor: usage cache orphan cleanup failed")
		}
	}
	if e.sentinelRuntime != nil {
		e.sentinelRuntime.UpdateConfig(chatGPTWebSentinelRuntimeConfig(snapshot))
	}
	if e.accountInfo != nil {
		e.accountInfo.updateConfig(snapshot)
		e.accountInfo.restoreImportAccountInfoIntents()
	}
}

func configureChatGPTWebImageAdmissions(resolved config.ResolvedChatGPTWebImageConfig) {
	capacityBytes, errCapacity := config.ChatGPTWebImageMegabytesToBytes(resolved.MemoryCapacityMegabytes)
	if errCapacity != nil {
		log.WithError(errCapacity).Error("chatgpt web executor: retain previous image memory capacity")
		return
	}
	helps.ConfigureChatGPTWebImageMemoryCapacity(capacityBytes)
	cliproxyexecutor.ConfigureChatGPTWebImageAdmissions(
		resolved.MaxInFlight,
		resolved.AdmissionQueueSize,
		resolved.MaxFinalizers,
	)
	cliproxyexecutor.ConfigureChatGPTWebImageRuntimeAdmissions(
		resolved.MaxInFlight,
		resolved.PollConcurrency,
		resolved.MemoryFinalizerConcurrency,
	)
}

func chatGPTWebSentinelRuntimeConfig(cfg *config.Config) chatgptwebauth.SentinelRuntimeConfig {
	if cfg == nil {
		return chatgptwebauth.SentinelRuntimeConfig{}
	}
	resolved := cfg.ChatGPTWeb.Sentinel.Resolved()
	return chatgptwebauth.SentinelRuntimeConfig{
		Enabled:       resolved.SDKRuntimeEnabled,
		Workers:       resolved.SDKWorkers,
		QueueSize:     resolved.SDKQueueSize,
		CacheVersions: resolved.SDKCacheVersions,
	}
}

// SentinelSnapshot returns the currently applied SDK runtime state.
func (e *ChatGPTWebExecutor) SentinelSnapshot() chatgptwebauth.SentinelRuntimeSnapshot {
	if e == nil {
		return chatgptwebauth.SentinelRuntimeSnapshot{}
	}
	snapshot := chatgptwebauth.SentinelRuntimeSnapshot{}
	if e.sentinelRuntime != nil {
		snapshot = e.sentinelRuntime.Snapshot()
	}
	e.personaOutcomeMu.Lock()
	if len(e.personaOutcomes) > 0 {
		snapshot.PersonaOutcomes = make([]chatgptwebauth.PersonaOutcomeSnapshot, 0, len(e.personaOutcomes))
		for _, outcome := range e.personaOutcomes {
			snapshot.PersonaOutcomes = append(snapshot.PersonaOutcomes, outcome)
		}
	}
	e.personaOutcomeMu.Unlock()
	sort.Slice(snapshot.PersonaOutcomes, func(i, j int) bool {
		if snapshot.PersonaOutcomes[i].CatalogID != snapshot.PersonaOutcomes[j].CatalogID {
			return snapshot.PersonaOutcomes[i].CatalogID < snapshot.PersonaOutcomes[j].CatalogID
		}
		return snapshot.PersonaOutcomes[i].BrowserEnvironmentID < snapshot.PersonaOutcomes[j].BrowserEnvironmentID
	})
	return snapshot
}

func (e *ChatGPTWebExecutor) recordPersonaOutcome(auth *cliproxyauth.Auth, persona chatgptwebauth.Persona, err error) {
	if e == nil || auth == nil {
		return
	}
	var browserEnvironment chatgptwebauth.BrowserEnvironmentIdentity
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential == nil && credential != nil {
		if strings.TrimSpace(persona.CatalogID) == "" {
			persona = chatgptwebauth.ResolveCredentialPersona(credential, auth.ID)
		} else {
			credential.Persona = persona
		}
		browserEnvironment = chatgptwebauth.ResolveCredentialBrowserEnvironment(credential, auth.ID)
	}
	if strings.TrimSpace(persona.CatalogID) == "" {
		return
	}
	if strings.TrimSpace(browserEnvironment.CatalogID) == "" {
		fallbackCredential := &chatgptwebauth.Credential{Persona: persona}
		browserEnvironment = chatgptwebauth.ResolveCredentialBrowserEnvironment(fallbackCredential, auth.ID)
	}
	if strings.TrimSpace(browserEnvironment.CatalogID) == "" {
		return
	}
	key := persona.CatalogVersion + "\x00" + persona.CatalogID + "\x00" + browserEnvironment.CatalogID
	e.personaOutcomeMu.Lock()
	if e.personaOutcomes == nil {
		e.personaOutcomes = make(map[string]chatgptwebauth.PersonaOutcomeSnapshot)
	}
	outcome := e.personaOutcomes[key]
	if outcome.CatalogID == "" {
		outcome = chatgptwebauth.PersonaOutcomeSnapshot{
			CatalogVersion:       persona.CatalogVersion,
			CatalogID:            persona.CatalogID,
			TransportPersonaID:   persona.CatalogID,
			BrowserEnvironmentID: browserEnvironment.CatalogID,
			TLSProfile:           persona.Profile,
			UAMajor:              chatGPTWebUserAgentMajor(persona.UserAgent),
			Platform:             persona.Platform,
		}
	}
	switch {
	case err == nil:
		outcome.Success200++
	default:
		status, cloudflare, sentinel := chatGPTWebPersonaOutcomeError(err)
		switch {
		case status == http.StatusForbidden && cloudflare:
			outcome.Cloudflare403++
		case sentinel:
			outcome.SentinelReject++
		case status == http.StatusForbidden:
			outcome.Forbidden403++
		case status >= http.StatusBadRequest:
			outcome.HTTPError++
		default:
			outcome.Other++
		}
	}
	e.personaOutcomes[key] = outcome
	e.personaOutcomeMu.Unlock()
}

func chatGPTWebPersonaOutcomeError(err error) (int, bool, bool) {
	if err == nil {
		return 0, false, false
	}
	type diagnosticProvider interface {
		AuthErrorDiagnostic() *cliproxyauth.ErrorDiagnostic
	}
	var provider diagnosticProvider
	cloudflare := false
	sentinel := false
	if errors.As(err, &provider) && provider != nil {
		if diagnostic := provider.AuthErrorDiagnostic(); diagnostic != nil {
			cloudflare = diagnostic.Cloudflare
			stage := strings.ToLower(strings.TrimSpace(diagnostic.Stage))
			code := strings.ToLower(strings.TrimSpace(diagnostic.Code))
			sentinel = strings.HasPrefix(stage, "sentinel_") || strings.HasPrefix(code, "sentinel_") ||
				code == "invalid_proof_token" || code == "chat_requirements" || code == "chat_requirements_failed"
			if diagnostic.HTTPStatus != 0 {
				return diagnostic.HTTPStatus, cloudflare, sentinel
			}
		}
	}
	type statusCoder interface {
		StatusCode() int
	}
	var status statusCoder
	if errors.As(err, &status) && status != nil {
		return status.StatusCode(), cloudflare, sentinel
	}
	return 0, cloudflare, sentinel
}

// UsageCacheSnapshot returns active storage and cumulative accounting outcomes.
func (e *ChatGPTWebExecutor) UsageCacheSnapshot() helps.ChatGPTWebUsageCacheSnapshot {
	if e == nil || e.usageCache == nil {
		return helps.ChatGPTWebUsageCacheSnapshot{}
	}
	return e.usageCache.Snapshot()
}

func (e *ChatGPTWebExecutor) configSnapshot() *config.Config {
	if e == nil {
		return nil
	}
	return e.cfg.Load()
}

func cloneChatGPTWebExecutorConfig(cfg *config.Config) (*config.Config, error) {
	data, errMarshal := json.Marshal(cfg)
	if errMarshal != nil {
		return nil, fmt.Errorf("marshal configuration snapshot: %w", errMarshal)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var snapshot config.Config
	if errDecode := decoder.Decode(&snapshot); errDecode != nil {
		return nil, fmt.Errorf("decode configuration snapshot: %w", errDecode)
	}
	restoreChatGPTWebRawPayloadBytes(snapshot.Payload.DefaultRaw, cfg.Payload.DefaultRaw)
	restoreChatGPTWebRawPayloadBytes(snapshot.Payload.OverrideRaw, cfg.Payload.OverrideRaw)
	return &snapshot, nil
}

func restoreChatGPTWebRawPayloadBytes(snapshot, source []config.PayloadRule) {
	for ruleIndex := range source {
		if ruleIndex >= len(snapshot) || snapshot[ruleIndex].Params == nil {
			continue
		}
		for key, value := range source[ruleIndex].Params {
			switch typed := value.(type) {
			case json.RawMessage:
				snapshot[ruleIndex].Params[key] = append(json.RawMessage(nil), typed...)
			case []byte:
				snapshot[ruleIndex].Params[key] = append([]byte(nil), typed...)
			}
		}
	}
}

// Execute runs a ChatGPT Web request and translates the result to the inbound
// protocol.
func (e *ChatGPTWebExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	response, err := e.executeRuntime(ctx, auth, req, opts)
	return response, e.handleChatGPTWebRuntimeLifecycleError(ctx, auth, err)
}

// PrepareProviderRequest completes credential-independent validation before
// auth selection and returns an immutable plan shared by provider attempts.
func (e *ChatGPTWebExecutor) PrepareProviderRequest(ctx context.Context, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, operation cliproxyexecutor.RequestOperation) (any, error) {
	if operation == cliproxyexecutor.RequestOperationCount {
		return nil, cliproxyexecutor.NewProviderIncompatibleRequestPreparationError(newChatGPTWebProtocolUnavailableError())
	}
	prepared, errPrepare := e.prepareRuntimeRequestTemplate(ctx, req, opts, operation == cliproxyexecutor.RequestOperationStream)
	if errPrepare == nil {
		return prepared, nil
	}
	var retryOtherProvider interface{ RetryOtherAuth() bool }
	if errors.As(errPrepare, &retryOtherProvider) && retryOtherProvider.RetryOtherAuth() {
		return nil, cliproxyexecutor.NewProviderIncompatibleRequestPreparationError(errPrepare)
	}
	return nil, cliproxyexecutor.NewGlobalProviderRequestPreparationError(errPrepare)
}

// DeferAuthRequestCommitUntilUpstream reports that the executor commits the
// auth request slot immediately before its first model upstream request.
func (*ChatGPTWebExecutor) DeferAuthRequestCommitUntilUpstream() bool {
	return true
}

// ExecuteStream runs a streaming ChatGPT Web request.
func (e *ChatGPTWebExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	result, err := e.executeRuntimeStream(ctx, auth, req, opts)
	return result, e.handleChatGPTWebRuntimeLifecycleError(ctx, auth, err)
}

func (e *ChatGPTWebExecutor) handleChatGPTWebRuntimeLifecycleError(ctx context.Context, auth *cliproxyauth.Auth, err error) error {
	if err == nil || e == nil || e.manager == nil || auth == nil {
		return err
	}
	var classified interface {
		ChatGPTWebLifecycleError() *chatgptwebauth.AuthError
	}
	if !errors.As(err, &classified) {
		return err
	}
	authError := classified.ChatGPTWebLifecycleError()
	if authError == nil || authError.State != chatgptwebauth.LifecycleDead {
		return err
	}
	updated := auth.Clone()
	setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateDead, authError.Code, e.currentTime())
	persistCtx := context.Background()
	if ctx != nil {
		persistCtx = context.WithoutCancel(ctx)
	}
	_, current, errUpdate := e.manager.UpdateIfCurrent(persistCtx, auth, updated)
	if errUpdate != nil {
		log.WithFields(log.Fields{
			"auth_id":    auth.ID,
			"error_code": chatgptwebauth.SafeLifecycleReason(authError.Code),
		}).WithError(errUpdate).Warn("chatgpt web runtime account termination could not be persisted")
	} else if current {
		log.WithFields(log.Fields{
			"auth_id":    auth.ID,
			"error_code": chatgptwebauth.SafeLifecycleReason(authError.Code),
		}).Warn("chatgpt web runtime account is permanently unavailable")
	}
	return err
}

// CountTokens is not exposed by the ChatGPT Web upstream.
func (e *ChatGPTWebExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, newChatGPTWebProtocolUnavailableError()
}

// HttpRequest is completed with the ChatGPT Web protocol integration.
func (e *ChatGPTWebExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, newChatGPTWebProtocolUnavailableError()
}

// ShouldPrepareRequestAuth reports whether the access token is missing or near expiry.
func (e *ChatGPTWebExecutor) ShouldPrepareRequestAuth(auth *cliproxyauth.Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) || !auth.LifecycleRefreshable() {
		return false
	}
	credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil || strings.TrimSpace(credential.AccessToken) == "" {
		return true
	}
	expiresAt, ok := chatGPTWebCredentialExpiry(credential)
	if credential.RefreshStrategy == chatgptwebauth.RefreshStrategyTokenOnly {
		return ok && !expiresAt.After(e.currentTime())
	}
	return ok && !expiresAt.After(e.currentTime().Add(chatgptwebauth.DefaultRefreshLead))
}

// ShouldRefresh applies refresh-strategy-aware background scheduling.
func (e *ChatGPTWebExecutor) ShouldRefresh(now time.Time, auth *cliproxyauth.Auth) bool {
	if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), chatgptwebauth.Provider) || !auth.LifecycleRefreshable() {
		return false
	}
	credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil || strings.TrimSpace(credential.AccessToken) == "" {
		return true
	}
	if credential.ImportSessionPending {
		return true
	}
	expiresAt, ok := chatGPTWebCredentialExpiry(credential)
	if credential.RefreshStrategy == chatgptwebauth.RefreshStrategyTokenOnly {
		return ok && !expiresAt.After(now)
	}
	return ok && !expiresAt.After(now.Add(chatgptwebauth.DefaultRefreshLead))
}

// PrepareRequestAuth refreshes an expiring token before execution. Terminal
// failures persist their lifecycle state while the returned error moves the
// current request to another credential without recording an auth failure.
func (e *ChatGPTWebExecutor) PrepareRequestAuth(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	updated, refreshErr, terminal := e.refreshCredential(ctx, auth, false)
	if refreshErr == nil {
		return updated, nil
	}
	if terminal {
		return updated, newChatGPTWebCredentialUnavailableError(refreshErr, true)
	}
	return nil, newChatGPTWebCredentialUnavailableError(refreshErr, false)
}

// Refresh implements the background refresh contract. Terminal failures are
// installed as lifecycle transitions and therefore return no manager-level
// refresh error; transient infrastructure errors remain retryable.
func (e *ChatGPTWebExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	updated, refreshErr, terminal := e.refreshCredential(ctx, auth, false)
	if refreshErr == nil {
		return updated, nil
	}
	if terminal {
		return updated, newChatGPTWebCredentialUnavailableError(refreshErr, true)
	}
	return nil, newChatGPTWebCredentialUnavailableError(refreshErr, false)
}

// RefreshToCompletion keeps waiting for a provider-owned token exchange after
// it starts so rotating refresh tokens cannot be discarded by caller timeout.
func (e *ChatGPTWebExecutor) RefreshToCompletion(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	updated, refreshErr, terminal := e.refreshCredential(ctx, auth, true)
	if refreshErr == nil {
		return updated, nil
	}
	if terminal {
		return updated, newChatGPTWebCredentialUnavailableError(refreshErr, true)
	}
	return nil, newChatGPTWebCredentialUnavailableError(refreshErr, false)
}

// Login exposes the provider-local login implementation to management tasks.
func (e *ChatGPTWebExecutor) Login(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	if e == nil || e.authService == nil {
		return nil, errors.New("chatgpt web authentication service is unavailable")
	}
	return e.loginWithRuntimeSnapshot(ctx, input, e.configSnapshot())
}

func (e *ChatGPTWebExecutor) loginWithRuntimeSnapshot(ctx context.Context, input chatgptwebauth.LoginInput, cfg *config.Config) (*chatgptwebauth.Credential, error) {
	if cfg != nil {
		input.AllowAutoAPI798 = cfg.ChatGPTWeb.API798AutoLoginEnabled
	}
	if input.BeginSentinelObserver == nil && e.sentinelRuntime != nil {
		input.BeginSentinelObserver = func(ctx context.Context, request chatgptwebauth.SentinelSDKRequest) (chatgptwebauth.SentinelObserverHandle, error) {
			observer, errBegin := e.sentinelRuntime.BeginObserver(ctx, request)
			if observer == nil {
				return nil, errBegin
			}
			return observer, errBegin
		}
	}
	if !input.LoginProxyResolved {
		input.LoginProxy = chatGPTWebLoginProxySnapshot(cfg)
		input.LoginProxyResolved = true
	}
	if input.LoginProxy.Enabled {
		input.ProxyURL = ""
	}
	return e.authService.Login(ctx, input)
}

// LoginProxySnapshot returns one immutable login-only proxy configuration copy.
func (e *ChatGPTWebExecutor) LoginProxySnapshot() chatgptwebauth.LoginProxyConfig {
	return chatGPTWebLoginProxySnapshot(e.configSnapshot())
}

func chatGPTWebLoginProxySnapshot(cfg *config.Config) chatgptwebauth.LoginProxyConfig {
	if cfg == nil {
		return chatgptwebauth.LoginProxyConfig{}
	}
	resolved := cfg.ChatGPTWeb.LoginProxy.Resolved()
	return chatgptwebauth.LoginProxyConfig{
		Enabled:            resolved.Enabled,
		URLTemplate:        resolved.URLTemplate,
		PlaceholderCharset: resolved.PlaceholderCharset,
		RotateOnRetry:      resolved.RotateOnRetry,
		RequestAttempts:    resolved.RequestAttempts,
		FlowAttempts:       resolved.FlowAttempts,
		RetryDelay:         time.Duration(resolved.RetryDelayMilliseconds) * time.Millisecond,
		AcquisitionTimeout: time.Duration(resolved.AcquisitionTimeoutSeconds) * time.Second,
	}
}

// NormalizeImportedCredential validates and refreshes one imported credential
// before management persists it.
func (e *ChatGPTWebExecutor) NormalizeImportedCredential(ctx context.Context, credential *chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
	if e == nil || e.authService == nil || credential == nil {
		return nil, errors.New("chatgpt web imported credential is unavailable")
	}
	result := cloneChatGPTWebCredential(credential)
	var err error
	switch result.RefreshStrategy {
	case chatgptwebauth.RefreshStrategyWebOAuthRT:
		result, err = e.authService.Refresh(ctx, *result, proxyURL)
	case chatgptwebauth.RefreshStrategyChatGPTSession:
		if e.shouldRefreshImportedSessionCredential(result) {
			result, err = e.authService.RefreshSession(ctx, *result, proxyURL)
		}
	case chatgptwebauth.RefreshStrategyCodexSource:
		result, err, _ = e.refreshFromCodexSource(ctx, result)
	case chatgptwebauth.RefreshStrategyTokenOnly:
		if expiresAt, ok := chatGPTWebCredentialExpiry(result); strings.TrimSpace(result.AccessToken) == "" || ok && !expiresAt.After(e.currentTime()) {
			err = newChatGPTWebRefreshModeError("token_only_expired", "token-only credential requires a new access token")
		}
	default:
		err = newChatGPTWebRefreshModeError("refresh_strategy_invalid", "chatgpt web refresh strategy is invalid")
	}
	if err != nil {
		return result, err
	}
	if result == nil || strings.TrimSpace(result.AccessToken) == "" {
		return result, newChatGPTWebRefreshModeError("access_token_missing", "chatgpt web access token is empty")
	}
	now := e.currentTime().UTC().Format(time.RFC3339)
	result.LifecycleState = chatgptwebauth.LifecycleActive
	result.LifecycleReason = ""
	result.LifecycleUpdatedAt = now
	return result, nil
}

func (e *ChatGPTWebExecutor) shouldRefreshImportedSessionCredential(credential *chatgptwebauth.Credential) bool {
	if credential == nil || strings.TrimSpace(credential.AccessToken) == "" {
		return true
	}
	cfg := e.configSnapshot()
	if cfg == nil || cfg.ChatGPTWeb.ForceSessionRefreshOnImportEnabled() {
		return true
	}
	expiresAt, known := chatGPTWebCredentialExpiry(credential)
	return known && !expiresAt.After(e.currentTime())
}

// BeginLoginOperation serializes an account login through persistence with
// background and manual re-login operations for the same email address.
func (e *ChatGPTWebExecutor) BeginLoginOperation(ctx context.Context, email string) (context.Context, func(), error) {
	if e == nil {
		return nil, nil, errors.New("chatgpt web executor is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := strings.ToLower(strings.TrimSpace(email))
	if key == "" {
		return nil, nil, errors.New("chatgpt web login email is empty")
	}

	e.backgroundMu.Lock()
	if e.closed {
		e.backgroundMu.Unlock()
		return nil, nil, context.Canceled
	}
	if e.lifecycleCtx == nil {
		e.lifecycleCtx, e.lifecycleCancel = context.WithCancel(context.Background())
	}
	lifecycleCtx := e.lifecycleCtx
	if e.loginCoordinator == nil {
		e.loginCoordinator = NewChatGPTWebLoginCoordinator()
	}
	coordinator := e.loginCoordinator
	e.loginWG.Add(1)
	e.backgroundMu.Unlock()

	gate := coordinator.retain(key)

	operationCtx, cancelOperation := context.WithCancel(ctx)
	stopLifecycleCancel := context.AfterFunc(lifecycleCtx, cancelOperation)
	select {
	case gate.signal <- struct{}{}:
	case <-operationCtx.Done():
		stopLifecycleCancel()
		cancelOperation()
		coordinator.release(key, gate, false)
		e.loginWG.Done()
		return nil, nil, operationCtx.Err()
	}

	var once sync.Once
	release := func() {
		once.Do(func() {
			stopLifecycleCancel()
			cancelOperation()
			coordinator.release(key, gate, true)
			e.loginWG.Done()
		})
	}
	return operationCtx, release, nil
}

func (coordinator *ChatGPTWebLoginCoordinator) retain(key string) *chatGPTWebLoginGate {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.gates == nil {
		coordinator.gates = make(map[string]*chatGPTWebLoginGate)
	}
	gate := coordinator.gates[key]
	if gate == nil {
		gate = &chatGPTWebLoginGate{signal: make(chan struct{}, 1)}
		coordinator.gates[key] = gate
	}
	gate.refs++
	return gate
}

func (coordinator *ChatGPTWebLoginCoordinator) release(key string, gate *chatGPTWebLoginGate, acquired bool) {
	if acquired {
		<-gate.signal
	}
	coordinator.mu.Lock()
	gate.refs--
	if gate.refs == 0 && coordinator.gates[key] == gate {
		delete(coordinator.gates, key)
	}
	coordinator.mu.Unlock()
}

// AutoReloginEnabled reports the provider-wide re-login setting.
func (e *ChatGPTWebExecutor) AutoReloginEnabled() bool {
	cfg := e.configSnapshot()
	return cfg != nil && cfg.ChatGPTWeb.AutoRelogin
}

func (e *ChatGPTWebExecutor) sessionCookieRefreshOnTokenFailureEnabled() bool {
	cfg := e.configSnapshot()
	return cfg != nil && cfg.ChatGPTWeb.SessionCookieRefreshOnTokenFailure
}

func (e *ChatGPTWebExecutor) invalidPasskeyResponseAsDeadEnabled() bool {
	return chatGPTWebInvalidPasskeyResponseAsDeadEnabled(e.configSnapshot())
}

func chatGPTWebInvalidPasskeyResponseAsDeadEnabled(cfg *config.Config) bool {
	return cfg != nil && cfg.ChatGPTWeb.InvalidPasskeyResponseAsDead
}

func (e *ChatGPTWebExecutor) api798AutoLoginEnabled() bool {
	cfg := e.configSnapshot()
	return cfg != nil && cfg.ChatGPTWeb.API798AutoLoginEnabled
}

func (e *ChatGPTWebExecutor) credentialCanRelogin(credential *chatgptwebauth.Credential) bool {
	return chatGPTWebCredentialCanRelogin(credential, e.api798AutoLoginEnabled())
}

// TriggerBackgroundRelogin enqueues a bounded re-login task for the current
// auth generation. Duplicate triggers share one lightweight queue entry.
func (e *ChatGPTWebExecutor) TriggerBackgroundRelogin(expected *cliproxyauth.Auth) bool {
	result := e.triggerBackgroundRelogin(expected)
	return result == chatGPTWebReloginEnqueueAccepted || result == chatGPTWebReloginEnqueueDeduplicated
}

func (e *ChatGPTWebExecutor) triggerBackgroundRelogin(expected *cliproxyauth.Auth) chatGPTWebReloginEnqueueResult {
	if e == nil || e.manager == nil || !e.AutoReloginEnabled() || expected == nil {
		return chatGPTWebReloginEnqueueRejected
	}
	current, ok := e.manager.GetByID(expected.ID)
	if !ok || current == nil || current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending ||
		(expected.RuntimeInstanceID() != "" && current.RuntimeInstanceID() != expected.RuntimeInstanceID()) {
		return chatGPTWebReloginEnqueueRejected
	}
	expected = current
	credential, errCredential := chatgptwebauth.ParseCredential(expected.Metadata)
	if errCredential != nil || !e.credentialCanRelogin(credential) {
		return chatGPTWebReloginEnqueueRejected
	}
	if e.backgroundQueue != nil {
		result := e.backgroundQueue.enqueueAuth(expected)
		switch result {
		case chatGPTWebReloginEnqueueBackpressured:
			e.markBackgroundReloginBackpressure(expected)
		}
		return result
	}
	return chatGPTWebReloginEnqueueRejected
}

// BackgroundReloginSnapshot returns bounded queue activity without exposing
// credential material.
func (e *ChatGPTWebExecutor) BackgroundReloginSnapshot() chatgptwebauth.BackgroundReloginRuntimeSnapshot {
	if e == nil || e.backgroundQueue == nil {
		return chatgptwebauth.BackgroundReloginRuntimeSnapshot{}
	}
	return e.backgroundQueue.snapshot()
}

func (e *ChatGPTWebExecutor) markBackgroundReloginBackpressure(expected *cliproxyauth.Auth) {
	if e == nil || e.manager == nil || expected == nil {
		return
	}
	current, ok := e.manager.GetByID(expected.ID)
	if !ok || current == nil || chatGPTWebReloginGenerationKey(current) != chatGPTWebReloginGenerationKey(expected) {
		return
	}
	if chatGPTWebLifecycleReason(current) == "auto_relogin_backpressure" {
		return
	}
	updatedAt := e.currentTime().UTC().Format(time.RFC3339)
	if _, _, errUpdate := e.manager.MutateRuntimeMetadataIfCurrent(context.Background(), current, func(auth *cliproxyauth.Auth) {
		if auth.Metadata == nil {
			auth.Metadata = make(map[string]any)
		}
		auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
		auth.Metadata["lifecycle_reason"] = "auto_relogin_backpressure"
		auth.Metadata["lifecycle_updated_at"] = updatedAt
	}); errUpdate != nil {
		log.WithError(errUpdate).Warn("chatgpt web auto re-login backpressure could not be persisted")
	}
}

// SyncUnauthorizedRecovery moves only a safely resolvable historical
// session-expired credential into the existing bounded re-login queue.
func (e *ChatGPTWebExecutor) SyncUnauthorizedRecovery(expected *cliproxyauth.Auth) bool {
	result := e.syncUnauthorizedRecovery(e.lifecycleContext(), expected)
	return result == chatGPTWebReloginEnqueueAccepted || result == chatGPTWebReloginEnqueueDeduplicated
}

func (e *ChatGPTWebExecutor) syncUnauthorizedRecovery(ctx context.Context, expected *cliproxyauth.Auth) chatGPTWebReloginEnqueueResult {
	if e == nil || e.manager == nil || !e.AutoReloginEnabled() || expected == nil ||
		expected.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired ||
		chatGPTWebLifecycleReason(expected) != "session_expired" {
		return chatGPTWebReloginEnqueueRejected
	}
	if expected.NextRetryAfter.After(e.currentTime()) {
		return chatGPTWebReloginEnqueueRejected
	}
	credential, errCredential := chatgptwebauth.ParseCredential(expected.Metadata)
	if errCredential != nil || !e.credentialCanRelogin(credential) {
		return chatGPTWebReloginEnqueueRejected
	}
	updated := expected.Clone()
	setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateReloginPending, "session_expired", e.currentTime())
	installed, current, errUpdate := e.manager.UpdateIfCurrent(ctx, expected, updated)
	if errUpdate != nil {
		log.WithError(errUpdate).Warn("chatgpt web historical session recovery could not be persisted")
		return chatGPTWebReloginEnqueueRejected
	}
	if !current || installed == nil {
		return chatGPTWebReloginEnqueueRejected
	}
	return e.triggerBackgroundRelogin(installed)
}

// ScheduleUnauthorizedRecoveryReconcile rechecks the bounded historical
// session-expired backlog after startup or a relevant configuration change.
func (e *ChatGPTWebExecutor) ScheduleUnauthorizedRecoveryReconcile() {
	e.scheduleBackgroundReloginReconcile(false)
}

func (e *ChatGPTWebExecutor) scheduleBackgroundReloginBackpressureReconcile() {
	e.scheduleBackgroundReloginReconcile(true)
}

func (e *ChatGPTWebExecutor) scheduleBackgroundReloginReconcile(backpressuredOnly bool) {
	if e == nil || e.manager == nil || !e.AutoReloginEnabled() || e.backgroundQueue == nil {
		return
	}
	e.backgroundMu.Lock()
	if e.closed {
		e.backgroundMu.Unlock()
		return
	}
	e.reloginReconcileRequested = true
	if !backpressuredOnly {
		e.reloginReconcileFull = true
	}
	if e.reloginReconcilePending {
		e.backgroundMu.Unlock()
		return
	}
	e.reloginReconcilePending = true
	e.reloginWG.Add(1)
	e.backgroundMu.Unlock()
	go func() {
		defer e.reloginWG.Done()
		for {
			e.backgroundMu.Lock()
			if e.closed {
				e.reloginReconcilePending = false
				e.reloginReconcileRequested = false
				e.reloginReconcileFull = false
				e.backgroundMu.Unlock()
				return
			}
			full := e.reloginReconcileFull
			e.reloginReconcileRequested = false
			e.reloginReconcileFull = false
			e.backgroundMu.Unlock()

			e.reconcileBackgroundRelogins(!full)

			e.backgroundMu.Lock()
			if !e.reloginReconcileRequested {
				e.reloginReconcilePending = false
				e.backgroundMu.Unlock()
				return
			}
			e.backgroundMu.Unlock()
		}
	}()
}

func (e *ChatGPTWebExecutor) reconcileBackgroundRelogins(backpressuredOnly bool) {
	ctx := e.lifecycleContext()
	auths := e.manager.ChatGPTWebAuths()
	candidates := make([]*cliproxyauth.Auth, 0)
	pending := make([]*cliproxyauth.Auth, 0)
	eligible := int64(0)
	blockedByMethod := int64(0)
	cooling := int64(0)
	exhausted := int64(0)
	now := e.currentTime()
	for _, auth := range auths {
		if auth == nil {
			continue
		}
		if auth.LifecycleState() == cliproxyauth.LifecycleStateReloginPending {
			credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
			if errCredential != nil || !e.credentialCanRelogin(credential) {
				blockedByMethod++
				continue
			}
			pending = append(pending, auth)
			continue
		}
		if backpressuredOnly || auth.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired {
			continue
		}
		reason := chatGPTWebLifecycleReason(auth)
		if reason == "auto_relogin_exhausted" {
			exhausted++
			continue
		}
		if reason != "session_expired" {
			continue
		}
		if auth.NextRetryAfter.After(now) {
			cooling++
			continue
		}
		credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
		if errCredential != nil || !e.credentialCanRelogin(credential) {
			blockedByMethod++
			continue
		}
		eligible++
		candidates = append(candidates, auth)
	}
	remainingEligible := eligible
	if !backpressuredOnly {
		e.backgroundQueue.historicalBlockedByMethod.Store(blockedByMethod)
		e.backgroundQueue.historicalCooling.Store(cooling)
		e.backgroundQueue.historicalExhausted.Store(exhausted)
		defer func() {
			e.backgroundQueue.historicalEligible.Store(remainingEligible)
		}()
	}
	for _, auth := range candidates {
		if ctx.Err() != nil {
			return
		}
		result := e.syncUnauthorizedRecovery(ctx, auth)
		if result == chatGPTWebReloginEnqueueAccepted || result == chatGPTWebReloginEnqueueDeduplicated {
			remainingEligible--
		}
		if result == chatGPTWebReloginEnqueueBackpressured {
			return
		}
	}
	for _, auth := range pending {
		if ctx.Err() != nil {
			return
		}
		if e.triggerBackgroundRelogin(auth) == chatGPTWebReloginEnqueueBackpressured {
			return
		}
	}
}

// ReloginCurrent performs a synchronous re-login and conditionally installs
// the result. It is used by management actions and background re-login.
func (e *ChatGPTWebExecutor) ReloginCurrent(ctx context.Context, expected *cliproxyauth.Auth) (*cliproxyauth.Auth, bool, error) {
	return e.reloginCurrentWithMode(ctx, expected, false)
}

func (e *ChatGPTWebExecutor) reloginCurrentWithMode(ctx context.Context, expected *cliproxyauth.Auth, background bool) (*cliproxyauth.Auth, bool, error) {
	if e == nil || e.manager == nil || e.authService == nil || expected == nil {
		return nil, false, errors.New("chatgpt web re-login is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	var promoted *chatGPTWebReloginQueueTask
	if !background && e.backgroundQueue != nil {
		promoted = e.backgroundQueue.promote(expected)
		if promoted != nil {
			defer e.backgroundQueue.restore(promoted)
		}
	}
	expected = cloneChatGPTWebAuth(expected)
	flight, errFlight := e.joinReloginFlight(ctx, expected, background)
	if errFlight != nil {
		return nil, false, errFlight
	}
	select {
	case <-flight.done:
		e.releaseReloginWaiter(flight, background)
	case <-ctx.Done():
		if e.releaseReloginWaiter(flight, background) {
			flight.cancel()
		}
		return nil, false, ctx.Err()
	}
	result := flight.result
	return cloneChatGPTWebAuth(result.auth), result.current, result.err
}

func (e *ChatGPTWebExecutor) joinReloginFlight(ctx context.Context, expected *cliproxyauth.Auth, background bool) (*chatGPTWebReloginFlight, error) {
	key := chatGPTWebReloginGenerationKey(expected)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		e.reloginMu.Lock()
		if flight := e.reloginFlights[key]; flight != nil {
			if flight.canceling {
				done := flight.done
				e.reloginMu.Unlock()
				select {
				case <-done:
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-e.lifecycleContext().Done():
					return nil, context.Canceled
				}
			}
			flight.waiters++
			if !background {
				flight.manualWaiters++
				signalReloginModeChange(flight)
			}
			e.reloginMu.Unlock()
			return flight, nil
		}
		if !e.beginReloginWorker() {
			e.reloginMu.Unlock()
			return nil, context.Canceled
		}
		flightCtx, cancel := context.WithCancel(e.lifecycleContext())
		flight := &chatGPTWebReloginFlight{
			key:         key,
			done:        make(chan struct{}),
			cancel:      cancel,
			waiters:     1,
			modeChanged: make(chan struct{}, 1),
		}
		if !background {
			flight.manualWaiters = 1
		}
		if e.reloginFlights == nil {
			e.reloginFlights = make(map[string]*chatGPTWebReloginFlight)
		}
		e.reloginFlights[key] = flight
		e.reloginMu.Unlock()
		go func() {
			defer e.reloginWG.Done()
			e.runReloginFlight(flightCtx, expected, flight)
		}()
		return flight, nil
	}
}

func (e *ChatGPTWebExecutor) beginReloginWorker() bool {
	e.backgroundMu.Lock()
	defer e.backgroundMu.Unlock()
	if e.closed {
		return false
	}
	e.reloginWG.Add(1)
	return true
}

func (e *ChatGPTWebExecutor) runReloginFlight(ctx context.Context, expected *cliproxyauth.Auth, flight *chatGPTWebReloginFlight) {
	defer flight.cancel()
	ctx, release, active := expected.BeginRuntimeExecution(ctx)
	result := chatGPTWebReloginResult{err: context.Canceled}
	if active {
		updated, current, errRelogin := e.reloginCurrent(ctx, expected, flight)
		result = chatGPTWebReloginResult{auth: updated, current: current, err: errRelogin}
		release()
	} else if latest, ok := e.manager.GetByID(expected.ID); !ok ||
		chatGPTWebReloginGenerationKey(latest) != chatGPTWebReloginGenerationKey(expected) {
		result = chatGPTWebReloginResult{
			auth: cloneChatGPTWebAuth(latest),
			err:  chatgptwebauth.ErrCredentialSuperseded,
		}
	}

	e.reloginMu.Lock()
	if flight.restartBackground && errors.Is(result.err, context.Canceled) {
		result.err = errChatGPTWebReloginOwnershipChanged
	}
	flight.result = result
	flight.completed = true
	if flight.waiters == 0 && e.reloginFlights[flight.key] == flight {
		delete(e.reloginFlights, flight.key)
	}
	close(flight.done)
	e.reloginMu.Unlock()
}

// releaseReloginWaiter reports whether the released waiter was the final
// owner of a still-running acquisition.
func (e *ChatGPTWebExecutor) releaseReloginWaiter(flight *chatGPTWebReloginFlight, background bool) bool {
	e.reloginMu.Lock()
	defer e.reloginMu.Unlock()
	if flight == nil || flight.waiters == 0 {
		return false
	}
	flight.waiters--
	if !background && flight.manualWaiters > 0 {
		flight.manualWaiters--
		signalReloginModeChange(flight)
	}
	lastWaiter := flight.waiters == 0 && !flight.completed
	manualOwnerReleased := !flight.completed && flight.waiters > 0 &&
		flight.manualWaiters == 0 && flight.mode == chatGPTWebReloginModeManual
	shouldCancel := lastWaiter || manualOwnerReleased
	if shouldCancel {
		flight.canceling = true
		flight.restartBackground = manualOwnerReleased
	}
	if flight.waiters == 0 && flight.completed && e.reloginFlights[flight.key] == flight {
		delete(e.reloginFlights, flight.key)
	}
	return shouldCancel
}

func signalReloginModeChange(flight *chatGPTWebReloginFlight) {
	if flight == nil || flight.modeChanged == nil {
		return
	}
	select {
	case flight.modeChanged <- struct{}{}:
	default:
	}
}

func (e *ChatGPTWebExecutor) runBackgroundRelogin(expected *cliproxyauth.Auth) {
	if e == nil || expected == nil {
		return
	}
	task := &chatGPTWebReloginQueueTask{
		authID:        strings.TrimSpace(expected.ID),
		instanceID:    strings.TrimSpace(expected.RuntimeInstanceID()),
		generationKey: chatGPTWebReloginGenerationKey(expected),
		attempt:       1,
		dueAt:         e.currentTime(),
		heapIndex:     -1,
	}
	if task.authID == "" || task.generationKey == "" {
		return
	}
	ctx := e.lifecycleContext()
	for {
		if !e.executeBackgroundReloginTask(ctx, task) {
			return
		}
		delay := task.dueAt.Sub(e.currentTime())
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			stopChatGPTWebReloginTimer(timer)
			return
		case <-timer.C:
		}
	}
}

func (e *ChatGPTWebExecutor) executeBackgroundReloginTask(ctx context.Context, task *chatGPTWebReloginQueueTask) bool {
	current, pending := e.backgroundReloginTaskCurrent(task)
	if !pending {
		return false
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil || !e.credentialCanRelogin(credential) {
		return false
	}
	_, _, errRelogin := e.reloginCurrentWithMode(ctx, current, true)
	if errRelogin == nil {
		if e.backgroundQueue != nil {
			e.backgroundQueue.succeeded.Add(1)
		}
		return false
	}
	if e.backgroundQueue != nil {
		e.backgroundQueue.failed.Add(1)
		if chatgptwebauth.IsLifecycleState(errRelogin, chatgptwebauth.LifecycleDead) {
			e.backgroundQueue.dead.Add(1)
		}
	}
	current, pending = e.backgroundReloginTaskCurrent(task)
	if !pending {
		return false
	}
	if !chatGPTWebBackgroundReloginRetryable(errRelogin) {
		logChatGPTWebBackgroundReloginFailure(current, errRelogin)
		return false
	}
	policy := config.ChatGPTWebConfig{}.ResolvedAutoRelogin()
	if cfg := e.configSnapshot(); cfg != nil {
		policy = cfg.ChatGPTWeb.ResolvedAutoRelogin()
	}
	maxAttempts := policy.MaxRetries + 1
	if task.attempt >= maxAttempts {
		logChatGPTWebBackgroundReloginFailure(current, errRelogin)
		e.markBackgroundReloginExhausted(ctx, current)
		if e.backgroundQueue != nil {
			e.backgroundQueue.exhausted.Add(1)
		}
		return false
	}
	if task.attempt%chatGPTWebBackgroundReloginLogInterval == 0 {
		logChatGPTWebBackgroundReloginFailure(current, errRelogin)
	}
	task.dueAt = e.currentTime().Add(e.backgroundReloginDelay(task.attempt, policy.JitterPercent))
	task.attempt++
	return true
}

func (e *ChatGPTWebExecutor) backgroundReloginTaskCurrent(task *chatGPTWebReloginQueueTask) (*cliproxyauth.Auth, bool) {
	if e == nil || task == nil || !e.AutoReloginEnabled() || e.manager == nil {
		return nil, false
	}
	current, ok := e.manager.GetByID(task.authID)
	if !ok || current == nil || current.RuntimeInstanceRetired() ||
		current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending ||
		chatGPTWebReloginGenerationKey(current) != task.generationKey {
		return current, false
	}
	if task.instanceID != "" && current.RuntimeInstanceID() != task.instanceID {
		return current, false
	}
	return current, true
}

func (e *ChatGPTWebExecutor) backgroundReloginTaskPending(task *chatGPTWebReloginQueueTask) bool {
	_, pending := e.backgroundReloginTaskCurrent(task)
	return pending
}

func (e *ChatGPTWebExecutor) markBackgroundReloginExhausted(ctx context.Context, expected *cliproxyauth.Auth) {
	if e == nil || e.manager == nil || expected == nil {
		return
	}
	updated := expected.Clone()
	setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateReauthRequired, "auto_relogin_exhausted", e.currentTime())
	persistCtx := context.Background()
	if ctx != nil {
		persistCtx = context.WithoutCancel(ctx)
	}
	_, current, errUpdate := e.manager.UpdateIfCurrent(persistCtx, expected, updated)
	if errUpdate != nil {
		log.WithFields(log.Fields{
			"auth_id":    expected.ID,
			"error_code": "auto_relogin_exhausted",
		}).WithError(errUpdate).Warn("chatgpt web auto re-login exhaustion could not be persisted")
		return
	}
	if current {
		log.WithFields(log.Fields{
			"auth_id":    expected.ID,
			"error_code": "auto_relogin_exhausted",
		}).Warn("chatgpt web auto re-login retries exhausted")
	}
}

func (e *ChatGPTWebExecutor) acquireReloginExecution(ctx context.Context, expected *cliproxyauth.Auth, flight *chatGPTWebReloginFlight) (background bool, release func(), ready bool) {
	slots := e.reloginSlots
	if slots == nil {
		slots = chatGPTWebBackgroundReloginSlots
	}
	for {
		e.reloginMu.Lock()
		if flight == nil || flight.canceling || flight.completed {
			e.reloginMu.Unlock()
			return false, nil, false
		}
		if flight.manualWaiters > 0 {
			flight.mode = chatGPTWebReloginModeManual
			e.reloginMu.Unlock()
			return false, nil, true
		}
		if queueWorker, _ := ctx.Value(chatGPTWebReloginQueueWorkerContextKey{}).(bool); queueWorker {
			flight.mode = chatGPTWebReloginModeBackground
			e.reloginMu.Unlock()
			if e.reloginSlotAcquired != nil {
				e.reloginSlotAcquired()
			}
			return true, func() {}, true
		}
		e.reloginMu.Unlock()
		if !e.backgroundReloginPending(expected) {
			return false, nil, false
		}
		select {
		case slots <- struct{}{}:
			e.reloginMu.Lock()
			if flight.canceling || flight.completed {
				e.reloginMu.Unlock()
				<-slots
				return false, nil, false
			}
			if flight.manualWaiters > 0 {
				flight.mode = chatGPTWebReloginModeManual
				e.reloginMu.Unlock()
				<-slots
				return false, nil, true
			}
			flight.mode = chatGPTWebReloginModeBackground
			e.reloginMu.Unlock()
			if e.reloginSlotAcquired != nil {
				e.reloginSlotAcquired()
			}
			return true, func() { <-slots }, true
		case <-ctx.Done():
			return false, nil, false
		case <-flight.modeChanged:
		}
	}
}

func (e *ChatGPTWebExecutor) backgroundReloginPending(expected *cliproxyauth.Auth) bool {
	if e == nil || !e.AutoReloginEnabled() || e.manager == nil || expected == nil || expected.RuntimeInstanceRetired() {
		return false
	}
	current, ok := e.manager.GetByID(expected.ID)
	return ok && current.LifecycleState() == cliproxyauth.LifecycleStateReloginPending && chatGPTWebReloginGenerationKey(current) == chatGPTWebReloginGenerationKey(expected)
}

func (e *ChatGPTWebExecutor) backgroundReloginDelay(attempt, jitterPercent int) time.Duration {
	delay := chatGPTWebBackgroundReloginBackoff(attempt)
	if e != nil && e.reloginBackoff != nil {
		delay = e.reloginBackoff(attempt)
	}
	return chatGPTWebJitterDuration(delay, jitterPercent, rand.Reader)
}

func chatGPTWebBackgroundReloginBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 12 {
		attempt = 12
	}
	delay := 100 * time.Millisecond * time.Duration(1<<(attempt-1))
	if delay > chatGPTWebBackgroundReloginMaxBackoff {
		return chatGPTWebBackgroundReloginMaxBackoff
	}
	return delay
}

func chatGPTWebJitterDuration(delay time.Duration, percent int, source io.Reader) time.Duration {
	if delay <= 0 || percent <= 0 || source == nil {
		return delay
	}
	maxJitter := delay * time.Duration(percent) / 100
	if maxJitter <= 0 {
		return delay
	}
	var random [8]byte
	if _, err := io.ReadFull(source, random[:]); err != nil {
		return delay
	}
	span := uint64(maxJitter)*2 + 1
	offset := time.Duration(binary.BigEndian.Uint64(random[:])%span) - maxJitter
	return delay + offset
}

func chatGPTWebBackgroundReloginRetryable(err error) bool {
	if errors.Is(err, errChatGPTWebReloginOwnershipChanged) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if chatgptwebauth.IsRetryable(err) {
		return true
	}
	var unavailable *proxypool.UnavailableError
	return errors.As(err, &unavailable)
}

func logChatGPTWebBackgroundReloginFailure(auth *cliproxyauth.Auth, err error) {
	code := chatGPTWebErrorCode(err)
	fields := log.Fields{"error_code": code}
	if auth != nil {
		fields["auth_id"] = auth.ID
		if resultError := cliproxyauth.NewProviderError(auth, err); resultError != nil && resultError.Diagnostic != nil {
			for key, value := range helps.ChatGPTWebDiagnosticLogFields(resultError.Diagnostic) {
				fields[key] = value
			}
		}
	}
	log.WithFields(fields).Warn("chatgpt web background re-login failed")
}

func (e *ChatGPTWebExecutor) reloginCurrent(ctx context.Context, expected *cliproxyauth.Auth, flight *chatGPTWebReloginFlight) (*cliproxyauth.Auth, bool, error) {
	if e == nil || e.manager == nil || e.authService == nil || expected == nil {
		return nil, false, errors.New("chatgpt web re-login is unavailable")
	}
	var email string
	if expected.Metadata != nil {
		email, _ = expected.Metadata["email"].(string)
	}
	if strings.TrimSpace(email) == "" {
		email = "auth:" + expected.ID
	}
	operationCtx, releaseOperation, errOperation := e.BeginLoginOperation(ctx, email)
	if errOperation != nil {
		return nil, false, errOperation
	}
	defer releaseOperation()
	ctx = operationCtx
	latest, ok := e.manager.GetByID(expected.ID)
	if !ok || chatGPTWebReloginGenerationKey(latest) != chatGPTWebReloginGenerationKey(expected) {
		return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
	}
	background, releaseSlot, ready := e.acquireReloginExecution(ctx, expected, flight)
	if !ready {
		if errContext := ctx.Err(); errContext != nil {
			return nil, false, errContext
		}
		latest, _ = e.manager.GetByID(expected.ID)
		return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
	}
	if releaseSlot != nil {
		defer releaseSlot()
	}
	if background {
		if !e.backgroundReloginPending(expected) {
			latest, _ = e.manager.GetByID(expected.ID)
			return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
		}
	}
	runtimeCfg := e.configSnapshot()
	loginProxy := chatGPTWebLoginProxySnapshot(runtimeCfg)
	allowAutoAPI798 := runtimeCfg != nil && runtimeCfg.ChatGPTWeb.API798AutoLoginEnabled
	acquisitionTimeout := e.reloginAcquisitionTimeout(expected, loginProxy, allowAutoAPI798)
	acquisitionCtx, cancelAcquisition := context.WithTimeoutCause(ctx, acquisitionTimeout, errChatGPTWebReloginAcquisitionLimit)
	defer cancelAcquisition()
	ctx = acquisitionCtx

	loginProxyEnabled := loginProxy.Enabled
	resolved := expected
	if !loginProxyEnabled {
		releaseProxyBinding := e.manager.HoldProxyBinding(expected.ID)
		defer releaseProxyBinding()
		var errResolve error
		resolved, errResolve = e.manager.ResolveProxyAuth(ctx, expected)
		if errResolve != nil {
			return nil, false, errResolve
		}
	}
	credential, errCredential := chatgptwebauth.ParseCredential(resolved.Metadata)
	if errCredential != nil {
		return nil, false, fmt.Errorf("parse chatgpt web credential: %w", errCredential)
	}
	result, errLogin := e.loginWithRuntimeSnapshot(ctx, chatgptwebauth.LoginInput{
		Credential:                  credential,
		ProxyURL:                    chatGPTWebProxyURLForTarget(resolved, chatgptwebauth.AuthBaseURL, runtimeCfg),
		LoginProxy:                  loginProxy,
		LoginProxyResolved:          true,
		Relogin:                     true,
		RetryInvalidPasskeyResponse: chatGPTWebInvalidPasskeyResponseAsDeadEnabled(runtimeCfg),
		PersistWebAuthn: func(persistCtx context.Context, updated chatgptwebauth.WebAuthnCredential) (chatgptwebauth.WebAuthnCredential, error) {
			return e.persistWebAuthnReloginState(persistCtx, expected, updated)
		},
		PersistAdvancedAccountSecurity: func(persistCtx context.Context, updated chatgptwebauth.AdvancedAccountSecurityCredential) (chatgptwebauth.AdvancedAccountSecurityCredential, error) {
			return e.persistAdvancedAccountSecurityReloginState(persistCtx, expected, updated)
		},
	}, runtimeCfg)
	if errContext := ctx.Err(); errContext != nil {
		if latest, ok := e.manager.GetByID(expected.ID); ok &&
			chatGPTWebReloginGenerationKey(latest) != chatGPTWebReloginGenerationKey(expected) {
			return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
		}
		if !errors.Is(context.Cause(ctx), errChatGPTWebReloginAcquisitionLimit) {
			return nil, false, errContext
		}
		if errLogin == nil {
			if result == nil {
				return nil, false, errContext
			}
			ctx = context.WithoutCancel(ctx)
		} else {
			if _, ok := chatgptwebauth.AsAuthError(errLogin); !ok {
				return nil, false, errContext
			}
			if chatgptwebauth.IsRetryable(errLogin) || result == nil {
				return nil, false, errLogin
			}
			ctx = context.WithoutCancel(ctx)
		}
	}
	if errors.Is(errLogin, chatgptwebauth.ErrCredentialSuperseded) {
		latest, _ = e.manager.GetByID(expected.ID)
		return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
	}
	if errLogin != nil && chatgptwebauth.IsRetryable(errLogin) && !loginProxyEnabled &&
		chatGPTWebErrorCode(errLogin) != "passkey_state_persist_failed" {
		return nil, false, e.manager.ReportProxyFailure(ctx, resolved, errLogin)
	}
	if result == nil {
		return nil, false, firstNonNilError(errLogin, errors.New("chatgpt web re-login returned no credential"))
	}
	if errLogin != nil {
		result, errLogin = promoteExhaustedInvalidPasskeyResponse(
			chatGPTWebInvalidPasskeyResponseAsDeadEnabled(runtimeCfg),
			expected,
			result,
			errLogin,
			loginProxy,
			e.currentTime(),
		)
	}
	updated := applyChatGPTWebCredential(expected, result)
	updateCtx := ctx
	if result.LifecycleState == chatgptwebauth.LifecycleDead &&
		strings.EqualFold(strings.TrimSpace(result.LifecycleReason), "invalid_passkey_response") {
		clearChatGPTWebUnauthorizedCooldown(updated, e.currentTime())
		// The dead lifecycle supersedes stale unauthorized cooldowns. Preserve
		// the explicitly cleared runtime state instead of carrying it forward
		// again from the currently installed credential during the update.
		updateCtx = cliproxyauth.WithSkipStateCarryForward(updateCtx)
	}
	if errLogin != nil {
		updated.LastError = cliproxyauth.NewProviderError(updated, errLogin)
	} else {
		updated.LastError = nil
	}
	installed, current, errUpdate := e.manager.UpdateChatGPTWebReloginIfCurrent(
		updateCtx,
		expected,
		updated,
	)
	if errUpdate != nil {
		if latest, ok := e.manager.GetByID(expected.ID); ok && chatGPTWebReloginGenerationKey(latest) != chatGPTWebReloginGenerationKey(expected) {
			return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
		}
		return nil, false, errUpdate
	}
	if !current {
		latest, _ := e.manager.GetByID(expected.ID)
		return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
	}
	if errLogin != nil {
		return cloneChatGPTWebAuth(installed), true, errLogin
	}
	return cloneChatGPTWebAuth(installed), true, nil
}

func promoteExhaustedInvalidPasskeyResponse(
	enabled bool,
	expected *cliproxyauth.Auth,
	credential *chatgptwebauth.Credential,
	errLogin error,
	loginProxy chatgptwebauth.LoginProxyConfig,
	now time.Time,
) (*chatgptwebauth.Credential, error) {
	if !enabled || expected == nil || credential == nil {
		return credential, errLogin
	}
	authError, ok := chatgptwebauth.AsAuthError(errLogin)
	if !ok || authError.StatusCode != http.StatusBadRequest || authError.Retryable || !authError.Terminal ||
		authError.FailureStage != "passkey_verify" || authError.DiagnosticCode != "invalid_passkey_response" {
		return credential, errLogin
	}
	requiredAttempts := 1
	if loginProxy.Enabled {
		requiredAttempts = max(1, loginProxy.FlowAttempts)
	}
	if authError.Attempts < requiredAttempts || !chatGPTWebPasskeyRecoveryExhausted(expected, credential) {
		return credential, errLogin
	}

	promotedCredential := cloneChatGPTWebCredential(credential)
	promotedCredential.LifecycleState = chatgptwebauth.LifecycleDead
	promotedCredential.LifecycleReason = "invalid_passkey_response"
	promotedCredential.LifecycleUpdatedAt = now.UTC().Format(time.RFC3339)
	promotedError := *authError
	promotedError.State = chatgptwebauth.LifecycleDead
	promotedError.LifecycleState = chatgptwebauth.LifecycleDead
	promotedError.Retryable = false
	promotedError.Terminal = true
	return promotedCredential, &promotedError
}

func chatGPTWebPasskeyRecoveryExhausted(expected *cliproxyauth.Auth, credential *chatgptwebauth.Credential) bool {
	if expected == nil || credential == nil || credential.WebAuthn == nil ||
		chatgptwebauth.ValidateWebAuthnCredential(credential.WebAuthn) != nil {
		return false
	}
	if strings.TrimSpace(credential.Password) != "" || strings.TrimSpace(credential.TOTPSecret) != "" ||
		strings.TrimSpace(credential.SourceAuthID) != "" || strings.TrimSpace(credential.SourceCredentialUID) != "" {
		return false
	}
	if !chatGPTWebInvalidAccessTokenEvidence(expected) {
		return false
	}
	reason := chatGPTWebLifecycleReason(expected)
	switch expected.LifecycleState() {
	case cliproxyauth.LifecycleStateReloginPending:
		return chatGPTWebRefreshRecoveryExhausted(credential, reason)
	case cliproxyauth.LifecycleStateReauthRequired:
		return (reason == "passkey_verification_failed" || reason == "invalid_passkey_response" || reason == "auto_relogin_exhausted") &&
			strings.TrimSpace(credential.RefreshToken) == "" && !chatgptwebauth.HasSessionCookie(credential.Cookies)
	default:
		return false
	}
}

func chatGPTWebRefreshRecoveryExhausted(credential *chatgptwebauth.Credential, reason string) bool {
	if credential == nil {
		return false
	}
	hasRefreshToken := strings.TrimSpace(credential.RefreshToken) != ""
	hasSessionCookie := chatgptwebauth.HasSessionCookie(credential.Cookies)
	if hasRefreshToken && (credential.RefreshStrategy != chatgptwebauth.RefreshStrategyWebOAuthRT || reason != "invalid_grant") {
		return false
	}
	if hasSessionCookie && reason != "session_expired" && reason != "access_token_missing" {
		return false
	}
	// A single lifecycle reason cannot prove that both independent recovery
	// materials were rejected. Keep the credential for manual inspection.
	return !hasRefreshToken || !hasSessionCookie
}

func chatGPTWebInvalidAccessTokenEvidence(auth *cliproxyauth.Auth) bool {
	if auth == nil {
		return false
	}
	switch chatGPTWebLifecycleReason(auth) {
	case "access_token_missing", "invalid_grant", "session_expired", "token_invalid", "token_invalidated", "token_only_expired", "unauthorized":
		return true
	}
	if chatGPTWebUnauthorizedProviderError(auth.LastError) {
		return true
	}
	for _, state := range auth.ModelStates {
		if state != nil && chatGPTWebUnauthorizedProviderError(state.LastError) {
			return true
		}
	}
	return false
}

func chatGPTWebLifecycleReason(auth *cliproxyauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	reason, _ := auth.Metadata["lifecycle_reason"].(string)
	return strings.ToLower(strings.TrimSpace(reason))
}

func chatGPTWebUnauthorizedProviderError(err *cliproxyauth.Error) bool {
	return err != nil && (err.StatusCode() == http.StatusUnauthorized ||
		strings.EqualFold(strings.TrimSpace(err.Code), "unauthorized") ||
		strings.EqualFold(strings.TrimSpace(err.Code), "token_invalid") ||
		strings.EqualFold(strings.TrimSpace(err.Code), "token_invalidated"))
}

func clearChatGPTWebUnauthorizedCooldown(auth *cliproxyauth.Auth, now time.Time) {
	if auth == nil {
		return
	}
	if chatGPTWebUnauthorizedProviderError(auth.LastError) || auth.CooldownScope == "auth" {
		auth.Unavailable = false
		auth.NextRetryAfter = time.Time{}
		auth.CooldownScope = ""
	}
	for _, state := range auth.ModelStates {
		if state == nil || !chatGPTWebUnauthorizedProviderError(state.LastError) {
			continue
		}
		state.Status = cliproxyauth.StatusActive
		state.StatusMessage = ""
		state.Unavailable = false
		state.NextRetryAfter = time.Time{}
		state.LastError = nil
		state.UpdatedAt = now
	}
}

func (e *ChatGPTWebExecutor) persistWebAuthnReloginState(
	ctx context.Context,
	expected *cliproxyauth.Auth,
	updated chatgptwebauth.WebAuthnCredential,
) (chatgptwebauth.WebAuthnCredential, error) {
	if e == nil || e.manager == nil || expected == nil {
		return chatgptwebauth.WebAuthnCredential{}, chatgptwebauth.ErrCredentialSuperseded
	}
	if errValidate := chatgptwebauth.ValidateWebAuthnCredential(&updated); errValidate != nil {
		return chatgptwebauth.WebAuthnCredential{}, errValidate
	}
	var mutationErr error
	installed, current, errMutate := e.manager.MutateRuntimeMetadataIfCurrent(ctx, expected, func(candidate *cliproxyauth.Auth) {
		if mutationErr != nil || candidate == nil {
			return
		}
		credential, errParse := chatgptwebauth.ParseCredential(candidate.Metadata)
		if errParse != nil || credential.WebAuthn == nil ||
			!chatgptwebauth.WebAuthnAuthenticatorMatches(credential.WebAuthn, &updated) {
			mutationErr = chatgptwebauth.ErrCredentialSuperseded
			return
		}
		currentCount := credential.WebAuthn.SignCount
		if updated.SignCount < currentCount || updated.SignCount-currentCount > 1 {
			mutationErr = errors.New("chatgpt web Passkey signature counter is not monotonic")
			return
		}
		if chatgptwebauth.CompareWebAuthnLastUsedAt(updated.LastUsedAt, credential.WebAuthn.LastUsedAt) < 0 {
			mutationErr = errors.New("chatgpt web Passkey last-used timestamp moved backwards")
			return
		}
		webAuthn := updated
		webAuthn.Transports = append([]string(nil), updated.Transports...)
		credential.WebAuthn = &webAuthn
		credential.ApplyToMetadata(candidate.Metadata)
	})
	if errMutate != nil {
		return chatgptwebauth.WebAuthnCredential{}, errMutate
	}
	if !current || mutationErr != nil {
		if mutationErr != nil {
			return chatgptwebauth.WebAuthnCredential{}, mutationErr
		}
		return chatgptwebauth.WebAuthnCredential{}, chatgptwebauth.ErrCredentialSuperseded
	}
	persisted, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || persisted.WebAuthn == nil ||
		!chatgptwebauth.WebAuthnAuthenticatorMatches(persisted.WebAuthn, &updated) ||
		persisted.WebAuthn.SignCount != updated.SignCount {
		return chatgptwebauth.WebAuthnCredential{}, errors.New("persisted chatgpt web Passkey state is invalid")
	}
	result := *persisted.WebAuthn
	result.Transports = append([]string(nil), persisted.WebAuthn.Transports...)
	return result, nil
}

func (e *ChatGPTWebExecutor) persistAdvancedAccountSecurityReloginState(
	ctx context.Context,
	expected *cliproxyauth.Auth,
	updated chatgptwebauth.AdvancedAccountSecurityCredential,
) (chatgptwebauth.AdvancedAccountSecurityCredential, error) {
	if e == nil || e.manager == nil || expected == nil {
		return chatgptwebauth.AdvancedAccountSecurityCredential{}, chatgptwebauth.ErrCredentialSuperseded
	}
	if errValidate := chatgptwebauth.ValidateAdvancedAccountSecurityCredential(&updated); errValidate != nil {
		return chatgptwebauth.AdvancedAccountSecurityCredential{}, errValidate
	}
	var mutationErr error
	installed, current, errMutate := e.manager.MutateRuntimeMetadataIfCurrent(ctx, expected, func(candidate *cliproxyauth.Auth) {
		if mutationErr != nil || candidate == nil {
			return
		}
		credential, errParse := chatgptwebauth.ParseCredential(candidate.Metadata)
		if errParse != nil || credential.AdvancedAccountSecurity == nil ||
			!chatgptwebauth.AdvancedAccountSecurityMaterialMatches(credential.AdvancedAccountSecurity, &updated) {
			mutationErr = chatgptwebauth.ErrCredentialSuperseded
			return
		}

		var incremented uint64
		for index := range updated.Passkeys {
			currentKey := &credential.AdvancedAccountSecurity.Passkeys[index].Credential
			updatedKey := &updated.Passkeys[index].Credential
			if updatedKey.SignCount < currentKey.SignCount || updatedKey.SignCount-currentKey.SignCount > 1 {
				mutationErr = errors.New("chatgpt web advanced security Passkey signature counter is not monotonic")
				return
			}
			incremented += uint64(updatedKey.SignCount - currentKey.SignCount)
			if chatgptwebauth.CompareWebAuthnLastUsedAt(updatedKey.LastUsedAt, currentKey.LastUsedAt) < 0 {
				mutationErr = errors.New("chatgpt web advanced security Passkey last-used timestamp moved backwards")
				return
			}
		}
		if incremented > 1 {
			mutationErr = errors.New("chatgpt web advanced security updated multiple Passkey counters")
			return
		}

		credential.AdvancedAccountSecurity = chatgptwebauth.CloneAdvancedAccountSecurityCredential(&updated)
		credential.ApplyToMetadata(candidate.Metadata)
	})
	if errMutate != nil {
		return chatgptwebauth.AdvancedAccountSecurityCredential{}, errMutate
	}
	if !current || mutationErr != nil {
		if mutationErr != nil {
			return chatgptwebauth.AdvancedAccountSecurityCredential{}, mutationErr
		}
		return chatgptwebauth.AdvancedAccountSecurityCredential{}, chatgptwebauth.ErrCredentialSuperseded
	}
	persisted, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || persisted.AdvancedAccountSecurity == nil ||
		!chatgptwebauth.AdvancedAccountSecurityMaterialMatches(persisted.AdvancedAccountSecurity, &updated) {
		return chatgptwebauth.AdvancedAccountSecurityCredential{}, errors.New("persisted chatgpt web advanced security state is invalid")
	}
	for index := range updated.Passkeys {
		persistedKey := &persisted.AdvancedAccountSecurity.Passkeys[index].Credential
		updatedKey := &updated.Passkeys[index].Credential
		if persistedKey.SignCount != updatedKey.SignCount || persistedKey.LastUsedAt != updatedKey.LastUsedAt {
			return chatgptwebauth.AdvancedAccountSecurityCredential{}, errors.New("persisted chatgpt web advanced security authenticator state is invalid")
		}
	}
	return *chatgptwebauth.CloneAdvancedAccountSecurityCredential(persisted.AdvancedAccountSecurity), nil
}

func (e *ChatGPTWebExecutor) refreshCredential(ctx context.Context, auth *cliproxyauth.Auth, waitForCompletion bool) (*cliproxyauth.Auth, error, bool) {
	if e == nil || e.authService == nil || auth == nil {
		return nil, errors.New("chatgpt web refresh is unavailable"), false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if errContext := ctx.Err(); errContext != nil {
		return nil, errContext, false
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		updated := auth.Clone()
		setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateReauthRequired, "credential_invalid", e.currentTime())
		return updated, fmt.Errorf("parse chatgpt web credential: %w", errCredential), true
	}
	chatgptwebauth.ResolveCredentialPersona(credential, auth.ID)
	if errIdentity := chatgptwebauth.EnsureCredentialRuntimeIDsForURL(credential, chatgptwebauth.CredentialRuntimeIdentityReader(auth.ID, credential), e.chatGPTWebBaseURL()); errIdentity != nil {
		return nil, fmt.Errorf("initialize chatgpt web browser identity: %w", errIdentity), false
	}
	key := chatGPTWebRefreshKey(auth)
	if !e.beginRefreshWait() {
		return nil, context.Canceled, false
	}
	resultChannel := e.refreshGroup.DoChan(key, func() (any, error) {
		var acquisitionCtx context.Context
		var cancel context.CancelFunc
		if credential.RefreshStrategy == chatgptwebauth.RefreshStrategyCodexSource {
			acquisitionCtx, cancel = e.acquisitionContextWithValues(ctx)
		} else {
			acquisitionCtx, cancel = e.acquisitionContext()
		}
		defer cancel()
		acquisitionCtx, release, active := auth.BeginRuntimeExecution(acquisitionCtx)
		if !active {
			return chatGPTWebRefreshResult{err: context.Canceled}, nil
		}
		defer release()
		result, errRefresh, terminal := e.refreshByStrategy(acquisitionCtx, auth, credential)
		return chatGPTWebRefreshResult{credential: result, err: errRefresh, terminal: terminal}, nil
	})
	trackedResult := make(chan singleflight.Result, 1)
	go func() {
		defer e.refreshWG.Done()
		if result, ok := <-resultChannel; ok {
			trackedResult <- result
		}
		close(trackedResult)
	}()
	var flightResult singleflight.Result
	if waitForCompletion || credential.RefreshStrategy == chatgptwebauth.RefreshStrategyCodexSource {
		result, ok := <-trackedResult
		if !ok {
			return nil, errors.New("chatgpt web refresh ended without a result"), false
		}
		flightResult = result
	} else {
		select {
		case <-ctx.Done():
			return nil, ctx.Err(), false
		case result, ok := <-trackedResult:
			if !ok {
				return nil, errors.New("chatgpt web refresh ended without a result"), false
			}
			flightResult = result
		}
	}
	if errLifecycle := e.lifecycleContext().Err(); errLifecycle != nil {
		return nil, errLifecycle, false
	}
	if flightResult.Err != nil {
		return nil, flightResult.Err, false
	}
	result, ok := flightResult.Val.(chatGPTWebRefreshResult)
	if !ok {
		return nil, errors.New("chatgpt web refresh returned an invalid result"), false
	}
	result.credential = cloneChatGPTWebCredential(result.credential)
	if result.err == nil {
		if result.credential == nil {
			return nil, errors.New("chatgpt web refresh returned no credential"), false
		}
		// The Manager persists the returned credential as the refresh result.
		// Clearing the import intent here prevents a successful background refresh
		// from being scheduled repeatedly after the returned auth is installed.
		result.credential.ImportSessionPending = false
		return applyChatGPTWebCredential(auth, result.credential), nil, false
	}
	if !result.terminal && !chatgptwebauth.IsTerminal(result.err) {
		return nil, result.err, false
	}
	if result.credential == nil {
		result.credential = credential
	}
	state := string(result.credential.LifecycleState)
	if authError, okAuthError := chatgptwebauth.AsAuthError(result.err); okAuthError {
		state = string(authError.State)
	}
	strategy := result.credential.RefreshStrategy
	if strategy == chatgptwebauth.RefreshStrategyCodexSource ||
		strategy == chatgptwebauth.RefreshStrategyTokenOnly && !e.credentialCanRelogin(result.credential) {
		state = cliproxyauth.LifecycleStateReauthRequired
	} else if state != cliproxyauth.LifecycleStateDead && state != cliproxyauth.LifecycleStateInteractionRequired {
		if e.AutoReloginEnabled() && e.credentialCanRelogin(result.credential) {
			state = cliproxyauth.LifecycleStateReloginPending
		} else {
			state = cliproxyauth.LifecycleStateReauthRequired
		}
	}
	reason := chatGPTWebErrorCode(result.err)
	result.credential.LifecycleState = chatgptwebauth.LifecycleState(state)
	result.credential.LifecycleReason = reason
	result.credential.LifecycleUpdatedAt = e.currentTime().UTC().Format(time.RFC3339)
	return applyChatGPTWebCredential(auth, result.credential), result.err, true
}

func (e *ChatGPTWebExecutor) beginRefreshWait() bool {
	e.backgroundMu.Lock()
	defer e.backgroundMu.Unlock()
	if e.closed {
		return false
	}
	e.refreshWG.Add(1)
	return true
}

type chatGPTWebRefreshResult struct {
	credential *chatgptwebauth.Credential
	err        error
	terminal   bool
}

func (e *ChatGPTWebExecutor) refreshByStrategy(ctx context.Context, auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential) (*chatgptwebauth.Credential, error, bool) {
	if credential == nil {
		return nil, newChatGPTWebRefreshModeError("credential_invalid", "chatgpt web credential is invalid"), true
	}
	if e.sessionCookieRefreshEligible(credential) {
		result, errRefresh := e.authService.RefreshSession(
			ctx,
			*credential,
			e.proxyURLForTarget(auth, chatgptwebauth.SessionBaseURL),
		)
		return classifyChatGPTWebSessionCookieRefresh(result, errRefresh, e.api798AutoLoginEnabled())
	}
	switch credential.RefreshStrategy {
	case chatgptwebauth.RefreshStrategyWebOAuthRT:
		result, err := e.authService.Refresh(ctx, *credential, e.proxyURLForTarget(auth, chatgptwebauth.AuthBaseURL))
		return result, err, false
	case chatgptwebauth.RefreshStrategyChatGPTSession:
		result, err := e.authService.RefreshSession(ctx, *credential, e.proxyURLForTarget(auth, chatgptwebauth.SessionBaseURL))
		if e.sessionCookieRefreshOnTokenFailureEnabled() && !chatGPTWebCredentialHasCompletePasswordLogin(credential) {
			return classifyChatGPTWebSessionCookieRefresh(result, err, e.api798AutoLoginEnabled())
		}
		return result, err, false
	case chatgptwebauth.RefreshStrategyCodexSource:
		return e.refreshFromCodexSource(ctx, credential)
	case chatgptwebauth.RefreshStrategyTokenOnly:
		return cloneChatGPTWebCredential(credential), newChatGPTWebRefreshModeError("token_only_expired", "token-only credential requires a new access token"), true
	default:
		return cloneChatGPTWebCredential(credential), newChatGPTWebRefreshModeError("refresh_strategy_invalid", "chatgpt web refresh strategy is invalid"), true
	}
}

func (e *ChatGPTWebExecutor) sessionCookieRefreshEligible(credential *chatgptwebauth.Credential) bool {
	return e.sessionCookieRefreshOnTokenFailureEnabled() &&
		credential != nil &&
		credential.RefreshStrategy != chatgptwebauth.RefreshStrategyChatGPTSession &&
		credential.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource &&
		!chatGPTWebCredentialHasCompletePasswordLogin(credential) &&
		chatgptwebauth.HasSessionCookie(credential.Cookies)
}

func chatGPTWebCredentialHasCompletePasswordLogin(credential *chatgptwebauth.Credential) bool {
	return credential != nil &&
		strings.TrimSpace(credential.Email) != "" &&
		strings.TrimSpace(credential.Password) != "" &&
		strings.TrimSpace(credential.TOTPSecret) != ""
}

func classifyChatGPTWebSessionCookieRefresh(
	credential *chatgptwebauth.Credential,
	err error,
	allowAutoAPI798 bool,
) (*chatgptwebauth.Credential, error, bool) {
	if err == nil {
		return credential, nil, false
	}
	authError, ok := chatgptwebauth.AsAuthError(err)
	if !ok || (authError.Code != "session_expired" && authError.Code != "access_token_missing") {
		return credential, err, false
	}
	promoted := *authError
	targetState := chatgptwebauth.LifecycleDead
	if chatGPTWebCredentialCanRelogin(credential, allowAutoAPI798) {
		targetState = chatgptwebauth.LifecycleReauthRequired
	}
	promoted.State = targetState
	promoted.LifecycleState = targetState
	promoted.Retryable = false
	promoted.Terminal = true
	if credential != nil {
		credential = cloneChatGPTWebCredential(credential)
		credential.LifecycleState = targetState
		credential.LifecycleReason = chatgptwebauth.SafeLifecycleReason(promoted.Code)
	}
	return credential, &promoted, true
}

func (e *ChatGPTWebExecutor) refreshFromCodexSource(ctx context.Context, credential *chatgptwebauth.Credential) (*chatgptwebauth.Credential, error, bool) {
	result := cloneChatGPTWebCredential(credential)
	if e == nil || e.manager == nil {
		return result, newChatGPTWebRefreshModeError("source_auth_missing", "linked codex credential is unavailable"), true
	}
	sourceToken, errRefresh := e.manager.RefreshLinkedCodexSource(
		ctx,
		credential.SourceAuthID,
		credential.SourceCredentialUID,
		credential.AccessToken,
		credential.SourceIdentity,
	)
	if errRefresh != nil {
		var coded interface{ ChatGPTWebErrorCode() string }
		if errors.As(errRefresh, &coded) {
			return result, errRefresh, true
		}
		return result, errRefresh, false
	}
	result.AccessToken = sourceToken.AccessToken
	result.IDToken = ""
	result.Expired = sourceToken.Expired
	if email := strings.TrimSpace(sourceToken.Email); email != "" {
		result.Email = email
	}
	result.AccountID = strings.TrimSpace(sourceToken.AccountID)
	result.SourceIdentity = cliproxyauth.MergeChatGPTWebCredentialReferenceValues(result.SourceIdentity, sourceToken.Identity)
	result.LastRefreshAt = e.currentTime().UTC().Format(time.RFC3339)
	result.LifecycleState = chatgptwebauth.LifecycleActive
	result.LifecycleReason = ""
	result.LifecycleUpdatedAt = result.LastRefreshAt
	return result, nil, false
}

func chatGPTWebCredentialCanRelogin(credential *chatgptwebauth.Credential, allowAutoAPI798 bool) bool {
	_, errResolve := chatgptwebauth.ResolveLoginMethod(credential, allowAutoAPI798)
	return errResolve == nil
}

type chatGPTWebRefreshModeError struct {
	code    string
	message string
}

func newChatGPTWebRefreshModeError(code, message string) *chatGPTWebRefreshModeError {
	return &chatGPTWebRefreshModeError{code: strings.TrimSpace(code), message: strings.TrimSpace(message)}
}

func (e *chatGPTWebRefreshModeError) Error() string {
	if e == nil || e.message == "" {
		return "chatgpt web credential refresh failed"
	}
	return e.message
}

func (e *chatGPTWebRefreshModeError) ChatGPTWebErrorCode() string {
	if e == nil {
		return ""
	}
	return e.code
}

type chatGPTWebReloginResult struct {
	auth    *cliproxyauth.Auth
	current bool
	err     error
}

type chatGPTWebReloginFlight struct {
	key               string
	done              chan struct{}
	cancel            context.CancelFunc
	modeChanged       chan struct{}
	waiters           int
	manualWaiters     int
	mode              chatGPTWebReloginMode
	restartBackground bool
	completed         bool
	canceling         bool
	result            chatGPTWebReloginResult
}

type chatGPTWebReloginMode uint8

const (
	chatGPTWebReloginModePending chatGPTWebReloginMode = iota
	chatGPTWebReloginModeManual
	chatGPTWebReloginModeBackground
)

func cloneChatGPTWebCredential(credential *chatgptwebauth.Credential) *chatgptwebauth.Credential {
	if credential == nil {
		return nil
	}
	clone := *credential
	if credential.Cookies != nil {
		clone.Cookies = append(make([]chatgptwebauth.Cookie, 0, len(credential.Cookies)), credential.Cookies...)
	}
	if credential.WebAuthn != nil {
		webAuthn := *credential.WebAuthn
		webAuthn.Transports = append([]string(nil), credential.WebAuthn.Transports...)
		clone.WebAuthn = &webAuthn
	}
	clone.AdvancedAccountSecurity = chatgptwebauth.CloneAdvancedAccountSecurityCredential(credential.AdvancedAccountSecurity)
	return &clone
}

func cloneChatGPTWebAuth(auth *cliproxyauth.Auth) *cliproxyauth.Auth {
	clone := auth.Clone()
	if clone == nil || auth.Metadata == nil {
		return clone
	}
	credential, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil {
		return clone
	}
	clone.Metadata = make(map[string]any, len(auth.Metadata))
	for key, value := range auth.Metadata {
		clone.Metadata[key] = value
	}
	cloneChatGPTWebCredential(credential).ApplyToMetadata(clone.Metadata)
	return clone
}

func applyChatGPTWebCredential(auth *cliproxyauth.Auth, credential *chatgptwebauth.Credential) *cliproxyauth.Auth {
	updated := auth.Clone()
	if updated == nil {
		return nil
	}
	if updated.Metadata == nil {
		updated.Metadata = make(map[string]any)
	}
	cloneChatGPTWebCredential(credential).ApplyToMetadata(updated.Metadata)
	return updated
}

func setChatGPTWebLifecycle(auth *cliproxyauth.Auth, state, reason string, now time.Time) {
	if auth == nil {
		return
	}
	if auth.Metadata == nil {
		auth.Metadata = make(map[string]any)
	}
	auth.Metadata["lifecycle_state"] = state
	auth.Metadata["lifecycle_reason"] = chatgptwebauth.SafeLifecycleReason(reason)
	auth.Metadata["lifecycle_updated_at"] = now.UTC().Format(time.RFC3339)
}

func chatGPTWebCredentialExpiry(credential *chatgptwebauth.Credential) (time.Time, bool) {
	if credential == nil {
		return time.Time{}, false
	}
	if value := strings.TrimSpace(credential.Expired); value != "" {
		if parsed, err := time.Parse(time.RFC3339, value); err == nil {
			return parsed, true
		}
		return time.Time{}, true
	}
	if expiresAt, ok := chatgptwebauth.JWTExpiry(credential.AccessToken); ok {
		return expiresAt, true
	}
	return chatgptwebauth.JWTExpiry(credential.IDToken)
}

func chatGPTWebRefreshKey(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return ""
	}
	material := ""
	if credential, err := chatgptwebauth.ParseCredential(auth.Metadata); err == nil {
		material = string(credential.RefreshStrategy) + "\x00"
		switch credential.RefreshStrategy {
		case chatgptwebauth.RefreshStrategyWebOAuthRT:
			material += credential.RefreshToken
		case chatgptwebauth.RefreshStrategyChatGPTSession:
			cookies, _ := json.Marshal(credential.Cookies)
			material += string(cookies)
		case chatgptwebauth.RefreshStrategyCodexSource:
			material += credential.SourceAuthID + "\x00" + credential.SourceCredentialUID
		default:
			material += credential.AccessToken
		}
	}
	digest := sha256.Sum256([]byte(material))
	return auth.ID + ":" + auth.RuntimeInstanceID() + ":" + fmt.Sprintf("%x", digest[:8])
}

func chatGPTWebReloginGenerationKey(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return ""
	}
	credentialUID := ""
	if auth.Metadata != nil {
		credentialUID, _ = auth.Metadata["credential_uid"].(string)
		credentialUID = strings.TrimSpace(credentialUID)
	}
	if credentialUID == "" {
		if credential, err := chatgptwebauth.ParseCredential(auth.Metadata); err == nil {
			credentialUID = strings.ToLower(strings.TrimSpace(credential.Email))
		}
	}
	if credentialUID == "" {
		credentialUID = cliproxyauth.ChatGPTWebCredentialIdentity(auth)
	}
	digest := sha256.Sum256([]byte(credentialUID))
	return strings.Join([]string{
		auth.ID,
		auth.RuntimeInstanceID(),
		auth.RuntimeInstallationID(),
		fmt.Sprintf("%x", digest[:8]),
	}, ":")
}

func (e *ChatGPTWebExecutor) currentTime() time.Time {
	if e != nil && e.now != nil {
		return e.now()
	}
	return time.Now()
}

func (e *ChatGPTWebExecutor) chatGPTWebTimezone() config.ResolvedChatGPTWebTimezone {
	cfg := config.ChatGPTWebConfig{}
	if snapshot := e.configSnapshot(); snapshot != nil {
		cfg = snapshot.ChatGPTWeb
	}
	return cfg.ResolvedTimezone(e.currentTime())
}

func (e *ChatGPTWebExecutor) lifecycleContext() context.Context {
	if e == nil {
		return context.Background()
	}
	e.backgroundMu.Lock()
	defer e.backgroundMu.Unlock()
	if e.lifecycleCtx == nil {
		e.lifecycleCtx, e.lifecycleCancel = context.WithCancel(context.Background())
		if e.closed {
			e.lifecycleCancel()
		}
	}
	return e.lifecycleCtx
}

func (e *ChatGPTWebExecutor) acquisitionContext() (context.Context, context.CancelFunc) {
	return e.acquisitionContextWithTimeout(chatgptwebauth.DefaultAcquisitionTimeout)
}

func (e *ChatGPTWebExecutor) acquisitionContextWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		timeout = chatgptwebauth.DefaultAcquisitionTimeout
	}
	return context.WithTimeout(e.lifecycleContext(), timeout)
}

func (e *ChatGPTWebExecutor) reloginAcquisitionTimeout(expected *cliproxyauth.Auth, loginProxy chatgptwebauth.LoginProxyConfig, allowAutoAPI798 bool) time.Duration {
	timeout := chatgptwebauth.DefaultAcquisitionTimeout
	if e.authService == nil || expected == nil {
		return timeout
	}
	credential, errCredential := chatgptwebauth.ParseCredential(expected.Metadata)
	if errCredential != nil {
		return timeout
	}
	input := chatgptwebauth.LoginInput{
		Credential:         credential,
		LoginProxy:         loginProxy,
		LoginProxyResolved: true,
		Relogin:            true,
		AllowAutoAPI798:    allowAutoAPI798,
	}
	if resolved := e.authService.LoginAcquisitionTimeout(input); resolved > 0 {
		return resolved
	}
	return timeout
}

func (e *ChatGPTWebExecutor) acquisitionContextWithValues(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	acquisitionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chatgptwebauth.DefaultAcquisitionTimeout)
	stopLifecycleCancel := context.AfterFunc(e.lifecycleContext(), cancel)
	return acquisitionCtx, func() {
		stopLifecycleCancel()
		cancel()
	}
}

func (e *ChatGPTWebExecutor) proxyURLForTarget(auth *cliproxyauth.Auth, targetURL string) string {
	return chatGPTWebProxyURLForTarget(auth, targetURL, e.configSnapshot())
}

func chatGPTWebProxyURLForTarget(auth *cliproxyauth.Auth, targetURL string, cfg *config.Config) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.EffectiveProxyURL()); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg != nil {
		if proxyURL := strings.TrimSpace(cfg.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	targetURL = strings.TrimSpace(targetURL)
	if targetURL == "" {
		targetURL = chatgptwebauth.AuthBaseURL
	}
	target, errParse := url.Parse(targetURL)
	if errParse != nil {
		return ""
	}
	proxyURL, errProxy := httpproxy.FromEnvironment().ProxyFunc()(target)
	if errProxy != nil || proxyURL == nil {
		return ""
	}
	return proxyURL.String()
}

func chatGPTWebErrorCode(err error) string {
	if authError, ok := chatgptwebauth.AsAuthError(err); ok && strings.TrimSpace(authError.Code) != "" {
		return chatgptwebauth.SafeLifecycleReason(authError.Code)
	}
	var coded interface{ ChatGPTWebErrorCode() string }
	if errors.As(err, &coded) {
		return chatgptwebauth.SafeLifecycleReason(coded.ChatGPTWebErrorCode())
	}
	return "authentication_failed"
}

func firstNonNilError(values ...error) error {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

type chatGPTWebCredentialUnavailableError struct {
	cause         error
	persistUpdate bool
}

func newChatGPTWebCredentialUnavailableError(cause error, persistUpdate bool) *chatGPTWebCredentialUnavailableError {
	return &chatGPTWebCredentialUnavailableError{cause: cause, persistUpdate: persistUpdate}
}

func (e *chatGPTWebCredentialUnavailableError) Error() string {
	return "chatgpt web credential is unavailable: " + chatGPTWebErrorCode(e.cause)
}

func (e *chatGPTWebCredentialUnavailableError) Unwrap() error      { return e.cause }
func (*chatGPTWebCredentialUnavailableError) StatusCode() int      { return http.StatusServiceUnavailable }
func (*chatGPTWebCredentialUnavailableError) SkipAuthResult() bool { return true }
func (*chatGPTWebCredentialUnavailableError) RetryOtherAuth() bool { return true }
func (*chatGPTWebCredentialUnavailableError) ChatGPTWebCredentialUnavailable() bool {
	return true
}
func (e *chatGPTWebCredentialUnavailableError) PersistAuthUpdateOnError() bool {
	return e != nil && e.persistUpdate
}

type chatGPTWebProtocolUnavailableError struct{}

func newChatGPTWebProtocolUnavailableError() chatGPTWebProtocolUnavailableError {
	return chatGPTWebProtocolUnavailableError{}
}

func (chatGPTWebProtocolUnavailableError) Error() string {
	return "chatgpt web request protocol is not available"
}
func (chatGPTWebProtocolUnavailableError) StatusCode() int      { return http.StatusNotImplemented }
func (chatGPTWebProtocolUnavailableError) SkipAuthResult() bool { return true }
func (chatGPTWebProtocolUnavailableError) RetryOtherAuth() bool { return true }
