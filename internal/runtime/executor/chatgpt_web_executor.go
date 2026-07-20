package executor

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/http/httpproxy"
	"golang.org/x/sync/singleflight"
)

type chatGPTWebAuthService interface {
	Login(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
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
	chatGPTWebBackgroundReloginConcurrency       = 4
	chatGPTWebBackgroundReloginLogInterval       = 3
	chatGPTWebBackgroundReloginStatePollInterval = 20 * time.Millisecond
	chatGPTWebBackgroundReloginMaxBackoff        = 5 * time.Minute
)

var chatGPTWebBackgroundReloginSlots = make(chan struct{}, chatGPTWebBackgroundReloginConcurrency)

var errChatGPTWebReloginOwnershipChanged = errors.New("chatgpt web re-login ownership changed")

// ChatGPTWebExecutor manages ChatGPT Web credential refresh and re-login.
// Request protocol support is added separately from the credential lifecycle.
type ChatGPTWebExecutor struct {
	cfg                 atomic.Pointer[config.Config]
	manager             *cliproxyauth.Manager
	authService         chatGPTWebAuthService
	runtimeBaseURL      string
	runtimeRand         io.Reader
	imageInitialWait    time.Duration
	imagePollInterval   time.Duration
	imageSettleWait     time.Duration
	imageMaxPolls       int
	searchPollInterval  time.Duration
	searchMaxPolls      int
	streamInitialWait   time.Duration
	streamHeartbeat     time.Duration
	now                 func() time.Time
	reloginBackoff      func(int) time.Duration
	reloginSlotAcquired func()
	reloginSlots        chan struct{}
	refreshGroup        singleflight.Group
	refreshWG           sync.WaitGroup
	reloginMu           sync.Mutex
	reloginFlights      map[string]*chatGPTWebReloginFlight
	reloginWG           sync.WaitGroup
	loginCoordinator    *ChatGPTWebLoginCoordinator
	loginWG             sync.WaitGroup
	backgroundMu        sync.Mutex
	backgroundWG        sync.WaitGroup
	backgroundRunning   map[string]struct{}
	lifecycleCtx        context.Context
	lifecycleCancel     context.CancelFunc
	closed              bool
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
		now:                time.Now,
		reloginBackoff:     chatGPTWebBackgroundReloginBackoff,
		reloginSlots:       chatGPTWebBackgroundReloginSlots,
		reloginFlights:     make(map[string]*chatGPTWebReloginFlight),
		loginCoordinator:   coordinator,
		backgroundRunning:  make(map[string]struct{}),
		lifecycleCtx:       lifecycleCtx,
		lifecycleCancel:    lifecycleCancel,
	}
	executor.UpdateConfig(cfg)
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
	e.refreshWG.Wait()
	e.backgroundWG.Wait()
	e.reloginWG.Wait()
	e.loginWG.Wait()
	return nil
}

// Identifier returns the provider identifier.
func (e *ChatGPTWebExecutor) Identifier() string { return chatgptwebauth.Provider }

// UpdateConfig replaces the immutable runtime configuration snapshot without
// interrupting in-flight login or request operations.
func (e *ChatGPTWebExecutor) UpdateConfig(cfg *config.Config) {
	if e == nil {
		return
	}
	if cfg == nil {
		e.cfg.Store(nil)
		return
	}
	snapshot, errClone := cloneChatGPTWebExecutorConfig(cfg)
	if errClone != nil {
		log.WithError(errClone).Error("chatgpt web executor: retain previous configuration after snapshot failure")
		return
	}
	e.cfg.Store(snapshot)
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
	return e.executeRuntime(ctx, auth, req, opts)
}

// ExecuteStream runs a streaming ChatGPT Web request.
func (e *ChatGPTWebExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return e.executeRuntimeStream(ctx, auth, req, opts)
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
	updated, refreshErr, terminal := e.refreshCredential(ctx, auth)
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
	updated, refreshErr, terminal := e.refreshCredential(ctx, auth)
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
	return e.authService.Login(ctx, input)
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
		result, err = e.authService.RefreshSession(ctx, *result, proxyURL)
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

// TriggerBackgroundRelogin starts a bounded re-login task for the current auth
// generation. Duplicate triggers share one background retry loop.
func (e *ChatGPTWebExecutor) TriggerBackgroundRelogin(expected *cliproxyauth.Auth) {
	if e == nil || e.manager == nil || !e.AutoReloginEnabled() || expected == nil || expected.LifecycleState() != cliproxyauth.LifecycleStateReloginPending {
		return
	}
	credential, errCredential := chatgptwebauth.ParseCredential(expected.Metadata)
	if errCredential != nil || !chatGPTWebCredentialCanRelogin(credential) {
		return
	}
	expected = cloneChatGPTWebAuth(expected)
	key := chatGPTWebReloginGenerationKey(expected)
	if !e.beginBackgroundRelogin(key) {
		return
	}
	go func() {
		defer e.finishBackgroundRelogin(key)
		e.runBackgroundRelogin(expected)
	}()
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
		acquisitionCtx, cancel := e.acquisitionContext()
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
			e.runReloginFlight(acquisitionCtx, expected, flight)
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
	ctx := e.lifecycleContext()
	ctx, release, active := expected.BeginRuntimeExecution(ctx)
	if !active {
		return
	}
	defer release()
	if !e.backgroundReloginPending(expected) {
		return
	}

	for attempt := 1; ; attempt++ {
		if !e.backgroundReloginPending(expected) {
			return
		}
		_, _, errRelogin := e.reloginCurrentWithMode(ctx, expected, true)
		if errRelogin == nil {
			return
		}
		if !e.backgroundReloginPending(expected) {
			return
		}
		if !chatGPTWebBackgroundReloginRetryable(errRelogin) {
			logChatGPTWebBackgroundReloginFailure(expected.ID, errRelogin)
			return
		}
		if attempt%chatGPTWebBackgroundReloginLogInterval == 0 {
			logChatGPTWebBackgroundReloginFailure(expected.ID, errRelogin)
		}
		if !e.waitForBackgroundReloginRetry(ctx, expected, e.backgroundReloginDelay(attempt)) {
			return
		}
	}
}

func (e *ChatGPTWebExecutor) beginBackgroundRelogin(key string) bool {
	if e == nil || strings.TrimSpace(key) == "" {
		return false
	}
	e.backgroundMu.Lock()
	defer e.backgroundMu.Unlock()
	if e.closed {
		return false
	}
	if e.backgroundRunning == nil {
		e.backgroundRunning = make(map[string]struct{})
	}
	if _, exists := e.backgroundRunning[key]; exists {
		return false
	}
	e.backgroundRunning[key] = struct{}{}
	e.backgroundWG.Add(1)
	return true
}

func (e *ChatGPTWebExecutor) finishBackgroundRelogin(key string) {
	e.backgroundMu.Lock()
	delete(e.backgroundRunning, key)
	e.backgroundMu.Unlock()
	e.backgroundWG.Done()
}

func (e *ChatGPTWebExecutor) acquireReloginExecution(ctx context.Context, expected *cliproxyauth.Auth, flight *chatGPTWebReloginFlight) (background bool, release func(), ready bool) {
	slots := e.reloginSlots
	if slots == nil {
		slots = chatGPTWebBackgroundReloginSlots
	}
	ticker := time.NewTicker(chatGPTWebBackgroundReloginStatePollInterval)
	defer ticker.Stop()
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
		case <-ticker.C:
		}
	}
}

func (e *ChatGPTWebExecutor) waitForBackgroundReloginRetry(ctx context.Context, expected *cliproxyauth.Auth, delay time.Duration) bool {
	if delay <= 0 {
		return e.backgroundReloginPending(expected)
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	interval := chatGPTWebBackgroundReloginStatePollInterval
	if delay < interval {
		interval = delay
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return e.backgroundReloginPending(expected)
		case <-ticker.C:
			if !e.backgroundReloginPending(expected) {
				return false
			}
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

func (e *ChatGPTWebExecutor) backgroundReloginDelay(attempt int) time.Duration {
	if e != nil && e.reloginBackoff != nil {
		return e.reloginBackoff(attempt)
	}
	return chatGPTWebBackgroundReloginBackoff(attempt)
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

func chatGPTWebBackgroundReloginRetryable(err error) bool {
	if errors.Is(err, errChatGPTWebReloginOwnershipChanged) {
		return true
	}
	if chatgptwebauth.IsRetryable(err) {
		return true
	}
	var unavailable *proxypool.UnavailableError
	return errors.As(err, &unavailable)
}

func logChatGPTWebBackgroundReloginFailure(authID string, err error) {
	code := chatGPTWebErrorCode(err)
	log.WithFields(log.Fields{"auth_id": authID, "error_code": code}).Warn("chatgpt web background re-login failed")
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

	releaseProxyBinding := e.manager.HoldProxyBinding(expected.ID)
	defer releaseProxyBinding()
	resolved, errResolve := e.manager.ResolveProxyAuth(ctx, expected)
	if errResolve != nil {
		return nil, false, errResolve
	}
	credential, errCredential := chatgptwebauth.ParseCredential(resolved.Metadata)
	if errCredential != nil {
		return nil, false, fmt.Errorf("parse chatgpt web credential: %w", errCredential)
	}
	result, errLogin := e.authService.Login(ctx, chatgptwebauth.LoginInput{
		Credential: credential,
		ProxyURL:   e.proxyURLForTarget(resolved, chatgptwebauth.AuthBaseURL),
		Relogin:    true,
	})
	if errContext := ctx.Err(); errContext != nil {
		if latest, ok := e.manager.GetByID(expected.ID); ok &&
			chatGPTWebReloginGenerationKey(latest) != chatGPTWebReloginGenerationKey(expected) {
			return cloneChatGPTWebAuth(latest), false, chatgptwebauth.ErrCredentialSuperseded
		}
		return nil, false, errContext
	}
	if errLogin != nil && chatgptwebauth.IsRetryable(errLogin) {
		return nil, false, e.manager.ReportProxyFailure(ctx, resolved, errLogin)
	}
	if result == nil {
		return nil, false, firstNonNilError(errLogin, errors.New("chatgpt web re-login returned no credential"))
	}
	updated := applyChatGPTWebCredential(expected, result)
	installed, current, errUpdate := e.manager.UpdateIfCurrent(
		cliproxyauth.WithForceRuntimeReplacement(ctx),
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

func (e *ChatGPTWebExecutor) refreshCredential(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error, bool) {
	if e == nil || e.authService == nil || auth == nil {
		return nil, errors.New("chatgpt web refresh is unavailable"), false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	credential, errCredential := chatgptwebauth.ParseCredential(auth.Metadata)
	if errCredential != nil {
		updated := auth.Clone()
		setChatGPTWebLifecycle(updated, cliproxyauth.LifecycleStateReauthRequired, "credential_invalid", e.currentTime())
		return updated, fmt.Errorf("parse chatgpt web credential: %w", errCredential), true
	}
	if errIdentity := chatgptwebauth.EnsureCredentialRuntimeIDsForURL(credential, chatgptwebauth.CredentialRuntimeIdentityReader(auth.ID, credential), e.chatGPTWebBaseURL()); errIdentity != nil {
		return nil, fmt.Errorf("initialize chatgpt web browser identity: %w", errIdentity), false
	}
	key := chatGPTWebRefreshKey(auth)
	if !e.beginRefreshWait() {
		return nil, context.Canceled, false
	}
	resultChannel := e.refreshGroup.DoChan(key, func() (any, error) {
		acquisitionCtx, cancel := e.acquisitionContext()
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err(), false
	case result, ok := <-trackedResult:
		if !ok {
			return nil, errors.New("chatgpt web refresh ended without a result"), false
		}
		flightResult = result
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
	if strategy == chatgptwebauth.RefreshStrategyTokenOnly || strategy == chatgptwebauth.RefreshStrategyCodexSource {
		state = cliproxyauth.LifecycleStateReauthRequired
	} else if state != cliproxyauth.LifecycleStateDead && state != cliproxyauth.LifecycleStateInteractionRequired {
		if e.AutoReloginEnabled() && chatGPTWebCredentialCanRelogin(result.credential) {
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
	switch credential.RefreshStrategy {
	case chatgptwebauth.RefreshStrategyWebOAuthRT:
		result, err := e.authService.Refresh(ctx, *credential, e.proxyURLForTarget(auth, chatgptwebauth.AuthBaseURL))
		return result, err, false
	case chatgptwebauth.RefreshStrategyChatGPTSession:
		result, err := e.authService.RefreshSession(ctx, *credential, e.proxyURLForTarget(auth, chatgptwebauth.SessionBaseURL))
		return result, err, false
	case chatgptwebauth.RefreshStrategyCodexSource:
		return e.refreshFromCodexSource(ctx, credential)
	case chatgptwebauth.RefreshStrategyTokenOnly:
		return cloneChatGPTWebCredential(credential), newChatGPTWebRefreshModeError("token_only_expired", "token-only credential requires a new access token"), true
	default:
		return cloneChatGPTWebCredential(credential), newChatGPTWebRefreshModeError("refresh_strategy_invalid", "chatgpt web refresh strategy is invalid"), true
	}
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

func chatGPTWebCredentialCanRelogin(credential *chatgptwebauth.Credential) bool {
	return credential != nil && strings.TrimSpace(credential.Email) != "" && strings.TrimSpace(credential.Password) != ""
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
	sourceGeneration := ""
	if auth.Attributes != nil {
		sourceGeneration = strings.TrimSpace(auth.Attributes[cliproxyauth.SourceHashAttributeKey])
	}
	return chatGPTWebRefreshKey(auth) + ":" + auth.RuntimeInstallationID() + ":" + sourceGeneration
}

func (e *ChatGPTWebExecutor) currentTime() time.Time {
	if e != nil && e.now != nil {
		return e.now()
	}
	return time.Now()
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
	return context.WithTimeout(e.lifecycleContext(), chatgptwebauth.DefaultAcquisitionTimeout)
}

func (e *ChatGPTWebExecutor) proxyURLForTarget(auth *cliproxyauth.Auth, targetURL string) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.EffectiveProxyURL()); proxyURL != "" {
			return proxyURL
		}
	}
	if cfg := e.configSnapshot(); cfg != nil {
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
