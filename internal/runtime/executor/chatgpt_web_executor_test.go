package executor

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/proxypool"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type fakeChatGPTWebAuthService struct {
	loginFn      func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	refreshFn    func(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	loginCalls   atomic.Int32
	refreshCalls atomic.Int32
}

type chatGPTWebLeaseResolver struct {
	active atomic.Int32
}

func (resolver *chatGPTWebLeaseResolver) Resolve(context.Context, *cliproxyauth.Auth) (cliproxyauth.ResolvedProxy, error) {
	if resolver.active.Load() == 0 {
		return cliproxyauth.ResolvedProxy{}, errors.New("proxy binding lease is not active")
	}
	return cliproxyauth.ResolvedProxy{URL: "http://proxy.example:8080", BindingID: "binding-a"}, nil
}

func (*chatGPTWebLeaseResolver) ReportFailure(_ context.Context, _ *cliproxyauth.Auth, err error) error {
	return err
}

func (resolver *chatGPTWebLeaseResolver) HoldBinding(string) func() {
	resolver.active.Add(1)
	var once sync.Once
	return func() {
		once.Do(func() { resolver.active.Add(-1) })
	}
}

func (service *fakeChatGPTWebAuthService) Login(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
	service.loginCalls.Add(1)
	if service.loginFn == nil {
		return nil, errors.New("unexpected login")
	}
	return service.loginFn(ctx, input)
}

func (service *fakeChatGPTWebAuthService) Refresh(ctx context.Context, credential chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
	service.refreshCalls.Add(1)
	if service.refreshFn == nil {
		return nil, errors.New("unexpected refresh")
	}
	return service.refreshFn(ctx, credential, proxyURL)
}

func TestChatGPTWebExecutorShouldPrepareExpiringCredential(t *testing.T) {
	now := time.Date(2026, time.July, 16, 12, 0, 0, 0, time.UTC)
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.now = func() time.Time { return now }

	auth := chatGPTWebTestAuth("prepare")
	auth.Metadata["expired"] = now.Add(chatgptwebauth.DefaultRefreshLead).Format(time.RFC3339)
	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("credential at refresh lead boundary should be prepared")
	}
	auth.Metadata["expired"] = now.Add(chatgptwebauth.DefaultRefreshLead + time.Second).Format(time.RFC3339)
	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("credential outside refresh lead should not be prepared")
	}
	auth.Metadata["expired"] = "invalid"
	if !executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("malformed explicit expiry should fail closed")
	}
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateDead
	if executor.ShouldPrepareRequestAuth(auth) {
		t.Fatal("dead credential should not be refreshed")
	}
}

func TestChatGPTWebExecutorRefreshUsesStableLegacyRuntimeIdentity(t *testing.T) {
	auth := chatGPTWebTestAuth("legacy-identity")
	delete(auth.Metadata, "device_id")
	delete(auth.Metadata, "session_id")
	auth.Metadata["email"] = "person@example.com"
	expected, err := chatgptwebauth.ParseCredential(auth.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	if err = chatgptwebauth.EnsureCredentialRuntimeIDs(expected, chatgptwebauth.CredentialRuntimeIdentityReader(auth.ID, expected)); err != nil {
		t.Fatal(err)
	}

	var received chatgptwebauth.Credential
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		received = credential
		credential.AccessToken = "refreshed-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	updated, err := executor.Refresh(t.Context(), auth)
	if err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}
	if received.DeviceID != expected.DeviceID || received.SessionID != expected.SessionID {
		t.Fatalf("refresh identity = %q/%q, want %q/%q", received.DeviceID, received.SessionID, expected.DeviceID, expected.SessionID)
	}
	if updated.Metadata["device_id"] != expected.DeviceID || updated.Metadata["session_id"] != expected.SessionID {
		t.Fatalf("persisted identity = %v/%v", updated.Metadata["device_id"], updated.Metadata["session_id"])
	}
}

func TestChatGPTWebExecutorTerminalRefreshLifecycle(t *testing.T) {
	for _, test := range []struct {
		name        string
		autoRelogin bool
		failure     chatgptwebauth.LifecycleState
		want        string
	}{
		{name: "reauth", failure: chatgptwebauth.LifecycleReauthRequired, want: cliproxyauth.LifecycleStateReauthRequired},
		{name: "auto relogin", autoRelogin: true, failure: chatgptwebauth.LifecycleReauthRequired, want: cliproxyauth.LifecycleStateReloginPending},
		{name: "dead never relogins", autoRelogin: true, failure: chatgptwebauth.LifecycleDead, want: cliproxyauth.LifecycleStateDead},
		{name: "interaction never relogins", autoRelogin: true, failure: chatgptwebauth.LifecycleInteractionRequired, want: cliproxyauth.LifecycleStateInteractionRequired},
	} {
		t.Run(test.name, func(t *testing.T) {
			fake := &fakeChatGPTWebAuthService{}
			fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
				credential.LifecycleState = test.failure
				return &credential, &chatgptwebauth.AuthError{Code: "terminal_failure", State: test.failure, LifecycleState: test.failure, Terminal: true}
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: test.autoRelogin}}, nil)
			executor.authService = fake
			updated, errRefresh := executor.Refresh(t.Context(), chatGPTWebTestAuth(test.name))
			if errRefresh == nil || updated == nil {
				t.Fatalf("Refresh() = (%v, %v), want persisted terminal update", updated, errRefresh)
			}
			if got := updated.LifecycleState(); got != test.want {
				t.Fatalf("lifecycle = %q, want %q", got, test.want)
			}
			persist, ok := errRefresh.(interface{ PersistAuthUpdateOnError() bool })
			if !ok || !persist.PersistAuthUpdateOnError() {
				t.Fatal("terminal refresh error must request lifecycle persistence")
			}
			if skipper, ok := errRefresh.(interface{ SkipAuthResult() bool }); !ok || !skipper.SkipAuthResult() {
				t.Fatal("terminal refresh must not record a credential failure")
			}
		})
	}
}

func TestChatGPTWebExecutorTransientRefreshDoesNotPersist(t *testing.T) {
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		return &credential, &chatgptwebauth.AuthError{Code: "network_error", State: chatgptwebauth.LifecycleActive, Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	updated, errRefresh := executor.Refresh(t.Context(), chatGPTWebTestAuth("transient"))
	if updated != nil || errRefresh == nil {
		t.Fatalf("Refresh() = (%v, %v), want transient error without update", updated, errRefresh)
	}
	if persist, ok := errRefresh.(interface{ PersistAuthUpdateOnError() bool }); ok && persist.PersistAuthUpdateOnError() {
		t.Fatal("transient refresh error must not persist a lifecycle transition")
	}
}

func TestChatGPTWebExecutorRefreshSingleflight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		if fake.refreshCalls.Load() == 1 {
			close(started)
		}
		<-release
		credential.AccessToken = "refreshed-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("singleflight")

	var group sync.WaitGroup
	errs := make(chan error, 2)
	group.Add(1)
	go func() {
		defer group.Done()
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		errs <- errPrepare
	}()
	<-started
	secondEntered := make(chan struct{})
	group.Add(1)
	go func() {
		defer group.Done()
		close(secondEntered)
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		errs <- errPrepare
	}()
	<-secondEntered
	// Keep the first refresh in flight long enough for the second caller to
	// join the same singleflight operation.
	time.Sleep(20 * time.Millisecond)
	close(release)
	group.Wait()
	close(errs)
	for errPrepare := range errs {
		if errPrepare != nil {
			t.Fatalf("PrepareRequestAuth() error: %v", errPrepare)
		}
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorRefreshSingleflightCallerCancellation(t *testing.T) {
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(ctx context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		started <- ctx
		select {
		case <-release:
			credential.AccessToken = "refreshed-token"
			return &credential, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("cancel")

	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() {
		_, errPrepare := executor.PrepareRequestAuth(firstCtx, auth)
		firstDone <- errPrepare
	}()
	acquisitionCtx := <-started

	secondDone := make(chan error, 1)
	secondEntered := make(chan struct{})
	go func() {
		close(secondEntered)
		_, errPrepare := executor.PrepareRequestAuth(t.Context(), auth)
		secondDone <- errPrepare
	}()
	<-secondEntered
	time.Sleep(20 * time.Millisecond)
	cancelFirst()
	if errPrepare := <-firstDone; !errors.Is(errPrepare, context.Canceled) {
		t.Fatalf("first PrepareRequestAuth() error = %v, want context canceled", errPrepare)
	}
	select {
	case <-acquisitionCtx.Done():
		t.Fatal("canceling one waiter canceled the shared refresh acquisition")
	default:
	}
	close(release)
	if errPrepare := <-secondDone; errPrepare != nil {
		t.Fatalf("second PrepareRequestAuth() error: %v", errPrepare)
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorCloseWaitsForRefreshAndDiscardsLateResult(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, credential chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential.AccessToken = "late-refreshed-token"
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	type refreshOutput struct {
		auth *cliproxyauth.Auth
		err  error
	}
	refreshDone := make(chan refreshOutput, 1)
	go func() {
		updated, errRefresh := executor.Refresh(context.Background(), chatGPTWebTestAuth("shutdown-refresh"))
		refreshDone <- refreshOutput{auth: updated, err: errRefresh}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- executor.Close() }()
	select {
	case errClose := <-closeDone:
		t.Fatalf("Close() returned before refresh exited: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("Close() error = %v", errClose)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not finish after refresh exited")
	}
	select {
	case output := <-refreshDone:
		if output.auth != nil || !errors.Is(output.err, context.Canceled) {
			t.Fatalf("late refresh result = (%v, %v), want canceled without update", output.auth, output.err)
		}
	case <-time.After(time.Second):
		t.Fatal("refresh caller did not finish")
	}
}

func TestChatGPTWebExecutorRefreshSingleflightStopsWhenAuthRetires(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("refresh-retirement")
	if _, err := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); err != nil {
		t.Fatal(err)
	}
	installed, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth not found")
	}

	started := make(chan struct{})
	canceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(ctx context.Context, _ chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake

	refreshDone := make(chan error, 1)
	go func() {
		_, err := executor.PrepareRequestAuth(t.Context(), installed)
		refreshDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("refresh did not start")
	}

	replacement := installed.Clone()
	replacement.Metadata["access_token"] = "replacement-token"
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "replacement-source"
	if _, err := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("retiring the auth did not cancel the shared refresh acquisition")
	}
	select {
	case err := <-refreshDone:
		if err == nil {
			t.Fatal("retired auth refresh returned no error")
		}
	case <-time.After(time.Second):
		t.Fatal("retired auth refresh did not finish")
	}
}

func TestChatGPTWebExecutorRefreshSingleflightClonesCredential(t *testing.T) {
	const waiters = 8
	started := make(chan struct{})
	release := make(chan struct{})
	sharedCredential := &chatgptwebauth.Credential{
		Type:           chatgptwebauth.Provider,
		AccessToken:    "shared-token",
		RefreshToken:   "refresh-token",
		Cookies:        []chatgptwebauth.Cookie{{Name: "session", Value: "shared-cookie"}},
		Persona:        chatgptwebauth.DefaultPersona(),
		LifecycleState: chatgptwebauth.LifecycleReauthRequired,
	}
	fake := &fakeChatGPTWebAuthService{}
	fake.refreshFn = func(_ context.Context, _ chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		return sharedCredential, &chatgptwebauth.AuthError{
			Code:           "terminal_failure",
			State:          chatgptwebauth.LifecycleReauthRequired,
			LifecycleState: chatgptwebauth.LifecycleReauthRequired,
			Terminal:       true,
		}
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	executor.authService = fake
	auth := chatGPTWebTestAuth("clone")

	type refreshOutput struct {
		auth *cliproxyauth.Auth
		err  error
	}
	outputs := make(chan refreshOutput, waiters)
	go func() {
		updated, errRefresh := executor.Refresh(t.Context(), auth)
		outputs <- refreshOutput{auth: updated, err: errRefresh}
	}()
	<-started
	for index := 1; index < waiters; index++ {
		go func() {
			updated, errRefresh := executor.Refresh(t.Context(), auth)
			outputs <- refreshOutput{auth: updated, err: errRefresh}
		}()
	}
	time.Sleep(20 * time.Millisecond)
	close(release)

	results := make([]*cliproxyauth.Auth, 0, waiters)
	for index := 0; index < waiters; index++ {
		output := <-outputs
		if output.auth == nil || output.err == nil {
			t.Fatalf("Refresh() = (%v, %v), want terminal update", output.auth, output.err)
		}
		results = append(results, output.auth)
	}
	if got := fake.refreshCalls.Load(); got != 1 {
		t.Fatalf("refresh calls = %d, want 1", got)
	}
	firstCookies := results[0].Metadata["cookies"].([]chatgptwebauth.Cookie)
	firstCookies[0].Value = "caller-mutation"
	for index, result := range results[1:] {
		cookies := result.Metadata["cookies"].([]chatgptwebauth.Cookie)
		if cookies[0].Value != "shared-cookie" {
			t.Fatalf("caller %d cookie = %q, want independent clone", index+1, cookies[0].Value)
		}
	}
	if sharedCredential.Cookies[0].Value != "shared-cookie" {
		t.Fatal("caller mutation changed the singleflight credential")
	}
}

func TestChatGPTWebExecutorManualAndBackgroundReloginSingleflight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "shared-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "relogin-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.TriggerBackgroundRelogin(expected)
	<-started

	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		manualDone <- errRelogin
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if errRelogin := <-manualDone; errRelogin != nil {
		t.Fatalf("ReloginCurrent() error: %v", errRelogin)
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
}

func TestChatGPTWebExecutorReloginHoldsProxyBindingLease(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "proxy-lease")
	resolver := &chatGPTWebLeaseResolver{}
	manager.SetProxyResolver(resolver)
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if resolver.active.Load() == 0 {
			return nil, errors.New("proxy binding lease was released during login")
		}
		credential := *input.Credential
		credential.AccessToken = "relogin-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
	if active := resolver.active.Load(); active != 0 {
		t.Fatalf("proxy binding lease count after re-login = %d, want 0", active)
	}
}

func TestChatGPTWebExecutorReloginCancellationStopsAcquisition(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "cancel-relogin")
	started := make(chan struct{})
	acquisitionCanceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(acquisitionCanceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() did not stop after cancellation")
	}
	select {
	case <-acquisitionCanceled:
	case <-time.After(time.Second):
		t.Fatal("re-login acquisition did not observe cancellation")
	}
}

func TestChatGPTWebExecutorReloginCancellationDoesNotWaitForStuckAcquisition(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "stuck-cancel-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		return nil, context.Canceled
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() waited for an acquisition that ignored cancellation")
	}

	close(release)
	if errClose := executor.Close(); errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
}

func TestChatGPTWebExecutorCanceledReloginDoesNotCommitReturnedCredential(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "cancel-returned-credential")
	started := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		credential := *input.Credential
		credential.AccessToken = "late-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		result <- errRelogin
	}()
	<-started
	cancel()
	select {
	case errRelogin := <-result:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("ReloginCurrent() did not stop after cancellation")
	}
	current, ok := manager.GetByID(expected.ID)
	if !ok {
		t.Fatal("current auth is missing")
	}
	if token := current.Metadata["access_token"]; token != "access-token" {
		t.Fatalf("access token after canceled re-login = %v, want original", token)
	}
	if state := current.LifecycleState(); state != cliproxyauth.LifecycleStateReloginPending {
		t.Fatalf("lifecycle after canceled re-login = %q, want pending", state)
	}
}

func TestChatGPTWebExecutorCanceledReloginWaiterKeepsSharedOperationTracked(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "shared-cancel-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "shared-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()
	executor.TriggerBackgroundRelogin(expected)
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	waitForChatGPTWebReloginWaiters(t, executor, expected, 2)
	cancel()
	select {
	case errRelogin := <-manualDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("shared ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled shared ReloginCurrent() did not return")
	}
	close(release)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorCanceledFirstReloginWaiterDoesNotCancelBackground(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "first-cancel-relogin")
	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "background-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	<-started
	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebReloginWaiters(t, executor, expected, 2)
	cancel()
	select {
	case errRelogin := <-manualDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("manual ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled manual ReloginCurrent() did not return")
	}

	close(release)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorBackgroundRetriesAfterJoiningCanceledFlight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "canceling-flight-relogin")
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 1 {
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			<-releaseFirst
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "background-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	manualDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(ctx, expected)
		manualDone <- errRelogin
	}()
	<-firstStarted
	cancel()
	<-firstCanceled
	executor.TriggerBackgroundRelogin(expected)
	close(releaseFirst)
	if errRelogin := <-manualDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("manual ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 2 {
		t.Fatalf("login calls = %d, want 2", got)
	}
}

func TestChatGPTWebExecutorWaitingForCanceledFlightHonorsCallerCancellation(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "wait-canceled-flight")
	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(firstStarted)
		<-ctx.Done()
		close(firstCanceled)
		<-releaseFirst
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	defer func() {
		if errClose := executor.Close(); errClose != nil {
			t.Errorf("Close() error = %v", errClose)
		}
	}()

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(firstCtx, expected)
		firstDone <- errRelogin
	}()
	<-firstStarted
	cancelFirst()
	<-firstCanceled

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(secondCtx, expected)
		secondDone <- errRelogin
	}()
	cancelSecond()
	select {
	case errRelogin := <-secondDone:
		if !errors.Is(errRelogin, context.Canceled) {
			t.Fatalf("second ReloginCurrent() error = %v, want context canceled", errRelogin)
		}
	case <-time.After(time.Second):
		t.Fatal("second ReloginCurrent() ignored caller cancellation")
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}

	close(releaseFirst)
	if errRelogin := <-firstDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("first ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
}

func TestChatGPTWebExecutorCloseWaitsForManualReloginFlight(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "close-manual-flight")
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	reloginDone := make(chan error, 1)
	go func() {
		_, _, errRelogin := executor.ReloginCurrent(context.Background(), expected)
		reloginDone <- errRelogin
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() { closeDone <- executor.Close() }()
	<-canceled
	select {
	case errClose := <-closeDone:
		t.Fatalf("Close() returned before manual flight stopped: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if errClose := <-closeDone; errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
	if errRelogin := <-reloginDone; !errors.Is(errRelogin, context.Canceled) {
		t.Fatalf("ReloginCurrent() error = %v, want context canceled", errRelogin)
	}
}

func TestChatGPTWebExecutorCloseCancelsBackgroundRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "close-background")
	started := make(chan struct{})
	canceled := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(ctx context.Context, _ chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return nil, ctx.Err()
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.TriggerBackgroundRelogin(expected)
	<-started
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel the background re-login")
	}
	executor.TriggerBackgroundRelogin(expected)
	time.Sleep(20 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls after Close() = %d, want 1", got)
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestChatGPTWebExecutorBackgroundReloginContinuesUntilSuccess(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "retry-until-success")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		if fake.loginCalls.Load() == 4 {
			credential := *input.Credential
			credential.AccessToken = "retried-token"
			credential.LifecycleState = chatgptwebauth.LifecycleActive
			return &credential, nil
		}
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return 0 }
	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		current, ok := manager.GetByID(expected.ID)
		return ok && current.LifecycleState() == cliproxyauth.LifecycleStateActive
	})
	if got := fake.loginCalls.Load(); got != 4 {
		t.Fatalf("login calls = %d, want 4", got)
	}
}

func TestChatGPTWebExecutorBackgroundReloginStopsOnClose(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "retry-until-close")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	executor.reloginBackoff = func(int) time.Duration { return time.Millisecond }
	executor.TriggerBackgroundRelogin(expected)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() >= 4
	})
	if errClose := executor.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	calls := fake.loginCalls.Load()
	time.Sleep(20 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != calls {
		t.Fatalf("login calls after Close() = %d, want %d", got, calls)
	}
}

func TestChatGPTWebBackgroundReloginRetryable(t *testing.T) {
	if !chatGPTWebBackgroundReloginRetryable(&chatgptwebauth.AuthError{Retryable: true}) {
		t.Fatal("retryable auth error was not retried")
	}
	if !chatGPTWebBackgroundReloginRetryable(&proxypool.UnavailableError{Pool: "test"}) {
		t.Fatal("proxy unavailable error was not retried")
	}
	if chatGPTWebBackgroundReloginRetryable(cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeRolledBack, errors.New("write failed"))) {
		t.Fatal("rolled-back persistence error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(cliproxyauth.NewSaveOutcomeError(cliproxyauth.SaveOutcomeUncertain, errors.New("write uncertain"))) {
		t.Fatal("uncertain persistence error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(&chatGPTWebCredentialUnavailableError{cause: errors.New("request routing failure")}) {
		t.Fatal("generic request-routing error was retried")
	}
	if chatGPTWebBackgroundReloginRetryable(errors.New("invalid credentials")) {
		t.Fatal("non-retryable error was retried")
	}
}

func TestChatGPTWebExecutorUpdateConfigIsConcurrentSafe(t *testing.T) {
	executor := NewChatGPTWebExecutor(&config.Config{}, nil)
	enabled := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}
	disabled := &config.Config{}
	var workers sync.WaitGroup
	for index := 0; index < 8; index++ {
		workers.Add(1)
		go func(writer bool) {
			defer workers.Done()
			for iteration := 0; iteration < 1000; iteration++ {
				if writer {
					if iteration%2 == 0 {
						executor.UpdateConfig(enabled)
					} else {
						executor.UpdateConfig(disabled)
					}
					continue
				}
				_ = executor.AutoReloginEnabled()
			}
		}(index%2 == 0)
	}
	workers.Wait()
	executor.UpdateConfig(enabled)
	if !executor.AutoReloginEnabled() {
		t.Fatal("updated config was not observed")
	}
}

func TestChatGPTWebExecutorUpdateConfigPublishesImmutableSnapshot(t *testing.T) {
	headers := map[string]string{"X-Test": "before"}
	items := []map[string]any{{"name": "before"}}
	defaultRaw := []byte(`{"default":true}`)
	overrideRaw := json.RawMessage(`{"override":true}`)
	cfg := &config.Config{
		SDKConfig: config.SDKConfig{
			ProxyURL:   "http://proxy-before.example",
			RequestLog: true,
		},
		ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true},
		Payload: config.PayloadConfig{
			DefaultRaw: []config.PayloadRule{{
				Params: map[string]any{"raw": defaultRaw},
			}},
			Override: []config.PayloadRule{{
				Models: []config.PayloadModelRule{{Name: "gpt-*", Protocol: "responses"}},
				Params: map[string]any{
					"temperature": 0.5,
					"headers":     headers,
					"items":       items,
					"large":       int64(9007199254740993),
				},
			}},
			OverrideRaw: []config.PayloadRule{{
				Params: map[string]any{"raw": overrideRaw},
			}},
		},
	}
	executor := NewChatGPTWebExecutor(cfg, nil)

	cfg.ProxyURL = "http://proxy-after.example"
	cfg.RequestLog = false
	cfg.ChatGPTWeb.AutoRelogin = false
	cfg.Payload.Override[0].Models[0].Name = "mutated-*"
	cfg.Payload.Override[0].Params["temperature"] = 1.0
	headers["X-Test"] = "after"
	items[0]["name"] = "after"
	defaultRaw[2] = 'X'
	overrideRaw[2] = 'X'

	snapshot := executor.configSnapshot()
	if snapshot == nil {
		t.Fatal("configuration snapshot is nil")
	}
	if snapshot.ProxyURL != "http://proxy-before.example" || !snapshot.RequestLog || !snapshot.ChatGPTWeb.AutoRelogin {
		t.Fatalf("configuration snapshot changed with caller-owned config: %+v", snapshot)
	}
	rule := snapshot.Payload.Override[0]
	if rule.Models[0].Name != "gpt-*" || rule.Params["temperature"] != json.Number("0.5") {
		t.Fatalf("payload snapshot changed with caller-owned config: %+v", rule)
	}
	snapshotHeaders, okHeaders := rule.Params["headers"].(map[string]any)
	snapshotItems, okItems := rule.Params["items"].([]any)
	if !okHeaders || snapshotHeaders["X-Test"] != "before" || !okItems || snapshotItems[0].(map[string]any)["name"] != "before" {
		t.Fatalf("nested payload snapshot changed with caller-owned config: %+v", rule.Params)
	}
	if rule.Params["large"] != json.Number("9007199254740993") {
		t.Fatalf("large payload number lost precision: %#v", rule.Params["large"])
	}
	snapshotDefaultRaw, okDefaultRaw := snapshot.Payload.DefaultRaw[0].Params["raw"].([]byte)
	snapshotOverrideRaw, okOverrideRaw := snapshot.Payload.OverrideRaw[0].Params["raw"].(json.RawMessage)
	if !okDefaultRaw || string(snapshotDefaultRaw) != `{"default":true}` {
		t.Fatalf("default raw payload type/value = %T %q", snapshot.Payload.DefaultRaw[0].Params["raw"], snapshotDefaultRaw)
	}
	if !okOverrideRaw || string(snapshotOverrideRaw) != `{"override":true}` {
		t.Fatalf("override raw payload type/value = %T %q", snapshot.Payload.OverrideRaw[0].Params["raw"], snapshotOverrideRaw)
	}
}

func TestChatGPTWebExecutorBackgroundReloginStopsWhenDisabled(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "stop-disabled")
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	backoffStarted := make(chan struct{})
	var backoffOnce sync.Once
	executor.reloginBackoff = func(int) time.Duration {
		backoffOnce.Do(func() { close(backoffStarted) })
		return 500 * time.Millisecond
	}
	done := make(chan struct{})
	go func() {
		executor.runBackgroundRelogin(expected)
		close(done)
	}()
	<-backoffStarted
	executor.UpdateConfig(&config.Config{})
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background re-login did not stop after auto-relogin was disabled")
	}
	if got := fake.loginCalls.Load(); got != 1 {
		t.Fatalf("login calls = %d, want 1", got)
	}
}

func TestChatGPTWebExecutorBackgroundReloginStopsWhenCurrentChanges(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*cliproxyauth.Auth)
	}{
		{name: "lifecycle", change: func(auth *cliproxyauth.Auth) {
			auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
		}},
		{name: "generation", change: func(auth *cliproxyauth.Auth) {
			auth.Attributes[cliproxyauth.SourceHashAttributeKey] = "replacement-generation"
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			expected := registerChatGPTWebPendingAuth(t, manager, "stop-"+test.name)
			fake := &fakeChatGPTWebAuthService{}
			fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				return nil, &chatgptwebauth.AuthError{Code: "network_error", Retryable: true}
			}
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
			executor.authService = fake
			backoffStarted := make(chan struct{})
			var backoffOnce sync.Once
			executor.reloginBackoff = func(int) time.Duration {
				backoffOnce.Do(func() { close(backoffStarted) })
				return 500 * time.Millisecond
			}
			done := make(chan struct{})
			go func() {
				executor.runBackgroundRelogin(expected)
				close(done)
			}()
			<-backoffStarted
			current, _ := manager.GetByID(expected.ID)
			test.change(current)
			if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), current); errUpdate != nil {
				t.Fatal(errUpdate)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("background re-login did not stop after auth changed")
			}
			if got := fake.loginCalls.Load(); got != 1 {
				t.Fatalf("login calls = %d, want 1", got)
			}
		})
	}
}

func TestChatGPTWebExecutorBackgroundReloginReleasesSlotWhenPendingChangesAfterAcquire(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "slot-lifecycle-race")
	fake := &fakeChatGPTWebAuthService{}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	initialSlots := len(chatGPTWebBackgroundReloginSlots)
	executor.reloginSlotAcquired = func() {
		current, ok := manager.GetByID(expected.ID)
		if !ok {
			t.Fatalf("auth %q not found", expected.ID)
		}
		current.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
		if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), current); errUpdate != nil {
			t.Fatal(errUpdate)
		}
	}

	executor.runBackgroundRelogin(expected)
	if got := fake.loginCalls.Load(); got != 0 {
		t.Fatalf("login calls = %d, want 0 after lifecycle changed", got)
	}
	if got := len(chatGPTWebBackgroundReloginSlots); got != initialSlots {
		t.Fatalf("occupied background slots = %d, want %d", got, initialSlots)
	}
}

func TestChatGPTWebExecutorBackgroundReloginGlobalConcurrencyLimit(t *testing.T) {
	const authCount = chatGPTWebBackgroundReloginConcurrency * 2
	manager := cliproxyauth.NewManager(nil, nil, nil)
	fake := &fakeChatGPTWebAuthService{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	var active atomic.Int32
	var maximum atomic.Int32
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := maximum.Load(); current > previous && !maximum.CompareAndSwap(previous, current); previous = maximum.Load() {
		}
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "limited-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executors := []*ChatGPTWebExecutor{
		NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager),
		NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager),
	}
	for _, executor := range executors {
		executor.authService = fake
	}
	for index := 0; index < authCount; index++ {
		expected := registerChatGPTWebPendingAuth(t, manager, "limit-"+string(rune('a'+index)))
		executors[index%len(executors)].TriggerBackgroundRelogin(expected)
	}
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == chatGPTWebBackgroundReloginConcurrency
	})
	time.Sleep(30 * time.Millisecond)
	if got := fake.loginCalls.Load(); got != chatGPTWebBackgroundReloginConcurrency {
		t.Fatalf("login calls before release = %d, want %d", got, chatGPTWebBackgroundReloginConcurrency)
	}
	if got := maximum.Load(); got > chatGPTWebBackgroundReloginConcurrency {
		t.Fatalf("maximum background concurrency = %d, limit %d", got, chatGPTWebBackgroundReloginConcurrency)
	}
	releaseOnce.Do(func() { close(release) })
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		return fake.loginCalls.Load() == authCount
	})
}

func TestChatGPTWebExecutorManagementLoginWaitsForBackgroundRelogin(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "management-serialization")
	fake := &fakeChatGPTWebAuthService{}
	started := make(chan struct{})
	continueLogin := make(chan struct{})
	fake.loginFn = func(ctx context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		select {
		case <-continueLogin:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		credential := *input.Credential
		credential.AccessToken = "serialized-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
	executor.authService = fake
	t.Cleanup(func() { _ = executor.Close() })

	executor.TriggerBackgroundRelogin(expected)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background re-login did not start")
	}

	type operationResult struct {
		release func()
		err     error
	}
	acquired := make(chan operationResult, 1)
	email, _ := expected.Metadata["email"].(string)
	go func() {
		_, release, errAcquire := executor.BeginLoginOperation(t.Context(), email)
		acquired <- operationResult{release: release, err: errAcquire}
	}()
	select {
	case result := <-acquired:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("management login acquired while background re-login was active: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	close(continueLogin)
	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatalf("BeginLoginOperation() error = %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("management login did not acquire after background re-login finished")
	}
}

func TestChatGPTWebExecutorLoginCoordinatorSurvivesReplacement(t *testing.T) {
	coordinator := NewChatGPTWebLoginCoordinator()
	previous := NewChatGPTWebExecutorWithLoginCoordinator(&config.Config{}, nil, coordinator)
	replacement := NewChatGPTWebExecutorWithLoginCoordinator(&config.Config{}, nil, coordinator)
	t.Cleanup(func() {
		_ = previous.Close()
		_ = replacement.Close()
	})

	_, releasePrevious, errPrevious := previous.BeginLoginOperation(t.Context(), "shared@example.com")
	if errPrevious != nil {
		t.Fatal(errPrevious)
	}
	type operationResult struct {
		release func()
		err     error
	}
	acquired := make(chan operationResult, 1)
	go func() {
		_, release, errAcquire := replacement.BeginLoginOperation(t.Context(), "SHARED@example.com")
		acquired <- operationResult{release: release, err: errAcquire}
	}()
	select {
	case result := <-acquired:
		if result.release != nil {
			result.release()
		}
		t.Fatalf("replacement executor bypassed previous login operation: %v", result.err)
	case <-time.After(50 * time.Millisecond):
	}

	releasePrevious()
	select {
	case result := <-acquired:
		if result.err != nil {
			t.Fatalf("replacement BeginLoginOperation() error = %v", result.err)
		}
		result.release()
	case <-time.After(time.Second):
		t.Fatal("replacement executor did not acquire shared login coordinator")
	}
}

func TestChatGPTWebExecutorReloginUsesConditionalUpdate(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("relogin")
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, _ := manager.GetByID(auth.ID)

	started := make(chan struct{})
	release := make(chan struct{})
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		close(started)
		<-release
		credential := *input.Credential
		credential.AccessToken = "stale-login-token"
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return &credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake

	type reloginResult struct {
		auth    *cliproxyauth.Auth
		current bool
		err     error
	}
	result := make(chan reloginResult, 1)
	go func() {
		updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
		result <- reloginResult{auth: updated, current: current, err: errRelogin}
	}()
	<-started
	replacement := expected.Clone()
	replacement.Metadata["access_token"] = "manual-token"
	replacement.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "manual-source"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}
	close(release)
	relogin := <-result
	if relogin.current {
		t.Fatal("stale re-login unexpectedly replaced the current auth")
	}
	if !errors.Is(relogin.err, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("stale re-login error = %v, want superseded", relogin.err)
	}
	if relogin.auth == nil || relogin.auth.Metadata["access_token"] != "manual-token" {
		t.Fatalf("stale re-login latest auth = %#v", relogin.auth)
	}
	got, _ := manager.GetByID(auth.ID)
	if got.Metadata["access_token"] != "manual-token" {
		t.Fatalf("access token = %v, want manual-token", got.Metadata["access_token"])
	}
}

func TestChatGPTWebExecutorReloginRejectsSupersededAuthBeforeFlightStarts(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("relogin-before-flight")
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, _ := manager.GetByID(auth.ID)

	replacement := expected.Clone()
	replacement.Metadata["access_token"] = "manual-token"
	replacement.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateActive
	replacement.Attributes[cliproxyauth.SourceHashAttributeKey] = "manual-source"
	if _, errUpdate := manager.Update(cliproxyauth.WithSkipPersist(t.Context()), replacement); errUpdate != nil {
		t.Fatal(errUpdate)
	}

	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		t.Fatal("superseded auth must not start a re-login")
		return nil, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if current || !errors.Is(errRelogin, chatgptwebauth.ErrCredentialSuperseded) {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v), want superseded", updated, current, errRelogin)
	}
	if updated == nil || updated.Metadata["access_token"] != "manual-token" {
		t.Fatalf("latest auth = %#v, want replacement", updated)
	}
}

func TestChatGPTWebExecutorReloginResultDoesNotShareCredentialMetadata(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	expected := registerChatGPTWebPendingAuth(t, manager, "clone-relogin")
	cleanupDone := expected.RuntimeInstanceCleanupDone()
	fake := &fakeChatGPTWebAuthService{}
	fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := cloneChatGPTWebCredential(input.Credential)
		credential.AccessToken = "fresh-token"
		credential.Cookies = []chatgptwebauth.Cookie{{Name: "session", Value: "fresh-cookie"}}
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor := NewChatGPTWebExecutor(&config.Config{}, manager)
	executor.authService = fake
	updated, current, errRelogin := executor.ReloginCurrent(t.Context(), expected)
	if errRelogin != nil || !current || updated == nil {
		t.Fatalf("ReloginCurrent() = (%v, %v, %v)", updated, current, errRelogin)
	}
	if updated.RuntimeInstanceID() == expected.RuntimeInstanceID() {
		t.Fatal("successful re-login reused the previous runtime instance")
	}
	if _, release, active := expected.BeginRuntimeExecution(t.Context()); active {
		release()
		t.Fatal("successful re-login left the previous runtime instance active")
	}
	cookies := updated.Metadata["cookies"].([]chatgptwebauth.Cookie)
	cookies[0].Value = "caller-mutation"
	installed, _ := manager.GetByID(expected.ID)
	installedCookies := installed.Metadata["cookies"].([]chatgptwebauth.Cookie)
	if installedCookies[0].Value != "fresh-cookie" {
		t.Fatalf("installed cookie = %q, want independent metadata", installedCookies[0].Value)
	}
	select {
	case <-cleanupDone:
	case <-time.After(5 * time.Second):
		t.Fatal("successful re-login cleanup did not finish")
	}
}

func chatGPTWebTestAuth(id string) *cliproxyauth.Auth {
	credential := &chatgptwebauth.Credential{
		Type:           chatgptwebauth.Provider,
		Email:          id + "@example.com",
		Password:       "password",
		TOTPSecret:     "JBSWY3DPEHPK3PXP",
		AccessToken:    "access-token",
		RefreshToken:   "refresh-token",
		Persona:        chatgptwebauth.DefaultPersona(),
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	return &cliproxyauth.Auth{
		ID:         "chatgpt-web-" + id,
		Provider:   chatgptwebauth.Provider,
		Status:     cliproxyauth.StatusActive,
		Attributes: map[string]string{cliproxyauth.SourceHashAttributeKey: "source-" + id},
		Metadata:   metadata,
	}
}

func registerChatGPTWebPendingAuth(t *testing.T, manager *cliproxyauth.Manager, id string) *cliproxyauth.Auth {
	t.Helper()
	auth := chatGPTWebTestAuth(id)
	auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	expected, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatalf("registered auth %q not found", auth.ID)
	}
	return expected
}

func waitForChatGPTWebReloginWaiters(t *testing.T, executor *ChatGPTWebExecutor, auth *cliproxyauth.Auth, want int) {
	t.Helper()
	key := chatGPTWebOperationKey(auth)
	waitForChatGPTWebCondition(t, time.Second, func() bool {
		executor.reloginMu.Lock()
		defer executor.reloginMu.Unlock()
		flight := executor.reloginFlights[key]
		return flight != nil && flight.waiters == want
	})
}

func waitForChatGPTWebCondition(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timed out waiting for condition")
		case <-ticker.C:
		}
	}
}
