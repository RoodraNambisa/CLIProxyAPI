package executor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type fakeChatGPTWebAuthService struct {
	loginFn      func(context.Context, chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error)
	refreshFn    func(context.Context, chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error)
	loginCalls   atomic.Int32
	refreshCalls atomic.Int32
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

func TestChatGPTWebExecutorBackgroundReloginRetriesAreBounded(t *testing.T) {
	for _, test := range []struct {
		name          string
		succeedOn     int32
		wantLifecycle string
	}{
		{name: "succeeds on final attempt", succeedOn: 3, wantLifecycle: cliproxyauth.LifecycleStateActive},
		{name: "stops after final attempt", wantLifecycle: cliproxyauth.LifecycleStateReloginPending},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			expected := registerChatGPTWebPendingAuth(t, manager, "retry-"+test.name)
			fake := &fakeChatGPTWebAuthService{}
			fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				if test.succeedOn > 0 && fake.loginCalls.Load() == test.succeedOn {
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
				return fake.loginCalls.Load() == chatGPTWebBackgroundReloginMaxAttempts
			})
			time.Sleep(30 * time.Millisecond)
			if got := fake.loginCalls.Load(); got != chatGPTWebBackgroundReloginMaxAttempts {
				t.Fatalf("login calls = %d, want %d", got, chatGPTWebBackgroundReloginMaxAttempts)
			}
			current, _ := manager.GetByID(expected.ID)
			if got := current.LifecycleState(); got != test.wantLifecycle {
				t.Fatalf("lifecycle = %q, want %q", got, test.wantLifecycle)
			}
		})
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
	if !errors.Is(relogin.err, errChatGPTWebReloginSuperseded) {
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
		Email:          "person@example.com",
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
