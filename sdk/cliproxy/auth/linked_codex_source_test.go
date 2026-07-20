package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type linkedCodexSourceTestExecutor struct {
	refresh func(context.Context, *Auth) (*Auth, error)
}

type linkedCodexRequestPrepareExecutor struct {
	*linkedCodexSourceTestExecutor
	manager           *Manager
	sourceID          string
	sourceUID         string
	failedSourceToken string
	expectedIdentity  string
}

func (*linkedCodexRequestPrepareExecutor) Identifier() string { return chatgptwebauth.Provider }

func (*linkedCodexRequestPrepareExecutor) ShouldPrepareRequestAuth(*Auth) bool { return true }

func (executor *linkedCodexRequestPrepareExecutor) PrepareRequestAuth(ctx context.Context, auth *Auth) (*Auth, error) {
	return executor.refreshLinkedSource(ctx, auth)
}

func (executor *linkedCodexRequestPrepareExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	return executor.refreshLinkedSource(ctx, auth)
}

func (executor *linkedCodexRequestPrepareExecutor) refreshLinkedSource(ctx context.Context, auth *Auth) (*Auth, error) {
	result, errRefresh := executor.manager.RefreshLinkedCodexSource(
		ctx,
		executor.sourceID,
		executor.sourceUID,
		executor.failedSourceToken,
		executor.expectedIdentity,
	)
	if errRefresh != nil {
		return nil, errRefresh
	}
	updated := auth.Clone()
	updated.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
	updated.Metadata["access_token"] = result.AccessToken
	return updated, nil
}

func (*linkedCodexSourceTestExecutor) Identifier() string { return "codex" }
func (*linkedCodexSourceTestExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (*linkedCodexSourceTestExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (executor *linkedCodexSourceTestExecutor) Refresh(ctx context.Context, auth *Auth) (*Auth, error) {
	return executor.refresh(ctx, auth)
}
func (*linkedCodexSourceTestExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (*linkedCodexSourceTestExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestChatGPTWebCredentialReferenceToleratesOptionalClaimChanges(t *testing.T) {
	source := linkedCodexSourceTestAuth("source.json", "uid-a", "access-a")
	source.Provider = chatgptwebauth.Provider
	source.Metadata["user_id"] = "user-a"
	source.Metadata["sub"] = "subject-a"
	reference := ChatGPTWebCredentialReferenceValue(source)
	if reference == "" {
		t.Fatal("reference is empty")
	}

	withoutOptionalClaims := source.Clone()
	delete(withoutOptionalClaims.Metadata, "user_id")
	delete(withoutOptionalClaims.Metadata, "sub")
	if !ChatGPTWebCredentialReferenceMatches(reference, withoutOptionalClaims) {
		t.Fatal("optional claim removal changed the account reference")
	}
	differentUser := source.Clone()
	differentUser.Metadata["user_id"] = "user-b"
	if ChatGPTWebCredentialReferenceMatches(reference, differentUser) {
		t.Fatal("different user matched the source reference")
	}
	differentAccount := source.Clone()
	differentAccount.Metadata["account_id"] = "account-b"
	if ChatGPTWebCredentialReferenceMatches(reference, differentAccount) {
		t.Fatal("different account matched the source reference")
	}
}

func TestMergeChatGPTWebCredentialReferenceValuesPreservesStrongClaims(t *testing.T) {
	strong := linkedCodexSourceTestAuth("source.json", "uid-a", "access-a")
	strong.Provider = chatgptwebauth.Provider
	strong.Metadata["sub"] = "subject-a"
	previous := ChatGPTWebCredentialReferenceValue(strong)
	weak := strong.Clone()
	delete(weak.Metadata, "user_id")
	delete(weak.Metadata, "sub")
	incoming := ChatGPTWebCredentialReferenceValue(weak)
	merged := MergeChatGPTWebCredentialReferenceValues(previous, incoming)
	previousParts := strings.Split(previous, ":")
	mergedParts := strings.Split(merged, ":")
	if len(previousParts) != 5 || len(mergedParts) != 5 || mergedParts[2] != previousParts[2] || mergedParts[3] != previousParts[3] {
		t.Fatalf("merged reference = %q, previous = %q", merged, previous)
	}
	if !ChatGPTWebCredentialReferenceMatches(merged, weak) {
		t.Fatal("merged reference no longer matches the refreshed source")
	}
}

func TestRefreshLinkedCodexSourceUsesBoundedWorkerAndStableIdentity(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source := linkedCodexSourceTestAuth("source.json", "uid-a", "access-a")
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), source)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor := &linkedCodexSourceTestExecutor{}
	executor.refresh = func(ctx context.Context, auth *Auth) (*Auth, error) {
		deadline, ok := ctx.Deadline()
		remaining := time.Until(deadline)
		if !ok || remaining <= 0 || remaining > chatgptwebauth.DefaultAcquisitionTimeout+time.Second {
			return nil, errors.New("refresh worker has no bounded acquisition deadline")
		}
		updated := auth.Clone()
		updated.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
		updated.Metadata["access_token"] = "access-b"
		delete(updated.Metadata, "user_id")
		return updated, nil
	}
	manager.RegisterExecutor(executor)
	identityAuth := installed.Clone()
	identityAuth.Provider = chatgptwebauth.Provider
	reference := ChatGPTWebCredentialReferenceValue(identityAuth)
	result, errRefresh := manager.RefreshLinkedCodexSource(t.Context(), installed.ID, "uid-a", "access-a", reference)
	if errRefresh != nil {
		t.Fatal(errRefresh)
	}
	if result.AccessToken != "access-b" || result.Identity == "" {
		t.Fatalf("result = %+v", result)
	}
	current, _ := manager.GetByID(installed.ID)
	current.Provider = chatgptwebauth.Provider
	if !ChatGPTWebCredentialReferenceMatches(reference, current) {
		t.Fatal("refreshed source no longer matches its stable identity")
	}
}

func TestRefreshLinkedCodexSourceRejectsChangedRefreshIdentityBeforePersist(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source := linkedCodexSourceTestAuth("source.json", "uid-a", "access-a")
	installed, errRegister := manager.Register(WithSkipPersist(t.Context()), source)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	manager.RegisterExecutor(&linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) {
		updated := auth.Clone()
		updated.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
		updated.Metadata["account_id"] = "account-b"
		updated.Metadata["user_id"] = "user-b"
		updated.Metadata["access_token"] = "access-b"
		return updated, nil
	}})
	identityAuth := installed.Clone()
	identityAuth.Provider = chatgptwebauth.Provider
	_, errRefresh := manager.RefreshLinkedCodexSource(t.Context(), installed.ID, "uid-a", "access-a", ChatGPTWebCredentialReferenceValue(identityAuth))
	assertLinkedCodexSourceErrorCode(t, errRefresh, "source_identity_mismatch")
	current, _ := manager.GetByID(installed.ID)
	if got := chatGPTWebIdentityMetadataString(current.Metadata, "account_id"); got != "account-a" {
		t.Fatalf("persisted account_id = %q, want account-a", got)
	}
	if got := chatGPTWebIdentityMetadataString(current.Metadata, "access_token"); got != "access-a" {
		t.Fatalf("persisted access_token = %q, want access-a", got)
	}
}

func TestRefreshLinkedCodexSourceClassifiesTerminalFailures(t *testing.T) {
	t.Run("missing source", func(t *testing.T) {
		manager := NewManager(nil, nil, nil)
		_, errRefresh := manager.RefreshLinkedCodexSource(t.Context(), "missing.json", "uid", "access", "identity")
		assertLinkedCodexSourceErrorCode(t, errRefresh, "source_auth_missing")
	})

	for _, test := range []struct {
		name    string
		message string
	}{
		{name: "invalid grant", message: "invalid_grant"},
		{name: "reused refresh token", message: "refresh_token_reused"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(nil, nil, nil)
			source := linkedCodexSourceTestAuth("source.json", "uid-a", "access-a")
			installed, errRegister := manager.Register(WithSkipPersist(t.Context()), source)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			manager.RegisterExecutor(&linkedCodexSourceTestExecutor{refresh: func(context.Context, *Auth) (*Auth, error) {
				return nil, &Error{HTTPStatus: http.StatusBadRequest, Message: test.message}
			}})
			identityAuth := installed.Clone()
			identityAuth.Provider = chatgptwebauth.Provider
			_, errRefresh := manager.RefreshLinkedCodexSource(t.Context(), installed.ID, "uid-a", "access-a", ChatGPTWebCredentialReferenceValue(identityAuth))
			assertLinkedCodexSourceErrorCode(t, errRefresh, "source_auth_invalid")
		})
	}
}

func TestLinkedCodexRequestPreparationUsesCanonicalRefreshLockOrder(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	updatedSource := source.Clone()
	updatedSource.Metadata = cloneStringAnyMapForLinkedSourceTest(source.Metadata)
	updatedSource.Metadata["access_token"] = "source-old"
	installedSource, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedSource)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	identitySource := installedSource.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	expectedIdentity := ChatGPTWebCredentialReferenceValue(identitySource)
	updatedWeb := web.Clone()
	updatedWeb.Metadata = cloneStringAnyMapForLinkedSourceTest(web.Metadata)
	updatedWeb.Metadata["source_identity"] = expectedIdentity
	installedWeb, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedWeb)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}

	codexExecutor := &linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) {
		refreshed := auth.Clone()
		refreshed.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
		refreshed.Metadata["access_token"] = "source-new"
		return refreshed, nil
	}}
	webExecutor := &linkedCodexRequestPrepareExecutor{
		linkedCodexSourceTestExecutor: &linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }},
		manager:                       manager, sourceID: installedSource.ID, sourceUID: "uid-a", failedSourceToken: "source-old", expectedIdentity: expectedIdentity,
	}
	manager.RegisterExecutor(codexExecutor)
	manager.RegisterExecutor(webExecutor)

	releaseSource, errLock := manager.LockCredentialRefresh(t.Context(), installedSource.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	releasedSource := false
	defer func() {
		if !releasedSource {
			releaseSource()
		}
	}()
	prepared := make(chan struct {
		auth *Auth
		err  error
	}, 1)
	go func() {
		auth, errPrepare := manager.prepareRequestAuth(t.Context(), webExecutor, installedWeb)
		prepared <- struct {
			auth *Auth
			err  error
		}{auth: auth, err: errPrepare}
	}()
	waitForRequestRefreshLockUsers(t, manager, installedSource.ID, 2)

	webLockCtx, cancelWebLock := context.WithTimeout(t.Context(), time.Second)
	releaseWeb, errWebLock := manager.LockCredentialRefresh(webLockCtx, installedWeb.ID)
	cancelWebLock()
	if errWebLock != nil {
		t.Fatalf("web refresh lock blocked behind linked preparation: %v", errWebLock)
	}
	releaseWeb()
	releaseSource()
	releasedSource = true

	select {
	case result := <-prepared:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if got := chatGPTWebIdentityMetadataString(result.auth.Metadata, "access_token"); got != "source-new" {
			t.Fatalf("prepared Web access token = %q, want source-new", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("linked request preparation deadlocked")
	}
}

func TestLinkedCodexBackgroundRefreshUsesCanonicalRefreshLockOrder(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	updatedSource := source.Clone()
	updatedSource.Metadata = cloneStringAnyMapForLinkedSourceTest(source.Metadata)
	updatedSource.Metadata["access_token"] = "source-old"
	installedSource, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedSource)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	identitySource := installedSource.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	expectedIdentity := ChatGPTWebCredentialReferenceValue(identitySource)
	updatedWeb := web.Clone()
	updatedWeb.Metadata = cloneStringAnyMapForLinkedSourceTest(web.Metadata)
	updatedWeb.Metadata["source_identity"] = expectedIdentity
	installedWeb, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedWeb)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}

	manager.RegisterExecutor(&linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) {
		refreshed := auth.Clone()
		refreshed.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
		refreshed.Metadata["access_token"] = "source-new"
		return refreshed, nil
	}})
	manager.RegisterExecutor(&linkedCodexRequestPrepareExecutor{
		linkedCodexSourceTestExecutor: &linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }},
		manager:                       manager, sourceID: installedSource.ID, sourceUID: "uid-a", failedSourceToken: "source-old", expectedIdentity: expectedIdentity,
	})

	releaseSource, errLock := manager.LockCredentialRefresh(t.Context(), installedSource.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	releasedSource := false
	defer func() {
		if !releasedSource {
			releaseSource()
		}
	}()
	refreshed := make(chan struct{}, 1)
	go func() {
		manager.refreshAuthExpected(t.Context(), installedWeb.ID, nil, time.Time{})
		refreshed <- struct{}{}
	}()
	waitForRequestRefreshLockUsers(t, manager, installedSource.ID, 2)

	webLockCtx, cancelWebLock := context.WithTimeout(t.Context(), time.Second)
	releaseWeb, errWebLock := manager.LockCredentialRefresh(webLockCtx, installedWeb.ID)
	cancelWebLock()
	if errWebLock != nil {
		t.Fatalf("web refresh lock blocked behind linked background refresh: %v", errWebLock)
	}
	releaseWeb()
	releaseSource()
	releasedSource = true

	select {
	case <-refreshed:
		currentWeb, ok := manager.GetByID(installedWeb.ID)
		if !ok {
			t.Fatal("refreshed Web credential is missing")
		}
		if got := chatGPTWebIdentityMetadataString(currentWeb.Metadata, "access_token"); got != "source-new" {
			t.Fatalf("refreshed Web access token = %q, want source-new", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("linked background refresh deadlocked")
	}
}

func TestLinkedCodexUnauthorizedRefreshUsesCanonicalRefreshLockOrder(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	source, web := linkedProxyTestAuths(t, manager)
	updatedSource := source.Clone()
	updatedSource.Metadata = cloneStringAnyMapForLinkedSourceTest(source.Metadata)
	updatedSource.Metadata["access_token"] = "source-old"
	installedSource, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedSource)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	identitySource := installedSource.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	expectedIdentity := ChatGPTWebCredentialReferenceValue(identitySource)
	updatedWeb := web.Clone()
	updatedWeb.Metadata = cloneStringAnyMapForLinkedSourceTest(web.Metadata)
	updatedWeb.Metadata["source_identity"] = expectedIdentity
	installedWeb, errUpdate := manager.Update(WithSkipPersist(t.Context()), updatedWeb)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}

	manager.RegisterExecutor(&linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) {
		refreshed := auth.Clone()
		refreshed.Metadata = cloneStringAnyMapForLinkedSourceTest(auth.Metadata)
		refreshed.Metadata["access_token"] = "source-new"
		return refreshed, nil
	}})
	manager.RegisterExecutor(&linkedCodexRequestPrepareExecutor{
		linkedCodexSourceTestExecutor: &linkedCodexSourceTestExecutor{refresh: func(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }},
		manager:                       manager, sourceID: installedSource.ID, sourceUID: "uid-a", failedSourceToken: "source-old", expectedIdentity: expectedIdentity,
	})

	releaseSource, errLock := manager.LockCredentialRefresh(t.Context(), installedSource.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	releasedSource := false
	defer func() {
		if !releasedSource {
			releaseSource()
		}
	}()
	refreshed := make(chan struct {
		auth *Auth
		err  error
	}, 1)
	go func() {
		auth, errRefresh := manager.refreshProviderForRequest(
			t.Context(), installedWeb.ID, "token", chatgptwebauth.Provider, installedWeb,
		)
		refreshed <- struct {
			auth *Auth
			err  error
		}{auth: auth, err: errRefresh}
	}()
	waitForRequestRefreshLockUsers(t, manager, installedSource.ID, 2)

	webLockCtx, cancelWebLock := context.WithTimeout(t.Context(), time.Second)
	releaseWeb, errWebLock := manager.LockCredentialRefresh(webLockCtx, installedWeb.ID)
	cancelWebLock()
	if errWebLock != nil {
		t.Fatalf("web refresh lock blocked behind linked unauthorized refresh: %v", errWebLock)
	}
	releaseWeb()
	releaseSource()
	releasedSource = true

	select {
	case result := <-refreshed:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if got := chatGPTWebIdentityMetadataString(result.auth.Metadata, "access_token"); got != "source-new" {
			t.Fatalf("refreshed Web access token = %q, want source-new", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("linked unauthorized refresh deadlocked")
	}
}

func waitForRequestRefreshLockUsers(t *testing.T, manager *Manager, id string, minimum int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		manager.requestRefreshLocksMu.Lock()
		value, _ := manager.requestRefreshLocks.Load(id)
		lock, _ := value.(*authRequestRefreshLock)
		active := 0
		if lock != nil {
			active = lock.active
		}
		manager.requestRefreshLocksMu.Unlock()
		if active >= minimum {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("refresh lock %q did not reach %d users", id, minimum)
}

func linkedCodexSourceTestAuth(id, uid, accessToken string) *Auth {
	return &Auth{
		ID: id, FileName: id, Provider: "codex",
		Metadata: map[string]any{
			"type": "codex", "credential_uid": uid, "account_id": "account-a", "user_id": "user-a",
			"email": "person@example.com", "access_token": accessToken,
		},
	}
}

func assertLinkedCodexSourceErrorCode(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %q", want)
	}
	var coded interface{ ChatGPTWebErrorCode() string }
	if !errors.As(err, &coded) || coded.ChatGPTWebErrorCode() != want {
		t.Fatalf("error = %#v, want code %q", err, want)
	}
}

func cloneStringAnyMapForLinkedSourceTest(values map[string]any) map[string]any {
	clone := make(map[string]any, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}
