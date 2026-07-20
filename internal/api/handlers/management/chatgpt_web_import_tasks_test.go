package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebImportTaskSupportsMultipleFilesAndLegacyField(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{
		{field: "files", name: "first.json", data: `{"email":"first@example.com","access_token":"first-secret"}`},
		{field: "files", name: "second.json", data: `{"email":"second@example.com","accessToken":"second-secret"}`},
	})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.State != chatGPTWebLoginTaskCompleted || completed.Succeeded != 2 || completed.Failed != 0 {
		t.Fatalf("task = %+v", completed)
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebMutationTask(t, completed), "first-secret", "second-secret")
	if got := len(manager.List()); got != 2 {
		t.Fatalf("registered credentials = %d, want 2", got)
	}
	for _, result := range completed.Results {
		info, errStat := os.Stat(filepath.Join(authDir, result.Name))
		if errStat != nil {
			t.Fatalf("stat imported credential %q: %v", result.Name, errStat)
		}
		if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
			t.Fatalf("credential mode = %o, want 600", info.Mode().Perm())
		}
	}

	legacy := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{
		{field: "file", name: "legacy.json", data: `{"email":"legacy@example.com","access_token":"legacy-secret"}`},
	})
	legacyCompleted := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, legacy.ID)
	if legacyCompleted.Succeeded != 1 || legacyCompleted.Results[0].Status != "created" {
		t.Fatalf("legacy task = %+v", legacyCompleted)
	}
}

func TestChatGPTWebImportTaskNormalizesSessionOnlyCredential(t *testing.T) {
	const sessionSecret = "session-secret"
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		credential.Email = "session@example.com"
		credential.AccessToken = "session-access-secret"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "session.json",
		data:  `{"session_cookie":"` + sessionSecret + `"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].CredentialMode != chatgptwebauth.CredentialModeNative {
		t.Fatalf("task = %+v", completed)
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebMutationTask(t, completed), sessionSecret, "session-access-secret")
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil {
		t.Fatal("imported session credential is missing")
	}
	credential, errParse := chatgptwebauth.ParseCredential(stored.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if credential.RefreshStrategy != chatgptwebauth.RefreshStrategyChatGPTSession || !chatgptwebauth.HasSessionCookie(credential.Cookies) {
		t.Fatalf("credential = %+v", credential)
	}
}

func TestChatGPTWebImportTaskPersistsRotatedRefreshTokenAfterCancellation(t *testing.T) {
	normalizeStarted := make(chan struct{}, 1)
	releaseNormalize := make(chan struct{})
	var fetchCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(ctx context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		normalizeStarted <- struct{}{}
		<-releaseNormalize
		if errContext := ctx.Err(); errContext != nil {
			return nil, errContext
		}
		credential.AccessToken = "rotated-access"
		credential.RefreshToken = "rotated-refresh"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		fetchCalls.Add(1)
		return nil, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "rotating.json",
		data: `{"email":"rotate@example.com","refresh_strategy":"web_oauth_rt","refresh_token":"old-refresh"}`,
	}})
	select {
	case <-normalizeStarted:
	case <-time.After(time.Second):
		t.Fatal("refresh normalization did not start")
	}
	canceled := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/import-tasks/"+task.ID, "")
	if canceled.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", canceled.Code, canceled.Body.String())
	}
	close(releaseNormalize)
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "created" || fetchCalls.Load() != 1 {
		t.Fatalf("task = %+v, fetch calls = %d", completed, fetchCalls.Load())
	}
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil || stringValue(stored.Metadata, "refresh_token") != "rotated-refresh" {
		t.Fatalf("stored rotated credential = %#v", stored)
	}
}

func TestChatGPTWebImportTaskPersistsRotatedSecretWhenProbeRejectsToken(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		credential.AccessToken = "rotated-access"
		credential.RefreshToken = "rotated-refresh"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		return nil, conversionStatusError{status: http.StatusUnauthorized, path: "/backend-api/models"}
	}
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "rotating.json",
		data: `{"email":"rejected@example.com","refresh_strategy":"web_oauth_rt","refresh_token":"old-refresh"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "token_incompatible" || completed.Results[0].Name == "" {
		t.Fatalf("task = %+v", completed)
	}
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil || stringValue(stored.Metadata, "refresh_token") != "rotated-refresh" ||
		stored.LifecycleState() != coreauth.LifecycleStateReauthRequired {
		t.Fatalf("stored rejected credential = %#v", stored)
	}
	if got := store.saves.Load(); got != 1 {
		t.Fatalf("save calls = %d, want 1 final-state write", got)
	}
}

func TestChatGPTWebImportTaskKeepsRotatedCredentialActiveOnTemporaryProbeFailure(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		credential.AccessToken = "rotated-access"
		credential.RefreshToken = "rotated-refresh"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		return nil, conversionStatusError{status: http.StatusServiceUnavailable, path: "/backend-api/models"}
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "rotating.json",
		data: `{"email":"temporary@example.com","refresh_strategy":"web_oauth_rt","refresh_token":"old-refresh"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "probe_unavailable" || completed.Results[0].Name == "" {
		t.Fatalf("task = %+v", completed)
	}
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil || stringValue(stored.Metadata, "refresh_token") != "rotated-refresh" ||
		stored.LifecycleState() != coreauth.LifecycleStateActive {
		t.Fatalf("stored credential after temporary probe failure = %#v", stored)
	}
}

func TestChatGPTWebTokenOnlyImportCancelsDuringProbeWithoutPersistence(t *testing.T) {
	probeStarted := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.fetchFn = func(ctx context.Context, _ *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		probeStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "token-only.json",
		data: `{"email":"cancel-token@example.com","access_token":"token-only-access"}`,
	}})
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("token-only probe did not start")
	}
	if canceled := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/import-tasks/"+task.ID, ""); canceled.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", canceled.Code, canceled.Body.String())
	}
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Canceled != 1 || completed.Results[0].Status != chatGPTWebLoginResultCanceled {
		t.Fatalf("task = %+v", completed)
	}
	if _, exists := manager.GetByID(chatGPTWebCredentialFileName("cancel-token@example.com")); exists {
		t.Fatal("canceled token-only import persisted a credential")
	}
}

func TestChatGPTWebImportTaskAllowsOpaqueRefreshTokenRotationForExistingAccount(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		credential.AccessToken = "access-b"
		credential.RefreshToken = "refresh-b"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	existingCredential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, CredentialUID: "web-uid", RefreshStrategy: chatgptwebauth.RefreshStrategyWebOAuthRT,
		Email: "rotate-existing@example.com", AccessToken: "access-a", RefreshToken: "refresh-a",
		Cookies: []chatgptwebauth.Cookie{}, LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	existingCredential.ApplyToMetadata(metadata)
	fileName := chatGPTWebCredentialFileName(existingCredential.Email)
	installedExisting, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider, Metadata: metadata, Status: coreauth.StatusActive,
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	originalRuntimeInstance := installedExisting.RuntimeInstanceID()

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "rotate.json",
		data: `{"email":"rotate-existing@example.com","refresh_strategy":"web_oauth_rt","refresh_token":"refresh-a"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" {
		t.Fatalf("task = %+v", completed)
	}
	current, _ := manager.GetByID(fileName)
	if got := stringValue(current.Metadata, "refresh_token"); got != "refresh-b" {
		t.Fatalf("refresh token = %q, want refresh-b", got)
	}
	if current.RuntimeInstanceID() != originalRuntimeInstance {
		t.Fatal("controlled opaque token refresh replaced the runtime credential instance")
	}
}

func TestChatGPTWebImportTaskRejectsStrongIdentityOwnedByAnotherEmail(t *testing.T) {
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	router := chatGPTWebManagementTestRouter(h)
	existingCredential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, CredentialUID: "existing-uid", Email: "old@example.com",
		AccountID: "account-a", UserID: "user-a", AccessToken: "old-access",
		RefreshStrategy: chatgptwebauth.RefreshStrategyTokenOnly, LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	existingCredential.ApplyToMetadata(metadata)
	existingName := chatGPTWebCredentialFileName(existingCredential.Email)
	payload, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, existingName), payload, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, exists := manager.GetByID(existingName); exists {
		t.Fatal("persisted owner unexpectedly exists in the runtime manager")
	}

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "renamed.json",
		data: `{"email":"new@example.com","account_id":"account-a","user_id":"user-a","access_token":"new-access"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "identity_conflict" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("task = %+v", completed)
	}
	if got := len(manager.List()); got != 0 {
		t.Fatalf("runtime credentials = %d, want 0", got)
	}
	if _, exists := manager.GetByID(chatGPTWebCredentialFileName("new@example.com")); exists {
		t.Fatal("same strong identity was registered under a second email")
	}
}

func TestChatGPTWebImportTaskSerializesExistingCredentialRefresh(t *testing.T) {
	beginStarted := make(chan struct{}, 2)
	normalizeStarted := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.beginFn = func(ctx context.Context, _ string) (context.Context, func(), error) {
		beginStarted <- struct{}{}
		return ctx, func() {}, nil
	}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		normalizeStarted <- struct{}{}
		credential.AccessToken = "fresh-access"
		credential.RefreshToken = "fresh-refresh"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	credential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, CredentialUID: "serialized-uid", Email: "serialized@example.com",
		AccessToken: "old-access", RefreshToken: "old-refresh", RefreshStrategy: chatgptwebauth.RefreshStrategyWebOAuthRT,
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	fileName := chatGPTWebCredentialFileName(credential.Email)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider, Metadata: metadata,
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	releaseRefresh, errLock := manager.LockCredentialRefresh(t.Context(), fileName)
	if errLock != nil {
		t.Fatal(errLock)
	}
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "serialized.json",
		data: `{"email":"serialized@example.com","refresh_strategy":"web_oauth_rt","refresh_token":"old-refresh"}`,
	}})
	select {
	case <-beginStarted:
		releaseRefresh()
		t.Fatal("import entered the login operation while the credential refresh lock was held")
	case <-normalizeStarted:
		releaseRefresh()
		t.Fatal("import refreshed while the credential refresh lock was held")
	case <-time.After(50 * time.Millisecond):
	}
	releaseRefresh()
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" {
		t.Fatalf("task = %+v", completed)
	}
}

func TestChatGPTWebImportTaskDoesNotRefreshAgainAfterIdentityResolution(t *testing.T) {
	var normalizeCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, proxyURL string) (*chatgptwebauth.Credential, error) {
		if proxyURL != "socks5h://staging.example:1080" {
			return nil, errors.New("credential was refreshed through a second proxy")
		}
		normalizeCalls.Add(1)
		credential.Email = "resolved-once@example.com"
		credential.AccessToken = "resolved-access"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	resolver := &chatGPTWebImportProxyResolver{finalID: chatGPTWebCredentialFileName("resolved-once@example.com")}
	manager.SetProxyResolver(resolver)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "session.json", data: `{"session_cookie":"session-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "created" {
		t.Fatalf("task = %+v", completed)
	}
	if got := normalizeCalls.Load(); got != 1 {
		t.Fatalf("normalize calls = %d, want 1", got)
	}
	if got := resolver.calls.Load(); got != 1 {
		t.Fatalf("proxy resolve calls = %d, want one non-destructive staging resolution", got)
	}
}

func TestChatGPTWebImportTaskSerializesUnknownIdentityWithExistingRefresh(t *testing.T) {
	beginStarted := make(chan struct{}, 2)
	normalizeStarted := make(chan struct{}, 1)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.beginFn = func(ctx context.Context, _ string) (context.Context, func(), error) {
		beginStarted <- struct{}{}
		return ctx, func() {}, nil
	}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		normalizeStarted <- struct{}{}
		credential.Email = "unknown-existing@example.com"
		credential.AccessToken = "fresh-session-access"
		credential.Expired = time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
		credential.LifecycleState = chatgptwebauth.LifecycleActive
		return credential, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	existingCredential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, CredentialUID: "unknown-existing-uid", Email: "unknown-existing@example.com",
		AccessToken: "old-session-access", RefreshStrategy: chatgptwebauth.RefreshStrategyChatGPTSession,
		Cookies:        []chatgptwebauth.Cookie{{Name: "__Secure-next-auth.session-token", Value: "old-session"}},
		LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	existingCredential.ApplyToMetadata(metadata)
	fileName := chatGPTWebCredentialFileName(existingCredential.Email)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider, Metadata: metadata,
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	releaseRefresh, errLock := manager.LockCredentialRefresh(t.Context(), fileName)
	if errLock != nil {
		t.Fatal(errLock)
	}
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "session.json", data: `{"session_cookie":"new-session"}`,
	}})
	select {
	case <-beginStarted:
		releaseRefresh()
		t.Fatal("unknown-identity import entered login while an existing credential refresh lock was held")
	case <-normalizeStarted:
		releaseRefresh()
		t.Fatal("unknown-identity import bypassed an existing credential refresh lock")
	case <-time.After(50 * time.Millisecond):
	}
	releaseRefresh()
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" {
		t.Fatalf("task = %+v", completed)
	}
}

func TestChatGPTWebImportTaskReleasesDeclaredIdentityBeforeLockingResolvedIdentity(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	beginCalls := 0
	declaredReleased := false
	executor.beginFn = func(ctx context.Context, _ string) (context.Context, func(), error) {
		beginCalls++
		if beginCalls == 2 && !declaredReleased {
			return nil, nil, errors.New("resolved identity lock acquired while declared identity remained locked")
		}
		return ctx, func() { declaredReleased = true }, nil
	}
	executor.normalizeFn = func(_ context.Context, credential *chatgptwebauth.Credential, _ string) (*chatgptwebauth.Credential, error) {
		credential.Email = "resolved@example.com"
		return credential, nil
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files",
		name:  "identity.json",
		data:  `{"email":"declared@example.com","access_token":"identity-access"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Email != "resolved@example.com" || beginCalls != 2 {
		t.Fatalf("task = %+v, begin calls = %d", completed, beginCalls)
	}
}

func TestChatGPTWebImportTaskReportsUnchangedAndPreservesLocalFieldsOnUpdate(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	credential := &chatgptwebauth.Credential{
		Type:               chatgptwebauth.Provider,
		CredentialUID:      "web-uid",
		RefreshStrategy:    chatgptwebauth.RefreshStrategyTokenOnly,
		Email:              "existing@example.com",
		AccessToken:        "old-access",
		Expired:            time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		Cookies:            []chatgptwebauth.Cookie{},
		Persona:            chatgptwebauth.DefaultPersona(),
		LifecycleState:     chatgptwebauth.LifecycleActive,
		LifecycleUpdatedAt: "2026-01-01T00:00:00Z",
	}
	metadata := map[string]any{"priority": -1, "note": "keep-note", "excluded_models": []string{"keep-model"}}
	credential.ApplyToMetadata(metadata)
	fileName := chatGPTWebCredentialFileName(credential.Email)
	existing := &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider,
		Metadata: metadata, Attributes: map[string]string{"priority": "-1", "note": "keep-note"},
		ProxyURL: "socks5h://proxy.example:1080", Disabled: true, Status: coreauth.StatusDisabled,
	}
	installed, errRegister := manager.Register(t.Context(), existing)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	payload, errMarshal := json.Marshal(credential)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}

	unchangedTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{field: "files", name: "same.json", data: string(payload)}})
	unchanged := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, unchangedTask.ID)
	if unchanged.Results[0].Status != "unchanged" {
		t.Fatalf("unchanged task = %+v", unchanged)
	}

	credential.AccessToken = "new-access"
	updatedPayload, errMarshal := json.Marshal(credential)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	updatedTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{field: "files", name: "updated.json", data: string(updatedPayload)}})
	updated := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, updatedTask.ID)
	if updated.Results[0].Status != "updated" {
		t.Fatalf("updated task = %+v", updated)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || current == nil {
		t.Fatal("updated credential is missing")
	}
	if current.ProxyURL != existing.ProxyURL || !current.Disabled || current.Attributes["priority"] != "-1" || current.Attributes["note"] != "keep-note" {
		t.Fatalf("local fields were not preserved: %+v", current)
	}
	if got := stringValue(current.Metadata, "access_token"); got != "new-access" {
		t.Fatalf("access token = %q", got)
	}
	if got := stringValue(current.Metadata, "note"); got != "keep-note" {
		t.Fatalf("metadata note = %q", got)
	}
}

func TestChatGPTWebImportTaskRejectsConcurrentCredentialUpdate(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	credential := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, CredentialUID: "web-uid", RefreshStrategy: chatgptwebauth.RefreshStrategyTokenOnly,
		Email: "stale@example.com", AccessToken: "old-access", Expired: time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		Cookies: []chatgptwebauth.Cookie{}, Persona: chatgptwebauth.DefaultPersona(), LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	fileName := chatGPTWebCredentialFileName(credential.Email)
	installed, errRegister := manager.Register(t.Context(), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider, Metadata: metadata, Status: coreauth.StatusActive,
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		changed := installed.Clone()
		changed.Metadata = cloneStringAnyMap(installed.Metadata)
		changed.Metadata["access_token"] = "concurrent-access"
		_, errUpdate := manager.Update(coreauth.WithSkipPersist(t.Context()), changed)
		return nil, errUpdate
	}
	payload, errMarshal := json.Marshal(credential)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{field: "files", name: "same.json", data: string(payload)}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "credential_changed" {
		t.Fatalf("task = %+v", completed)
	}
	current, ok := manager.GetByID(installed.ID)
	if !ok || stringValue(current.Metadata, "access_token") != "concurrent-access" {
		t.Fatalf("concurrent credential update was overwritten: %#v", current)
	}
}

func TestChatGPTWebImportTaskRejectsIdentityConflictWithoutLeakingSecrets(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	fileName := chatGPTWebCredentialFileName("target@example.com")
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID: fileName, FileName: fileName, Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{"type": chatgptwebauth.Provider, "email": "other@example.com", "access_token": "existing-secret"},
	}); errRegister != nil {
		t.Fatal(errRegister)
	}

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "conflict.json", data: `{"email":"target@example.com","access_token":"uploaded-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "identity_conflict" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("task = %+v", completed)
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebMutationTask(t, completed), "existing-secret", "uploaded-secret")
	terminal := performChatGPTWebManagementRequest(t, router, http.MethodGet, "/chatgpt-web/import-tasks/"+task.ID, "")
	if terminal.Code != http.StatusMultiStatus {
		t.Fatalf("failed task status = %d, want 207", terminal.Code)
	}
}

func TestChatGPTWebImportTaskReportsUncertainPersistence(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, _, store := newChatGPTWebManagementCountingTestHandler(t, executor)
	store.saveIfAbsentErr = errors.New("storage completion is unknown")
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "uncertain.json", data: `{"email":"uncertain@example.com","access_token":"uncertain-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "persist_uncertain" || completed.Results[0].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("task = %+v", completed)
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebMutationTask(t, completed), "uncertain-secret")
}

type chatGPTWebImportTestFile struct {
	field string
	name  string
	data  string
}

type chatGPTWebImportProxyResolver struct {
	finalID string
	calls   atomic.Int32
}

func (resolver *chatGPTWebImportProxyResolver) Resolve(_ context.Context, auth *coreauth.Auth) (coreauth.ResolvedProxy, error) {
	resolver.calls.Add(1)
	if auth != nil && auth.ID == resolver.finalID {
		return coreauth.ResolvedProxy{URL: "socks5h://final.example:1080"}, nil
	}
	return coreauth.ResolvedProxy{URL: "socks5h://staging.example:1080"}, nil
}

func (*chatGPTWebImportProxyResolver) ReportFailure(_ context.Context, _ *coreauth.Auth, err error) error {
	return err
}

func startChatGPTWebImportTask(t *testing.T, router http.Handler, files []chatGPTWebImportTestFile) chatGPTWebMutationTask {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for _, file := range files {
		part, errPart := writer.CreateFormFile(file.field, file.name)
		if errPart != nil {
			t.Fatal(errPart)
		}
		if _, errWrite := part.Write([]byte(file.data)); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/import-tasks", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("start import status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var task chatGPTWebMutationTask
	decodeChatGPTWebManagementResponse(t, recorder, &task)
	return task
}

func waitForChatGPTWebMutationTask(t *testing.T, router http.Handler, kind, id string) chatGPTWebMutationTask {
	t.Helper()
	path := "/chatgpt-web/" + kind + "-tasks/" + id
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		recorder := performChatGPTWebManagementRequest(t, router, http.MethodGet, path, "")
		if recorder.Code != http.StatusOK && recorder.Code != http.StatusMultiStatus {
			t.Fatalf("get %s task status = %d, body=%s", kind, recorder.Code, recorder.Body.String())
		}
		var task chatGPTWebMutationTask
		decodeChatGPTWebManagementResponse(t, recorder, &task)
		if isTerminalChatGPTWebLoginTaskState(task.State) {
			return task
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s task %s did not complete", kind, id)
	return chatGPTWebMutationTask{}
}

func mustMarshalChatGPTWebMutationTask(t *testing.T, task chatGPTWebMutationTask) string {
	t.Helper()
	payload, errMarshal := json.Marshal(task)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	return string(payload)
}
