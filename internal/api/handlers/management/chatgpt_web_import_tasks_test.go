package management

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
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

func TestChatGPTWebImportTaskPersistsWebAuthnV1AndConfirmsCapability(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	payload, privateKey, credentialID, userHandle := chatGPTWebImportWebAuthnFixture(t)

	capabilities := performChatGPTWebManagementRequest(t, router, http.MethodGet, "/chatgpt-web/capabilities", "")
	if capabilities.Code != http.StatusOK || capabilities.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("capabilities status = %d headers=%v body=%s", capabilities.Code, capabilities.Header(), capabilities.Body.String())
	}
	if got := capabilities.Body.String(); !strings.Contains(got, `"credential_schema_versions":[1,2,3]`) || !strings.Contains(got, `"webauthn_v1"`) || !strings.Contains(got, `"api798_login_v1"`) || !strings.Contains(got, `"advanced_account_security_v1"`) {
		t.Fatalf("capabilities body = %s", got)
	}

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "passkey.json",
		data:  payload,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Failed != 0 || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	result := completed.Results[0]
	if result.CredentialSchemaVersion != 2 || !result.WebAuthnV1Persisted || len(result.PersistedFeatures) != 1 || result.PersistedFeatures[0] != "webauthn_v1" {
		t.Fatalf("import result = %+v", result)
	}
	assertChatGPTWebManagementSecretsAbsent(t, mustMarshalChatGPTWebMutationTask(t, completed), privateKey, credentialID)
	installed, ok := manager.GetByID(result.Name)
	if !ok {
		t.Fatal("persisted credential was not registered")
	}
	credential, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || credential.WebAuthn == nil || credential.WebAuthn.PrivateKeyPKCS8 != privateKey {
		t.Fatalf("persisted credential = %#v error=%v", credential, errParse)
	}
	listEntry, errMarshalEntry := json.Marshal(h.buildAuthFileEntry(installed))
	if errMarshalEntry != nil {
		t.Fatal(errMarshalEntry)
	}
	assertChatGPTWebManagementSecretsAbsent(t, string(listEntry), privateKey, credentialID, userHandle)
	raw, errRead := os.ReadFile(filepath.Join(authDir, result.Name))
	if errRead != nil {
		t.Fatal(errRead)
	}
	if !bytes.Contains(raw, []byte(privateKey)) {
		t.Fatal("persisted auth file omitted WebAuthn private key")
	}
}

func TestChatGPTWebImportTaskPersistsAdvancedAccountSecurityAndMergesRuntimeState(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	payload, advanced := chatGPTWebImportAdvancedSecurityFixture(t)

	firstTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "advanced-security.json",
		data:  payload,
	}})
	first := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, firstTask.ID)
	if first.Succeeded != 1 || len(first.Results) != 1 {
		t.Fatalf("first task = %+v", first)
	}
	result := first.Results[0]
	if result.CredentialSchemaVersion != chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity ||
		!result.AdvancedSecurityPersisted || !slices.Contains(result.PersistedFeatures, chatgptwebauth.AdvancedAccountSecurityFeature) {
		t.Fatalf("first result = %+v", result)
	}
	serializedTask := mustMarshalChatGPTWebMutationTask(t, first)
	assertChatGPTWebManagementSecretsAbsent(t, serializedTask,
		advanced.Passkeys[0].Credential.PrivateKeyPKCS8,
		advanced.Passkeys[1].Credential.CredentialID,
		advanced.RecoveryKeys[0].RecoveryKey,
		advanced.RecoveryKeys[0].AuthenticationSecretBase64,
	)

	installed, ok := manager.GetByID(result.Name)
	if !ok {
		t.Fatal("persisted advanced credential was not registered")
	}
	current, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || !reflect.DeepEqual(current.AdvancedAccountSecurity, advanced) {
		t.Fatalf("persisted advanced credential = %#v error=%v", current, errParse)
	}
	current.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount = 17
	current.AdvancedAccountSecurity.Passkeys[0].Credential.LastUsedAt = "2026-08-06T03:00:00Z"
	_, currentInstance, errMutate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), installed, func(candidate *coreauth.Auth) {
		current.ApplyToMetadata(candidate.Metadata)
	})
	if errMutate != nil || !currentInstance {
		t.Fatalf("MutateRuntimeMetadataIfCurrent() current=%t error=%v", currentInstance, errMutate)
	}

	secondTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "advanced-security-copy.json",
		data:  payload,
	}})
	second := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, secondTask.ID)
	if second.Succeeded != 1 || len(second.Results) != 1 || !second.Results[0].AdvancedSecurityPersisted {
		t.Fatalf("second task = %+v", second)
	}
	reinstalled, ok := manager.GetByID(result.Name)
	if !ok {
		t.Fatal("reimported advanced credential is missing")
	}
	reparsed, errReparse := chatgptwebauth.ParseCredential(reinstalled.Metadata)
	if errReparse != nil || reparsed.AdvancedAccountSecurity == nil {
		t.Fatalf("reparsed advanced credential = %#v error=%v", reparsed, errReparse)
	}
	if got := reparsed.AdvancedAccountSecurity.Passkeys[0].Credential.SignCount; got != 17 {
		t.Fatalf("sign_count = %d, want 17", got)
	}
	if !reflect.DeepEqual(reparsed.AdvancedAccountSecurity.RecoveryKeys, advanced.RecoveryKeys) {
		t.Fatal("reimport changed recovery keys")
	}
}

func TestChatGPTWebImportTaskPersistsAPI798LoginAndConfirmsCapability(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	rawURL := "https://api798.com/get_code?email=mailbox%40example.com&auth_code=opaque%252Bvalue"
	payload := `{"email":"mailbox@example.com","access_token":"access-secret","login_method":"api798","api798_url":"` + rawURL + `"}`

	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "api798.json",
		data:  payload,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Failed != 0 || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	result := completed.Results[0]
	if len(result.PersistedFeatures) != 1 || result.PersistedFeatures[0] != chatgptwebauth.API798LoginFeature {
		t.Fatalf("persisted features = %v", result.PersistedFeatures)
	}
	if strings.Contains(mustMarshalChatGPTWebMutationTask(t, completed), rawURL) {
		t.Fatal("import task response leaked api798_url")
	}
	installed, ok := manager.GetByID(result.Name)
	if !ok {
		t.Fatal("persisted credential was not registered")
	}
	credential, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || credential.LoginMethod != chatgptwebauth.LoginMethodAPI798 || credential.API798URL != rawURL {
		t.Fatalf("persisted credential = %#v error=%v", credential, errParse)
	}
	listEntry, errMarshalEntry := json.Marshal(h.buildAuthFileEntry(installed))
	if errMarshalEntry != nil {
		t.Fatal(errMarshalEntry)
	}
	if bytes.Contains(listEntry, []byte(rawURL)) {
		t.Fatal("normal auth list leaked api798_url")
	}
	raw, errRead := os.ReadFile(filepath.Join(authDir, result.Name))
	if errRead != nil {
		t.Fatal(errRead)
	}
	var persisted map[string]any
	if errUnmarshal := json.Unmarshal(raw, &persisted); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	if persisted["api798_url"] != rawURL || persisted["login_method"] != "api798" {
		t.Fatalf("persisted auth file omitted API798 settings: %#v", persisted)
	}
}

func TestChatGPTWebReimportPreservesAPI798LoginSettings(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	rawURL := "https://api798.com/get_code?email=mailbox%40example.com&auth_code=opaque"
	first := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "api798.json",
		data:  `{"email":"mailbox@example.com","access_token":"first","login_method":"api798","api798_url":"` + rawURL + `"}`,
	}})
	firstResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, first.ID)
	if firstResult.Succeeded != 1 {
		t.Fatalf("first task = %+v", firstResult)
	}

	second := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "replacement.json",
		data:  `{"email":"mailbox@example.com","access_token":"second"}`,
	}})
	secondResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, second.ID)
	if secondResult.Succeeded != 1 || len(secondResult.Results) != 1 {
		t.Fatalf("second task = %+v", secondResult)
	}
	installed, ok := manager.GetByID(secondResult.Results[0].Name)
	if !ok {
		t.Fatal("reimported credential was not registered")
	}
	credential, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || credential.LoginMethod != chatgptwebauth.LoginMethodAPI798 || credential.API798URL != rawURL {
		t.Fatalf("reimported credential = %#v error=%v", credential, errParse)
	}
}

func TestChatGPTWebImportTaskDoesNotRollBackWebAuthnRuntimeState(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	payload, _, _, _ := chatGPTWebImportWebAuthnFixture(t)

	firstTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "passkey.json",
		data:  payload,
	}})
	first := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, firstTask.ID)
	if first.Succeeded != 1 || len(first.Results) != 1 {
		t.Fatalf("first task = %+v", first)
	}
	installed, ok := manager.GetByID(first.Results[0].Name)
	if !ok {
		t.Fatal("persisted credential was not registered")
	}
	advanced, errParse := chatgptwebauth.ParseCredential(installed.Metadata)
	if errParse != nil || advanced.WebAuthn == nil {
		t.Fatalf("ParseCredential() credential=%#v error=%v", advanced, errParse)
	}
	advanced.WebAuthn.SignCount = 9
	advanced.WebAuthn.LastUsedAt = "2026-08-05T12:00:00Z"
	updated, current, errUpdate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), installed, func(candidate *coreauth.Auth) {
		advanced.ApplyToMetadata(candidate.Metadata)
	})
	if errUpdate != nil || !current {
		t.Fatalf("MutateRuntimeMetadataIfCurrent() current=%t error=%v", current, errUpdate)
	}

	secondTask := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "old-passkey-copy.json",
		data:  payload,
	}})
	second := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, secondTask.ID)
	if second.Succeeded != 1 || len(second.Results) != 1 || !second.Results[0].WebAuthnV1Persisted {
		t.Fatalf("second task = %+v", second)
	}
	currentAuth, ok := manager.GetByID(updated.ID)
	if !ok {
		t.Fatal("reimported credential is missing")
	}
	currentCredential, errCurrent := chatgptwebauth.ParseCredential(currentAuth.Metadata)
	if errCurrent != nil || currentCredential.WebAuthn == nil {
		t.Fatalf("ParseCredential() credential=%#v error=%v", currentCredential, errCurrent)
	}
	if currentCredential.WebAuthn.SignCount != 9 || currentCredential.WebAuthn.LastUsedAt != "2026-08-05T12:00:00Z" {
		t.Fatalf("WebAuthn runtime state = %+v", currentCredential.WebAuthn)
	}
}

func TestChatGPTWebImportTaskQueuesConfiguredAccountInfoRefresh(t *testing.T) {
	type triggerCall struct {
		authID string
		force  bool
	}
	triggered := make(chan triggerCall, 2)
	executor := &chatGPTWebManagementTestExecutor{}
	executor.accountInfoTriggerFn = func(authID string, force bool) bool {
		triggered <- triggerCall{authID: authID, force: force}
		return true
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	enabled := true
	h.cfg.ChatGPTWeb.Import.RefreshAccountInfoAfterUpload = &enabled
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "initial-quota.json",
		data:  `{"email":"quota@example.com","access_token":"quota-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	if completed.Results[0].AccountInfoRefreshState != "queued" {
		t.Fatalf("account-info state = %q, want queued", completed.Results[0].AccountInfoRefreshState)
	}
	select {
	case call := <-triggered:
		if call.authID != completed.Results[0].Name || call.force {
			t.Fatalf("account-info trigger = %+v, result = %+v", call, completed.Results[0])
		}
	default:
		t.Fatal("successful upload did not trigger account-info refresh")
	}
	select {
	case call := <-triggered:
		t.Fatalf("unexpected duplicate account-info trigger: %+v", call)
	default:
	}
}

func TestChatGPTWebImportTaskReportsReusedAccountInfoRefresh(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	executor.accountInfoStateFn = func(string, bool) string { return "reused" }
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	enabled := true
	h.cfg.ChatGPTWeb.Import.RefreshAccountInfoAfterUpload = &enabled
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files",
		name:  "reused-quota.json",
		data:  `{"email":"reused-quota@example.com","access_token":"quota-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || len(completed.Results) != 1 || completed.Results[0].AccountInfoRefreshState != "reused" {
		t.Fatalf("task = %+v", completed)
	}
}

func TestChatGPTWebImportTaskDefersAccountInfoRefreshUntilSessionRefresh(t *testing.T) {
	var triggerCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.accountInfoTriggerFn = func(string, bool) bool {
		triggerCalls.Add(1)
		return true
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	enabled := true
	h.cfg.ChatGPTWeb.Import.RefreshAccountInfoAfterUpload = &enabled
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files",
		name:  "session-priority.json",
		data:  `{"email":"session-priority@example.com","access_token":"access-secret","session_cookie":"session-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || len(completed.Results) != 1 {
		t.Fatalf("task = %+v", completed)
	}
	result := completed.Results[0]
	if result.SessionRefreshState != "queued" || result.AccountInfoRefreshState != "queued" {
		t.Fatalf("background states = session %q, account info %q", result.SessionRefreshState, result.AccountInfoRefreshState)
	}
	if triggerCalls.Load() != 0 {
		t.Fatalf("account-info trigger calls = %d, want 0 before Session refresh", triggerCalls.Load())
	}
	stored, ok := manager.GetByID(result.Name)
	if !ok || stored == nil ||
		!coreauth.ChatGPTWebImportIntent(stored, coreauth.ChatGPTWebImportSessionIntent) ||
		!coreauth.ChatGPTWebImportIntent(stored, coreauth.ChatGPTWebImportAccountInfoIntent) {
		t.Fatalf("stored credential = %#v, want both deferred intents", stored)
	}
}

func TestChatGPTWebImportTaskDoesNotCallUpstreamByDefault(t *testing.T) {
	var normalizeCalls atomic.Int32
	var fetchCalls atomic.Int32
	var triggerCalls atomic.Int32
	var loginOperationCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.beginFn = func(ctx context.Context, _ string) (context.Context, func(), error) {
		loginOperationCalls.Add(1)
		return ctx, func() {}, nil
	}
	executor.normalizeFn = func(context.Context, *chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error) {
		normalizeCalls.Add(1)
		return nil, errors.New("unexpected normalization")
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		fetchCalls.Add(1)
		return nil, errors.New("unexpected model probe")
	}
	executor.accountInfoTriggerFn = func(string, bool) bool {
		triggerCalls.Add(1)
		return true
	}
	h, _, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files",
		name:  "invalid.json",
		data:  `{"email":"invalid@example.com","access_token":"invalid-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Failed != 0 {
		t.Fatalf("task = %+v", completed)
	}
	if got := normalizeCalls.Load(); got != 0 {
		t.Fatalf("normalize calls = %d, want 0", got)
	}
	if got := fetchCalls.Load(); got != 0 {
		t.Fatalf("model probe calls = %d, want 0", got)
	}
	if got := triggerCalls.Load(); got != 0 {
		t.Fatalf("account-info trigger calls = %d, want 0", got)
	}
	if got := loginOperationCalls.Load(); got != 0 {
		t.Fatalf("login operation calls = %d, want 0", got)
	}
	result := completed.Results[0]
	if result.ModelValidationState != "skipped" || result.AccountInfoRefreshState != "skipped" {
		t.Fatalf("background states = %+v", result)
	}
}

func TestChatGPTWebImportTaskAllowsSameEmailForDistinctNamedWorkspaces(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	files := []chatGPTWebImportTestFile{
		{field: "files", name: "first.json", data: `{"email":"same@example.com","account_id":"workspace-a","access_token":"first-secret"}`},
		{field: "files", name: "second.json", data: `{"email":"same@example.com","account_id":"workspace-b","access_token":"second-secret"}`},
	}
	task := startChatGPTWebImportTaskWithNames(t, router, files, []string{"workspace-a", "workspace-b.json"})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 2 || completed.Failed != 0 {
		t.Fatalf("task = %+v", completed)
	}
	for _, name := range []string{"workspace-a.json", "workspace-b.json"} {
		if _, ok := manager.GetByID(name); !ok {
			t.Fatalf("custom-named workspace %q is missing", name)
		}
	}
}

func TestChatGPTWebImportTaskSameCustomNameReplacesWorkspace(t *testing.T) {
	h, manager, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	router := chatGPTWebManagementTestRouter(h)
	first := startChatGPTWebImportTaskWithNames(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "first.json", data: `{"email":"same@example.com","account_id":"workspace-a","access_token":"first-secret"}`,
	}}, []string{"shared"})
	if completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, first.ID); completed.Succeeded != 1 {
		t.Fatalf("first task = %+v", completed)
	}
	firstStored, ok := manager.GetByID("shared.json")
	if !ok || firstStored == nil || coreauth.ChatGPTWebCredentialUID(firstStored) == "" {
		t.Fatalf("first stored credential = %#v", firstStored)
	}
	firstUID := coreauth.ChatGPTWebCredentialUID(firstStored)
	secondData, errMarshal := json.Marshal(map[string]string{
		"email":          "same@example.com",
		"account_id":     "workspace-b",
		"access_token":   "second-secret",
		"credential_uid": firstUID,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	second := startChatGPTWebImportTaskWithNames(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "second.json", data: string(secondData),
	}}, []string{"shared.json"})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, second.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" {
		t.Fatalf("second task = %+v", completed)
	}
	stored, ok := manager.GetByID("shared.json")
	if !ok || stored == nil || stored.Metadata["account_id"] != "workspace-b" {
		t.Fatalf("stored credential = %#v", stored)
	}
	if secondUID := coreauth.ChatGPTWebCredentialUID(stored); secondUID == "" || secondUID == firstUID {
		t.Fatalf("replacement credential UID = %q, want a new non-empty value", secondUID)
	}
}

func TestChatGPTWebImportTaskRejectsDuplicateCustomNames(t *testing.T) {
	h, _, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	router := chatGPTWebManagementTestRouter(h)
	files := []chatGPTWebImportTestFile{
		{field: "files", name: "first.json", data: `{"email":"first@example.com","access_token":"first-secret"}`},
		{field: "files", name: "second.json", data: `{"email":"second@example.com","access_token":"second-secret"}`},
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writeChatGPTWebImportParts(t, writer, files, []string{"same", "same.json"})
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/import-tasks", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "must be unique") {
		t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestChatGPTWebImportTaskRejectsSessionOnlyCredentialWithoutLocalIdentity(t *testing.T) {
	var normalizeCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(context.Context, *chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error) {
		normalizeCalls.Add(1)
		return nil, errors.New("unexpected normalization")
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files", name: "session.json", data: `{"session_cookie":"session-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "identity_missing" ||
		completed.Results[0].HTTPStatus != http.StatusUnprocessableEntity {
		t.Fatalf("task = %+v", completed)
	}
	if normalizeCalls.Load() != 0 || len(manager.List()) != 0 {
		t.Fatalf("normalize calls = %d, auths = %d", normalizeCalls.Load(), len(manager.List()))
	}
}

func TestChatGPTWebImportTaskPersistsRefreshableCredentialAndQueuesSessionRefresh(t *testing.T) {
	var normalizeCalls atomic.Int32
	var fetchCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.normalizeFn = func(context.Context, *chatgptwebauth.Credential, string) (*chatgptwebauth.Credential, error) {
		normalizeCalls.Add(1)
		return nil, errors.New("unexpected normalization")
	}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		fetchCalls.Add(1)
		return nil, errors.New("unexpected model probe")
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files", name: "session.json",
		data: `{"email":"session@example.com","session_cookie":"session-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].SessionRefreshState != "queued" {
		t.Fatalf("task = %+v", completed)
	}
	if normalizeCalls.Load() != 0 || fetchCalls.Load() != 0 {
		t.Fatalf("normalize calls = %d, fetch calls = %d", normalizeCalls.Load(), fetchCalls.Load())
	}
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil || stored.LifecycleState() != coreauth.LifecycleStateRefreshing ||
		!coreauth.ChatGPTWebImportIntent(stored, coreauth.ChatGPTWebImportSessionIntent) {
		t.Fatalf("stored credential = %#v", stored)
	}
}

func TestChatGPTWebImportTaskDoesNotProbeOpaqueValidAccessToken(t *testing.T) {
	var fetchCalls atomic.Int32
	executor := &chatGPTWebManagementTestExecutor{}
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		fetchCalls.Add(1)
		return nil, conversionStatusError{status: http.StatusUnauthorized, path: "/backend-api/models"}
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	forceRefresh := false
	h.cfg.ChatGPTWeb.ForceSessionRefreshOnImport = &forceRefresh
	task := startChatGPTWebImportTask(t, chatGPTWebManagementTestRouter(h), []chatGPTWebImportTestFile{{
		field: "files", name: "opaque.json",
		data: `{"email":"opaque@example.com","access_token":"opaque-access","session_cookie":"session-secret"}`,
	}})
	completed := waitForChatGPTWebMutationTask(t, chatGPTWebManagementTestRouter(h), chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].SessionRefreshState != "skipped" {
		t.Fatalf("task = %+v", completed)
	}
	if fetchCalls.Load() != 0 {
		t.Fatalf("fetch calls = %d, want 0", fetchCalls.Load())
	}
	stored, _ := manager.GetByID(completed.Results[0].Name)
	if stored == nil || stringValue(stored.Metadata, "access_token") != "opaque-access" || !stored.LifecycleSelectable() {
		t.Fatalf("stored credential = %#v", stored)
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

func TestChatGPTWebImportTaskDoesNotWaitForExistingCredentialRefresh(t *testing.T) {
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
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	releaseRefresh()
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" {
		t.Fatalf("task = %+v", completed)
	}
	select {
	case <-beginStarted:
		t.Fatal("import entered the login operation")
	case <-normalizeStarted:
		t.Fatal("import performed a synchronous credential refresh")
	default:
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

func TestChatGPTWebImportTaskSerializesConcurrentCreatesForSameIdentity(t *testing.T) {
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, &chatGPTWebManagementTestExecutor{})
	store.block.Store(true)
	router := chatGPTWebManagementTestRouter(h)
	first := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "first.json", data: `{"email":"same-concurrent@example.com","access_token":"first-access"}`,
	}})
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("first import did not reach persistence")
	}
	second := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "second.json", data: `{"email":"same-concurrent@example.com","access_token":"second-access"}`,
	}})
	close(store.release)
	store.block.Store(false)
	firstResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, first.ID)
	secondResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, second.ID)
	if firstResult.Succeeded != 1 {
		t.Fatalf("first task = %+v", firstResult)
	}
	if secondResult.Failed != 1 || secondResult.Results[0].ErrorCategory != "credential_changed" {
		t.Fatalf("second task = %+v", secondResult)
	}
	if got := len(manager.List()); got != 1 {
		t.Fatalf("registered credentials = %d, want 1", got)
	}
	stored, ok := manager.GetByID(firstResult.Results[0].Name)
	if !ok || stringValue(stored.Metadata, "access_token") != "first-access" {
		t.Fatalf("stored credential = %#v", stored)
	}
}

func TestChatGPTWebImportTaskReservesStrongIdentityAcrossDistinctNames(t *testing.T) {
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, &chatGPTWebManagementTestExecutor{})
	store.block.Store(true)
	router := chatGPTWebManagementTestRouter(h)
	first := startChatGPTWebImportTaskWithNames(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "first.json",
		data: `{"email":"shared@example.com","account_id":"shared-account","user_id":"shared-user","access_token":"first-access"}`,
	}}, []string{"first-workspace"})
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("first import did not reach persistence")
	}

	second := startChatGPTWebImportTaskWithNames(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "second.json",
		data: `{"email":"shared@example.com","account_id":"shared-account","user_id":"shared-user","access_token":"second-access"}`,
	}}, []string{"second-workspace"})
	secondResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, second.ID)
	if secondResult.Failed != 1 || secondResult.Results[0].ErrorCategory != "identity_conflict" {
		t.Fatalf("second task = %+v", secondResult)
	}

	close(store.release)
	store.block.Store(false)
	firstResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, first.ID)
	if firstResult.Succeeded != 1 {
		t.Fatalf("first task = %+v", firstResult)
	}
	if _, exists := manager.GetByID("second-workspace.json"); exists {
		t.Fatal("conflicting strong identity was persisted under a second name")
	}
}

func TestChatGPTWebImportTaskCancellationAfterCommitKeepsPersistedCredential(t *testing.T) {
	h, manager, store := newChatGPTWebManagementCountingTestHandler(t, &chatGPTWebManagementTestExecutor{})
	store.block.Store(true)
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebImportTask(t, router, []chatGPTWebImportTestFile{{
		field: "files", name: "committed.json",
		data: `{"email":"committed@example.com","access_token":"committed-access"}`,
	}})
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("import did not reach persistence")
	}
	if canceled := performChatGPTWebManagementRequest(t, router, http.MethodDelete, "/chatgpt-web/import-tasks/"+task.ID, ""); canceled.Code != http.StatusOK {
		t.Fatalf("cancel status = %d, body=%s", canceled.Code, canceled.Body.String())
	}
	close(store.release)
	store.block.Store(false)

	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskImport, task.ID)
	if completed.Succeeded != 1 || completed.Canceled != 0 || completed.Results[0].Status != "created" {
		t.Fatalf("task = %+v", completed)
	}
	stored, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || stored == nil || stringValue(stored.Metadata, "access_token") != "committed-access" {
		t.Fatalf("persisted credential = %#v", stored)
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

func chatGPTWebImportWebAuthnFixture(t *testing.T) (payload, privateKeyPKCS8, credentialID, userHandle string) {
	t.Helper()
	raw, errRead := os.ReadFile("testdata/chatgpt-web-webauthn-v1.json")
	if errRead != nil {
		t.Fatal(errRead)
	}
	credential, errDecode := chatgptwebauth.DecodeImportCredential(raw)
	if errDecode != nil || credential.WebAuthn == nil {
		t.Fatalf("DecodeImportCredential() credential=%#v error=%v", credential, errDecode)
	}
	return string(raw), credential.WebAuthn.PrivateKeyPKCS8, credential.WebAuthn.CredentialID, credential.WebAuthn.UserHandle
}

func chatGPTWebImportAdvancedSecurityFixture(t *testing.T) (string, *chatgptwebauth.AdvancedAccountSecurityCredential) {
	t.Helper()
	newAuthenticator := func(label string, signCount uint32, transports []string, backupEligible, backupState bool) chatgptwebauth.WebAuthnCredential {
		privateKey, errGenerate := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if errGenerate != nil {
			t.Fatal(errGenerate)
		}
		privateDER, errMarshal := x509.MarshalPKCS8PrivateKey(privateKey)
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		return chatgptwebauth.WebAuthnCredential{
			Version:         chatgptwebauth.WebAuthnCredentialVersion,
			CredentialID:    base64.RawURLEncoding.EncodeToString([]byte("credential-" + label)),
			UserHandle:      base64.RawURLEncoding.EncodeToString([]byte("user-handle-" + label)),
			RPID:            chatgptwebauth.WebAuthnRPID,
			Origin:          chatgptwebauth.WebAuthnOrigin,
			Algorithm:       chatgptwebauth.WebAuthnES256Algorithm,
			PrivateKeyPKCS8: base64.StdEncoding.EncodeToString(privateDER),
			SignCount:       signCount,
			MFAFactorID:     "factor-" + label,
			Transports:      transports,
			UserPresent:     true,
			UserVerified:    true,
			BackupEligible:  backupEligible,
			BackupState:     backupState,
			CreatedAt:       "2026-08-05T00:00:00Z",
			LastUsedAt:      "2026-08-05T01:00:00Z",
		}
	}
	recoveryKeys := make([]chatgptwebauth.AdvancedAccountRecoveryKey, 5)
	for index := range recoveryKeys {
		recoveryKeys[index] = chatgptwebauth.AdvancedAccountRecoveryKey{
			RecoveryKey:                fmt.Sprintf("display-%d", index),
			AccountRecoveryCode:        fmt.Sprintf("account-%d", index),
			XWingPublicKeyBase64:       base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 1}),
			AuthenticationSecretBase64: base64.StdEncoding.EncodeToString([]byte{byte(index + 1), 2}),
		}
	}
	advanced := &chatgptwebauth.AdvancedAccountSecurityCredential{
		Version: chatgptwebauth.AdvancedAccountSecurityCredentialVersion,
		Enabled: true,
		Passkeys: []chatgptwebauth.AdvancedAccountSecurityPasskeyCredential{
			{Kind: "passkey", IsNonDeviceBound: true, Credential: newAuthenticator("passkey", 6, []string{"hybrid"}, true, true)},
			{Kind: "security-key", IsSecurityKey: true, Credential: newAuthenticator("security-key", 4, []string{"usb"}, false, false)},
		},
		RecoveryKeys: recoveryKeys,
		EnrolledAt:   "2026-08-05T00:00:00Z",
		VerifiedAt:   "2026-08-05T01:00:00Z",
		LoginMethod:  "passkey",
	}
	payload, errMarshal := json.Marshal(map[string]any{
		"type":                      chatgptwebauth.Provider,
		"credential_schema_version": chatgptwebauth.CredentialSchemaVersionAdvancedAccountSecurity,
		"email":                     "advanced@example.com",
		"access_token":              "advanced-access-token",
		"login_method":              chatgptwebauth.LoginMethodAdvancedSecurityPasskey,
		"advanced_account_security": advanced,
	})
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	return string(payload), advanced
}

func startChatGPTWebImportTask(t *testing.T, router http.Handler, files []chatGPTWebImportTestFile) chatGPTWebMutationTask {
	return startChatGPTWebImportTaskWithNames(t, router, files, nil)
}

func startChatGPTWebImportTaskWithNames(t *testing.T, router http.Handler, files []chatGPTWebImportTestFile, targetNames []string) chatGPTWebMutationTask {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	writeChatGPTWebImportParts(t, writer, files, targetNames)
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

func writeChatGPTWebImportParts(t *testing.T, writer *multipart.Writer, files []chatGPTWebImportTestFile, targetNames []string) {
	t.Helper()
	for _, file := range files {
		part, errPart := writer.CreateFormFile(file.field, file.name)
		if errPart != nil {
			t.Fatal(errPart)
		}
		if _, errWrite := part.Write([]byte(file.data)); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	for _, name := range targetNames {
		if errField := writer.WriteField("names", name); errField != nil {
			t.Fatal(errField)
		}
	}
	if errClose := writer.Close(); errClose != nil {
		t.Fatal(errClose)
	}
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
