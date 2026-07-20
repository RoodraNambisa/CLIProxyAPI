package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestStartChatGPTWebConversionTaskRejectsOversizedBody(t *testing.T) {
	h, _, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/conversion-tasks", strings.NewReader(strings.Repeat("x", (1<<20)+1)))
	request.Header.Set("Content-Type", "application/json")
	chatGPTWebManagementTestRouter(h).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
}

func TestStartChatGPTWebConversionTaskRejectsTrailingJSON(t *testing.T) {
	h, _, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/chatgpt-web/conversion-tasks", strings.NewReader(`{"names":["a.json"]}{"names":["b.json"]}`))
	request.Header.Set("Content-Type", "application/json")
	chatGPTWebManagementTestRouter(h).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
	}
}

func TestChatGPTWebConversionRejectsRuntimeOnlyCodexSource(t *testing.T) {
	h, manager, _ := newChatGPTWebManagementTestHandler(t, &chatGPTWebManagementTestExecutor{})
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID: "runtime-source", Provider: "codex", FileName: "runtime-source", Attributes: map[string]string{"runtime_only": "true"},
		Metadata: map[string]any{"type": "codex", "credential_uid": "runtime-uid", "access_token": "runtime-access", "email": "runtime@example.com", "account_id": "runtime-account"},
	}); errRegister != nil {
		t.Fatal(errRegister)
	}
	router := chatGPTWebManagementTestRouter(h)
	task := startChatGPTWebConversionTask(t, router, []string{"runtime-source"})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "source_not_managed" {
		t.Fatalf("task = %+v", completed)
	}
}

func TestChatGPTWebConversionClassifiesCredentialSnapshotFailure(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, store := newChatGPTWebManagementFailingListTestHandler(t, executor)
	source := registerChatGPTWebConversionSource(t, manager, "codex-snapshot.json", "snapshot@example.com", "")
	updated := source.Clone()
	updated.Metadata = cloneStringAnyMap(source.Metadata)
	updated.Metadata["credential_uid"] = "source-snapshot-uid"
	installed, current, errUpdate := manager.UpdateIfCurrentSourceHash(t.Context(), source, updated)
	if errUpdate != nil || !current || installed == nil {
		t.Fatalf("install source UID: current=%v auth=%#v err=%v", current, installed, errUpdate)
	}
	store.fail.Store(true)

	result := h.executeChatGPTWebConversion(t.Context(), chatGPTWebConversionInput{name: installed.FileName}, executor, manager, nil)
	if result.ErrorCategory != "credential_lookup_failed" || result.HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("conversion result = %+v", result)
	}
}

func TestChatGPTWebConversionUsesSourceProxyWithoutCopyingRefreshToken(t *testing.T) {
	const sourceProxy = "socks5h://user:pass@127.0.0.1:1080"
	executor := &chatGPTWebManagementTestExecutor{}
	var probed *coreauth.Auth
	executor.fetchFn = func(_ context.Context, auth *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		probed = auth.Clone()
		return nil, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-source.json", "linked@example.com", sourceProxy)

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.State != chatGPTWebLoginTaskCompleted || completed.Succeeded != 1 || completed.Results[0].Status != "created" {
		t.Fatalf("task = %+v", completed)
	}
	if probed == nil || probed.EffectiveProxyURL() != sourceProxy || stringValue(probed.Metadata, "access_token") != "codex-access" {
		t.Fatalf("probe auth = %+v", probed)
	}
	if stringValue(probed.Metadata, "refresh_token") != "" {
		t.Fatal("conversion probe received the Codex refresh token")
	}
	target, ok := manager.GetByID(completed.Results[0].Name)
	if !ok || target == nil {
		t.Fatal("linked Web credential is missing")
	}
	credential, errParse := chatgptwebauth.ParseCredential(target.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	if credential.RefreshStrategy != chatgptwebauth.RefreshStrategyCodexSource || credential.SourceAuthID != source.ID || credential.SourceCredentialUID == "" {
		t.Fatalf("credential = %+v", credential)
	}
	if credential.SourceProxyURL != sourceProxy {
		t.Fatalf("source proxy snapshot = %q, want %q", credential.SourceProxyURL, sourceProxy)
	}
	if credential.RefreshToken != "" || credential.IDToken != "" {
		t.Fatal("linked Web credential copied a Codex refresh or ID token")
	}
	resolved, errResolve := manager.ResolveProxyAuth(t.Context(), target)
	if errResolve != nil {
		t.Fatal(errResolve)
	}
	if got := resolved.EffectiveProxyURL(); got != sourceProxy {
		t.Fatalf("linked proxy = %q, want %q", got, sourceProxy)
	}
	if target.Attributes["priority"] != "-1" || target.Attributes["note"] != "source-note" {
		t.Fatalf("target attributes = %+v", target.Attributes)
	}
	currentSource, _ := manager.GetByID(source.ID)
	if coreauth.ChatGPTWebCredentialUID(currentSource) == "" {
		t.Fatal("source credential UID was not persisted")
	}
	if reservations := coreauth.ChatGPTWebActiveDependencyReservations(currentSource, time.Now()); len(reservations) != 0 {
		t.Fatalf("completed conversion retained dependency reservations: %+v", reservations)
	}
	if errDelete := manager.Delete(coreauth.WithSkipPersist(t.Context()), source.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	resolvedAfterRemoval, errResolveAfterRemoval := manager.ResolveProxyAuth(t.Context(), target)
	if errResolveAfterRemoval != nil {
		t.Fatal(errResolveAfterRemoval)
	}
	if got := resolvedAfterRemoval.EffectiveProxyURL(); got != sourceProxy {
		t.Fatalf("source proxy after removal = %q, want %q", got, sourceProxy)
	}
}

func TestChatGPTWebConversionIsConcurrentAndPreservesTargetLocalFields(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	var accountLock sync.Mutex
	executor.beginFn = func(ctx context.Context, _ string) (context.Context, func(), error) {
		accountLock.Lock()
		return ctx, accountLock.Unlock, nil
	}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-concurrent.json", "concurrent@example.com", "")

	first := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	second := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	firstCompleted := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, first.ID)
	secondCompleted := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, second.ID)
	if firstCompleted.Succeeded != 1 || secondCompleted.Succeeded != 1 {
		t.Fatalf("tasks = %+v / %+v", firstCompleted, secondCompleted)
	}
	targetName := firstCompleted.Results[0].Name
	if targetName == "" {
		targetName = secondCompleted.Results[0].Name
	}
	target, ok := manager.GetByID(targetName)
	if !ok || target == nil {
		t.Fatal("linked target is missing")
	}
	updated := target.Clone()
	updated.Metadata = cloneStringAnyMap(target.Metadata)
	updated.Metadata["headers"] = map[string]any{"X-Test": "keep"}
	updated.Metadata["excluded_models"] = []string{"keep-model"}
	updated.Attributes = map[string]string{"priority": target.Attributes["priority"], "note": target.Attributes["note"], "custom": "keep"}
	installed, current, errUpdate := manager.UpdateIfCurrent(t.Context(), target, updated)
	if errUpdate != nil || !current {
		t.Fatalf("update target: current=%v err=%v", current, errUpdate)
	}

	repeat := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	repeated := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, repeat.ID)
	if repeated.Succeeded != 1 || repeated.Results[0].Status != "updated" {
		t.Fatalf("repeat task = %+v", repeated)
	}
	currentTarget, _ := manager.GetByID(installed.ID)
	if currentTarget.Attributes["custom"] != "keep" || currentTarget.Metadata["headers"] == nil || currentTarget.Metadata["excluded_models"] == nil {
		t.Fatalf("target local fields were lost: %+v", currentTarget)
	}
}

func TestChatGPTWebConversionUpdatesPersistedLinkedTargetMissingFromRuntime(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-persisted-target.json", "persisted-target@example.com", "")
	withUID := source.Clone()
	withUID.Metadata = cloneStringAnyMap(source.Metadata)
	withUID.Metadata["credential_uid"] = "persisted-source-uid"
	installedSource, current, errUpdate := manager.UpdateIfCurrentSourceHash(t.Context(), source, withUID)
	if errUpdate != nil || !current || installedSource == nil {
		t.Fatalf("install source UID: current=%v auth=%#v err=%v", current, installedSource, errUpdate)
	}
	source = installedSource
	identitySource := source.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	credential := &chatgptwebauth.Credential{
		Type:                chatgptwebauth.Provider,
		CredentialUID:       "persisted-target-uid",
		CredentialMode:      chatgptwebauth.CredentialModeLinkedCodex,
		RefreshStrategy:     chatgptwebauth.RefreshStrategyCodexSource,
		SourceAuthID:        source.ID,
		SourceCredentialUID: coreauth.ChatGPTWebCredentialUID(source),
		SourceIdentity:      coreauth.ChatGPTWebCredentialReferenceValue(identitySource),
		Email:               "persisted-target@example.com",
		AccessToken:         "stale-target-access",
		Cookies:             []chatgptwebauth.Cookie{},
		LifecycleState:      chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	credential.ApplyToMetadata(metadata)
	targetName := chatGPTWebCredentialFileName(credential.Email)
	if _, errSave := h.tokenStore.Save(t.Context(), &coreauth.Auth{
		ID: targetName, FileName: targetName, Provider: chatgptwebauth.Provider, Metadata: metadata,
	}); errSave != nil {
		t.Fatal(errSave)
	}
	if _, loaded := manager.GetByID(targetName); loaded {
		t.Fatal("persisted target unexpectedly exists in runtime")
	}

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Succeeded != 1 || completed.Results[0].Status != "updated" || completed.Results[0].Name != targetName {
		t.Fatalf("conversion = %+v", completed)
	}
	persisted, errList := manager.PersistedAuthSnapshot(t.Context())
	if errList != nil {
		t.Fatal(errList)
	}
	linkedCount := 0
	for _, auth := range persisted {
		if coreauth.ChatGPTWebLinkedSourceUID(auth) != coreauth.ChatGPTWebCredentialUID(source) {
			continue
		}
		linkedCount++
		if got := stringValue(auth.Metadata, "access_token"); got != "codex-access" {
			t.Fatalf("persisted target access token = %q", got)
		}
	}
	if linkedCount != 1 {
		t.Fatalf("persisted linked target count = %d, want 1", linkedCount)
	}
}

func TestChatGPTWebConversionReusesSourceUIDAcrossEmailChangeAndPreservesIdentityReference(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-email-change.json", "old@example.com", "")
	withUser := source.Clone()
	withUser.Metadata = cloneStringAnyMap(source.Metadata)
	withUser.Metadata["user_id"] = "stable-user"
	var errUpdate error
	source, errUpdate = manager.Update(t.Context(), withUser)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}

	firstTask := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	first := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, firstTask.ID)
	if first.Succeeded != 1 {
		t.Fatalf("first conversion = %+v", first)
	}
	target, ok := manager.GetByID(first.Results[0].Name)
	if !ok || target == nil {
		t.Fatal("first linked target is missing")
	}
	firstCredential, errParse := chatgptwebauth.ParseCredential(target.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}

	currentSource, _ := manager.GetByID(source.ID)
	changedSource := currentSource.Clone()
	changedSource.Metadata = cloneStringAnyMap(currentSource.Metadata)
	changedSource.Metadata["email"] = "new@example.com"
	delete(changedSource.Metadata, "user_id")
	changedSource, errUpdate = manager.Update(t.Context(), changedSource)
	if errUpdate != nil {
		t.Fatal(errUpdate)
	}
	secondTask := startChatGPTWebConversionTask(t, router, []string{changedSource.FileName})
	second := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, secondTask.ID)
	if second.Succeeded != 1 || second.Results[0].Status != "updated" || second.Results[0].Name != target.FileName {
		t.Fatalf("second conversion = %+v", second)
	}
	updatedTarget, ok := manager.GetByID(target.ID)
	if !ok || updatedTarget == nil {
		t.Fatal("linked target disappeared after email change")
	}
	updatedCredential, errParse := chatgptwebauth.ParseCredential(updatedTarget.Metadata)
	if errParse != nil {
		t.Fatal(errParse)
	}
	identitySource := changedSource.Clone()
	identitySource.Provider = chatgptwebauth.Provider
	wantReference := coreauth.MergeChatGPTWebCredentialReferenceValues(firstCredential.SourceIdentity, coreauth.ChatGPTWebCredentialReferenceValue(identitySource))
	if updatedCredential.Email != "old@example.com" || updatedCredential.SourceIdentity != wantReference {
		t.Fatalf("updated linked credential = %+v, want reference %q", updatedCredential, wantReference)
	}
	linkedCount := 0
	for _, auth := range manager.List() {
		if coreauth.ChatGPTWebLinkedSourceUID(auth) == updatedCredential.SourceCredentialUID {
			linkedCount++
		}
	}
	if linkedCount != 1 {
		t.Fatalf("linked credentials for source UID = %d, want 1", linkedCount)
	}
}

func TestChatGPTWebConversionClassifiesProbeFailures(t *testing.T) {
	tests := []struct {
		name         string
		err          error
		wantCategory string
		wantStatus   int
	}{
		{name: "unauthorized", err: conversionStatusError{status: http.StatusUnauthorized, path: "/backend-api/models?history_and_training_disabled=false"}, wantCategory: "token_incompatible", wantStatus: http.StatusUnprocessableEntity},
		{name: "forbidden", err: conversionStatusError{status: http.StatusForbidden, path: "/backend-api/models"}, wantCategory: "token_incompatible", wantStatus: http.StatusUnprocessableEntity},
		{name: "bootstrap unauthorized", err: conversionStatusError{status: http.StatusUnauthorized, path: "/"}, wantCategory: "probe_unavailable", wantStatus: http.StatusServiceUnavailable},
		{name: "rate limited", err: conversionStatusError{status: http.StatusTooManyRequests}, wantCategory: "probe_unavailable", wantStatus: http.StatusServiceUnavailable},
		{name: "upstream unavailable", err: conversionStatusError{status: http.StatusBadGateway}, wantCategory: "probe_unavailable", wantStatus: http.StatusServiceUnavailable},
		{name: "network", err: errors.New("network unavailable"), wantCategory: "probe_unavailable", wantStatus: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := &chatGPTWebManagementTestExecutor{fetchFn: func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
				return nil, test.err
			}}
			h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
			router := chatGPTWebManagementTestRouter(h)
			source := registerChatGPTWebConversionSource(t, manager, "codex-probe.json", "probe@example.com", "")
			task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
			completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
			result := completed.Results[0]
			if result.ErrorCategory != test.wantCategory || result.HTTPStatus != test.wantStatus {
				t.Fatalf("result = %+v", result)
			}
			if _, ok := manager.GetByID(chatGPTWebCredentialFileName("probe@example.com")); ok {
				t.Fatal("failed probe persisted a Web credential")
			}
		})
	}
}

func TestChatGPTWebConversionRejectsSourceGenerationChangedAfterProbe(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-generation.json", "generation@example.com", "")
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		current, _ := manager.GetByID(source.ID)
		updated := current.Clone()
		updated.Metadata = cloneStringAnyMap(current.Metadata)
		updated.Metadata["note"] = "changed-after-probe"
		updated.Attributes = make(map[string]string, len(current.Attributes))
		for key, value := range current.Attributes {
			updated.Attributes[key] = value
		}
		updated.Attributes["note"] = "changed-after-probe"
		_, installed, errUpdate := manager.UpdateIfCurrent(t.Context(), current, updated)
		if errUpdate != nil || !installed {
			t.Fatalf("update source generation: current=%v err=%v", installed, errUpdate)
		}
		return nil, nil
	}

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Results[0].ErrorCategory != "source_changed" {
		t.Fatalf("task = %+v", completed)
	}
	if _, ok := manager.GetByID(chatGPTWebCredentialFileName("generation@example.com")); ok {
		t.Fatal("conversion persisted a target from a stale source generation")
	}
}

func TestChatGPTWebConversionReservationRejectsExternalSourceReplacement(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-reservation-race.json", "reservation-race@example.com", "")
	withUID := source.Clone()
	withUID.Metadata = cloneStringAnyMap(source.Metadata)
	withUID.Metadata["credential_uid"] = "reservation-source-uid"
	installed, current, errUpdate := manager.UpdateIfCurrentSourceHash(t.Context(), source, withUID)
	if errUpdate != nil || !current || installed == nil {
		t.Fatalf("install source UID: current=%v auth=%#v err=%v", current, installed, errUpdate)
	}
	source = installed
	path := filepath.Join(authDir, source.FileName)
	store.setBeforeConditionalSave(func() {
		replaceManagementDependencyAuthFile(t, path, "external reservation replacement", "")
	})

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Results[0].ErrorCategory != "source_changed" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("conversion = %+v", completed)
	}
	if _, ok := manager.GetByID(chatGPTWebCredentialFileName("reservation-race@example.com")); ok {
		t.Fatal("conversion persisted a target after its source reservation lost the CAS race")
	}
	assertManagementDependencyAuthNote(t, path, "external reservation replacement")
}

func TestChatGPTWebConversionRollsBackTargetWhenSourceDisappearsAfterPersist(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-finalize-race.json", "finalize-race@example.com", "")
	store.setAfterSave(func(savedID string) {
		if savedID == source.ID {
			return
		}
		store.setAfterSave(nil)
		if errDelete := store.FileTokenStore.Delete(t.Context(), source.ID); errDelete != nil {
			t.Fatal(errDelete)
		}
	})

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Results[0].ErrorCategory != "source_changed" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("conversion = %+v", completed)
	}
	targetID := chatGPTWebCredentialFileName("finalize-race@example.com")
	if _, exists := manager.GetByID(targetID); exists {
		t.Fatal("conversion kept a linked target after source finalization failed")
	}
	if _, errStat := os.Stat(filepath.Join(authDir, targetID)); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("target file after rollback: %v", errStat)
	}
}

func TestChatGPTWebConversionReleasesReservationAfterTargetCASRollback(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-target-race.json", "target-race@example.com", "")

	first := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	created := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, first.ID)
	if created.Succeeded != 1 {
		t.Fatalf("initial conversion = %+v", created)
	}
	targetName := created.Results[0].Name
	targetPath := filepath.Join(authDir, targetName)
	currentSource, sourceExists := manager.GetByID(source.ID)
	if !sourceExists || coreauth.ChatGPTWebCredentialUID(currentSource) == "" {
		t.Fatalf("source after initial conversion = %#v", currentSource)
	}
	store.setAfterSave(func(savedID string) {
		if savedID != source.ID {
			return
		}
		store.setAfterSave(nil)
		store.setBeforeConditionalSave(func() {
			replaceManagementDependencyAuthFile(t, targetPath, "external target replacement", "")
		})
	})

	second := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, second.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "target_conflict" {
		t.Fatalf("conversion = %+v", completed)
	}
	latestSource, sourceExists := manager.GetByID(source.ID)
	if !sourceExists {
		t.Fatal("source disappeared after target conflict")
	}
	if reservations := coreauth.ChatGPTWebActiveDependencyReservations(latestSource, time.Now()); len(reservations) != 0 {
		t.Fatalf("target CAS rollback retained reservations: %+v", reservations)
	}
	assertManagementDependencyAuthNote(t, targetPath, "external target replacement")
}

func TestChatGPTWebConversionReturnsUncertainWhenTargetRollbackFails(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-uncertain-rollback.json", "uncertain-rollback@example.com", "")
	targetID := chatGPTWebCredentialFileName("uncertain-rollback@example.com")
	store.setAfterSave(func(savedID string) {
		if savedID != targetID {
			return
		}
		store.setAfterSave(nil)
		replaceManagementDependencyAuthFile(t, filepath.Join(authDir, targetID), "external target replacement", "")
		if errDelete := store.FileTokenStore.Delete(t.Context(), source.ID); errDelete != nil {
			t.Fatal(errDelete)
		}
	})

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "persist_uncertain" || completed.Results[0].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("conversion = %+v", completed)
	}
	assertManagementDependencyAuthNote(t, filepath.Join(authDir, targetID), "external target replacement")
}

func TestChatGPTWebConversionMapsConcurrentTargetCreateToConflict(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-target-create.json", "target-create@example.com", "")
	targetID := chatGPTWebCredentialFileName("target-create@example.com")
	store.setAfterSave(func(savedID string) {
		if savedID != source.ID {
			return
		}
		store.setAfterSave(nil)
		_, errCreate := store.FileTokenStore.SaveIfAbsent(t.Context(), &coreauth.Auth{
			ID: targetID, FileName: targetID, Provider: chatgptwebauth.Provider,
			Metadata: map[string]any{"type": chatgptwebauth.Provider, "email": "target-create@example.com", "access_token": "other"},
		})
		if errCreate != nil {
			t.Fatal(errCreate)
		}
	})

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Failed != 1 || completed.Results[0].ErrorCategory != "target_conflict" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("conversion = %+v", completed)
	}
	currentSource, sourceExists := manager.GetByID(source.ID)
	if !sourceExists {
		t.Fatal("source disappeared after target conflict")
	}
	if reservations := coreauth.ChatGPTWebActiveDependencyReservations(currentSource, time.Now()); len(reservations) != 0 {
		t.Fatalf("concurrent target conflict retained reservations: %+v", reservations)
	}
}

func TestChatGPTWebConversionCancellationClassification(t *testing.T) {
	base := chatGPTWebMutationTaskResult{SourceName: "codex.json", Status: "failed"}
	for name, result := range map[string]chatGPTWebMutationTaskResult{
		"probe":   failedChatGPTWebProbeResult(base, context.Canceled),
		"refresh": failedChatGPTWebConversionRefresh(base, context.Canceled),
		"uid":     failedChatGPTWebConversionSourceUpdate(base, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, context.Canceled)),
	} {
		t.Run(name, func(t *testing.T) {
			if result.Status != chatGPTWebLoginResultCanceled || result.ErrorCategory != "canceled" {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestChatGPTWebConversionTimeoutIsNotCancellation(t *testing.T) {
	base := chatGPTWebMutationTaskResult{SourceName: "codex.json", Status: "failed"}
	for name, result := range map[string]chatGPTWebMutationTaskResult{
		"probe":   failedChatGPTWebProbeResult(base, context.DeadlineExceeded),
		"refresh": failedChatGPTWebConversionRefresh(base, context.DeadlineExceeded),
		"uid":     failedChatGPTWebConversionSourceUpdate(base, coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, context.DeadlineExceeded)),
	} {
		t.Run(name, func(t *testing.T) {
			if result.Status == chatGPTWebLoginResultCanceled || result.ErrorCategory == "canceled" {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestChatGPTWebConversionRefreshErrorStatusMapping(t *testing.T) {
	base := chatGPTWebMutationTaskResult{SourceName: "codex.json", Status: "failed"}
	tests := []struct {
		code       string
		wantStatus int
	}{
		{code: "source_auth_disabled", wantStatus: http.StatusConflict},
		{code: "source_identity_changed", wantStatus: http.StatusConflict},
		{code: "source_refresh_unavailable", wantStatus: http.StatusServiceUnavailable},
		{code: "source_token_unavailable", wantStatus: http.StatusUnprocessableEntity},
	}
	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			result := failedChatGPTWebConversionRefresh(base, chatGPTWebManagementCodedError(test.code))
			if result.ErrorCategory != test.code || result.HTTPStatus != test.wantStatus {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestChatGPTWebConversionSourceUpdateClassification(t *testing.T) {
	base := chatGPTWebMutationTaskResult{SourceName: "codex.json", Status: "failed"}
	tests := []struct {
		name         string
		err          error
		wantCategory string
		wantStatus   int
	}{
		{name: "stale", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, authfileguard.ErrPersistGenerationStale), wantCategory: "source_changed", wantStatus: http.StatusConflict},
		{name: "uncertain", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, errors.New("store unavailable")), wantCategory: "source_persist_uncertain", wantStatus: http.StatusServiceUnavailable},
		{name: "committed warning", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeCommitted, errors.New("cleanup warning")), wantCategory: "source_persist_uncertain", wantStatus: http.StatusServiceUnavailable},
		{name: "rolled back failure", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, errors.New("disk full")), wantCategory: "source_persist_failed", wantStatus: http.StatusServiceUnavailable},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := failedChatGPTWebConversionSourceUpdate(base, test.err)
			if result.ErrorCategory != test.wantCategory || result.HTTPStatus != test.wantStatus {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestEnsureCodexCredentialUIDRejectsExternalSourceReplacement(t *testing.T) {
	_, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "", false))
	path := filepath.Join(authDir, source.FileName)
	store.setBeforeConditionalSave(func() { replaceManagementDependencyAuthFile(t, path, "external uid replacement", "") })

	if _, errEnsure := ensureCodexCredentialUID(t.Context(), manager, source, codexSourceWebIdentity(source)); errEnsure == nil {
		t.Fatal("ensureCodexCredentialUID() accepted an externally replaced source")
	}
	assertManagementDependencyAuthNote(t, path, "external uid replacement")
	current, _ := manager.GetByID(source.ID)
	if coreauth.ChatGPTWebCredentialUID(current) != "" {
		t.Fatalf("runtime credential UID = %q, want empty", coreauth.ChatGPTWebCredentialUID(current))
	}
}

func TestEnsureCodexCredentialUIDWaitsForCredentialRefreshLock(t *testing.T) {
	_, manager, _, _ := newChatGPTWebDependencyRaceManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "", false))
	releaseRefresh, errLock := manager.LockCredentialRefresh(t.Context(), source.ID)
	if errLock != nil {
		t.Fatal(errLock)
	}
	type ensureResult struct {
		auth *coreauth.Auth
		err  error
	}
	done := make(chan ensureResult, 1)
	go func() {
		auth, errEnsure := ensureCodexCredentialUID(t.Context(), manager, source, codexSourceWebIdentity(source))
		done <- ensureResult{auth: auth, err: errEnsure}
	}()
	select {
	case result := <-done:
		releaseRefresh()
		t.Fatalf("UID update bypassed refresh lock: auth=%#v err=%v", result.auth, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseRefresh()
	select {
	case result := <-done:
		if result.err != nil || result.auth == nil || coreauth.ChatGPTWebCredentialUID(result.auth) == "" {
			t.Fatalf("UID update after refresh lock = %#v, %v", result.auth, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("UID update did not resume after refresh lock release")
	}
}

func TestChatGPTWebConversionHoldsSourceRefreshLockThroughProbeAndCommit(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-lock.json", "lock@example.com", "")
	acquired := make(chan struct{})
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		go func() {
			release, errLock := manager.LockCredentialRefresh(t.Context(), source.ID)
			if errLock != nil {
				return
			}
			close(acquired)
			release()
		}()
		select {
		case <-acquired:
			return nil, errors.New("source refresh lock was released during conversion probe")
		case <-time.After(50 * time.Millisecond):
		}
		return nil, nil
	}

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Succeeded != 1 {
		t.Fatalf("conversion = %+v", completed)
	}
	select {
	case <-acquired:
	case <-time.After(time.Second):
		t.Fatal("source refresh lock remained held after conversion commit")
	}
}

func TestChatGPTWebConversionRejectsSourceChangedAfterProbe(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-changing.json", "changing@example.com", "")
	executor.fetchFn = func(context.Context, *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
		current, _ := manager.GetByID(source.ID)
		updated := current.Clone()
		updated.Metadata = cloneStringAnyMap(current.Metadata)
		updated.Metadata["access_token"] = "rotated-after-probe"
		_, installed, errUpdate := manager.UpdateIfCurrent(t.Context(), current, updated)
		if errUpdate != nil || !installed {
			t.Fatalf("rotate source: current=%v err=%v", installed, errUpdate)
		}
		return nil, nil
	}

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Results[0].ErrorCategory != "source_changed" {
		t.Fatalf("task = %+v", completed)
	}
	if _, ok := manager.GetByID(chatGPTWebCredentialFileName("changing@example.com")); ok {
		t.Fatal("conversion persisted a target from a stale source token")
	}
}

func TestChatGPTWebConversionRejectsExternalTargetReplacement(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	manager.RegisterExecutor(executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-target-race.json", "target-race@example.com", "")

	first := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	firstResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, first.ID)
	if firstResult.Succeeded != 1 {
		t.Fatalf("first conversion = %+v", firstResult)
	}
	targetName := chatGPTWebCredentialFileName("target-race@example.com")
	targetPath := filepath.Join(authDir, targetName)
	store.setBeforeConditionalSave(func() {
		replaceManagementDependencyAuthFile(t, targetPath, "external target replacement", "")
	})

	second := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	secondResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, second.ID)
	if secondResult.Results[0].ErrorCategory != "target_conflict" || secondResult.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("second conversion = %+v", secondResult)
	}
	assertManagementDependencyAuthNote(t, targetPath, "external target replacement")
}

func TestChatGPTWebConversionRejectsNativeTargetAndInvalidSource(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-conflict.json", "conflict@example.com", "")
	native := &chatgptwebauth.Credential{
		Type: chatgptwebauth.Provider, RefreshStrategy: chatgptwebauth.RefreshStrategyTokenOnly,
		Email: "conflict@example.com", AccessToken: "native-access", Cookies: []chatgptwebauth.Cookie{}, LifecycleState: chatgptwebauth.LifecycleActive,
	}
	metadata := make(map[string]any)
	native.ApplyToMetadata(metadata)
	name := chatGPTWebCredentialFileName(native.Email)
	if _, errRegister := manager.Register(t.Context(), &coreauth.Auth{ID: name, FileName: name, Provider: chatgptwebauth.Provider, Metadata: metadata}); errRegister != nil {
		t.Fatal(errRegister)
	}

	conflict := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	conflictResult := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, conflict.ID)
	if conflictResult.Results[0].ErrorCategory != "target_conflict" {
		t.Fatalf("conflict task = %+v", conflictResult)
	}
	invalidRequest := performChatGPTWebJSONRequest(t, router, "/chatgpt-web/conversion-tasks", map[string]any{
		"names": []string{source.FileName}, "target_provider": "chatgpt-web", "mode": "move", "validate": true,
	})
	if invalidRequest.Code != http.StatusBadRequest {
		t.Fatalf("invalid request status = %d, body=%s", invalidRequest.Code, invalidRequest.Body.String())
	}
}

func TestChatGPTWebConversionRejectsDisabledSource(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	router := chatGPTWebManagementTestRouter(h)
	source := registerChatGPTWebConversionSource(t, manager, "codex-disabled.json", "disabled@example.com", "")
	updated := source.Clone()
	updated.Disabled = true
	updated.Status = coreauth.StatusDisabled
	updated.Metadata = cloneStringAnyMap(source.Metadata)
	updated.Metadata["disabled"] = true
	if _, errUpdate := manager.Update(t.Context(), updated); errUpdate != nil {
		t.Fatalf("disable source: %v", errUpdate)
	}

	task := startChatGPTWebConversionTask(t, router, []string{source.FileName})
	completed := waitForChatGPTWebMutationTask(t, router, chatGPTWebMutationTaskConversion, task.ID)
	if completed.Results[0].ErrorCategory != "source_disabled" || completed.Results[0].HTTPStatus != http.StatusConflict {
		t.Fatalf("task = %+v", completed)
	}
}

func registerChatGPTWebConversionSource(t *testing.T, manager *coreauth.Manager, name, email, proxyURL string) *coreauth.Auth {
	t.Helper()
	expires := time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	source := &coreauth.Auth{
		ID: name, FileName: name, Provider: "codex", ProxyURL: proxyURL,
		Attributes: map[string]string{"priority": "-1", "note": "source-note"},
		Metadata: map[string]any{
			"type": "codex", "email": email, "account_id": "account-" + email,
			"access_token": "codex-access", "refresh_token": "codex-refresh-secret", "expired": expires,
			"priority": -1, "note": "source-note",
		},
	}
	installed, errRegister := manager.Register(t.Context(), source)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return installed
}

func startChatGPTWebConversionTask(t *testing.T, router http.Handler, names []string) chatGPTWebMutationTask {
	t.Helper()
	recorder := performChatGPTWebJSONRequest(t, router, "/chatgpt-web/conversion-tasks", map[string]any{
		"names": names, "target_provider": chatgptwebauth.Provider, "mode": "copy", "validate": true,
	})
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("start conversion status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	var task chatGPTWebMutationTask
	decodeChatGPTWebManagementResponse(t, recorder, &task)
	return task
}

func performChatGPTWebJSONRequest(t *testing.T, router http.Handler, path string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	data, errMarshal := json.Marshal(payload)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(data))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

type conversionStatusError struct {
	status int
	path   string
}

func (err conversionStatusError) Error() string                 { return http.StatusText(err.status) }
func (err conversionStatusError) StatusCode() int               { return err.status }
func (err conversionStatusError) ChatGPTWebRequestPath() string { return err.path }
