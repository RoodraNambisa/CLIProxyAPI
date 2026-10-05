package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestManualReloginPersistenceWithConcurrentFileChanges(t *testing.T) {
	for _, external := range []bool{false, true} {
		name := "managed runtime update"
		if external {
			name = "external credential replacement"
		}
		t.Run(name, func(t *testing.T) {
			executor := &chatGPTWebManagementTestExecutor{}
			h, manager, authDir := newChatGPTWebManagementTestHandler(t, executor)
			journalPath := configureChatGPTWebManualReloginJournalForTest(t, h)
			credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{
				Email: "persist@example.com", Password: "private-password", TOTPSecret: "JBSWY3DPEHPK3PXP",
			})
			credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
			installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(credential.Email), credential, nil, nil)
			if errPersist != nil {
				t.Fatal(errPersist)
			}
			filePath := filepath.Join(authDir, installed.FileName)
			executor.reloginFn = func(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
				if external {
					replacement := auth.Clone()
					replacement.Metadata["access_token"] = "externally-replaced-token"
					payload, errMarshal := coreauth.CanonicalMetadataBytes(replacement)
					if errMarshal != nil {
						return nil, false, errMarshal
					}
					if errWrite := os.WriteFile(filePath, payload, 0o600); errWrite != nil {
						return nil, false, errWrite
					}
				} else {
					_, current, errUpdate := manager.MutateRuntimeMetadataIfCurrent(ctx, auth, func(candidate *coreauth.Auth) {
						candidate.Metadata["image_quota_remaining"] = 7
					})
					if errUpdate != nil || !current {
						return nil, false, errUpdate
					}
				}
				updated := auth.Clone()
				updated.Metadata["access_token"] = "new-login-token"
				updated.Metadata["lifecycle_state"] = string(chatgptwebauth.LifecycleActive)
				return manager.UpdateChatGPTWebReloginIfCurrent(ctx, auth, updated)
			}
			router := chatGPTWebManagementTestRouter(h)
			accepted := performChatGPTWebManagementRequest(t, router, http.MethodPost,
				"/chatgpt-web/auth-files/"+url.PathEscape(installed.FileName)+"/relogin?async=true", "")
			if accepted.Code != http.StatusAccepted {
				t.Fatalf("start operation = %d: %s", accepted.Code, accepted.Body.String())
			}
			var response struct {
				Operation chatGPTWebManualReloginOperationSnapshot `json:"operation"`
			}
			decodeChatGPTWebManagementResponse(t, accepted, &response)
			completed := waitForChatGPTWebManualReloginOperation(t, router, response.Operation.OperationID)
			wantToken := "new-login-token"
			if external {
				wantToken = "externally-replaced-token"
				if completed.Status != chatGPTWebManualReloginOperationFailed || completed.HTTPStatus != http.StatusConflict || completed.ErrorCategory != "credential_changed" {
					t.Fatalf("external replacement result = %+v, want credential_changed/409", completed)
				}
				if completed.FailureStage != "credential_persist" || completed.PersistenceOutcome != "rolled_back" || completed.PersistenceReason != "source_generation_changed" {
					t.Fatalf("missing persistence diagnostic: %+v", completed)
				}
			} else if completed.Status != chatGPTWebManualReloginOperationCompleted || completed.Outcome != "succeeded" {
				t.Fatalf("managed runtime update result = %+v", completed)
			}
			payload, errRead := os.ReadFile(filePath)
			if errRead != nil {
				t.Fatal(errRead)
			}
			var persisted map[string]any
			if errJSON := json.Unmarshal(payload, &persisted); errJSON != nil {
				t.Fatal(errJSON)
			}
			if persisted["access_token"] != wantToken {
				t.Fatal("unexpected persisted token")
			}
			if !external && persisted["image_quota_remaining"] != float64(7) {
				t.Fatal("lost concurrent runtime update")
			}
			journal, errJournal := os.ReadFile(journalPath)
			if errJournal != nil {
				t.Fatal(errJournal)
			}
			assertChatGPTWebManagementSecretsAbsent(t, string(journal), credential.Password, credential.TOTPSecret, "new-login-token", "externally-replaced-token")
		})
	}
}

func TestManualReloginPersistenceOutcomesAreDurableAndSafe(t *testing.T) {
	const secret = "private-store-auth-material"
	for _, test := range []struct {
		name         string
		err          error
		current      bool
		wantStatus   int
		wantCategory string
		wantOutcome  string
		wantReason   string
	}{
		{name: "permission denied", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, &os.PathError{Op: "open", Path: secret, Err: os.ErrPermission}), wantStatus: 500, wantCategory: "persist_failed", wantOutcome: "rolled_back", wantReason: "permission_denied"},
		{name: "storage full", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, syscall.ENOSPC), wantStatus: 500, wantCategory: "persist_failed", wantOutcome: "rolled_back", wantReason: "storage_full"},
		{name: "read only", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, syscall.EROFS), wantStatus: 500, wantCategory: "persist_failed", wantOutcome: "rolled_back", wantReason: "read_only_filesystem"},
		{name: "missing path", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, os.ErrNotExist), wantStatus: 500, wantCategory: "persist_failed", wantOutcome: "rolled_back", wantReason: "storage_path_missing"},
		{name: "uncertain stale generation", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, authfileguard.ErrPersistGenerationStale), wantStatus: 503, wantCategory: "persist_uncertain", wantOutcome: "uncertain", wantReason: "source_generation_changed"},
		{name: "uncertain cancellation", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeUncertain, context.Canceled), wantStatus: 503, wantCategory: "persist_uncertain", wantOutcome: "uncertain", wantReason: "canceled"},
		{name: "unknown store error", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeRolledBack, errors.New(secret)), wantStatus: 500, wantCategory: "persist_failed", wantOutcome: "rolled_back", wantReason: "storage_error"},
		{name: "committed and installed", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeCommitted, errors.New(secret)), current: true, wantStatus: 200, wantOutcome: "committed", wantReason: "storage_error"},
		{name: "committed without installation", err: coreauth.NewSaveOutcomeError(coreauth.SaveOutcomeCommitted, errors.New(secret)), wantStatus: 503, wantCategory: "persist_uncertain", wantOutcome: "committed", wantReason: "storage_error"},
	} {
		t.Run(test.name, func(t *testing.T) {
			executor := &chatGPTWebManagementTestExecutor{reloginFn: func(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
				return auth.Clone(), test.current, fmt.Errorf("private wrapper %s: %w", secret, test.err)
			}}
			h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
			journalPath := configureChatGPTWebManualReloginJournalForTest(t, h)
			credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{Email: "outcome@example.com", Password: "private-password"})
			installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(credential.Email), credential, nil, nil)
			if errPersist != nil {
				t.Fatal(errPersist)
			}
			router := chatGPTWebManagementTestRouter(h)
			path := "/chatgpt-web/auth-files/" + url.PathEscape(installed.FileName) + "/relogin"
			syncResult := performChatGPTWebManagementRequest(t, router, http.MethodPost, path, "")
			if syncResult.Code != test.wantStatus {
				t.Fatalf("sync status = %d, want %d", syncResult.Code, test.wantStatus)
			}
			assertChatGPTWebManagementSecretsAbsent(t, syncResult.Body.String(), secret, credential.Password)
			accepted := performChatGPTWebManagementRequest(t, router, http.MethodPost, path+"?async=true", "")
			if accepted.Code != http.StatusAccepted {
				t.Fatalf("async admission = %d", accepted.Code)
			}
			var response struct {
				Operation chatGPTWebManualReloginOperationSnapshot `json:"operation"`
			}
			decodeChatGPTWebManagementResponse(t, accepted, &response)
			completed := waitForChatGPTWebManualReloginOperation(t, router, response.Operation.OperationID)
			if completed.HTTPStatus != test.wantStatus || completed.ErrorCategory != test.wantCategory || completed.PersistenceOutcome != test.wantOutcome || completed.PersistenceReason != test.wantReason || completed.FailureStage != "credential_persist" {
				t.Fatalf("async persistence diagnostic = %+v", completed)
			}
			if (completed.Status == chatGPTWebManualReloginOperationCompleted) != (test.wantStatus == 200) || !completed.ResultDurable {
				t.Fatalf("incorrect completion/durability: %+v", completed)
			}
			restored := newChatGPTWebLoginTaskManager()
			t.Cleanup(func() {
				if err := restored.shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if errLoad := restored.configureManualReloginJournal(journalPath); errLoad != nil {
				t.Fatal(errLoad)
			}
			loaded, ok := restored.getManualReloginOperation(completed.OperationID)
			if !ok || loaded.PersistenceReason != test.wantReason || loaded.PersistenceOutcome != test.wantOutcome || loaded.ErrorCategory != test.wantCategory || !loaded.ResultDurable {
				t.Fatalf("diagnostics not retained after journal reload: %+v", loaded)
			}
			journal, errRead := os.ReadFile(journalPath)
			if errRead != nil {
				t.Fatal(errRead)
			}
			assertChatGPTWebManagementSecretsAbsent(t, string(journal), secret, credential.Password)
		})
	}
}

type manualReloginFailingSaveStore struct{ coreauth.Store }

func (store *manualReloginFailingSaveStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", &os.PathError{Op: "open", Path: "private-path", Err: os.ErrPermission}
}

func TestManualReloginUntypedStoreFailureIsNotAuthenticationFailure(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{Email: "untyped@example.com", Password: "password"})
	credential.LifecycleState = chatgptwebauth.LifecycleReauthRequired
	installed, errPersist := h.persistChatGPTWebLoginCredential(t.Context(), manager, chatGPTWebCredentialFileName(credential.Email), credential, nil, nil)
	if errPersist != nil {
		t.Fatal(errPersist)
	}
	manager.SetStore(&manualReloginFailingSaveStore{Store: h.tokenStore})
	executor.reloginFn = func(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
		updated := auth.Clone()
		updated.Metadata["access_token"] = "unsaved-token"
		updated.Metadata["lifecycle_state"] = string(chatgptwebauth.LifecycleActive)
		return manager.UpdateChatGPTWebReloginIfCurrent(ctx, auth, updated)
	}
	status, response := h.executeChatGPTWebManualRelogin(t.Context(), installed, executor, manager)
	if status != http.StatusServiceUnavailable || response["error_category"] != "persist_uncertain" || response["persistence_reason"] != "permission_denied" || response["persistence_outcome"] != "unknown" {
		t.Fatalf("untyped store failure = %d %+v", status, response)
	}
	current, _ := manager.GetByID(installed.ID)
	if current.Metadata["access_token"] == "unsaved-token" || current.LifecycleSelectable() {
		t.Fatal("failed commit activated the unsaved credential")
	}
}
