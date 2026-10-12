package management

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestManualReloginVerificationReasonsSurviveJournalAndAuthSummary(t *testing.T) {
	for _, test := range []struct {
		code, stage                string
		upstream, managementStatus int
		state                      chatgptwebauth.LifecycleState
	}{
		{"password_reset_required", "password_verify", 401, 409, chatgptwebauth.LifecycleInteractionRequired},
		{"password_reset_required", "password_verify", 400, 409, chatgptwebauth.LifecycleInteractionRequired},
		{"incorrect_code", "mfa_verify", 403, 422, chatgptwebauth.LifecycleReauthRequired},
	} {
		t.Run(test.code+"/"+http.StatusText(test.upstream), func(t *testing.T) {
			executor := &chatGPTWebManagementTestExecutor{}
			h, manager, dir := newChatGPTWebManagementTestHandler(t, executor)
			journalPath := configureChatGPTWebManualReloginJournalForTest(t, h)
			credential := activeChatGPTWebManagementTestCredential(chatgptwebauth.LoginInput{Email: "fixture@example.com", Password: "private-password", TOTPSecret: "private-totp"})
			installed, err := h.persistChatGPTWebLoginCredential(t.Context(), manager, "fixture.json", credential, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			executor.reloginFn = func(ctx context.Context, auth *coreauth.Auth) (*coreauth.Auth, bool, error) {
				failure := &chatgptwebauth.AuthError{Code: test.code, State: test.state, LifecycleState: test.state, StatusCode: test.upstream, Terminal: true, FailureStage: test.stage, Attempts: 1, Message: "private-upstream-body"}
				updated := auth.Clone()
				updated.Metadata["lifecycle_state"] = string(test.state)
				updated.Metadata["lifecycle_reason"] = test.code
				chatgptwebauth.RecordLifecycleFailure(updated.Metadata, failure)
				updated.LastError = coreauth.NewProviderError(updated, failure)
				committed, current, errSave := manager.UpdateChatGPTWebReloginIfCurrent(ctx, auth, updated)
				if errSave != nil {
					return committed, current, errSave
				}
				return committed, current, failure
			}
			router := chatGPTWebManagementTestRouter(h)
			accepted := performChatGPTWebManagementRequest(t, router, http.MethodPost, "/chatgpt-web/auth-files/"+url.PathEscape(installed.FileName)+"/relogin?async=true", "")
			if accepted.Code != http.StatusAccepted {
				t.Fatalf("admission status = %d", accepted.Code)
			}
			var response struct {
				Operation chatGPTWebManualReloginOperationSnapshot `json:"operation"`
			}
			decodeChatGPTWebManagementResponse(t, accepted, &response)
			completed := waitForChatGPTWebManualReloginOperation(t, router, response.Operation.OperationID)
			if completed.Status != chatGPTWebManualReloginOperationFailed || completed.Outcome != "failed" || completed.ErrorCategory != test.code || completed.LifecycleState != string(test.state) || completed.FailureStage != test.stage || completed.HTTPStatus != test.managementStatus || completed.UpstreamHTTPStatus != test.upstream || !completed.ResultDurable {
				t.Fatalf("operation classification = %+v", completed)
			}
			current, exists := manager.GetByID(installed.ID)
			if !exists || current.Disabled || current.LifecycleSelectable() || chatgptwebauth.DeadLifecycleDeletionAllowed(current.Metadata) {
				t.Fatal("manual verification deleted, disabled or activated the credential")
			}
			entry := h.buildAuthFileEntry(current)
			if entry["lifecycle_reason"] != test.code || entry["reason"] != test.code {
				t.Fatal("auth summary lost the verification reason")
			}
			journal, err := os.ReadFile(journalPath)
			if err != nil {
				t.Fatal(err)
			}
			assertChatGPTWebManagementSecretsAbsent(t, string(journal), "private-upstream-body", credential.Password, credential.TOTPSecret, credential.AccessToken)
			restored := newChatGPTWebLoginTaskManager()
			t.Cleanup(func() { _ = restored.shutdown(context.Background()) })
			if err := restored.configureManualReloginJournal(journalPath); err != nil {
				t.Fatal(err)
			}
			loaded, ok := restored.getManualReloginOperation(completed.OperationID)
			if !ok || loaded.ErrorCategory != test.code || loaded.UpstreamHTTPStatus != test.upstream || loaded.FailureStage != test.stage {
				t.Fatal("verification evidence did not survive journal reload")
			}
			if _, err := os.Stat(filepath.Join(dir, installed.FileName)); err != nil {
				t.Fatal("manual verification credential was not retained")
			}
		})
	}
}

func TestLoginTaskPreservesPasswordResetReasonAndUpstreamStatus(t *testing.T) {
	executor := &chatGPTWebManagementTestExecutor{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
		credential := activeChatGPTWebManagementTestCredential(input)
		failure := &chatgptwebauth.AuthError{Code: "password_reset_required", State: chatgptwebauth.LifecycleInteractionRequired, StatusCode: 401, Terminal: true, FailureStage: "password_verify"}
		credential.LifecycleState, credential.LifecycleReason = failure.State, failure.Code
		credential.RecordLifecycleFailure(failure)
		return credential, failure
	}}
	h, manager, _ := newChatGPTWebManagementTestHandler(t, executor)
	result := h.executeChatGPTWebLogin(t.Context(), chatGPTWebLoginInput{Email: "fixture@example.com", Password: "password"}, executor, manager, func() (context.Context, bool) { return t.Context(), true })
	if result.Status != chatGPTWebLoginResultFailed || result.ErrorCategory != "password_reset_required" || result.UpstreamHTTPStatus != 401 || result.FailureStage != "password_verify" || result.LifecycleState != "interaction_required" {
		t.Fatalf("login task result = %+v", result)
	}
}
