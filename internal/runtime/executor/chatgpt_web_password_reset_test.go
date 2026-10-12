package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebVerificationFailureStopsBackgroundReloginAndAllowsManualRecovery(t *testing.T) {
	for _, test := range []struct {
		code, stage string
		status      int
		state       chatgptwebauth.LifecycleState
	}{
		{"password_reset_required", "password_verify", 401, chatgptwebauth.LifecycleInteractionRequired},
		{"password_reset_required", "password_verify", 400, chatgptwebauth.LifecycleInteractionRequired},
		{"incorrect_code", "mfa_verify", 403, chatgptwebauth.LifecycleReauthRequired},
	} {
		t.Run(fmt.Sprintf("%s/%s/%d", test.code, test.stage, test.status), func(t *testing.T) {
			dir := t.TempDir()
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(dir)
			manager := coreauth.NewManager(store, nil, nil)
			executor := NewChatGPTWebExecutor(&config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AutoRelogin: true}}, manager)
			manager.RegisterExecutor(executor)
			t.Cleanup(func() { _ = manager.CloseExecutors() })
			auth := chatGPTWebTestAuth("manual-verification")
			auth.ID, auth.FileName, auth.Attributes = "fixture.json", "fixture.json", nil
			auth.Metadata["account_id"] = "fixture-account"
			auth.Metadata["login_method"] = "password_totp"
			setChatGPTWebLifecycle(auth, coreauth.LifecycleStateReloginPending, "session_expired", time.Now())
			expected, err := manager.Register(t.Context(), auth)
			if err != nil {
				t.Fatal(err)
			}
			fake := &fakeChatGPTWebAuthService{loginFn: func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				updated := cloneChatGPTWebCredential(input.Credential)
				updated.LifecycleState, updated.LifecycleReason = test.state, test.code
				return updated, &chatgptwebauth.AuthError{Code: test.code, State: test.state, LifecycleState: test.state, StatusCode: test.status, Terminal: true, FailureStage: test.stage}
			}}
			executor.authService = fake
			if !executor.TriggerBackgroundRelogin(expected) {
				t.Fatal("background recovery was not queued")
			}
			waitForChatGPTWebCondition(t, 5*time.Second, func() bool {
				current, _ := manager.GetByID(expected.ID)
				snapshot := executor.BackgroundReloginSnapshot()
				return current.LifecycleState() == string(test.state) && snapshot.Running == 0 && snapshot.Queued == 0 && snapshot.Delayed == 0
			})
			current, _ := manager.GetByID(expected.ID)
			if fake.loginCalls.Load() != 1 || executor.TriggerBackgroundRelogin(current) || executor.SyncUnauthorizedRecovery(current) || executor.ShouldRefresh(time.Now(), current) {
				t.Fatal("manual verification was automatically retried")
			}
			if current.Disabled || current.LifecycleSelectable() || current.Metadata["lifecycle_reason"] != test.code || current.Metadata["lifecycle_failure_code"] != test.code || chatgptwebauth.DeadLifecycleDeletionAllowed(current.Metadata) {
				t.Fatal("failed verification lost quarantine, reason or deletion safety")
			}
			if current.LastError == nil || current.LastError.Diagnostic == nil || current.LastError.Diagnostic.Code != test.code || current.LastError.Diagnostic.HTTPStatus != test.status || current.LastError.Diagnostic.Stage != test.stage {
				t.Fatal("runtime diagnostic lost the specific verification evidence")
			}
			filePath := filepath.Join(dir, expected.FileName)
			before, err := os.ReadFile(filePath)
			if err != nil || !json.Valid(before) {
				t.Fatal("manual recovery credential was not retained on disk")
			}
			fake.loginFn = func(_ context.Context, input chatgptwebauth.LoginInput) (*chatgptwebauth.Credential, error) {
				updated := cloneChatGPTWebCredential(input.Credential)
				updated.LifecycleState, updated.LifecycleReason = chatgptwebauth.LifecycleActive, ""
				updated.AccessToken, updated.RefreshToken = "new-access", "new-refresh"
				updated.Cookies = []chatgptwebauth.Cookie{{Name: "__Secure-next-auth.session-token", Value: "new-session", Host: "chatgpt.com", Path: "/"}}
				return updated, nil
			}
			installed, isCurrent, err := executor.ReloginCurrent(t.Context(), current)
			if err != nil || !isCurrent || installed.ID != expected.ID || installed.FileName != expected.FileName || !installed.LifecycleSelectable() {
				t.Fatal("manual recovery did not atomically replace the same credential")
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			credentials := 0
			for _, file := range files {
				if strings.HasSuffix(file.Name(), ".json") {
					credentials++
				}
			}
			if credentials != 1 {
				t.Fatal("manual recovery created another credential file")
			}
			raw, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(raw, &metadata); err != nil {
				t.Fatal(err)
			}
			credential, err := chatgptwebauth.ParseCredential(metadata)
			if err != nil || credential.AccountID != "fixture-account" || credential.Email != auth.Metadata["email"] || credential.Password != auth.Metadata["password"] || credential.TOTPSecret != auth.Metadata["totp_secret"] || credential.AccessToken != "new-access" || credential.SessionToken != "new-session" || credential.LifecycleReason != "" || credential.LifecycleFailureCode != "" || installed.LastError != nil {
				t.Fatal("manual recovery lost identity/material, restored an old session or retained the failure state")
			}
		})
	}
}
