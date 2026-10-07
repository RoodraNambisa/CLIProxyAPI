package executor

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebAccountInfoForcedRefreshUsesSameIdentityReimport(t *testing.T) {
	for _, queuedBeforeImport := range []bool{false, true} {
		name := "after import"
		if queuedBeforeImport {
			name = "waiting across import"
		}
		t.Run(name, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				if r.Header.Get("Authorization") != "Bearer new-access" {
					t.Error("refresh used the pre-import access token")
				}
				switch r.URL.Path {
				case chatgptwebauth.AccountCheckPath:
					_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
						"default": map[string]any{"account": map[string]any{"account_id": "same-account", "plan_type": "free"}},
					}})
				case chatgptwebauth.ConversationInitPath:
					_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
						map[string]any{"feature_name": "image_gen", "remaining": 7},
					}})
				default:
					http.NotFound(w, r)
				}
			}))
			t.Cleanup(server.Close)
			root := t.TempDir()
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(root)
			manager := cliproxyauth.NewManager(store, nil, nil)
			executor := NewChatGPTWebExecutor(&config.Config{}, manager)
			executor.runtimeBaseURL = server.URL
			manager.RegisterExecutor(executor)
			t.Cleanup(func() { _ = manager.CloseExecutors() })
			auth := chatGPTWebTestAuth("same-import")
			auth.ID, auth.FileName, auth.Attributes = "same-import.json", "same-import.json", nil
			auth.Metadata["account_id"] = "same-account"
			auth.Metadata["login_method"] = "api798"
			auth.Metadata["api798_url"] = "https://api798.com/get_code?email=same-import%40example.com&auth_code=fixture"
			installed, errRegister := manager.Register(t.Context(), auth)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			started, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			t.Cleanup(release)
			executor.accountInfo.mu.Lock()
			executor.accountInfo.beforeAccountInfoExecution = func(chatGPTWebAccountInfoWork, *chatGPTWebAccountInfoCall, bool) {
				close(started)
				<-proceed
			}
			executor.accountInfo.mu.Unlock()
			start := func(target *cliproxyauth.Auth) *chatgptwebauth.AccountInfoRefreshTask {
				t.Helper()
				task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
					Name: target.FileName, AuthID: target.ID, AuthInstanceID: target.RuntimeInstanceID(),
				}}, true)
				if errStart != nil {
					t.Fatal(errStart)
				}
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("refresh did not reach the pre-execution barrier")
				}
				return task
			}
			var task *chatgptwebauth.AccountInfoRefreshTask
			if queuedBeforeImport {
				task = start(installed)
			}
			replacement := installed.Clone()
			replacement.Metadata["access_token"] = "new-access"
			replacement.Metadata["cookies"] = []chatgptwebauth.Cookie{{
				Name: "__Secure-next-auth.session-token", Value: "new-session", Domain: "chatgpt.com", Path: "/", Secure: true,
			}}
			// This is the same guarded installation API used by named imports.
			current, accepted, errImport := manager.UpdateRefreshedIfCurrent(t.Context(), installed, replacement)
			if errImport != nil || !accepted || current == nil {
				t.Fatalf("import installation accepted=%t error=%v", accepted, errImport)
			}
			if current.RuntimeInstanceID() != installed.RuntimeInstanceID() ||
				current.RuntimeInstallationID() == installed.RuntimeInstallationID() {
				t.Fatal("same-account import must advance installation without retiring the runtime")
			}
			if _, errCredential := chatgptwebauth.ParseCredential(current.Metadata); errCredential != nil {
				t.Fatalf("import fixture is not a valid credential: %v", errCredential)
			}
			if !current.LifecycleRefreshable() || current.Disabled {
				t.Fatalf("import fixture lifecycle=%s disabled=%t", current.LifecycleState(), current.Disabled)
			}
			if !queuedBeforeImport {
				task = start(current)
			}
			release()
			task = waitForAccountInfoTask(t, executor, task.ID)
			if task.State != chatgptwebauth.AccountInfoTaskCompleted || len(task.Results) != 1 || task.Results[0].Error != "" {
				t.Fatalf("forced refresh = %+v", task)
			}
			if upstreamCalls.Load() != 2 {
				t.Fatalf("upstream calls = %d, want one profile and one quota request", upstreamCalls.Load())
			}
			data, errRead := os.ReadFile(filepath.Join(root, current.FileName))
			if errRead != nil {
				t.Fatal(errRead)
			}
			persisted, errDecode := chatgptwebauth.DecodeCredential(data)
			if errDecode != nil || persisted.AccessToken != "new-access" || persisted.SessionToken != "new-session" ||
				persisted.ImageQuotaRemaining == nil || *persisted.ImageQuotaRemaining != 7 {
				t.Fatal("refresh did not preserve the imported credential and persist the new quota")
			}
		})
	}
}

func TestChatGPTWebAccountInfoForcedTaskReportsReloginPendingWithoutMutation(t *testing.T) {
	for _, afterStart := range []bool{false, true} {
		name := "already pending"
		if afterStart {
			name = "pending during runtime cleanup"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(root)
			manager := cliproxyauth.NewManager(store, nil, nil)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusInternalServerError)
			}))
			t.Cleanup(server.Close)
			executor := NewChatGPTWebExecutor(&config.Config{}, manager)
			executor.runtimeBaseURL = server.URL
			manager.RegisterExecutor(executor)
			t.Cleanup(func() { _ = manager.CloseExecutors() })
			auth := chatGPTWebTestAuth("pending-import")
			auth.ID, auth.FileName, auth.Attributes = "pending-import.json", "pending-import.json", nil
			if !afterStart {
				auth.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
			}
			installed, errRegister := manager.Register(t.Context(), auth)
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			path := filepath.Join(root, installed.FileName)
			before, errRead := os.ReadFile(path)
			if errRead != nil {
				t.Fatal(errRead)
			}
			started, proceed := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(proceed) }) }
			t.Cleanup(release)
			if afterStart {
				executor.accountInfo.mu.Lock()
				executor.accountInfo.beforeAccountInfoExecution = func(chatGPTWebAccountInfoWork, *chatGPTWebAccountInfoCall, bool) {
					close(started)
					<-proceed
				}
				executor.accountInfo.mu.Unlock()
			}
			task, errStart := executor.StartAccountInfoRefreshTask([]chatgptwebauth.AccountInfoRefreshTarget{{
				Name: installed.FileName, AuthID: installed.ID, AuthInstanceID: installed.RuntimeInstanceID(),
			}}, true)
			if errStart != nil {
				t.Fatal(errStart)
			}
			if afterStart {
				select {
				case <-started:
				case <-time.After(3 * time.Second):
					t.Fatal("refresh did not reach the pre-execution barrier")
				}
				pending, current, errUpdate := manager.MutateRuntimeMetadataIfCurrent(t.Context(), installed, func(candidate *cliproxyauth.Auth) {
					candidate.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
				})
				if errUpdate != nil || !current {
					t.Fatalf("pending transition current=%t error=%v", current, errUpdate)
				}
				executor.CloseAuthInstanceExecutionSessions(pending.ID, pending.RuntimeInstanceID(), "auth_lifecycle_unavailable")
				before, errRead = os.ReadFile(path)
				if errRead != nil {
					t.Fatal(errRead)
				}
				release()
			}
			task = waitForAccountInfoTask(t, executor, task.ID)
			if task.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors || len(task.Results) != 1 || task.Results[0].Error != "relogin_pending" {
				t.Fatalf("pending credential result = %+v", task)
			}
			current, ok := manager.GetByID(installed.ID)
			if !ok || current.LifecycleState() != cliproxyauth.LifecycleStateReloginPending || calls.Load() != 0 {
				t.Fatal("diagnostic force refresh changed or executed a pending credential")
			}
			after, errRead := os.ReadFile(path)
			if errRead != nil || !bytes.Equal(before, after) {
				t.Fatal("diagnostic force refresh changed or deleted the credential file")
			}
		})
	}
}

func TestChatGPTWebAccountInfoOldTargetDoesNotAdoptReplacementLifecycle(t *testing.T) {
	manager := cliproxyauth.NewManager(nil, nil, nil)
	installed, errRegister := manager.Register(cliproxyauth.WithSkipPersist(t.Context()), chatGPTWebTestAuth("replaced-pending"))
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	next := installed.Clone()
	next.Metadata["lifecycle_state"] = cliproxyauth.LifecycleStateReloginPending
	replaced, current, errUpdate := manager.UpdateIfCurrent(
		cliproxyauth.WithForceRuntimeReplacement(cliproxyauth.WithSkipPersist(t.Context())), installed, next,
	)
	if errUpdate != nil || !current || replaced.RuntimeInstanceID() == installed.RuntimeInstanceID() {
		t.Fatalf("replacement current=%t error=%v", current, errUpdate)
	}
	runtime := &chatGPTWebAccountInfoRuntime{executor: &ChatGPTWebExecutor{manager: manager}}
	for _, test := range []struct{ instance, want string }{
		{installed.RuntimeInstanceID(), "credential_unavailable"},
		{replaced.RuntimeInstanceID(), "relogin_pending"},
	} {
		outcome := runtime.unavailableTargetOutcome(chatgptwebauth.AccountInfoRefreshTarget{AuthID: installed.ID, AuthInstanceID: test.instance})
		if outcome.errorCode != test.want {
			t.Fatalf("unavailable outcome = %s, want %s", outcome.errorCode, test.want)
		}
	}
}

func TestChatGPTWebAccountInfoCleanupReportsPendingForQueuedAndScheduledTasks(t *testing.T) {
	for _, scheduled := range []bool{false, true} {
		name := "queued"
		if scheduled {
			name = "scheduled"
		}
		t.Run(name, func(t *testing.T) {
			manager := cliproxyauth.NewManager(nil, nil, nil)
			auth := registerChatGPTWebPendingAuth(t, manager, "pending-cleanup")
			executor := &ChatGPTWebExecutor{manager: manager}
			runtime := newChatGPTWebAccountInfoRuntime(executor, &config.Config{})
			executor.accountInfo = runtime
			t.Cleanup(runtime.close)
			task, errStart := runtime.startTask([]chatgptwebauth.AccountInfoRefreshTarget{{
				AuthID: auth.ID, AuthInstanceID: auth.RuntimeInstanceID(),
			}}, true)
			if errStart != nil {
				t.Fatal(errStart)
			}
			if scheduled {
				runtime.mu.Lock()
				work, ok := runtime.dequeueLocked()
				accepted := ok && runtime.scheduleLocked("pending-retry", time.Now().Add(time.Hour), work)
				runtime.mu.Unlock()
				if !accepted {
					t.Fatal("could not schedule the queued task")
				}
			}
			runtime.removeAuthInstance(auth.ID, auth.RuntimeInstanceID())
			result, ok := runtime.task(task.ID)
			if !ok || result.State != chatgptwebauth.AccountInfoTaskCompletedWithErrors || result.Results[0].Error != "relogin_pending" {
				t.Fatalf("cleanup result = %+v", result)
			}
			runtime.mu.Lock()
			defer runtime.mu.Unlock()
			if runtime.queueLengthLocked() != 0 || len(runtime.scheduled) != 0 || len(runtime.authEpochRefs) != 0 {
				t.Fatal("cleanup retained queued work or epoch references")
			}
		})
	}
}
