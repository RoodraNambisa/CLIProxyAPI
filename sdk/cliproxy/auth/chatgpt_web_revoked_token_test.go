package auth

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type revokedWebTokenError struct{}

func (revokedWebTokenError) Error() string   { return "explicit token revocation" }
func (revokedWebTokenError) StatusCode() int { return http.StatusUnauthorized }
func (revokedWebTokenError) ChatGPTWebLifecycleError() *chatgptwebauth.AuthError {
	return &chatgptwebauth.AuthError{Code: "token_revoked", State: chatgptwebauth.LifecycleReauthRequired, Terminal: true}
}

type revokedWebTokenExecutor struct {
	*chatGPTWebUnauthorizedRefreshExecutor
	started chan struct{}
	release chan struct{}
	once    sync.Once
	rejects atomic.Int32
	relogin atomic.Int32
}

func (executor *revokedWebTokenExecutor) RejectRevokedAccessToken(auth *Auth) (*Auth, error) {
	executor.rejects.Add(1)
	executor.once.Do(func() { close(executor.started) })
	<-executor.release
	updated := auth.Clone()
	updated.Metadata["lifecycle_state"] = LifecycleStateReloginPending
	updated.Metadata["lifecycle_reason"] = "token_revoked"
	return updated, chatGPTWebRequestRefreshError{persist: true}
}

func (executor *revokedWebTokenExecutor) TriggerBackgroundRelogin(*Auth) bool {
	executor.relogin.Add(1)
	return true
}

func TestChatGPTWebRevokedTokenStopsSelectionAndRetriesOtherCredential(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		t.Run(map[bool]string{false: "execute", true: "stream"}[streaming], func(t *testing.T) {
			manager, base, primary, backup, model := newChatGPTWebUnauthorizedRefreshFixture(t)
			base.executeErr = revokedWebTokenError{}
			executor := &revokedWebTokenExecutor{chatGPTWebUnauthorizedRefreshExecutor: base, started: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(executor.release) }) }
			t.Cleanup(release)
			manager.RegisterExecutor(executor)
			manager.SetRetryConfig(1, 0, 1)
			if streaming {
				result, err := manager.ExecuteStream(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				if err != nil {
					t.Fatal(err)
				}
				for chunk := range result.Chunks {
					if chunk.Err != nil || string(chunk.Payload) != backup.ID+":backup" {
						t.Fatalf("backup stream = %s, %v", chunk.Payload, chunk.Err)
					}
				}
			} else {
				result, err := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				if err != nil || string(result.Payload) != backup.ID+":backup" {
					t.Fatalf("backup result = %s, %v", result.Payload, err)
				}
			}
			select {
			case <-executor.started:
			case <-time.After(5 * time.Second):
				t.Fatal("revocation recovery did not start")
			}
			if chatGPTWebRequestRefreshBlockCount(manager, primary.ID) != 1 {
				t.Fatal("revoked credential stayed selectable during recovery")
			}
			release()
			deadline := time.Now().Add(5 * time.Second)
			for {
				current, _ := manager.GetByID(primary.ID)
				if current.LifecycleState() == LifecycleStateReloginPending && executor.relogin.Load() == 1 && chatGPTWebRequestRefreshBlockCount(manager, primary.ID) == 0 {
					if current.LifecycleSelectable() || current.Metadata["lifecycle_reason"] != "token_revoked" {
						t.Fatal("revoked credential returned to routing")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("revocation transition did not finish: %s", current.LifecycleState())
				}
				time.Sleep(time.Millisecond)
			}
			if base.refreshCalls != 0 || base.validateCalls.Load() != 0 || executor.rejects.Load() != 1 {
				t.Fatal("explicit revocation unexpectedly exchanged or revalidated the revoked token")
			}
		})
	}
}

func TestChatGPTWebRevocationDoesNotInvalidateReplacementToken(t *testing.T) {
	manager, base, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	executor := &revokedWebTokenExecutor{chatGPTWebUnauthorizedRefreshExecutor: base, started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(executor.release) })
	manager.RegisterExecutor(executor)
	updated, _ := manager.GetByID(primary.ID)
	updated.Metadata["access_token"] = "replacement"
	if _, err := manager.Update(t.Context(), updated); err != nil {
		t.Fatal(err)
	}
	err := manager.RecoverChatGPTWebUnauthorizedInBackground(t.Context(), primary, revokedWebTokenError{})
	if err == nil {
		t.Fatal("original unauthorized error was lost")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := manager.CloseExecutorsContext(ctx); err != nil {
		t.Fatal(err)
	}
	current, _ := manager.GetByID(primary.ID)
	if executor.rejects.Load() != 0 || !current.LifecycleSelectable() || authAccessToken(current) != "replacement" {
		t.Fatal("late revocation invalidated the replacement token")
	}
}

func TestChatGPTWebMaintenanceRecoveryPreservesSingleAttempt(t *testing.T) {
	manager, _, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	err := manager.RecoverChatGPTWebUnauthorizedInBackground(cliproxyexecutor.WithSingleAttempt(t.Context()), primary, revokedWebTokenError{})
	if _, ok := err.(revokedWebTokenError); !ok {
		t.Fatal("single-attempt diagnostic error was wrapped for recovery")
	}
	if manager.ChatGPTWebRequestRefreshSnapshot().Received != 0 || chatGPTWebRequestRefreshBlockCount(manager, primary.ID) != 0 {
		t.Fatal("single-attempt diagnostic started background recovery")
	}
}

func TestChatGPTWebMaintenanceRecoveryDoesNotReenterCredentialAcquisition(t *testing.T) {
	manager, _, primary, _, _ := newChatGPTWebUnauthorizedRefreshFixture(t)
	ctx, release, err := manager.lockCredentialRefreshesContext(t.Context(), []string{primary.ID})
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	err = manager.RecoverChatGPTWebUnauthorizedInBackground(ctx, primary, revokedWebTokenError{})
	if _, ok := err.(revokedWebTokenError); !ok || manager.ChatGPTWebRequestRefreshSnapshot().Received != 0 {
		t.Fatal("credential acquisition recursively started background recovery")
	}
}
