package auth

import (
	"context"
	"net/http"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type persistentRefreshError struct{}

func (persistentRefreshError) Error() string                  { return "refresh requires reauthentication" }
func (persistentRefreshError) SkipAuthResult() bool           { return true }
func (persistentRefreshError) PersistAuthUpdateOnError() bool { return true }

type persistentRefreshExecutor struct{}

func (persistentRefreshExecutor) Identifier() string { return "refresh-state-test" }
func (persistentRefreshExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (persistentRefreshExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}
func (persistentRefreshExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	updated := auth.Clone()
	updated.Metadata["lifecycle_state"] = LifecycleStateDead
	updated.Metadata["lifecycle_reason"] = "account_deactivated"
	return updated, persistentRefreshError{}
}
func (persistentRefreshExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}
func (persistentRefreshExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func TestRefreshAuthPersistsLifecycleUpdateWithoutMarkingSuccess(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.RegisterExecutor(persistentRefreshExecutor{})
	auth := &Auth{
		ID:         "refresh-state-auth",
		Provider:   "refresh-state-test",
		Status:     StatusActive,
		Attributes: map[string]string{SourceHashAttributeKey: "refresh-source"},
		Metadata: map[string]any{
			"access_token":    "access-token",
			"lifecycle_state": LifecycleStateActive,
		},
	}
	if _, errRegister := manager.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}

	manager.refreshAuth(t.Context(), auth.ID)
	current, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("refreshed auth was removed")
	}
	if current.LifecycleState() != LifecycleStateDead {
		t.Fatalf("lifecycle = %q, want dead", current.LifecycleState())
	}
	if !current.LastRefreshedAt.IsZero() {
		t.Fatalf("last refreshed at = %v, want zero after failed refresh", current.LastRefreshedAt)
	}
	if current.LastError != nil || !current.NextRefreshAfter.IsZero() {
		t.Fatalf("refresh lifecycle transition recorded runtime failure: last_error=%v next=%v", current.LastError, current.NextRefreshAfter)
	}
}
