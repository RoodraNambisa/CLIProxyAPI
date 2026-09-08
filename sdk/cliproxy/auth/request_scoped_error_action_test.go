package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type ruleResponseBodyError struct{ failure *Error }

func (ruleResponseBodyError) ResponseBody() []byte { return []byte("fixture") }
func (err ruleResponseBodyError) Error() string    { return err.failure.Error() }
func (err ruleResponseBodyError) StatusCode() int  { return err.failure.StatusCode() }

func TestRequestScopedErrorActionScopePrecedenceAndCause(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(requestScopedConfig("continue"))
	ctx := m.WithRoutingPolicySnapshot(t.Context())
	auth, err := m.Register(WithSkipPersist(t.Context()), authWithRequestScopedRules("stop"))
	if err != nil {
		t.Fatal(err)
	}
	upstream := &Error{HTTPStatus: 400, Code: "custom_failure", Message: "fixture"}
	action, ok := m.matchRequestScopedErrorAction(ctx, auth, core.Options{}, fmt.Errorf("wrapped: %w", upstream))
	if !ok || action != config.RequestScopedActionStop {
		t.Fatal("credential rules did not take precedence")
	}
	ctrl := core.NewRequestBodyReleaseController(1, nil)
	ctrl.Release()
	released := core.Options{Metadata: map[string]any{core.BodyReleaseControllerMetadataKey: ctrl}}
	if _, ok := m.matchRequestScopedErrorAction(ctx, auth, released, upstream); !ok || requestBodyReplayable(ctx, released) {
		t.Fatal("availability rule matching changed replayability or needed the released body")
	}
	noMatch := &Error{HTTPStatus: 400, Message: "provider-only"}
	m.SetConfig(&config.Config{OAuthRequestScopedErrors: map[string][]config.RequestScopedErrorRule{"codex": {{Status: 400, Match: []string{"provider-only"}, Action: "continue"}}}})
	if _, ok := m.matchRequestScopedErrorAction(t.Context(), auth, core.Options{}, noMatch); ok {
		t.Fatal("unmatched credential list fell through to provider rules")
	}
	delete(auth.Metadata, "request_scoped_errors")
	auth, err = m.Update(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	if action, ok := m.matchRequestScopedErrorAction(ctx, auth, core.Options{}, upstream); !ok || action != config.RequestScopedActionContinue {
		t.Fatal("empty credential list lost frozen OAuth rules")
	}
	auth.Attributes = map[string]string{"api_key": "fixture"}
	auth.Metadata["email"] = "fixture@example.test"
	if _, ok := m.matchRequestScopedErrorAction(ctx, auth, core.Options{}, upstream); ok {
		t.Fatal("API key inherited OAuth rules")
	}
	auth.Attributes = nil
	bodyError := ruleResponseBodyError{&Error{HTTPStatus: 400, Message: "different diagnostic"}}
	if _, ok := m.matchRequestScopedErrorAction(ctx, auth, core.Options{}, bodyError); !ok {
		t.Fatal("response body interface was ignored")
	}
	wrapped := &requestScopedActionError{error: upstream, action: config.RequestScopedActionStop}
	if !errors.Is(wrapped, upstream) || statusCodeFromError(wrapped) != 400 || !isRequestScopedStopError(wrapped) || wrapped.Error() != upstream.Error() {
		t.Fatal("stop wrapper changed cause, status or message")
	}
}

func TestRequestScopedErrorActionCannotOverrideHardLimits(t *testing.T) {
	m := NewManager(nil, nil, nil)
	auth := authWithRequestScopedRules("continue-and-cooldown")
	auth.Metadata["request_scoped_errors"] = []config.RequestScopedErrorRule{{Status: 403, Match: []string{"fixture"}, Action: "continue-and-cooldown"}}
	auth, err := m.Register(WithSkipPersist(t.Context()), auth)
	if err != nil {
		t.Fatal(err)
	}
	for _, failure := range []error{
		&Error{HTTPStatus: 403, Code: "misalignment_policy_violation", Message: "fixture"},
		&Error{HTTPStatus: 403, Message: `{"error":{"type":"content_policy_violation","message":"fixture"}}`},
		fmt.Errorf("fixture: %w", context.Canceled),
		runtimeAuthInstanceRetiredError(),
	} {
		if _, ok := m.matchRequestScopedErrorAction(t.Context(), auth, core.Options{}, failure); ok {
			t.Fatal("rule overrode a hard error")
		}
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, ok := m.matchRequestScopedErrorAction(cancelled, auth, core.Options{}, &Error{HTTPStatus: 403, Message: "fixture"}); ok {
		t.Fatal("rule overrode cancellation")
	}
}
