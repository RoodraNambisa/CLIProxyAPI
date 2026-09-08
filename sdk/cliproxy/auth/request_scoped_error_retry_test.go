package auth

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestRequestScopedStopSurvivesErrorHistoryAndOuterStreamPolicy(t *testing.T) {
	m := NewManager(nil, nil, nil)
	policy := m.SnapshotRequestErrorRetryPolicy()
	for _, action := range []config.RequestScopedErrorAction{config.RequestScopedActionStop, config.RequestScopedActionStopAndCooldown, config.RequestScopedActionContinue, config.RequestScopedActionContinueAndCooldown} {
		for _, status := range []int{429, 500} {
			failure := &Error{HTTPStatus: status, Code: "upstream_fixture", Message: "original failure"}
			ctx, history := withUpstreamErrorHistory(core.WithUpstreamAttempt(t.Context()))
			original := recordExecutionAttemptError(ctx, &Auth{ID: "fixture"}, "codex", failure)
			marked := &requestScopedActionError{error: original, action: action}
			returned := history.preferred(newStreamBootstrapError(fmt.Errorf("stream: %w", marked), http.Header{"X-Fixture": {"preserved"}}))
			stop := action == config.RequestScopedActionStop || action == config.RequestScopedActionStopAndCooldown
			if policy(returned) == stop {
				t.Fatal("outer stream policy ignored the stop action or changed continue")
			}
			if m.isRequestInvalidError(returned) != stop {
				t.Fatal("inner credential loop disagreed with outer policy")
			}
			if !errors.Is(returned, failure) || statusCodeFromError(returned) != status {
				t.Fatal("stop state lost the original upstream cause")
			}
			var bootstrap *streamBootstrapError
			if !errors.As(returned, &bootstrap) || bootstrap.headers.Get("X-Fixture") != "preserved" {
				t.Fatal("stop state lost stream headers")
			}
		}
	}
}
