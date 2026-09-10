package auth

import (
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestPayloadLimitExistingRulesPreserveDefaultFailoverChoice(t *testing.T) {
	testStatusOnlyFailureRules(t, 413, "upstream payload too large")
}

func TestUnclassifiedErrorsKeepConfiguredFailoverChoice(t *testing.T) {
	for _, fixture := range []struct {
		status  int
		message string
	}{
		{400, "bad request"}, {409, "request conflict"},
		{500, `{"error":{"status":"UNKNOWN","message":"fixture upstream failure"}}`},
	} {
		t.Run(fmt.Sprint(fixture.status), func(t *testing.T) {
			testStatusOnlyFailureRules(t, fixture.status, fixture.message)
		})
	}
}

func testStatusOnlyFailureRules(t *testing.T, status int, message string) {
	t.Helper()
	for _, stop := range []bool{false, true} {
		for _, operation := range []string{"execute", "count", "stream"} {
			t.Run(fmt.Sprintf("stop=%t/%s", stop, operation), func(t *testing.T) {
				manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 3)
				manager.SetRetryConfig(0, 0, 0)
				cfg := &config.Config{NoCooldownStatusCodes: []int{status}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				if stop {
					cfg.NonRetryableErrors = []config.NonRetryableErrorRule{{StatusCode: status}}
				}
				manager.SetConfig(cfg)
				exec.err = &Error{HTTPStatus: status, Message: message}
				request := coreexecutor.Request{Model: "test-model"}
				var err error
				switch operation {
				case "execute":
					_, err = manager.Execute(t.Context(), []string{"claude"}, request, coreexecutor.Options{})
				case "count":
					_, err = manager.ExecuteCount(t.Context(), []string{"claude"}, request, coreexecutor.Options{})
				case "stream":
					_, err = manager.ExecuteStream(t.Context(), []string{"claude"}, request, coreexecutor.Options{})
				}
				want := 3
				if stop {
					want = 1
				}
				if !errors.Is(err, exec.err) || statusCodeFromError(err) != status || exec.Calls() != want {
					t.Fatalf("calls=%d want=%d status=%d error preserved=%t", exec.Calls(), want, statusCodeFromError(err), errors.Is(err, exec.err))
				}
				for _, auth := range manager.List() {
					if auth.Unavailable || !auth.NextRetryAfter.IsZero() || auth.Quota.Exceeded {
						t.Fatal("configured no-cooldown request failure cooled credential")
					}
					if state := auth.ModelStates["test-model"]; state != nil && (state.Unavailable || !state.NextRetryAfter.IsZero()) {
						t.Fatal("configured no-cooldown request failure cooled model")
					}
				}
			})
		}
	}
}
