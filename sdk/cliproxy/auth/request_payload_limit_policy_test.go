package auth

import (
	"errors"
	"fmt"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestPayloadLimitExistingRulesPreserveDefaultFailoverChoice(t *testing.T) {
	for _, stop := range []bool{false, true} {
		for _, operation := range []string{"execute", "count", "stream"} {
			t.Run(fmt.Sprintf("stop=%t/%s", stop, operation), func(t *testing.T) {
				manager, exec := newCredentialRetryLimitTestManagerWithAuthCount(t, 0, 3)
				manager.SetRetryConfig(0, 0, 0)
				cfg := &config.Config{NoCooldownStatusCodes: []int{413}, NonRetryableErrors: []config.NonRetryableErrorRule{}}
				if stop {
					cfg.NonRetryableErrors = []config.NonRetryableErrorRule{{StatusCode: 413}}
				}
				manager.SetConfig(cfg)
				exec.err = &Error{HTTPStatus: 413, Message: "upstream payload too large"}
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
				if !errors.Is(err, exec.err) || statusCodeFromError(err) != 413 || exec.Calls() != want {
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
