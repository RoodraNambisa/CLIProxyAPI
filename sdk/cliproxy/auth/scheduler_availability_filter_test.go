package auth

import (
	"errors"
	"testing"
	"time"

	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestMixedSchedulerAvailabilityUsesCurrentCandidateFilters(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		selector     Selector
		secondWeight int
		rejectSecond bool
		wantCooldown bool
	}{
		{"weighted zero", &WeightedRoundRobinSelector{}, 0, false, false},
		{"weighted rejected", &WeightedRoundRobinSelector{}, 1, true, false},
		{"weighted eligible", &WeightedRoundRobinSelector{}, 1, false, true},
		{"legacy ignores weight", &RoundRobinSelector{}, 0, false, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			first := weightedTestAuth(schedulerTestID(t, "first"), 0)
			second := weightedTestAuth(schedulerTestID(t, "second"), scenario.secondWeight)
			second.Provider = "openai"
			for _, credential := range []*Auth{first, second} {
				registerSchedulerModels(t, credential.Provider, "filtered-availability", credential.ID)
				credential.Unavailable = true
				credential.CooldownScope = cooldownScopeAuth
				credential.NextRetryAfter = time.Now().Add(time.Minute)
				credential.Quota.Exceeded = true
			}
			scheduler := newSchedulerForTest(scenario.selector, first, second)
			picked, _, errPick := scheduler.pickMixed(t.Context(), []string{"codex", "openai"}, "filtered-availability", core.Options{}, nil, func(auth *Auth) bool {
				return !scenario.rejectSecond || auth.ID != second.ID
			})
			if picked != nil || errPick == nil {
				t.Fatal("blocked pool did not report a selection error")
			}
			var cooldown *modelCooldownError
			if errors.As(errPick, &cooldown) != scenario.wantCooldown {
				t.Fatal("excluded credential influenced the cooldown result")
			}
			if !scenario.wantCooldown {
				var selection *Error
				if !errors.As(errPick, &selection) || (selection.Code != "auth_not_found" && selection.Code != "auth_unavailable") {
					t.Fatal("missing local availability error")
				}
			}
		})
	}
}
