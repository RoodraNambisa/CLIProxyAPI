package auth

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestRequestScopedErrorActionsKeepCooldownBoundaries(t *testing.T) {
	for _, model := range []string{"", "fixture-model"} {
		for _, status := range []int{400, 429, 500} {
			for _, action := range []config.RequestScopedErrorAction{"", config.RequestScopedActionStop, config.RequestScopedActionContinue, config.RequestScopedActionStopAndCooldown, config.RequestScopedActionContinueAndCooldown} {
				for _, disabled := range []string{"", "status", "credential"} {
					t.Run(fmt.Sprintf("model=%s/status=%d/action=%s/disabled=%s", model, status, action, disabled), func(t *testing.T) {
						m := NewManager(nil, nil, nil)
						cfg := &config.Config{}
						if disabled == "status" {
							cfg.NoCooldownStatusCodes = []int{status}
						}
						m.SetConfig(cfg)
						a := &Auth{ID: "rule-cooldown", Provider: "codex", Status: StatusActive}
						if disabled == "credential" {
							a.Metadata = map[string]any{"disable_cooling": true}
						}
						a, err := m.Register(WithSkipPersist(t.Context()), a)
						if err != nil {
							t.Fatal(err)
						}
						original := &Error{HTTPStatus: status, Code: "fixture", Message: "original error"}
						result := resultForAuth(a, "codex", model, false)
						result.Error = original
						delay := 20 * time.Millisecond
						if action == config.RequestScopedActionStopAndCooldown {
							delay = 0
						}
						if status == 429 {
							result.RetryAfter = &delay
						}
						applyRequestScopedActionToResult(action, action != "", &result)
						if original.requestScopedAction != "" {
							t.Fatal("action mutated original error")
						}
						before, _ := json.Marshal(original)
						after, _ := json.Marshal(result.Error)
						if !bytes.Equal(before, after) {
							t.Fatal("private action leaked into public error")
						}
						started := time.Now()
						m.markExecutionResult(t.Context(), result)
						finished := time.Now()
						current, _ := m.GetByID(a.ID)
						next := current.NextRetryAfter
						if model != "" {
							next = time.Time{}
							if state := current.ModelStates[model]; state != nil {
								next = state.NextRetryAfter
							}
						}
						want := disabled == "" && action != config.RequestScopedActionStop && action != config.RequestScopedActionContinue && (action != "" || status != 400)
						if !want {
							if !next.IsZero() {
								t.Fatal("action bypassed disabled cooling or changed default")
							}
						} else if status == 429 {
							if !(delay == 0 && next.IsZero()) && (next.Before(started.Add(delay)) || next.After(finished.Add(delay))) {
								t.Fatal("request action changed Retry-After or imposed a minimum")
							}
						} else if next.Before(started.Add(time.Minute)) || next.After(finished.Add(time.Minute)) {
							t.Fatal("wrong transient cooldown")
						}
					})
				}
			}
		}
	}
}

func TestRequestScopedErrorCooldownKeepsPolicyAndRevocationHardLimits(t *testing.T) {
	for _, policy := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		a, err := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "hard-limit", Provider: "codex", Status: StatusActive})
		if err != nil {
			t.Fatal(err)
		}
		result := resultForAuth(a, "codex", "", false)
		result.Error = &Error{HTTPStatus: 401, Code: "invalid_grant", Message: "revoked"}
		action := config.RequestScopedActionStop
		if policy {
			result.Error = &Error{HTTPStatus: 403, Code: "misalignment_policy_violation", Message: "rejected"}
			action = config.RequestScopedActionStopAndCooldown
		}
		applyRequestScopedActionToResult(action, true, &result)
		m.markExecutionResult(t.Context(), result)
		current, _ := m.GetByID(a.ID)
		if policy && (!current.NextRetryAfter.IsZero() || current.Disabled || current.Status != StatusActive) {
			t.Fatal("policy refusal cooled a credential")
		}
		if !policy && !current.Disabled {
			t.Fatal("no-cooldown action hid revoked authentication")
		}
	}
}
