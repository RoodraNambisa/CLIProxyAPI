package auth

import (
	"errors"
	"strconv"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestLegacySelectionStoredFailuresRespectEligibilityAndPriority(t *testing.T) {
	now := time.Now()
	for _, fillFirst := range []bool{false, true} {
		t.Run(strconv.FormatBool(fillFirst), func(t *testing.T) {
			makeAuth := func(id string, priority int, age time.Duration) *Auth {
				return &Auth{ID: id, Provider: "codex", Attributes: map[string]string{"priority": strconv.Itoa(priority)},
					Unavailable: true, CooldownScope: cooldownScopeAuth, NextRetryAfter: now.Add(time.Minute),
					LastError: &Error{Code: id, Message: "fixture failure", HTTPStatus: 429}, UpdatedAt: now.Add(age)}
			}
			choose := func(auths []*Auth, attempt int, allowed func(*Auth) bool) error {
				if fillFirst {
					_, err := selectFillFirstRangeAuthsForAttempt(auths, "codex", "route(high)", now, attempt, 1, func(*Auth) string { return "upstream(high)" }, allowed)
					return err
				}
				_, err := selectAvailableAuthsForAttemptFiltered(auths, "codex", "route(high)", now, attempt, func(*Auth) string { return "upstream(high)" }, allowed)
				return err
			}
			high := makeAuth("high", 10, 10*time.Second)
			low := makeAuth("low", 0, 0)
			excluded := makeAuth("excluded", 0, time.Hour)
			disabled := makeAuth("disabled", 0, time.Hour)
			disabled.Disabled = true
			calls := make(map[string]int)
			err := choose([]*Auth{high, low, excluded, disabled}, 1, func(auth *Auth) bool {
				calls[auth.ID]++
				return auth.ID != "excluded"
			})
			failure := StoredAuthFailureOf(err)
			if failure == nil || failure.Code != "low" {
				t.Fatal("used a filtered or earlier-round failure")
			}
			for _, count := range calls {
				if count != 1 {
					t.Fatal("diagnostics evaluated the selection predicate again")
				}
			}
			var cooldown *modelCooldownError
			if !errors.As(err, &cooldown) || !shouldFallbackFromWebsocketFillFirstError(err) ||
				!chatGPTWebImageSelectionErrorEligible(err) || cliproxyexecutor.IsUpstreamAttemptError(err) {
				t.Fatal("stored failure changed fallback or attempt classification")
			}
			low.LastError.Code = "changed"
			if StoredAuthFailureOf(err).Code != "low" {
				t.Fatal("stored failure aliases auth state")
			}
			if failure := StoredAuthFailureOf(choose([]*Auth{high, low}, 2, nil)); failure == nil || failure.Code != "changed" {
				t.Fatal("later rounds did not retain the lowest priority")
			}
			if StoredAuthFailureOf(choose([]*Auth{excluded, disabled}, 0, func(auth *Auth) bool { return auth.ID != "excluded" })) != nil {
				t.Fatal("ineligible candidates supplied history")
			}
			ready := makeAuth("ready", 0, time.Hour)
			ready.Unavailable = false
			ready.NextRetryAfter = time.Time{}
			if errReady := choose([]*Auth{ready}, 0, nil); errReady != nil {
				t.Fatal("historical failure blocked a ready auth")
			}
			if StoredAuthFailureOf(choose(nil, 0, nil)) != nil {
				t.Fatal("empty pool supplied history")
			}

			low.LastError.Code = "low"
			low.ModelStates = map[string]*ModelState{"upstream": {LastError: &Error{Code: "matched-model", HTTPStatus: 503}, UpdatedAt: now.Add(-time.Hour)}}
			err = choose([]*Auth{high, low}, 0, nil)
			if failure := StoredAuthFailureOf(err); failure == nil || failure.Code != "matched-model" {
				t.Fatal("alias/base-model state was not preferred")
			}
			low.ModelStates = map[string]*ModelState{"unrelated": {LastError: &Error{Code: "wrong-model"}, UpdatedAt: now.Add(time.Hour)}}
			if failure := StoredAuthFailureOf(choose([]*Auth{low}, 0, nil)); failure == nil || failure.Code != "low" {
				t.Fatal("unrelated model supplied history")
			}

			expired := makeAuth("retired", 0, 0)
			expired.Metadata = map[string]any{"lifecycle_state": LifecycleStateDead}
			if failure := StoredAuthFailureOf(choose([]*Auth{expired}, 0, nil)); failure == nil || failure.Code != "retired" {
				t.Fatal("untimed unavailable state lost its failure")
			}
		})
	}
}

func TestCandidateFailureChoiceIsStableAndPrefersModelContext(t *testing.T) {
	now := time.Now()
	a := &Auth{ID: "a", UpdatedAt: now, LastError: &Error{Code: "auth-a"}}
	b := &Auth{ID: "b", UpdatedAt: now, LastError: &Error{Code: "auth-b"}}
	for _, order := range [][]*Auth{{a, b}, {b, a}} {
		var choice candidateFailureChoice
		for _, candidate := range order {
			choice.observe(candidate, "model")
		}
		if choice.latest().Code != "auth-b" {
			t.Fatal("timestamp ties depend on iteration order")
		}
	}
	b.ModelStates = map[string]*ModelState{"model": {StatusMessage: "model status"}}
	var modelChoice, authChoice candidateFailureChoice
	modelChoice.observe(b, "model(high)")
	authChoice.observe(a, "model")
	authChoice.merge(modelChoice)
	if got := authChoice.latest(); got == nil || got.Message != "model status" {
		t.Fatal("model status fallback was discarded")
	}
	var empty candidateFailureChoice
	empty.observe(nil, "")
	if empty.latest() != nil {
		t.Fatal("nil candidate supplied failure")
	}
}
