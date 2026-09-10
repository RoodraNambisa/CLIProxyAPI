package auth

import (
	"reflect"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type suffixCooldownExecutor struct{ credentialRetryLimitExecutor }

func (*suffixCooldownExecutor) Identifier() string { return "codex" }

func TestModelSuffixCooldownCoversManagerExecutionEntrypoints(t *testing.T) {
	for _, operation := range []string{"execute", "count", "stream"} {
		t.Run(operation, func(t *testing.T) {
			manager := NewManager(nil, &RoundRobinSelector{}, nil)
			manager.SetRetryConfig(0, 0, 0)
			exec := &suffixCooldownExecutor{}
			manager.RegisterExecutor(exec)
			auth := &Auth{ID: "suffix-manager", Provider: "codex", ModelStates: map[string]*ModelState{"suffix-model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour), Quota: QuotaState{Exceeded: true}}}}
			if _, err := manager.Register(t.Context(), auth); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "suffix-model"}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			request := core.Request{Model: "suffix-model(low)"}
			var err error
			switch operation {
			case "execute":
				_, err = manager.Execute(t.Context(), []string{"codex"}, request, core.Options{})
			case "count":
				_, err = manager.ExecuteCount(t.Context(), []string{"codex"}, request, core.Options{})
			case "stream":
				_, err = manager.ExecuteStream(t.Context(), []string{"codex"}, request, core.Options{})
			}
			if err == nil || exec.Calls() != 0 {
				t.Fatalf("manager bypassed model cooldown: err=%v calls=%d", err, exec.Calls())
			}
		})
	}
}

func TestModelSuffixCooldownCannotBeBypassedByAnotherEffort(t *testing.T) {
	now := time.Now()
	deadline := now.Add(time.Hour)
	auth := &Auth{ID: "suffix-fixture", Provider: "codex", ModelStates: map[string]*ModelState{
		"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: deadline, Quota: QuotaState{Exceeded: true}, LastError: &Error{HTTPStatus: 429, Code: "fixture_limit", Message: "fixture failure"}},
	}}
	before := auth.Clone()
	for _, model := range []string{"model", "model(low)", "model(high)"} {
		blocked, reason, next := isAuthBlockedForModel(auth, model, now)
		if !blocked || reason != blockReasonCooldown || !next.Equal(deadline) {
			t.Fatalf("%s bypassed a sibling effort cooldown: blocked=%v reason=%v", model, blocked, reason)
		}
	}
	for name, selector := range map[string]Selector{"round-robin": &RoundRobinSelector{}, "weighted": &WeightedRoundRobinSelector{}, "fill-first": &FillFirstSelector{}, "random": &RandomSelector{}} {
		t.Run(name, func(t *testing.T) {
			selected, err := selector.Pick(t.Context(), "codex", "model(low)", core.Options{}, []*Auth{auth})
			if err == nil || selected != nil {
				t.Fatal("selector bypassed cooldown through another effort")
			}
			if failure := StoredAuthFailureOf(err); failure == nil || failure.Code != "fixture_limit" {
				t.Fatal("sibling cooldown lost its stored failure evidence")
			}
		})
	}
	if blocked, _, _ := isAuthBlockedForModel(auth, "different/model(low)", now); blocked {
		t.Fatal("distinct model prefix was cooled")
	}
	if !reflect.DeepEqual(before, auth) {
		t.Fatal("selection mutated stored model states")
	}
}

func TestModelSuffixCooldownPreservesZeroAndStrongestState(t *testing.T) {
	now := time.Now()
	auth := &Auth{ModelStates: map[string]*ModelState{
		"model(high)": {Status: StatusError, Unavailable: true, Quota: QuotaState{Exceeded: true, NextRecoverAt: now.Add(time.Hour)}},
		"model(low)":  {Status: StatusActive, NextRetryAfter: now.Add(time.Hour)},
	}}
	if blocked, _, _ := isAuthBlockedForModel(auth, "model", now); blocked {
		t.Fatal("zero no-cooldown state or inactive deadline became blocking")
	}
	deadline := now.Add(2 * time.Hour)
	auth.ModelStates["model(max)"] = &ModelState{Status: StatusError, Unavailable: true, NextRetryAfter: deadline}
	if blocked, _, next := isAuthBlockedForModel(auth, "model(high)", now); !blocked || !next.Equal(deadline) {
		t.Fatal("available exact suffix masked another active suffix")
	}
	auth.ModelStates["model"] = &ModelState{Status: StatusDisabled}
	if blocked, reason, _ := isAuthBlockedForModel(auth, "model(low)", now); !blocked || reason != blockReasonDisabled {
		t.Fatal("explicit disabled model state lost precedence")
	}
}

func TestModelSuffixCooldownCreditsBypassOnlyQuota(t *testing.T) {
	now := time.Now()
	auth := &Auth{ModelStates: map[string]*ModelState{
		"model(high)": {Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Hour), Quota: QuotaState{Exceeded: true}},
		"unrelated":   {Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Hour)},
	}}
	if !antigravityCreditsQuotaCooldown(auth, "model(low)", now) {
		t.Fatal("sibling quota state was invisible to credits eligibility")
	}
	auth.ModelStates["model(max)"] = &ModelState{Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Hour)}
	if antigravityCreditsQuotaCooldown(auth, "model(high)", now) {
		t.Fatal("credits bypassed a sibling non-quota blocker")
	}
	delete(auth.ModelStates, "model(max)")
	auth.ModelStates["model"] = &ModelState{Status: StatusDisabled}
	if antigravityCreditsQuotaCooldown(auth, "model(high)", now) {
		t.Fatal("credits bypassed a disabled model")
	}
}

func TestModelSuffixFailureEvidenceHasDeterministicTieBreak(t *testing.T) {
	now := time.Now()
	auth := &Auth{ID: "failure-fixture", ModelStates: map[string]*ModelState{
		"model(high)": {UpdatedAt: now, LastError: &Error{Code: "high"}},
		"model(low)":  {UpdatedAt: now, LastError: &Error{Code: "low"}},
		"other":       {UpdatedAt: now.Add(time.Hour), LastError: &Error{Code: "unrelated"}},
	}}
	for range 50 {
		var choice candidateFailureChoice
		choice.observe(auth, "model(max)")
		if failure := choice.latest(); failure == nil || failure.Code != "low" {
			t.Fatal("failure selection depends on map traversal or another model")
		}
	}
}
