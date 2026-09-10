package auth

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestLegacySelectionDoesNotReapplyOtherModelCooldown(t *testing.T) {
	previous := quotaCooldownDisabled.Load()
	quotaCooldownDisabled.Store(false)
	t.Cleanup(func() { quotaCooldownDisabled.Store(previous) })
	for _, mixed := range []bool{false, true} {
		for name, selector := range map[string]Selector{
			"round-robin": &RoundRobinSelector{}, "weighted": &WeightedRoundRobinSelector{},
			"fill-first": &FillFirstSelector{}, "random": &RandomSelector{},
		} {
			t.Run(fmt.Sprintf("%s/mixed=%t", name, mixed), func(t *testing.T) {
				manager := NewManager(nil, selector, nil)
				manager.SetConfig(&config.Config{FixedErrorCooldowns: []config.FixedErrorCooldownRule{{StatusCode: http.StatusUnauthorized, MessageContains: "model-only fixture", CooldownSeconds: 180, Scope: "model"}}})
				manager.RegisterExecutor(&authFallbackExecutor{id: "codex"})
				manager.RegisterExecutor(&authFallbackExecutor{id: "openai"})
				credential := &Auth{ID: uuid.NewString(), Provider: "codex"}
				model := "available-" + uuid.NewString()
				registerFallbackAuthForModel(t, manager, credential, model)
				registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: model}, {ID: "other-model"}})
				manager.MarkResult(t.Context(), Result{AuthID: credential.ID, Provider: "codex", Model: "other-model", Error: &Error{HTTPStatus: http.StatusUnauthorized, Message: "model-only fixture"}})
				current, _ := manager.GetByID(credential.ID)
				if blocked, _, _ := isAuthBlockedForModel(current, model, time.Now()); blocked {
					t.Fatal("fixture incorrectly blocked the requested model before selection")
				}
				pick := func(requested string) (*Auth, error) {
					if mixed {
						selected, _, _, err := manager.pickNextMixedLegacy(t.Context(), []string{"codex", "openai"}, requested, core.Options{}, nil, nil)
						return selected, err
					}
					selected, _, err := manager.pickNextLegacy(t.Context(), "codex", requested, core.Options{}, nil)
					return selected, err
				}
				selected, err := pick(model)
				if err != nil || selected == nil || selected.ID != credential.ID {
					t.Fatalf("another model's cooldown affected selection: %v", err)
				}
				after, _ := manager.GetByID(credential.ID)
				if !after.Unavailable || !selected.Unavailable || after.NextRetryAfter != current.NextRetryAfter || !after.ModelStates["other-model"].Unavailable {
					t.Fatal("selector view mutated runtime or returned a modified execution credential")
				}
				if selected, err := pick("other-model"); err == nil || selected != nil {
					t.Fatal("the actually cooled model bypassed its availability check")
				}
			})
		}
	}
}

func TestPreparedModelSelectionKeepsSharedRoundRobinSequence(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	manager.RegisterExecutor(&authFallbackExecutor{id: "codex"})
	model := "rotation-" + uuid.NewString()
	firstID, secondID := "a-"+uuid.NewString(), "b-"+uuid.NewString()
	for _, id := range []string{firstID, secondID} {
		credential := &Auth{ID: id, Provider: "codex"}
		if id == firstID {
			credential.Unavailable = true
			credential.NextRetryAfter = time.Now().Add(time.Hour)
			credential.CooldownScope = "model"
			credential.ModelStates = map[string]*ModelState{"other": {Unavailable: true, NextRetryAfter: credential.NextRetryAfter}}
		}
		registerFallbackAuthForModel(t, manager, credential, model)
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}, {ID: model + "-second"}})
	}
	for index, requested := range []string{model, model + "-second", model, model + "-second"} {
		selected, _, err := manager.pickNextLegacy(t.Context(), "codex", requested, core.Options{}, nil)
		want := firstID
		if index%2 == 1 {
			want = secondID
		}
		if err != nil || selected == nil || selected.ID != want {
			t.Fatalf("turn %d changed the shared rotation sequence: %v", index, err)
		}
	}
}

func TestPreparedEmptyModelSelectionPreservesAuthCooldownAndOriginalState(t *testing.T) {
	modelBlocked := &Auth{ID: "model", Unavailable: true, ModelStates: map[string]*ModelState{"other": {Unavailable: true}}}
	authBlocked := &Auth{ID: "auth", Unavailable: true, CooldownScope: cooldownScopeAuth, ModelStates: map[string]*ModelState{"other": {Unavailable: true}}}
	legacyBlocked := &Auth{ID: "legacy", Unavailable: true}
	ready := &Auth{ID: "ready"}
	pool := []*Auth{modelBlocked, authBlocked, legacyBlocked, ready}
	prepared := preparedAuthsForEmptyModelSelection(pool)
	if prepared[0] == modelBlocked || prepared[0].Unavailable || !modelBlocked.Unavailable || prepared[1] != authBlocked || !prepared[1].Unavailable || prepared[2] != legacyBlocked || prepared[3] != ready {
		t.Fatal("prepared view changed auth-wide/legacy state or source snapshots")
	}
	_, err := (&RoundRobinSelector{}).Pick(t.Context(), "codex", "", core.Options{}, []*Auth{{Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour), ModelStates: modelBlocked.ModelStates}})
	if err == nil {
		t.Fatal("standalone empty-model selector unexpectedly bypassed availability")
	}
}
