package management

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestClearAuthCooldownsRestoresGrokRegistry(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusTooManyRequests, http.StatusUpgradeRequired} {
		for _, action := range []string{"all", "credential", "model", "already-cleared"} {
			t.Run(fmt.Sprintf("%d/%s", status, action), func(t *testing.T) {
				manager := coreauth.NewManager(nil, nil, nil)
				id, model := t.Name(), "grok-"+t.Name()
				other := model + "-other"
				_, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
					ID: id, Provider: "xai", Status: coreauth.StatusActive,
					Attributes: map[string]string{"source": "config:xai[test]", "api_key": "test"},
				})
				if err != nil {
					t.Fatal(err)
				}
				reg := registry.GetGlobalRegistry()
				models := []*registry.ModelInfo{{ID: model, Object: "model", OwnedBy: "xai", Type: "xai"}, {ID: other, Object: "model", OwnedBy: "xai", Type: "xai"}}
				reg.RegisterClient(id, "xai", models)
				t.Cleanup(func() { reg.UnregisterClient(id) })
				// A later 426 has no built-in deadline and must not retain a prior suspension.
				manager.MarkResult(t.Context(), coreauth.Result{AuthID: id, Provider: "xai", Model: model, Error: &coreauth.Error{HTTPStatus: http.StatusForbidden, Message: "temporary failure"}})
				manager.MarkResult(t.Context(), coreauth.Result{AuthID: id, Provider: "xai", Model: model, Error: &coreauth.Error{HTTPStatus: status, Message: "temporary failure"}})
				manager.MarkResult(t.Context(), coreauth.Result{AuthID: id, Provider: "xai", Model: other, Error: &coreauth.Error{HTTPStatus: http.StatusForbidden, Message: "keep this cooldown"}})
				if action == "already-cleared" {
					current, _ := manager.GetByID(id)
					clearFullAuthCooldownState(current, time.Now())
					if _, err = manager.Update(coreauth.WithSkipStateCarryForward(t.Context()), current); err != nil {
						t.Fatal(err)
					}
				}
				if reg.GetModelCount(model) != 0 {
					t.Fatal("fixture did not suspend the model")
				}
				h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
				h.SetAuthStatusHook(func(_ context.Context, a *coreauth.Auth) {
					reg.RegisterClientPreservingState(a.ID, "xai", models)
				})
				request := map[string]any{"names": []string{id}}
				if action == "model" {
					request = map[string]any{"items": []map[string]any{{"id": id, "models": []string{model}}}}
				}
				body, _ := json.Marshal(request)
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				c.Request = httptest.NewRequest(http.MethodPost, "/auth-files/cooldowns/clear", strings.NewReader(string(body)))
				if action == "all" {
					h.ClearAllAuthCooldowns(c)
				} else {
					h.ClearSelectedAuthCooldowns(c)
				}
				if w.Code != http.StatusOK {
					t.Fatal(w.Code, w.Body.String())
				}
				if reg.GetModelCount(model) != 1 || !slices.Contains(reg.GetModelProviders(model), "xai") {
					t.Fatal("cooldown cleared but Grok registry still suspended", w.Body.String())
				}
				current, _ := manager.GetByID(id)
				state := current.ModelStates[model]
				if state.Unavailable || !state.NextRetryAfter.IsZero() || state.LastError != nil {
					t.Fatal("model runtime still blocked")
				}
				if action == "model" {
					if reg.GetModelCount(other) != 0 || !current.ModelStates[other].NextRetryAfter.After(time.Now()) {
						t.Fatal("selective clear changed another model")
					}
				} else if reg.GetModelCount(other) != 1 || current.LastError != nil || current.Status != coreauth.StatusActive {
					t.Fatal("whole-credential recovery left stale error state")
				}
			})
		}
	}
}

func TestClearAuthCooldownsPreservesDisabledAndOtherCredentials(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			id, model := t.Name(), "grok-"+t.Name()
			blocked := model + "-disabled"
			a, err := manager.Register(t.Context(), &coreauth.Auth{
				ID: id, Provider: "xai", Disabled: disabled, Status: coreauth.StatusActive,
				Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour),
				ModelStates: map[string]*coreauth.ModelState{blocked: {Status: coreauth.StatusDisabled}},
			})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			models := []*registry.ModelInfo{{ID: model}, {ID: blocked}}
			reg.RegisterClient(id, "xai", models)
			reg.RegisterClient(id+"-other", "xai", models)
			t.Cleanup(func() { reg.UnregisterClient(id); reg.UnregisterClient(id + "-other") })
			for _, client := range []string{id, id + "-other"} {
				for _, m := range models {
					reg.SuspendClientModel(client, m.ID, "payment_required")
					reg.SetModelQuotaExceeded(client, m.ID)
				}
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
			if _, err = h.clearCurrentAuthCooldown(t.Context(), a, nil, true); err != nil {
				t.Fatal(err)
			}
			current, _ := manager.GetByID(id)
			if current.Disabled != disabled || current.ModelStates[blocked].Status != coreauth.StatusDisabled || reg.GetModelCount(blocked) != 0 {
				t.Fatal("manual disable was cleared")
			}
			reg.UnregisterClient(id)
			if reg.GetModelCount(model) != 0 {
				t.Fatal("another credential's cooldown was cleared")
			}
		})
	}
}

type cooldownFailingStore struct {
	coreauth.Store
}

func (cooldownFailingStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", errors.New("save failed")
}

func TestClearAuthCooldownsDoesNotResumeOnSaveFailureOrReplacement(t *testing.T) {
	for _, failure := range []string{"save", "replacement"} {
		t.Run(failure, func(t *testing.T) {
			manager := coreauth.NewManager(cooldownFailingStore{}, nil, nil)
			id, model := t.Name(), "grok-"+t.Name()
			a, err := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
				ID: id, Provider: "xai", Metadata: map[string]any{"access_token": "old"},
				Status: coreauth.StatusError, Unavailable: true, NextRetryAfter: time.Now().Add(time.Hour),
			})
			if err != nil {
				t.Fatal(err)
			}
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(id, "xai", []*registry.ModelInfo{{ID: model}})
			reg.SuspendClientModel(id, model, "payment_required")
			t.Cleanup(func() { reg.UnregisterClient(id) })
			if failure == "replacement" {
				next := a.Clone()
				next.Metadata["access_token"] = "new"
				if _, err = manager.Update(coreauth.WithSkipPersist(t.Context()), next); err != nil {
					t.Fatal(err)
				}
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{}, manager)
			if changed, errClear := h.clearCurrentAuthCooldown(t.Context(), a, nil, true); changed || errClear == nil {
				t.Fatal("failed clear reported success", changed, errClear)
			}
			current, _ := manager.GetByID(id)
			if !current.Unavailable || reg.GetModelCount(model) != 0 || failure == "replacement" && current.Metadata["access_token"] != "new" {
				t.Fatal("failed clear changed credential or resumed the model")
			}
		})
	}
}
