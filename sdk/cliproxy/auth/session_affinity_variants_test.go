package auth

import (
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestSessionAffinityUsesOneBindingForModelReasoningVariants(t *testing.T) {
	selector := NewSessionAffinitySelector(&RoundRobinSelector{})
	defer selector.Stop()
	opts := executor.Options{Headers: http.Header{"X-Session-Id": []string{"variant-session"}}}
	auths := []*Auth{{ID: "a"}, {ID: "b"}}
	for _, model := range []string{"gpt-5.4(high)", "gpt-5.4(low)", "gpt-5.4(4096)", "gpt-5.4"} {
		selected, err := selector.Pick(t.Context(), "codex", model, opts, auths)
		if err != nil || selected.ID != "a" {
			t.Fatal("reasoning variant created an independent binding")
		}
		if selector.cachedAuthID("codex", model, opts) != "a" {
			t.Fatal("cache lookup did not canonicalize model")
		}
	}
	rollback := selector.BindSessionWithRollback(t.Context(), "codex", "gpt-5.4(xhigh)", opts, "b")
	if selector.cachedAuthID("codex", "gpt-5.4(high)", opts) != "b" {
		t.Fatal("successful rebind did not reach every variant")
	}
	rollback()
	if selector.cachedAuthID("codex", "gpt-5.4(low)", opts) != "a" {
		t.Fatal("rollback did not restore the canonical binding")
	}
	if selector.cachedAuthID("other", "gpt-5.4", opts) != "" || selector.cachedAuthID("codex", "gpt-6-astra", opts) != "" {
		t.Fatal("variant normalization merged different providers or base models")
	}
	selector.InvalidateAuth("a")
	if selector.cachedAuthID("codex", "gpt-5.4(high)", opts) != "" {
		t.Fatal("credential invalidation left a variant binding")
	}
}

func TestSingleModelCooldownDoesNotBlockHealthyModelOrEmptySelection(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	credential, err := manager.Register(t.Context(), &Auth{ID: "model-cooldown-scope", Provider: "codex", Status: StatusActive})
	if err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: "model-a"}, {ID: "model-b"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	manager.MarkResult(t.Context(), Result{AuthID: credential.ID, Provider: "codex", Model: "model-b", Success: true})
	wait := time.Hour
	manager.MarkResult(t.Context(), Result{AuthID: credential.ID, Provider: "codex", Model: "model-a", Error: &Error{HTTPStatus: 429, Message: "limited model"}, RetryAfter: &wait})
	current, _ := manager.GetByID(credential.ID)
	for _, model := range []string{"", "model-b", "model-a", "model-a(high)"} {
		blocked, _, _ := isAuthBlockedForModel(current, model, time.Now())
		if blocked != (model == "model-a" || model == "model-a(high)") {
			t.Fatal("model cooldown escaped its scope or missed its reasoning variant")
		}
	}
	current.Unavailable = true
	current.CooldownScope = cooldownScopeAuth
	current.NextRetryAfter = time.Now().Add(time.Hour)
	for _, model := range []string{"", "model-b"} {
		if blocked, _, _ := isAuthBlockedForModel(current, model, time.Now()); !blocked {
			t.Fatal("credential-wide cooldown was ignored")
		}
	}
}
