package auth

import (
	"context"
	"net/http"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestMarkExecutionResultDoesNotRecreateRemovedChatGPTWebModelState(t *testing.T) {
	const (
		authID  = "chatgpt-web-removed-model-result"
		modelID = "removed-model"
	)
	manager := NewManager(nil, nil, nil)
	registered, err := manager.Register(context.Background(), &Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(authID, "chatgpt-web", []*registry.ModelInfo{
		{ID: modelID, UpstreamID: modelID},
	})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})

	registry.GetGlobalRegistry().RegisterClient(authID, "chatgpt-web", []*registry.ModelInfo{
		{ID: "gpt-image-2", UpstreamID: "gpt-image-2"},
	})
	manager.markExecutionResult(context.Background(), executionResult{
		Result: Result{
			AuthID:   authID,
			Provider: "chatgpt-web",
			Model:    modelID,
			Error: &Error{
				HTTPStatus: http.StatusTooManyRequests,
				Message:    "rate limited",
			},
		},
		authInstanceID: registered.RuntimeInstanceID(),
	})

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared")
	}
	if _, exists := current.ModelStates[modelID]; exists {
		t.Fatalf("removed model state was recreated: %#v", current.ModelStates[modelID])
	}
}

func TestMarkExecutionResultKeepsAuthWideFixedCooldownAfterChatGPTWebModelRemoval(t *testing.T) {
	const (
		authID  = "chatgpt-web-removed-model-auth-cooldown"
		modelID = "removed-model"
	)
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{FixedErrorCooldowns: []internalconfig.FixedErrorCooldownRule{
		{
			MessageContains: "account disabled",
			CooldownSeconds: 3600,
			Scope:           cooldownScopeAuth,
		},
	}})
	registered, err := manager.Register(context.Background(), &Auth{
		ID:       authID,
		Provider: "chatgpt-web",
		Status:   StatusActive,
		Metadata: map[string]any{
			"access_token":    "token",
			"lifecycle_state": LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(authID)
	})

	manager.markExecutionResult(context.Background(), executionResult{
		Result: Result{
			AuthID:   authID,
			Provider: "chatgpt-web",
			Model:    modelID,
			Error: &Error{
				HTTPStatus: http.StatusUnauthorized,
				Message:    "account disabled",
			},
		},
		authInstanceID: registered.RuntimeInstanceID(),
	})

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("auth disappeared")
	}
	if !current.Unavailable || current.CooldownScope != cooldownScopeAuth || current.NextRetryAfter.IsZero() {
		t.Fatalf("auth-wide cooldown was discarded: %#v", current)
	}
	if _, exists := current.ModelStates[modelID]; exists {
		t.Fatalf("removed model state was recreated alongside auth cooldown: %#v", current.ModelStates[modelID])
	}
}
