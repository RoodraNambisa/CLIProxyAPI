package auth

import (
	"context"
	"net/http"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCodexQuotaContextUsesLogicalRequestPolicyAndManagerOwner(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	installed, err := manager.Register(t.Context(), &Auth{ID: "quota-context", Provider: "codex"})
	if err != nil {
		t.Fatal(err)
	}
	inherited := core.WithCodexQuotaObserver(t.Context(), func(string, string, string, http.Header) { t.Fatal("inherited sink bypassed manager policy") })
	off := manager.withCodexQuotaObservation(manager.WithRoutingPolicySnapshot(inherited))
	if core.CodexQuotaObserverFromContext(off) != nil {
		t.Fatal("disabled manager retained an inherited quota observer")
	}
	manager.SetConfig(&config.Config{Codex: config.CodexConfig{ObserveQuota: true}})
	logical := manager.WithRoutingPolicySnapshot(t.Context())
	request := manager.withCodexQuotaObservation(logical)
	manager.SetConfig(&config.Config{})
	for _, ctx := range []context.Context{request, manager.withCodexQuotaObservation(logical)} {
		observer := core.CodexQuotaObserverFromContext(ctx)
		if observer == nil {
			t.Fatal("hot reload disabled an in-flight logical request or retry")
		}
		observer(installed.ID, installed.RuntimeInstanceID(), "http", http.Header{"X-Codex-Plan-Type": {"pro"}})
	}
	current, _ := manager.GetByID(installed.ID)
	if current.CodexQuotaSnapshot() == nil || core.CodexQuotaObserverFromContext(manager.withCodexQuotaObservation(manager.WithRoutingPolicySnapshot(t.Context()))) != nil {
		t.Fatal("request quota snapshot or fresh disabled request is incorrect")
	}
	other := NewManager(nil, nil, nil)
	if core.CodexQuotaObserverFromContext(other.withCodexQuotaObservation(other.WithRoutingPolicySnapshot(request))) != nil {
		t.Fatal("quota observer leaked across manager ownership")
	}
}
