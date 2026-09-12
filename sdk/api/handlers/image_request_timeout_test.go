package handlers

import (
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"sync"
	"testing"
	"time"
)

func TestImageRequestTimeoutHotReloadSnapshots(t *testing.T) {
	initial := &config.SDKConfig{}
	initial.Images.CodexRequestTimeoutSeconds = 1800
	initial.Images.ChatGPTWeb.RequestTimeoutSeconds = 1200
	h := NewBaseAPIHandlers(initial, nil)
	snapshot := h.newImageRequestBudget(t.Context())
	defer snapshot.Close()
	var workers sync.WaitGroup
	workers.Go(func() {
		for range 100 {
			h.UpdateClients(&config.SDKConfig{})
		}
	})
	workers.Go(func() {
		for range 100 {
			budget := h.newImageRequestBudget(t.Context())
			budget.Close()
		}
	})
	workers.Wait()
	if snapshot.Limit("codex") != 1800*time.Second || snapshot.Limit("chatgpt-web") != 1200*time.Second {
		t.Fatal("hot update changed active request")
	}
	if h.newImageRequestBudget(t.Context()) != nil {
		t.Fatal("new request ignored disabled policy")
	}
}
