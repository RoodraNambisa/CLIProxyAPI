package registry

import (
	"testing"
	"time"
)

func TestGetModelInfoReturnsClone(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "gemini", []*ModelInfo{{
		ID:          "m1",
		DisplayName: "Model One",
		Thinking:    &ThinkingSupport{Min: 1, Max: 2, Levels: []string{"low", "high"}},
	}})

	first := r.GetModelInfo("m1", "gemini")
	if first == nil {
		t.Fatal("expected model info")
	}
	first.DisplayName = "mutated"
	first.Thinking.Levels[0] = "mutated"

	second := r.GetModelInfo("m1", "gemini")
	if second.DisplayName != "Model One" {
		t.Fatalf("expected cloned display name, got %q", second.DisplayName)
	}
	if second.Thinking == nil || len(second.Thinking.Levels) == 0 || second.Thinking.Levels[0] != "low" {
		t.Fatalf("expected cloned thinking levels, got %+v", second.Thinking)
	}
}

func TestGetModelsForClientReturnsClones(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "gemini", []*ModelInfo{{
		ID:          "m1",
		DisplayName: "Model One",
		Thinking:    &ThinkingSupport{Levels: []string{"low", "high"}},
	}})

	first := r.GetModelsForClient("client-1")
	if len(first) != 1 || first[0] == nil {
		t.Fatalf("expected one model, got %+v", first)
	}
	first[0].DisplayName = "mutated"
	first[0].Thinking.Levels[0] = "mutated"

	second := r.GetModelsForClient("client-1")
	if len(second) != 1 || second[0] == nil {
		t.Fatalf("expected one model on second fetch, got %+v", second)
	}
	if second[0].DisplayName != "Model One" {
		t.Fatalf("expected cloned display name, got %q", second[0].DisplayName)
	}
	if second[0].Thinking == nil || len(second[0].Thinking.Levels) == 0 || second[0].Thinking.Levels[0] != "low" {
		t.Fatalf("expected cloned thinking levels, got %+v", second[0].Thinking)
	}
}

func TestUnregisterClientRebindsModelInfoToRemainingClient(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-a", "chatgpt-web", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Remaining",
	}})
	r.RegisterClient("client-z", "chatgpt-web", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Departing",
	}})

	r.UnregisterClient("client-z")

	for _, provider := range []string{"", "chatgpt-web"} {
		info := r.GetModelInfo("shared", provider)
		if info == nil || info.DisplayName != "Remaining" {
			t.Fatalf("provider %q info after unregister = %#v", provider, info)
		}
	}
}

func TestProviderChangeRebindsPreviousProviderModelInfo(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-a", "provider-a", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Provider A",
	}})
	r.RegisterClient("client-z", "provider-a", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Moving",
	}})

	r.RegisterClient("client-z", "provider-b", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Provider B",
	}})

	infoA := r.GetModelInfo("shared", "provider-a")
	if infoA == nil || infoA.DisplayName != "Provider A" {
		t.Fatalf("provider-a info after provider change = %#v", infoA)
	}
	infoB := r.GetModelInfo("shared", "provider-b")
	if infoB == nil || infoB.DisplayName != "Provider B" {
		t.Fatalf("provider-b info after provider change = %#v", infoB)
	}
	global := r.GetModelInfo("shared", "")
	if global == nil || global.DisplayName != "Provider B" {
		t.Fatalf("global info after provider change = %#v", global)
	}
}

func TestRegisterClientPreservingStateKeepsOverlappingTransientState(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "openai", []*ModelInfo{{ID: "keep"}, {ID: "remove"}})
	r.SetModelQuotaExceeded("client-1", "keep")
	r.SuspendClientModel("client-1", "keep", "cooldown")
	r.SetModelQuotaExceeded("client-1", "remove")
	r.SuspendClientModel("client-1", "remove", "cooldown")

	r.RegisterClientPreservingState("client-1", "openai", []*ModelInfo{{ID: "keep"}, {ID: "add"}})

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	keep := r.models["keep"]
	if keep == nil || keep.QuotaExceededClients["client-1"] == nil || keep.SuspendedClients["client-1"] != "cooldown" {
		t.Fatalf("overlapping state = %#v", keep)
	}
	if _, exists := r.models["remove"]; exists {
		t.Fatal("removed model registration remained")
	}
	added := r.models["add"]
	if added == nil || added.QuotaExceededClients["client-1"] != nil || added.SuspendedClients["client-1"] != "" {
		t.Fatalf("new model inherited transient state: %#v", added)
	}
}

func TestRegisterClientDeduplicatesModelIDs(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "openai", []*ModelInfo{{ID: "keep"}, {ID: "keep"}})
	r.SetModelQuotaExceeded("client-1", "keep")
	r.SuspendClientModel("client-1", "keep", "cooldown")

	r.RegisterClientPreservingState("client-1", "openai", []*ModelInfo{{ID: "keep"}, {ID: "keep"}})

	r.mutex.RLock()
	keep := r.models["keep"]
	count := 0
	providerCount := 0
	quotaRetained := false
	suspensionRetained := false
	if keep != nil {
		count = keep.Count
		providerCount = keep.Providers["openai"]
		quotaRetained = keep.QuotaExceededClients["client-1"] != nil
		suspensionRetained = keep.SuspendedClients["client-1"] == "cooldown"
	}
	clientModels := append([]string(nil), r.clientModels["client-1"]...)
	r.mutex.RUnlock()

	if keep == nil || count != 1 {
		t.Fatalf("remaining registration = %#v", keep)
	}
	if providerCount != 1 {
		t.Fatalf("provider count = %d, want 1", providerCount)
	}
	if len(clientModels) != 1 || clientModels[0] != "keep" {
		t.Fatalf("client models = %#v, want one keep", clientModels)
	}
	if !quotaRetained || !suspensionRetained {
		t.Fatalf("remaining transient state = %#v", keep)
	}

	if count := r.GetModelCount("keep"); count != 0 {
		t.Fatalf("available count with quota and suspension = %d, want 0", count)
	}
	r.UnregisterClient("client-1")
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	if _, exists := r.models["keep"]; exists {
		t.Fatal("deduplicated model remained after client unregister")
	}
}

func TestRegisterClientEmptySetClearsBookkeepingBeforeProviderChange(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-a", "provider-a", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Remaining",
	}})
	r.RegisterClient("client-z", "provider-a", []*ModelInfo{{
		ID:          "shared",
		DisplayName: "Departing",
	}})

	r.RegisterClient("client-z", "provider-a", nil)
	r.mutex.RLock()
	_, staleBookkeeping := r.clientModels["client-z"]
	r.mutex.RUnlock()
	if staleBookkeeping {
		t.Fatal("empty model set retained client bookkeeping")
	}
	r.RegisterClient("client-z", "provider-b", []*ModelInfo{{ID: "other"}})
	r.UnregisterClient("client-z")

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	shared := r.models["shared"]
	if shared == nil || shared.Count != 1 || shared.Providers["provider-a"] != 1 {
		t.Fatalf("remaining shared registration = %#v", shared)
	}
	if info := shared.Info; info == nil || info.DisplayName != "Remaining" {
		t.Fatalf("remaining shared metadata = %#v", info)
	}
	if _, exists := r.clientModels["client-z"]; exists {
		t.Fatalf("empty client bookkeeping remained: %#v", r.clientModels["client-z"])
	}
}

func TestRegisterClientPreservingStateClearsTransientStateOnProviderChange(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "provider-a", []*ModelInfo{{ID: "shared"}})
	r.SetModelQuotaExceeded("client-1", "shared")
	r.SuspendClientModel("client-1", "shared", "cooldown")

	r.RegisterClientPreservingState("client-1", "provider-b", []*ModelInfo{{ID: "shared"}})

	r.mutex.RLock()
	defer r.mutex.RUnlock()
	shared := r.models["shared"]
	if shared == nil {
		t.Fatal("shared model was removed")
	}
	if shared.QuotaExceededClients["client-1"] != nil || shared.SuspendedClients["client-1"] != "" {
		t.Fatalf("provider replacement inherited transient state: %#v", shared)
	}
}

func TestGetAvailableModelsByProviderReturnsClones(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "gemini", []*ModelInfo{{
		ID:          "m1",
		DisplayName: "Model One",
		Thinking:    &ThinkingSupport{Levels: []string{"low", "high"}},
	}})

	first := r.GetAvailableModelsByProvider("gemini")
	if len(first) != 1 || first[0] == nil {
		t.Fatalf("expected one model, got %+v", first)
	}
	first[0].DisplayName = "mutated"
	first[0].Thinking.Levels[0] = "mutated"

	second := r.GetAvailableModelsByProvider("gemini")
	if len(second) != 1 || second[0] == nil {
		t.Fatalf("expected one model on second fetch, got %+v", second)
	}
	if second[0].DisplayName != "Model One" {
		t.Fatalf("expected cloned display name, got %q", second[0].DisplayName)
	}
	if second[0].Thinking == nil || len(second[0].Thinking.Levels) == 0 || second[0].Thinking.Levels[0] != "low" {
		t.Fatalf("expected cloned thinking levels, got %+v", second[0].Thinking)
	}
}

func TestCleanupExpiredQuotasInvalidatesAvailableModelsCache(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "openai", []*ModelInfo{{ID: "m1", Created: 1}})
	r.SetModelQuotaExceeded("client-1", "m1")
	if models := r.GetAvailableModels("openai"); len(models) != 1 {
		t.Fatalf("expected cooldown model to remain listed before cleanup, got %d", len(models))
	}

	r.mutex.Lock()
	quotaTime := time.Now().Add(-6 * time.Minute)
	r.models["m1"].QuotaExceededClients["client-1"] = &quotaTime
	r.mutex.Unlock()

	r.CleanupExpiredQuotas()

	if count := r.GetModelCount("m1"); count != 1 {
		t.Fatalf("expected model count 1 after cleanup, got %d", count)
	}
	models := r.GetAvailableModels("openai")
	if len(models) != 1 {
		t.Fatalf("expected model to stay available after cleanup, got %d", len(models))
	}
	if got := models[0]["id"]; got != "m1" {
		t.Fatalf("expected model id m1, got %v", got)
	}
}

func TestGetAvailableModelsReturnsClonedSupportedParameters(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("client-1", "openai", []*ModelInfo{{
		ID:                  "m1",
		DisplayName:         "Model One",
		SupportedParameters: []string{"temperature", "top_p"},
	}})

	first := r.GetAvailableModels("openai")
	if len(first) != 1 {
		t.Fatalf("expected one model, got %d", len(first))
	}
	params, ok := first[0]["supported_parameters"].([]string)
	if !ok || len(params) != 2 {
		t.Fatalf("expected supported_parameters slice, got %#v", first[0]["supported_parameters"])
	}
	params[0] = "mutated"

	second := r.GetAvailableModels("openai")
	params, ok = second[0]["supported_parameters"].([]string)
	if !ok || len(params) != 2 || params[0] != "temperature" {
		t.Fatalf("expected cloned supported_parameters, got %#v", second[0]["supported_parameters"])
	}
}

func TestLookupModelInfoReturnsCloneForStaticDefinitions(t *testing.T) {
	first := LookupModelInfo("claude-sonnet-4-6")
	if first == nil || first.Thinking == nil || len(first.Thinking.Levels) == 0 {
		t.Fatalf("expected static model with thinking levels, got %+v", first)
	}
	first.Thinking.Levels[0] = "mutated"

	second := LookupModelInfo("claude-sonnet-4-6")
	if second == nil || second.Thinking == nil || len(second.Thinking.Levels) == 0 || second.Thinking.Levels[0] == "mutated" {
		t.Fatalf("expected static lookup clone, got %+v", second)
	}
}
