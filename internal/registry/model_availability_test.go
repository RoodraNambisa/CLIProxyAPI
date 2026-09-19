package registry

import (
	"testing"
	"time"
)

func TestTemporaryModelAvailabilityPreservesAcquisitionCatalog(t *testing.T) {
	r := newTestModelRegistry()
	r.RegisterClient("a", "codex", []*ModelInfo{{ID: "shared"}, {ID: "only-a"}})
	r.RegisterClient("b", "xai", []*ModelInfo{{ID: "shared"}})
	// Prime the ordinary cache before enabling gating.
	_ = r.GetAvailableModels("openai")
	view := ClientModelAvailability{"a": {"shared": {}, "only-a": {}}}
	r.SetClientModelAvailability(func() ClientModelAvailability { return view })
	if len(r.GetModelsForClient("a")) != 2 || !r.ClientSupportsModel("a", "only-a") {
		t.Fatal("hidden route was removed from the acquisition catalog")
	}
	if len(r.GetSelectableModelsForClient("a")) != 0 || r.ClientModelAvailable("a", "shared") {
		t.Fatal("hidden credential model remained selectable")
	}
	for _, models := range [][]map[string]any{r.GetAvailableModels("openai"), r.GetOpenAIModelCatalog().Models} {
		if len(models) != 1 || models[0]["id"] != "shared" {
			t.Fatalf("global catalog=%v", models)
		}
	}
	if len(r.GetModelCatalogForProviders("openai", []string{"codex"}).Models) != 0 || len(r.GetAvailableModelsByProvider("codex")) != 0 || len(r.GetModelCatalogForClients("openai", []string{"a"}, nil).Models) != 0 || len(r.GetModelCatalogForClient("openai", "a").Models) != 0 {
		t.Fatal("restricted catalog exposed a hidden model")
	}
	if providers := r.GetModelProviders("shared"); len(providers) != 1 || providers[0] != "xai" || r.GetModelCount("shared") != 1 {
		t.Fatal("hidden provider still supplied shared model")
	}
	r.SuspendClientModel("a", "only-a", "manual")
	view = ClientModelAvailability{"a": {"shared": time.Now().Add(time.Hour), "only-a": time.Now().Add(time.Hour)}}
	if !r.ClientModelAvailable("a", "shared") || len(r.GetModelProviders("shared")) != 2 {
		t.Fatal("new valid State did not restore route immediately")
	}
	if len(r.GetModelProviders("only-a")) != 0 {
		t.Fatal("State recovery cleared an unrelated suspension")
	}
	view = ClientModelAvailability{"a": {"shared": time.Now().Add(-time.Second)}}
	if r.ClientModelAvailable("a", "shared") || len(r.GetModelCatalogForClient("openai", "a").Models) != 1 {
		t.Fatal("expiry did not remove the model without a new snapshot")
	}
	view = nil
	if !r.ClientModelAvailable("a", "shared") || len(r.GetModelProviders("shared")) != 2 {
		t.Fatal("policy removal retained gating")
	}
}
