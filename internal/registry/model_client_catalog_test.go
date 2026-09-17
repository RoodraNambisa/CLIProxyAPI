package registry

import (
	"reflect"
	"testing"
)

func TestClientCatalogKeepsCapabilitiesInsideCredentialAndProviderScope(t *testing.T) {
	r := newTestModelRegistry()
	allowed := &ModelInfo{ID: "shared", Name: "models/shared", DisplayName: "Allowed", ContextLength: 131072, Thinking: &ThinkingSupport{Levels: []string{"low"}}}
	r.RegisterClient("allowed", "codex", []*ModelInfo{allowed})
	r.RegisterClient("other-provider", "xai", []*ModelInfo{{ID: "shared", DisplayName: "Other provider"}})
	r.RegisterClient("forbidden", "codex", []*ModelInfo{{ID: "shared", DisplayName: "Forbidden", ContextLength: 1048576, Thinking: &ThinkingSupport{Levels: []string{"high"}}}})
	for _, format := range []string{"openai", "claude", "gemini"} {
		t.Run(format, func(t *testing.T) {
			catalog := r.GetModelCatalogForClients(format, []string{"allowed", "other-provider", "allowed", "missing"}, []string{" CODEX "})
			if len(catalog.Models) != 1 || !reflect.DeepEqual(catalog.Models[0], r.convertModelToMap(allowed, format)) || !reflect.DeepEqual(catalog.Metadata["shared"], allowed) || !reflect.DeepEqual(catalog.Providers["shared"], []string{"codex"}) {
				t.Fatalf("catalog escaped permitted credentials: %#v", catalog)
			}
			catalog.Models[0]["display_name"] = "mutated"
			catalog.Metadata["shared"].Thinking.Levels[0] = "mutated"
			catalog.Providers["shared"][0] = "mutated"
			next := r.GetModelCatalogForClients(format, []string{"allowed"}, nil)
			if !reflect.DeepEqual(next.Models[0], r.convertModelToMap(allowed, format)) || !reflect.DeepEqual(next.Metadata["shared"], allowed) {
				t.Fatal("returned catalog changed registry metadata")
			}
		})
	}
	for _, clients := range [][]string{nil, {}, {"missing"}} {
		if catalog := r.GetModelCatalogForClients("openai", clients, nil); len(catalog.Models) != 0 || len(catalog.Metadata) != 0 || len(catalog.Providers) != 0 {
			t.Fatal("empty credential scope fell back to the global catalog")
		}
	}
}

func TestClientCatalogAvailabilityAndMetadataSelection(t *testing.T) {
	r := newTestModelRegistry()
	for _, entry := range []struct{ client, provider string }{{"a", "codex"}, {"b", "xai"}, {"c", "xai"}, {"forbidden", "other"}} {
		r.RegisterClient(entry.client, entry.provider, []*ModelInfo{{ID: "shared", DisplayName: entry.client}})
	}
	clients := []string{"c", "a", "b", "a"}
	assertCatalog := func(label string, providers []string) {
		t.Helper()
		catalog := r.GetModelCatalogForClients("openai", clients, nil)
		if len(catalog.Models) != 1 || catalog.Metadata["shared"].DisplayName != label || !reflect.DeepEqual(catalog.Providers["shared"], providers) {
			t.Fatalf("catalog=%#v, want label=%s providers=%v", catalog, label, providers)
		}
	}
	assertCatalog("c", []string{"xai", "codex"})
	r.UnregisterClient("c")
	assertCatalog("a", []string{"codex", "xai"})
	for _, reason := range []string{"quota", "model_not_found"} {
		r.SuspendClientModel("a", "shared", reason)
		assertCatalog("a", []string{"codex", "xai"})
		r.ResumeClientModel("a", "shared")
	}
	r.SetModelQuotaExceeded("a", "shared")
	assertCatalog("a", []string{"codex", "xai"})
	r.SuspendClientModel("a", "shared", "disabled")
	assertCatalog("b", []string{"xai"})
	r.UnregisterClient("b")
	if catalog := r.GetModelCatalogForClients("openai", clients, nil); len(catalog.Models) != 0 {
		t.Fatal("unavailable allowed credentials used forbidden metadata")
	}
	clients = []string{"forbidden"}
	assertCatalog("forbidden", []string{"other"})
}
