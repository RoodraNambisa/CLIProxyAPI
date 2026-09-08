package registry

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
)

func TestProviderCatalogUsesAllowedMetadataAndClones(t *testing.T) {
	r := newTestModelRegistry()
	allowed := &ModelInfo{ID: "shared", Name: "models/shared", DisplayName: "Allowed", OwnedBy: "allowed", ContextLength: 131072, MaxContextLength: 131072, InputTokenLimit: 131072, SupportedParameters: []string{"reasoning"}}
	r.RegisterClient("allowed", "codex", []*ModelInfo{allowed})
	r.RegisterClient("forbidden", "xai", []*ModelInfo{{ID: "shared", DisplayName: "Forbidden", OwnedBy: "forbidden", ContextLength: 1048576, MaxContextLength: 1048576}})
	for _, format := range []string{"openai", "claude", "gemini"} {
		t.Run(format, func(t *testing.T) {
			got := r.GetAvailableModelsForProviders(format, []string{" CODEX ", "codex", ""})
			want := r.convertModelToMap(allowed, format)
			if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
				t.Fatalf("catalog = %#v, want %#v", got, want)
			}
			got[0]["display_name"] = "mutated"
			if params, ok := got[0]["supported_parameters"].([]string); ok {
				params[0] = "mutated"
			}
			if next := r.GetAvailableModelsForProviders(format, []string{"codex"}); !reflect.DeepEqual(next[0], want) {
				t.Fatalf("query mutated registry: %#v", next)
			}
		})
	}
	for _, providers := range [][]string{nil, {}, {"unknown"}, {" "}} {
		if got := r.GetAvailableModelsForProviders("openai", providers); len(got) != 0 {
			t.Fatalf("allowlist %#v returned %#v", providers, got)
		}
	}
}

func TestProviderCatalogOrderingAvailabilityAndRemoval(t *testing.T) {
	r := newTestModelRegistry()
	for _, entry := range []struct{ client, provider string }{{"a", "codex"}, {"b", "xai"}, {"c", "xai"}} {
		r.RegisterClient(entry.client, entry.provider, []*ModelInfo{{ID: "shared", DisplayName: entry.provider}})
	}
	assertLabel := func(want string) {
		t.Helper()
		got := r.GetAvailableModelsForProviders("openai", []string{"xai", "codex"})
		if len(got) != 1 || got[0]["display_name"] != want {
			t.Fatalf("catalog = %#v, want %s", got, want)
		}
	}
	assertLabel("xai")
	r.UnregisterClient("c")
	assertLabel("codex")
	r.SuspendClientModel("a", "shared", "disabled")
	assertLabel("xai")
	if got := r.GetAvailableModelsForProviders("openai", []string{"codex"}); len(got) != 0 {
		t.Fatalf("suspended provider exposed catalog: %#v", got)
	}
	r.ResumeClientModel("a", "shared")
	r.SetModelQuotaExceeded("a", "shared")
	assertLabel("codex")
	r.SuspendClientModel("a", "shared", "quota")
	assertLabel("codex")
	r.UnregisterClient("a")
	assertLabel("xai")
	if got := r.GetAvailableModelsForProviders("openai", []string{"codex"}); len(got) != 0 {
		t.Fatalf("removed provider fell back to forbidden metadata: %#v", got)
	}
}

func TestProviderCatalogConcurrentUpdatesKeepOneSnapshot(t *testing.T) {
	r := newTestModelRegistry()
	register := func(revision int) {
		r.RegisterClient("allowed", "codex", []*ModelInfo{
			{ID: "one", DisplayName: fmt.Sprint(revision), MaxContextLength: revision},
			{ID: "two", DisplayName: fmt.Sprint(revision), MaxContextLength: revision},
		})
	}
	register(1)
	var readers sync.WaitGroup
	for range 8 {
		readers.Go(func() {
			for range 100 {
				models := r.GetAvailableModelsForProviders("openai", []string{"codex"})
				if len(models) != 2 || models[0]["display_name"] != models[1]["display_name"] || models[0]["max_context_length"] != models[1]["max_context_length"] {
					t.Errorf("mixed snapshot: %#v", models)
					return
				}
			}
		})
	}
	for revision := 2; revision < 100; revision++ {
		register(revision)
	}
	readers.Wait()
}
