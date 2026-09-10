package registry

import (
	"reflect"
	"slices"
	"testing"
)

func TestGeminiCatalogChangesNotifyBothProtocols(t *testing.T) {
	before := &staticModelsJSON{Gemini: []*ModelInfo{{ID: "fixture", ContextLength: 128000}}}
	after := &staticModelsJSON{Gemini: []*ModelInfo{{ID: "fixture", ContextLength: 256000}}}
	if got := detectChangedProviders(before, after); !slices.Equal(got, []string{"gemini", "gemini-interactions"}) {
		t.Fatalf("changed providers = %v; both Gemini protocols need re-registration", got)
	}
	if got := detectChangedProviders(after, after); len(got) != 0 {
		t.Fatalf("unchanged catalog notified providers: %v", got)
	}
	if got := detectChangedProviders(before, &staticModelsJSON{}); !slices.Equal(got, []string{"gemini", "gemini-interactions"}) {
		t.Fatalf("removed model notifications = %v", got)
	}
	if got := detectChangedProviders(before, nil); len(got) != 0 {
		t.Fatalf("missing catalog notified providers: %v", got)
	}
}

func TestInteractionsStaticCatalogUsesGeminiDefinitions(t *testing.T) {
	want := GetStaticModelDefinitionsByChannel("gemini")
	got := GetStaticModelDefinitionsByChannel(" Gemini-Interactions ")
	if len(want) == 0 || !reflect.DeepEqual(got, want) {
		t.Fatal("Gemini Interactions is missing the shared static catalog")
	}
	got[0].ID = "modified-fixture"
	if fresh := GetStaticModelDefinitionsByChannel("gemini-interactions"); !reflect.DeepEqual(fresh, want) {
		t.Fatal("caller changes affected the shared catalog")
	}
}
