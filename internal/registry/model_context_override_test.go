package registry

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
)

func TestModelContextOverrideRetainsPerClientIsolation(t *testing.T) {
	for _, secondProvider := range []string{"codex", "other"} {
		t.Run(secondProvider, func(t *testing.T) {
			r := newTestModelRegistry()
			first := &ModelInfo{ID: "context-shared", ContextLength: 131072, MaxContextLength: 131072}
			second := &ModelInfo{ID: "context-shared", ContextLength: 1048576, MaxContextLength: 1048576}
			r.RegisterClient("first", "codex", []*ModelInfo{first})
			r.RegisterClient("second", secondProvider, []*ModelInfo{second})
			first.MaxContextLength = 1
			for id, want := range map[string]int{"first": 131072, "second": 1048576} {
				got := r.GetModelsForClient(id)
				if len(got) != 1 || got[0].MaxContextLength != want {
					t.Fatal("credential catalogs shared an override")
				}
				got[0].MaxContextLength = 2
				if r.GetModelsForClient(id)[0].MaxContextLength != want {
					t.Fatal("query mutated the installed override")
				}
			}
			var readers sync.WaitGroup
			for range 4 {
				readers.Go(func() {
					for range 30 {
						got := r.GetModelsForClient("second")
						if len(got) != 1 || got[0].MaxContextLength != 1048576 {
							t.Error("updating another credential changed its context override")
						}
					}
				})
			}
			r.RegisterClientPreservingState("first", "codex", []*ModelInfo{{ID: "context-shared", ContextLength: 32768}})
			readers.Wait()
			if r.GetModelsForClient("first")[0].MaxContextLength != 0 {
				t.Fatal("cleared override remained")
			}
			r.UnregisterClient("first")
			if r.GetModelInfo("context-shared", secondProvider).MaxContextLength != 1048576 {
				t.Fatal("unregister lost remaining client's override")
			}
		})
	}
}

func TestModelContextOverrideOpenAIMappingAndCacheCopies(t *testing.T) {
	r := newTestModelRegistry()
	model := &ModelInfo{ID: "context-mapped", ContextLength: 131072, MaxContextLength: 131072}
	r.RegisterClient("client", "codex", []*ModelInfo{model})
	response := r.GetAvailableModels("openai")
	if len(response) != 1 || response[0]["max_context_length"] != 131072 {
		t.Fatal("catalog omitted explicit context override")
	}
	response[0]["max_context_length"] = 1
	if r.GetAvailableModels("openai")[0]["max_context_length"] != 131072 {
		t.Fatal("caller changed cached model declaration")
	}
	encoded, err := json.Marshal(model)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "max_context") {
		t.Fatal("internal override leaked into generic model JSON")
	}
	for _, value := range []int{0, -1} {
		if _, exists := r.convertModelToMap(&ModelInfo{ID: "unset", MaxContextLength: value}, "openai")["max_context_length"]; exists {
			t.Fatal("non-positive override changed old response")
		}
	}
}
