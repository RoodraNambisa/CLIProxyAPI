package openai

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientTemplateRevisionPublishesWholeSnapshots(t *testing.T) {
	raw, current := registry.GetCodexClientModelsSnapshot()
	t.Cleanup(func() { _, _, _ = loadCodexClientModelTemplatesSnapshot(raw, current) })
	var original codexClientModelsPayload
	if err := json.Unmarshal(raw, &original); err != nil {
		t.Fatal(err)
	}
	for _, model := range original.Models {
		model["description"] = "revision A"
	}
	first, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	for _, model := range original.Models {
		model["description"] = "revision B"
	}
	second, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	old, _, err := loadCodexClientModelTemplatesSnapshot(first, current+1)
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for index := range 12 {
		workers.Go(func() {
			body, revision, want := first, current+1, "revision A"
			if index%2 == 1 {
				body, revision, want = second, current+2, "revision B"
			}
			for range 10 {
				templates, fallback, errLoad := loadCodexClientModelTemplatesSnapshot(body, revision)
				if errLoad != nil || fallback["description"] != want {
					t.Error("fallback template used a different revision")
					return
				}
				for _, model := range templates {
					if model["description"] != want {
						t.Error("mixed template revisions")
					}
				}
			}
		})
	}
	workers.Wait()
	for _, model := range old {
		if model["description"] != "revision A" {
			t.Fatal("refresh mutated an in-flight response snapshot")
		}
	}
	if _, _, err = loadCodexClientModelTemplatesSnapshot([]byte("invalid"), current+3); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	_, fallback, err := loadCodexClientModelTemplatesSnapshot(first, current+1)
	if err != nil || fallback["description"] != "revision A" {
		t.Fatal("failed parse poisoned a valid template cache")
	}
}
