package openai

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestCodexClientTemplateKeepsExactRemoteNumbers(t *testing.T) {
	raw, revision := registry.GetCodexClientModelsSnapshot()
	t.Cleanup(func() { _, _, _ = loadCodexClientModelTemplatesSnapshot(raw, revision) })
	var payload codexClientModelsPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	payload.Models[0]["future_numeric_capability"] = json.Number("9007199254740993")
	payload.Models[0]["priority"] = -7
	changed, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	templates, _, err := loadCodexClientModelTemplatesSnapshot(changed, revision+1)
	if err != nil {
		t.Fatal(err)
	}
	model := cloneCodexClientModelMap(templates["gpt-6-astra"])
	output, err := json.Marshal(model)
	if err != nil || !bytes.Contains(output, []byte(`"future_numeric_capability":9007199254740993`)) {
		t.Fatal("model response rounded a remote integer")
	}
	if codexClientModelPriority(model) != -7 || intModelValue(model, "context_window") != 272000 {
		t.Fatal("numeric metadata stopped participating in ordering or context lookup")
	}
}

func TestCodexClientNonTemplatePriorityStaysInClientRange(t *testing.T) {
	for _, base := range []int{10, math.MaxInt32 - 150, math.MaxInt32} {
		models := []map[string]any{{"slug": "b"}, {"slug": "a"}}
		templates := map[string]map[string]any{"known": {"priority": base}}
		applyCodexClientNonTemplatePriorities(models, templates)
		for index, rank := range []int{2, 1} {
			want := min(int64(math.MaxInt32), int64(base)+100*int64(rank))
			got := int64(codexClientModelPriority(models[index]))
			if got != want {
				t.Fatalf("base=%d rank=%d priority=%d, want %d", base, rank, got, want)
			}
		}
	}
}
