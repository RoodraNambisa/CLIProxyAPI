package helps

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestParseXAICLIModelCapabilitiesSurviveCache(t *testing.T) {
	raw := []byte(`{"data":[{"id":"grok-cli-fixture","name":"Grok CLI Fixture","context_window":500000,"supports_reasoning_effort":true,"reasoning_efforts":[{"value":"xhigh"},{"value":"high"},{"value":"medium"},{"value":"low"}]}]}`)
	catalog, err := ParseXAIModels(raw, "https://cli-chat-proxy.grok.com/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(catalog)
	if err != nil {
		t.Fatal(err)
	}
	var stored any
	if err = json.Unmarshal(encoded, &stored); err != nil {
		t.Fatal(err)
	}
	cached := XAIModelsForAuth(&coreauth.Auth{Metadata: map[string]any{XAIModelCatalogKey: stored}})
	if cached == nil || len(cached.Models) != 1 {
		t.Fatal("model catalog did not survive persistence")
	}
	model := cached.Models[0]
	if model.ContextLength != 500000 || model.DisplayName != "Grok CLI Fixture" || model.Thinking == nil || !reflect.DeepEqual(model.Thinking.Levels, []string{"xhigh", "high", "medium", "low"}) {
		t.Fatalf("CLI model capabilities lost: %+v", model)
	}
}

func TestParseXAIModelCapabilitiesRespectExplicitValues(t *testing.T) {
	catalog, err := ParseXAIModels([]byte(`{"data":[{"id":"fixture","display_name":"Explicit","name":"CLI name","context_length":1000000,"context_window":500000,"thinking":{"levels":["low","high"]},"reasoning_efforts":[{"value":"xhigh"}]}]}`), "fixture")
	if err != nil {
		t.Fatal(err)
	}
	model := catalog.Models[0]
	if model.ContextLength != 1000000 || model.DisplayName != "Explicit" || !reflect.DeepEqual(model.Thinking.Levels, []string{"low", "high"}) {
		t.Fatal("CLI aliases overwrote explicit registry capabilities")
	}
	for _, baseline := range registry.GetXAIModels() {
		if baseline.Thinking == nil {
			continue
		}
		raw, errMarshal := json.Marshal(map[string]any{"data": []any{map[string]any{"id": baseline.ID, "supports_reasoning_effort": false}}})
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		catalog, err = ParseXAIModels(raw, "fixture")
		if err != nil {
			t.Fatal(err)
		}
		if catalog.Models[0].Thinking != nil {
			t.Fatal("static fallback overrode explicit lack of reasoning support")
		}
		return
	}
	t.Fatal("missing static reasoning model fixture")
}
