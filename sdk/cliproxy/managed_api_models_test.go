package cliproxy

import (
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestSelectedAPIModelsExcludeDynamicallyAddedModels(t *testing.T) {
	a := &coreauth.Auth{Attributes: map[string]string{"config_models_mode": "selected", "config_models": "[\"public\"]"}}
	result := filterSelectedAPIModels(a, []*ModelInfo{{ID: "public"}, {ID: "gpt-image-1"}, {ID: "other"}})
	if len(result) != 1 || result[0].ID != "public" {
		t.Fatal("selected scope expanded", result)
	}
	delete(a.Attributes, "config_models_mode")
	if len(filterSelectedAPIModels(a, []*ModelInfo{{ID: "public"}, {ID: "other"}})) != 2 {
		t.Fatal("default mode changed")
	}
}
