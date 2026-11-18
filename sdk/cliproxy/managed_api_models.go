package cliproxy

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func filterSelectedAPIModels(a *coreauth.Auth, models []*ModelInfo) []*ModelInfo {
	if a == nil || a.Attributes["config_models_mode"] != "selected" {
		return models
	}
	var names []string
	if json.Unmarshal([]byte(a.Attributes["config_models"]), &names) != nil {
		return nil
	}
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[strings.ToLower(name)] = true
	}
	out := make([]*ModelInfo, 0, len(models))
	for _, model := range models {
		if model != nil && allowed[strings.ToLower(model.ID)] {
			out = append(out, model)
		}
	}
	return out
}

func apiCredentialDefinitionsChanged(a, b *config.Config) bool {
	if a == nil || b == nil {
		return a != b
	}
	return !reflect.DeepEqual(a.GeminiKey, b.GeminiKey) || !reflect.DeepEqual(a.InteractionsKey, b.InteractionsKey) || !reflect.DeepEqual(a.ClaudeKey, b.ClaudeKey) || !reflect.DeepEqual(a.CodexKey, b.CodexKey) || !reflect.DeepEqual(a.XAIKey, b.XAIKey) || !reflect.DeepEqual(a.VertexCompatAPIKey, b.VertexCompatAPIKey) || !reflect.DeepEqual(a.OpenAICompatibility, b.OpenAICompatibility)
}

func managedAPIConfig[T any](cfg *config.Config, a *coreauth.Auth) *T {
	if a == nil {
		return nil
	}
	ref, ok := cfg.APICredentialByID(a.Attributes["config_id"])
	if !ok {
		return nil
	}
	entry, _ := ref.Value.(*T)
	return entry
}
