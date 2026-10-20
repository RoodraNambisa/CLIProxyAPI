package auth

import (
	"testing"

	"github.com/google/uuid"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestCompactionKeepsTextCredentialWithImageFallbackEnabled(t *testing.T) {
	for _, alt := range []string{"", "responses/compact"} {
		m := NewManager(nil, nil, nil)
		m.SetRetryConfig(0, 0, 0)
		m.SetConfig(&internalconfig.Config{
			DisabledImageGenerationToolFallback: true,
			AuthModelExclusions:                 []internalconfig.AuthModelExclusionRule{{DisableImageGeneration: true, Priorities: []int{10}}},
		})
		executor := &authFallbackExecutor{id: "codex"}
		m.RegisterExecutor(executor)
		model := "compact-routing-" + uuid.NewString()
		high := "compact-high-" + uuid.NewString()
		registerCodexFallbackAuthForModel(t, m, &Auth{ID: high, Provider: "codex", Attributes: map[string]string{"priority": "10"}}, model)
		registerCodexFallbackAuthForModel(t, m, &Auth{ID: "compact-low-" + uuid.NewString(), Provider: "codex", Attributes: map[string]string{"priority": "0"}}, model)
		input := `[{"type":"compaction_trigger"}]`
		if alt != "" {
			input = `[]`
		}
		body := []byte(`{"input":` + input + `,"tools":[{"type":"image_generation"}]}`)
		result, err := m.Execute(t.Context(), []string{"codex"}, core.Request{Model: model, Payload: body}, core.Options{Alt: alt})
		if err != nil {
			t.Fatal(err)
		}
		if string(result.Payload) != high {
			t.Fatalf("compaction changed credential: %s", result.Payload)
		}
	}
}
