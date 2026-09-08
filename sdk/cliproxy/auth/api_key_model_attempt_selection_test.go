package auth

import (
	"strings"
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func configuredAttemptFixture(provider, version string, force bool) *config.Config {
	support := &registry.ThinkingSupport{Levels: []string{"high"}}
	if provider == "codex" {
		return &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", Models: []config.CodexModel{{Name: version + "-first", Alias: "shared", ForceMapping: force, Thinking: support}}}}}
	}
	return &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "compat", Models: []config.OpenAICompatibilityModel{
		{Name: version + "-first", Alias: "shared", ForceMapping: force, Thinking: support},
		{Name: version + "-second", Alias: "shared", Thinking: support},
	}}}}
}

func TestConfiguredModelAttemptKeepsOneMappingAndCapabilitySnapshot(t *testing.T) {
	for _, provider := range []string{"codex", "compat"} {
		t.Run(provider, func(t *testing.T) {
			m := NewManager(nil, nil, nil)
			initial := configuredAttemptFixture(provider, "old", true)
			m.SetConfig(initial)
			auth, errRegister := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "one", Provider: provider, Prefix: "tenant", Attributes: map[string]string{"api_key": "fixture", "compat_name": provider}})
			if errRegister != nil {
				t.Fatal(errRegister)
			}
			opts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.ExecutionModelOverrideMetadataKey: "tenant/shared(xhigh)"}}
			models, pooled, alias, snapshot := m.preparedExecutionModelsWithAlias(auth, "gpt-image-2", opts)
			if pooled != (provider == "compat") || !alias.ForceMapping || alias.OriginalAlias != "shared" || alias.UpstreamModel != "old-first(xhigh)" {
				t.Fatalf("unexpected prepared alias: %v", alias)
			}
			if provider == "codex" {
				initial.CodexKey[0].Models[0].Name = "caller-mutated"
				initial.CodexKey[0].Models[0].Thinking.Levels[0] = "low"
			} else {
				initial.OpenAICompatibility[0].Models[0].Name = "caller-mutated"
				initial.OpenAICompatibility[0].Disabled = true
			}
			m.SetConfig(configuredAttemptFixture(provider, "new", false))
			for _, model := range models {
				info, ok := lookupAPIKeyModelCapability(snapshot, auth, effectiveExecutionRouteModel("gpt-image-2", opts), model)
				if !ok || !strings.HasPrefix(info.ID, "old-") || info.Thinking.Levels[0] != "high" {
					t.Fatal("image override or delayed capability lookup used a newer snapshot")
				}
			}
			if got := m.applyAPIKeyModelAlias(auth, "shared(xhigh)", snapshot); got != "old-first(xhigh)" {
				t.Fatalf("fast lookup drifted: %s", got)
			}
			unregistered := auth.Clone()
			unregistered.ID = "unregistered"
			if got := m.applyAPIKeyModelAlias(unregistered, "shared(xhigh)", snapshot); got != "old-first(xhigh)" {
				t.Fatalf("config fallback drifted: %s", got)
			}
			for _, model := range m.executionModelCandidates(auth, "tenant/shared(xhigh)", snapshot) {
				if !strings.HasPrefix(model, "old-") {
					t.Fatal("model pool drifted after hot reload")
				}
			}
			if got := m.resolveExecutionAliasResult(auth, "tenant/shared(xhigh)", snapshot); !got.ForceMapping || got.UpstreamModel != alias.UpstreamModel {
				t.Fatal("response rewriting used newer configuration")
			}
			fresh, _, freshAlias, _ := m.preparedExecutionModelsWithAlias(auth, "gpt-image-2", opts)
			if len(fresh) == 0 || !strings.HasPrefix(fresh[0], "new-") || freshAlias.ForceMapping {
				t.Fatal("next attempt failed to see new configuration")
			}
		})
	}
}

func TestConfiguredModelAttemptSelectionDuringHotReload(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(configuredAttemptFixture("compat", "old", true))
	auth, errRegister := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "one", Provider: "compat", Attributes: map[string]string{"api_key": "fixture", "compat_name": "compat"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 50 {
				models, pooled, alias, snapshot := m.preparedExecutionModelsWithAlias(auth, "shared", cliproxyexecutor.Options{})
				definition := snapshot.config.OpenAICompatibility[0].Models[0]
				if !pooled || len(models) != 2 || alias.UpstreamModel != definition.Name || alias.ForceMapping != definition.ForceMapping {
					t.Error("model pool and force mapping came from different versions")
				}
				for _, model := range models {
					if _, ok := lookupAPIKeyModelCapability(snapshot, auth, "shared", model); !ok {
						t.Error("selected upstream model is absent from attempt capabilities")
					}
				}
			}
		})
	}
	for i := range 20 {
		version := "old"
		if i%2 == 0 {
			version = "new"
		}
		m.SetConfig(configuredAttemptFixture("compat", version, i%2 == 0))
	}
	readers.Wait()
}
