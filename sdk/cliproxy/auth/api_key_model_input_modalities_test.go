package auth

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

func TestModelInputModalitiesAttemptMatchesExactModelAndDoesNotInheritCatalog(t *testing.T) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "fixture", Models: []config.OpenAICompatibilityModel{
		{Name: "kimi-k2", Alias: "shared"},
		{Name: "gpt-5.5", Alias: "shared", InputModalities: []string{"text"}},
		{Name: "gpt-5.5(high)", Alias: "shared", InputModalities: []string{"text", "image"}},
	}}}}
	auth := &Auth{ID: "fixture", Provider: "fixture", Prefix: "tenant", Attributes: map[string]string{"api_key": "test", "provider_key": "fixture", "compat_name": "fixture"}}
	routing := buildAPIKeyModelRoutingSnapshot(map[string]*Auth{auth.ID: auth}, cfg)
	for _, tc := range []struct {
		upstream string
		want     []string
	}{
		{"kimi-k2", nil}, {"gpt-5.5", []string{"text"}}, {"gpt-5.5(high)", []string{"text", "image"}}, {"gpt-5.5(low)", []string{"text"}}, {"unknown", nil},
	} {
		bound := attachResolvedAPIKeyModelInfo(routing, core.Request{Model: tc.upstream}, auth, "tenant/shared", tc.upstream)
		got, known := ResolvedModelInputModalities(bound)
		if !known || !reflect.DeepEqual(got, tc.want) {
			t.Fatalf("upstream %s has modalities %v, known=%t", tc.upstream, got, known)
		}
		if len(got) > 0 {
			got[0] = "changed"
			info, _ := ResolvedAPIKeyModelInfo(bound)
			info.SupportedInputModalities[0] = "also changed"
			retained, _ := ResolvedModelInputModalities(bound)
			if !reflect.DeepEqual(retained, tc.want) {
				t.Fatal("accessors mutated a selected model snapshot")
			}
		}
		cfg.OpenAICompatibility[0].Models = nil
		if retained, _ := ResolvedModelInputModalities(bound); !reflect.DeepEqual(retained, tc.want) {
			t.Fatal("a config edit changed an already selected attempt")
		}
		cleared := attachResolvedAPIKeyModelInfo(routing, bound, nil, "shared", tc.upstream)
		if _, known := ResolvedModelInputModalities(cleared); known {
			t.Fatal("a new unrelated attempt inherited the previous model declaration")
		}
	}
	if _, known := ResolvedModelInputModalities(core.Request{Metadata: map[string]any{resolvedAPIKeyModelInfoMetadataKey: map[string]any{"input-modalities": []string{"text"}}}}); known {
		t.Fatal("public request metadata forged a model policy")
	}
}
