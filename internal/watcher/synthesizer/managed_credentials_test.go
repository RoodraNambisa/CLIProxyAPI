package synthesizer

import (
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestManagedCredentialMigrationKeepsRuntimeIdentityAcrossRotation(t *testing.T) {
	cfg := &config.Config{CodexKey: []config.CodexKey{{APIKey: "original-key", BaseURL: "https://first.invalid", Priority: 5}}}
	before, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
	if err != nil {
		t.Fatal(err)
	}
	id, index := before[0].ID, before[0].EnsureIndex()
	changed, err := PrepareManagedCredentials(cfg)
	if err != nil || !changed {
		t.Fatal(changed, err)
	}
	scope := cfg.CodexKey[0].FillOrderScope
	cfg.CodexKey[0].APIKey = "rotated-key"
	cfg.CodexKey[0].BaseURL = "https://second.invalid"
	cfg.CodexKey[0].ProxyURL = "http://proxy.invalid"
	cfg.CodexKey[0].ChannelName = "Main channel"
	after, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
	if err != nil {
		t.Fatal(err)
	}
	if after[0].ID != id || after[0].EnsureIndex() != index || after[0].Attributes["config_fill_scope"] != scope || after[0].Label != "Main channel" {
		t.Fatal("rotation changed management identity")
	}
	if changed, err = PrepareManagedCredentials(cfg); err != nil || changed {
		t.Fatal("migration is not idempotent", err)
	}
}

func TestManagedOpenAIKeyInheritsAndOverridesIndependently(t *testing.T) {
	zero := 0
	models := []config.OpenAICompatibilityModel{{Name: "upstream-b", Alias: "client"}}
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "upstream", BaseURL: "https://example.invalid", Priority: 5, Models: []config.OpenAICompatibilityModel{{Name: "upstream-a", Alias: "client"}}, APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "one"}, {APIKey: "two", Priority: &zero, Models: &models, Disabled: true}}}}}
	if _, err := PrepareManagedCredentials(cfg); err != nil {
		t.Fatal(err)
	}
	auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
	if err != nil || len(auths) != 2 {
		t.Fatal(err, len(auths))
	}
	if auths[0].Attributes["priority"] != "5" || auths[1].Attributes["priority"] != "" || !auths[1].Disabled || auths[0].Disabled {
		t.Fatal("per-key policy incorrect")
	}
	if auths[0].ID == auths[1].ID || auths[0].EnsureIndex() == auths[1].EnsureIndex() {
		t.Fatal("key identity collision")
	}
	parent := &cfg.OpenAICompatibility[0]
	if config.EffectiveOpenAIKey(parent, auths[1].ID, "two").Models[0].Name != "upstream-b" {
		t.Fatal("model override ignored")
	}
}

func TestManagedOpenAILegacyPathIdentityAndSiblingRemoval(t *testing.T) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "team/backend", BaseURL: "https://example.invalid", APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "shared"}, {APIKey: "shared"}}}}}
	synth := func() []*coreauth.Auth {
		auths, err := NewConfigSynthesizer().Synthesize(&SynthesisContext{Config: cfg, Now: time.Now(), IDGenerator: NewStableIDGenerator()})
		if err != nil {
			t.Fatal(err)
		}
		return auths
	}
	old := synth()
	if _, err := PrepareManagedCredentials(cfg); err != nil {
		t.Fatal(err)
	}
	migrated := synth()
	for i, a := range migrated {
		if a.ID != old[i].ID || a.EnsureIndex() != old[i].EnsureIndex() || strings.ContainsAny(a.Attributes["config_id"], "/\\") {
			t.Fatal("migration changed runtime identity or left an unsafe management ID")
		}
	}
	cfg.OpenAICompatibility[0].APIKeyEntries = cfg.OpenAICompatibility[0].APIKeyEntries[1:]
	after := synth()
	if len(after) != 1 || after[0].ID != migrated[1].ID || after[0].Attributes["source"] != migrated[1].Attributes["source"] {
		t.Fatal("removing a sibling changed the surviving credential")
	}
	cfg.OpenAICompatibility[0].APIKeyEntries = nil
	cfg.OpenAICompatibility[0].Disabled = true
	if _, err := PrepareManagedCredentials(cfg); err != nil {
		t.Fatal(err)
	}
	if len(synth()) != 0 {
		t.Fatal("empty disabled channel synthesized a phantom credential")
	}
}
