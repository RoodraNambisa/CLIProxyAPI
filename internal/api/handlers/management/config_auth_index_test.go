package management

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestOpenAICompatibilityAuthIndexUsesRuntimeProviderName(t *testing.T) {
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
		Name:    "chatgpt-web",
		BaseURL: "https://compat.example/v1",
		APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
			APIKey: "compat-key",
		}},
	}}}
	auths, errSynthesize := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{
		Config:      cfg,
		Now:         time.Now(),
		IDGenerator: synthesizer.NewStableIDGenerator(),
	})
	if errSynthesize != nil || len(auths) != 1 {
		t.Fatalf("Synthesize() = (%d auths, %v), want one auth", len(auths), errSynthesize)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	registered, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auths[0])
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}
	h := NewHandlerWithoutConfigFilePath(cfg, manager)

	entries := h.openAICompatibilityWithAuthIndex()
	if len(entries) != 1 || len(entries[0].APIKeyEntries) != 1 {
		t.Fatalf("compatibility entries = %#v", entries)
	}
	if got := entries[0].APIKeyEntries[0].AuthIndex; got == "" || got != registered.Index {
		t.Fatalf("auth-index = %q, want %q", got, registered.Index)
	}
}

func TestOpenAICompatibilityAuthIndexSkipsDisabledIDGeneration(t *testing.T) {
	entry := config.OpenAICompatibility{
		Name:    "duplicate",
		BaseURL: "https://compat.example/v1",
		APIKeyEntries: []config.OpenAICompatibilityAPIKey{{
			APIKey: "compat-key",
		}},
	}
	disabled := entry
	disabled.Disabled = true
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{disabled, entry}}
	auths, errSynthesize := synthesizer.NewConfigSynthesizer().Synthesize(&synthesizer.SynthesisContext{
		Config:      cfg,
		Now:         time.Now(),
		IDGenerator: synthesizer.NewStableIDGenerator(),
	})
	if errSynthesize != nil || len(auths) != 1 {
		t.Fatalf("Synthesize() = (%d auths, %v), want one active auth", len(auths), errSynthesize)
	}
	manager := coreauth.NewManager(nil, nil, nil)
	registered, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auths[0])
	if errRegister != nil {
		t.Fatal(errRegister)
	}

	entries := NewHandlerWithoutConfigFilePath(cfg, manager).openAICompatibilityWithAuthIndex()
	if len(entries) != 2 || len(entries[0].APIKeyEntries) != 1 || len(entries[1].APIKeyEntries) != 1 {
		t.Fatalf("compatibility entries = %#v", entries)
	}
	if entries[0].APIKeyEntries[0].AuthIndex != "" {
		t.Fatalf("disabled auth-index = %q, want empty", entries[0].APIKeyEntries[0].AuthIndex)
	}
	if got := entries[1].APIKeyEntries[0].AuthIndex; got != registered.Index {
		t.Fatalf("active auth-index = %q, want %q", got, registered.Index)
	}
}
