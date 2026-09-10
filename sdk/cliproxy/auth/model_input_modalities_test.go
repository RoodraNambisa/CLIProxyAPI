package auth

import (
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelInputModalitiesRuntimeSnapshotAndInvalidUpdates(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{Name: "fixture", Models: []config.OpenAICompatibilityModel{{Name: "upstream", InputModalities: []string{" TEXT "}}}}}}
	m.SetConfig(cfg)
	before := m.loadAPIKeyModelRouting()
	if got := before.config.OpenAICompatibility[0].Models[0].InputModalities; len(got) != 1 || got[0] != "text" {
		t.Fatal("runtime modalities are not normalized")
	}
	cfg.OpenAICompatibility[0].Models[0].InputModalities[0] = "invalid"
	for _, update := range []func(){func() { m.SetConfig(cfg) }, func() { m.SetConfigAndSelector(cfg, &FillFirstSelector{}) }} {
		update()
		if m.loadAPIKeyModelRouting() != before || before.config.OpenAICompatibility[0].Models[0].InputModalities[0] != "text" {
			t.Fatal("invalid update or caller mutation changed an installed model snapshot")
		}
	}
}
