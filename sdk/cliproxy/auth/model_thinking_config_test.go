package auth

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func TestModelThinkingRejectsManagerPublication(t *testing.T) {
	for _, replaceSelector := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		m.SetConfig(&config.Config{RequestRetry: 1})
		oldPolicy, oldConfig := m.routingPolicy.Load(), m.currentConfig()
		cfg := &config.Config{RequestRetry: 9, CodexKey: []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", Thinking: &registry.ThinkingSupport{Min: -1}}}}}}
		var readers sync.WaitGroup
		for range 4 {
			readers.Go(func() {
				for range 20 {
					if m.routingPolicy.Load() != oldPolicy || m.currentConfig() != oldConfig {
						t.Error("invalid thinking partially published")
					}
				}
			})
		}
		if replaceSelector {
			m.SetConfigAndSelector(cfg, &FillFirstSelector{})
		} else {
			m.SetConfig(cfg)
		}
		readers.Wait()
		if m.routingPolicy.Load() != oldPolicy || m.currentConfig() != oldConfig || m.selectorForContext(t.Context()) != oldPolicy.selector {
			t.Fatal("invalid thinking changed config or selection")
		}
		cfg.CodexKey[0].Models[0].Thinking = &registry.ThinkingSupport{Levels: []string{"low", "high"}}
		m.SetConfigAndSelector(cfg, &FillFirstSelector{})
		if m.currentConfig().CodexKey[0].Models[0].Thinking.Levels[1] != "high" {
			t.Fatal("valid thinking did not publish")
		}
		m.SetConfig(nil)
		if len(m.currentConfig().CodexKey) != 0 {
			t.Fatal("cleared config retained thinking overrides")
		}
	}
}
