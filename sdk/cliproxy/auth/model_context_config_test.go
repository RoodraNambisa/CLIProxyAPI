package auth

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
)

func TestModelContextLengthRejectsManagerPublication(t *testing.T) {
	for _, replaceSelector := range []bool{false, true} {
		m := NewManager(nil, nil, nil)
		m.SetConfig(&config.Config{RequestRetry: 1})
		oldPolicy, oldConfig := m.routingPolicy.Load(), m.currentConfig()
		cfg := &config.Config{RequestRetry: 9, CodexKey: []config.CodexKey{{Models: []config.CodexModel{{Name: "upstream", MaxContextLength: -1}}}}}
		var readers sync.WaitGroup
		for range 4 {
			readers.Go(func() {
				for range 20 {
					if m.routingPolicy.Load() != oldPolicy || m.currentConfig() != oldConfig {
						t.Error("invalid model override partially published")
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
			t.Fatal("invalid context length changed the active config or selector")
		}
		cfg.CodexKey[0].Models[0].MaxContextLength = config.MaxModelContextLength
		m.SetConfigAndSelector(cfg, &FillFirstSelector{})
		if m.currentConfig().CodexKey[0].Models[0].MaxContextLength != config.MaxModelContextLength {
			t.Fatal("valid model declaration did not publish")
		}
		m.SetConfig(nil)
		if len(m.currentConfig().CodexKey) != 0 {
			t.Fatal("clearing config retained model declarations")
		}
	}
}
