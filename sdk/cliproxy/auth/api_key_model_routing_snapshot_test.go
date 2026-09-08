package auth

import (
	"sync"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
)

func modelRoutingFixture(level string) *config.Config {
	return &config.Config{CodexKey: []config.CodexKey{{APIKey: "fixture", Models: []config.CodexModel{{Name: level, Alias: "local", Thinking: &registry.ThinkingSupport{Levels: []string{level}}}}}}}
}

func TestAPIKeyModelRoutingPublishesCoherentSnapshots(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(modelRoutingFixture("low"))
	auth, errRegister := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "one", Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	old := m.loadAPIKeyModelRouting()
	check := func(snapshot *apiKeyModelRoutingSnapshot) {
		name := snapshot.config.CodexKey[0].Models[0].Name
		info, ok := lookupAPIKeyModelCapability(snapshot, auth, "local", name)
		if !ok || snapshot.aliases[auth.ID]["local"] != name || info.Thinking.Levels[0] != name {
			t.Error("alias, config and capabilities came from different publications")
		}
	}
	var workers sync.WaitGroup
	for range 4 {
		workers.Go(func() {
			for range 100 {
				check(m.loadAPIKeyModelRouting())
			}
		})
	}
	for i := range 40 {
		level := "low"
		if i%2 == 0 {
			level = "high"
		}
		m.SetConfig(modelRoutingFixture(level))
	}
	workers.Wait()
	check(old)
	if old.config.CodexKey[0].Models[0].Name != "low" {
		t.Fatal("old attempt snapshot changed")
	}
	before := m.loadAPIKeyModelRouting()
	bad := modelRoutingFixture("unknown")
	m.SetConfig(bad)
	if m.loadAPIKeyModelRouting() != before {
		t.Fatal("invalid configuration published partial model routing")
	}
}

func TestAPIKeyModelRoutingLoadDeleteAndReregister(t *testing.T) {
	store := newSharedRegisterIfAbsentTestStore()
	auth := &Auth{ID: "one", Provider: "codex", Attributes: map[string]string{"api_key": "fixture"}}
	if _, errSave := store.Save(t.Context(), auth); errSave != nil {
		t.Fatal(errSave)
	}
	m := NewManager(store, nil, nil)
	m.SetConfig(modelRoutingFixture("low"))
	if errLoad := m.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	old := m.loadAPIKeyModelRouting()
	if _, ok := lookupAPIKeyModelCapability(old, auth, "local", "low"); !ok {
		t.Fatal("store load omitted configured capability publication")
	}
	if errDelete := m.Delete(WithSkipPersist(t.Context()), auth.ID); errDelete != nil {
		t.Fatal(errDelete)
	}
	deleted := m.loadAPIKeyModelRouting()
	if len(deleted.aliases) != 0 || len(deleted.capabilities) != 0 || len(old.capabilities) != 1 {
		t.Fatal("deletion retained current entries or modified an old attempt")
	}
	m.SetConfig(modelRoutingFixture("high"))
	if _, errRegister := m.Register(WithSkipPersist(t.Context()), auth); errRegister != nil {
		t.Fatal(errRegister)
	}
	if _, ok := lookupAPIKeyModelCapability(m.loadAPIKeyModelRouting(), auth, "local", "high"); !ok {
		t.Fatal("re-registration reused removed capability data")
	}
	if _, ok := lookupAPIKeyModelCapability(old, auth, "local", "low"); !ok {
		t.Fatal("re-registration overwrote old snapshot")
	}
	m.SetConfig(nil)
	if len(m.loadAPIKeyModelRouting().capabilities) != 0 {
		t.Fatal("configuration reset retained model capabilities")
	}
}

func TestAPIKeyModelRoutingCredentialReplacementPublishesTogether(t *testing.T) {
	m := NewManager(nil, nil, nil)
	cfg := &config.Config{}
	for _, level := range []string{"low", "high"} {
		cfg.CodexKey = append(cfg.CodexKey, config.CodexKey{APIKey: level, Models: []config.CodexModel{{Name: level, Alias: "local", Thinking: &registry.ThinkingSupport{Levels: []string{level}}}}})
	}
	m.SetConfig(cfg)
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for range 200 {
				m.mu.RLock()
				current := m.auths["one"].Clone()
				snapshot := m.loadAPIKeyModelRouting()
				m.mu.RUnlock()
				if current == nil {
					continue
				}
				name := current.Attributes["api_key"]
				info, ok := lookupAPIKeyModelCapability(snapshot, current, "local", name)
				if !ok || info.Thinking.Levels[0] != name || snapshot.aliases[current.ID]["local"] != name {
					t.Error("new credential became visible with an older model definition")
				}
			}
		})
	}
	for i := range 20 {
		key := "low"
		if i%2 == 0 {
			key = "high"
		}
		if _, errRegister := m.Register(WithSkipPersist(t.Context()), &Auth{ID: "one", Provider: "codex", Attributes: map[string]string{"api_key": key}}); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	readers.Wait()
}
