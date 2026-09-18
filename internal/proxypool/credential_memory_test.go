package proxypool

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

type memoryCountingStore struct {
	*sdkauth.FileTokenStore
	writes atomic.Int64
}

func (s *memoryCountingStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	s.writes.Add(1)
	return s.FileTokenStore.Save(ctx, auth)
}

func credentialMemoryFixture(t *testing.T, cfg *config.Config, id string, metadata map[string]any) (*Manager, *coreauth.Manager, *coreauth.Auth, *memoryCountingStore, string) {
	t.Helper()
	root := t.TempDir()
	store := &memoryCountingStore{FileTokenStore: sdkauth.NewFileTokenStore()}
	store.SetBaseDir(root)
	auths := coreauth.NewManager(store, nil, nil)
	auths.SetConfig(cfg)
	manager := newTestManager(t, filepath.Join(root, "config.yaml"), cfg)
	manager.check = successfulTrace
	manager.SetAuthSource(auths)
	auths.SetProxyResolver(manager)
	if metadata == nil {
		metadata = map[string]any{"type": "codex", "account_id": "stable-test-account", "access_token": "fixture-token"}
	}
	a, err := auths.Register(t.Context(), &coreauth.Auth{ID: id, FileName: id, Provider: "codex", Metadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	return manager, auths, a, store, root
}

func TestCredentialProxyMemorySurvivesDownloadAndMoveWithoutSidecar(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3336")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	m, auths, a, store, root := credentialMemoryFixture(t, cfg, "original.json", nil)
	before := store.writes.Load()
	resolved, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	current, _ := auths.GetByID(a.ID)
	if current.RuntimeInstanceID() != a.RuntimeInstanceID() || coreauth.ReadProxyBindingMemory(current) == nil {
		t.Fatal("memory replaced the active instance or was not stored")
	}
	data, err := os.ReadFile(filepath.Join(root, a.FileName))
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err = json.Unmarshal(data, &metadata); err != nil {
		t.Fatal(err)
	}
	rawMemory, _ := json.Marshal(metadata[coreauth.ProxyBindingMemoryKey])
	for _, secret := range []string{"proxy.example", "password", "user-session", "fixture-token", "http"} {
		if strings.Contains(string(rawMemory), secret) {
			t.Fatal("binding metadata contains proxy credentials/address")
		}
	}
	if !authfileguard.ConsumeManagerPersistedGeneration(filepath.Join(root, a.FileName), coreauth.SourceHashFromBytes(data)) {
		t.Fatal("binding write would replace runtime through a watcher echo")
	}
	for range 3 {
		fresh, _ := auths.GetByID(a.ID)
		if _, err = auths.ResolveProxyAuth(t.Context(), fresh); err != nil {
			t.Fatal(err)
		}
	}
	if store.writes.Load()-before != 1 {
		t.Fatal("unchanged binding caused repeated credential writes")
	}
	// Pool and entry names may differ: the allowed endpoint fingerprint is authoritative.
	cfg.ProxyPools[0].Name = "renamed"
	cfg.ProxyPools[0].Entries[0].ID = "different-id"
	cfg.ProxyRules[0].Pool = "renamed"
	moved, movedAuths, movedAuth, _, _ := credentialMemoryFixture(t, cfg, "downloaded-and-renamed.json", metadata)
	if len(moved.SortedBindings()) != 0 {
		t.Fatal("new server unexpectedly has a sidecar binding")
	}
	again, err := movedAuths.ResolveProxyAuth(t.Context(), movedAuth)
	if err != nil {
		t.Fatal(err)
	}
	if again.EffectiveProxyURL() != resolved.EffectiveProxyURL() {
		t.Fatal("download/move changed the selected proxy")
	}
	if len(m.SortedBindings()) != 1 || len(moved.SortedBindings()) != 1 {
		t.Fatal("unexpected binding count")
	}
}

func TestCredentialProxyMemoryInitialReplicaSelectionIsDeterministic(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3344")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].Entries = append(cfg.ProxyPools[0].Entries, config.ProxyPoolEntryConfig{ID: "other", URLTemplate: "http://other-{5}:pw@other.example", Ports: "8000-8010"})
	_, first, a, _, _ := credentialMemoryFixture(t, cfg, "one.json", nil)
	x, err := first.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ProxyPools[0].Entries[0], cfg.ProxyPools[0].Entries[1] = cfg.ProxyPools[0].Entries[1], cfg.ProxyPools[0].Entries[0]
	_, second, b, _, _ := credentialMemoryFixture(t, cfg, "another-name.json", map[string]any{"type": "codex", "account_id": "stable-test-account", "access_token": "rotated-token"})
	y, err := second.ResolveProxyAuth(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if x.EffectiveProxyURL() != y.EffectiveProxyURL() {
		t.Fatal("replicas chose different initial nodes or sessions")
	}
}

func TestCredentialProxyMemoryStrictFailureAndExplicitRebind(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "stable.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := auths.GetByID(a.ID)
	original := coreauth.ReadProxyBindingMemory(fresh)
	// Mark the current node unhealthy. Do not jump to another IP automatically.
	m.check = func(context.Context, string) TraceResult { return TraceResult{Error: "offline"} }
	m.CheckNow(t.Context())
	if _, err = auths.ResolveProxyAuth(t.Context(), fresh); err == nil {
		t.Fatal("unhealthy pin silently switched node")
	}
	if len(m.SortedBindings()) != 1 || m.SortedBindings()[0].ID != first.EffectiveProxyBindingID() {
		t.Fatal("failed pin allocated another node")
	}
	m.check = successfulTrace
	result := m.Rebind(t.Context(), []string{a.ID})
	if len(result) != 1 || !result[0].Updated {
		t.Fatalf("manual rebind failed: %+v", result)
	}
	updated, _ := auths.GetByID(a.ID)
	if reflect.DeepEqual(original, coreauth.ReadProxyBindingMemory(updated)) {
		t.Fatal("manual rebind did not update the portable reference")
	}
	// Even a stale request snapshot must not resurrect the previous node.
	resolved, err := auths.ResolveProxyAuth(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.EffectiveProxyURL() == first.EffectiveProxyURL() {
		t.Fatal("stale caller restored old binding after manual rebind")
	}
}

func TestCredentialProxyMemoryWinsOverOtherServerSidecar(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "pin.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := auths.GetByID(a.ID)
	other, _, err := m.randomBinding(m.snapshot().pools["residential"], a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.saveBinding(other); err != nil {
		t.Fatal(err)
	}
	resolved, err := auths.ResolveProxyAuth(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.EffectiveProxyURL() != first.EffectiveProxyURL() {
		t.Fatal("local sidecar overrode portable credential identity")
	}
}

func TestCredentialProxyMemoryRespectsScopeAndOptOut(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	_, auths, a, _, _ := credentialMemoryFixture(t, cfg, "pin.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := auths.GetByID(a.ID)
	for _, mode := range []string{"off", "outside-rule", "removed-node"} {
		t.Run(mode, func(t *testing.T) {
			other := proxyPoolTestConfig("3334-3340")
			other.ProxyPools[0].RememberCredentialBinding = mode != "off"
			if mode == "outside-rule" {
				other.ProxyRules[0].Providers = []string{"xai"}
			}
			if mode == "removed-node" {
				other.ProxyPools[0].Entries[0].URLTemplate = "http://other-session-{3}:pw@new.example"
			}
			_, core, b, store, _ := credentialMemoryFixture(t, other, "moved.json", fresh.Metadata)
			before := store.writes.Load()
			resolved, err := core.ResolveProxyAuth(t.Context(), b)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "outside-rule" && resolved.EffectiveProxyURL() != "" {
				t.Fatal("memory escaped routing scope")
			}
			if mode == "removed-node" && resolved.EffectiveProxyURL() == first.EffectiveProxyURL() {
				t.Fatal("removed node was resurrected")
			}
			if mode != "removed-node" && store.writes.Load() != before {
				t.Fatal("disabled/nonmatching feature wrote metadata")
			}
		})
	}
}

func TestCredentialProxyMemoryConcurrentResolveAndRebind(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "parallel.json", nil)
	if _, err := auths.ResolveProxyAuth(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			if i%4 == 0 {
				results := m.Rebind(t.Context(), []string{a.ID})
				if len(results) != 1 || !results[0].Updated {
					t.Errorf("rebind failed: %+v", results)
				}
			} else {
				fresh, _ := auths.GetByID(a.ID)
				_, err := auths.ResolveProxyAuth(t.Context(), fresh)
				if err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	fresh, _ := auths.GetByID(a.ID)
	binding := m.SortedBindings()[0]
	raw, _ := m.bindingURL(m.snapshot(), binding)
	if coreauth.ReadProxyBindingMemory(fresh).NodeID != rememberedNodeID(raw) {
		t.Fatal("late persistence overwrote current rebind")
	}
}

func TestCredentialProxyMemoryRejectsRetiredCredential(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyPools[0].RememberCredentialBinding = true
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "replace.json", nil)
	resolved, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	if err = auths.Delete(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	_, err = m.RememberCredentialBinding(t.Context(), a, resolved.EffectiveProxyBindingID())
	if err == nil {
		t.Fatal("retired credential was accepted for persistence")
	}
}
