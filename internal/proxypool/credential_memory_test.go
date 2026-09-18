package proxypool

import (
	"context"
	"encoding/json"
	"fmt"
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

func TestCredentialProxyMemoryPreservesSpreadAllocation(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3337")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://user:password@proxy.example"
	_, auths, first, _, _ := credentialMemoryFixture(t, cfg, "one.json", nil)
	seen := make(map[string]bool)
	for i := range 4 {
		a := first
		if i > 0 {
			var err error
			a, err = auths.Register(t.Context(), &coreauth.Auth{ID: fmt.Sprintf("copy-%d.json", i), Provider: first.Provider, Metadata: first.Metadata})
			if err != nil {
				t.Fatal(err)
			}
		}
		resolved, err := auths.ResolveProxyAuth(t.Context(), a)
		if err != nil {
			t.Fatal(err)
		}
		if seen[resolved.EffectiveProxyURL()] {
			t.Fatal("credential persistence bypassed spread allocation")
		}
		seen[resolved.EffectiveProxyURL()] = true
		current, _ := auths.GetByID(a.ID)
		if coreauth.ReadProxyBindingMemory(current) == nil {
			t.Fatal("chosen binding was not saved automatically")
		}
	}
}

func TestCredentialProxyMemoryFailsOverAndUpdatesReference(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://user:password@proxy.example"
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "stable.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := auths.GetByID(a.ID)
	original := coreauth.ReadProxyBindingMemory(fresh)
	m.check = func(ctx context.Context, raw string) TraceResult {
		if raw == first.EffectiveProxyURL() {
			return TraceResult{Error: "offline"}
		}
		return successfulTrace(ctx, raw)
	}
	m.CheckNow(t.Context())
	resolved, err := auths.ResolveProxyAuth(t.Context(), fresh)
	if err != nil {
		t.Fatalf("unhealthy node did not fail over: %v", err)
	}
	if resolved.EffectiveProxyURL() == first.EffectiveProxyURL() {
		t.Fatal("unhealthy node was retained")
	}
	updated, _ := auths.GetByID(a.ID)
	if reflect.DeepEqual(original, coreauth.ReadProxyBindingMemory(updated)) {
		t.Fatal("automatic failover did not update the portable reference")
	}
	_, movedAuths, moved, _, _ := credentialMemoryFixture(t, cfg, "moved.json", updated.Metadata)
	afterMove, err := movedAuths.ResolveProxyAuth(t.Context(), moved)
	if err != nil || afterMove.EffectiveProxyURL() != resolved.EffectiveProxyURL() {
		t.Fatalf("download after failover did not preserve replacement: %v", err)
	}
	m.check = successfulTrace
	result := m.Rebind(t.Context(), []string{a.ID})
	if len(result) != 1 || !result[0].Updated {
		t.Fatalf("manual rebind failed: %+v", result)
	}
	manual, _ := auths.GetByID(a.ID)
	if reflect.DeepEqual(coreauth.ReadProxyBindingMemory(updated), coreauth.ReadProxyBindingMemory(manual)) {
		t.Fatal("manual rebind did not update the portable reference")
	}
	// A stale request must not restore the previous binding after manual rebind.
	again, err := auths.ResolveProxyAuth(t.Context(), fresh)
	if err != nil {
		t.Fatal(err)
	}
	binding := m.SortedBindings()[0]
	raw, _ := m.bindingURL(m.snapshot(), binding)
	if again.EffectiveProxyURL() != raw || coreauth.ReadProxyBindingMemory(manual).NodeID != rememberedNodeID(raw) {
		t.Fatal("stale caller restored an old binding")
	}
}

func TestCredentialProxyMemoryPreservesTargetBalancing(t *testing.T) {
	cfg := proxyMultiTargetTestConfig()
	m, auths, a, _, _ := credentialMemoryFixture(t, cfg, "first.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	b, err := auths.Register(t.Context(), &coreauth.Auth{ID: "second.json", Provider: a.Provider, Metadata: a.Metadata})
	if err != nil {
		t.Fatal(err)
	}
	second, err := auths.ResolveProxyAuth(t.Context(), b)
	if err != nil {
		t.Fatal(err)
	}
	if first.EffectiveProxyURL() == second.EffectiveProxyURL() {
		t.Fatal("portable persistence bypassed equal-priority target balancing")
	}
	m.check = func(ctx context.Context, raw string) TraceResult {
		if raw == first.EffectiveProxyURL() {
			return TraceResult{Error: "offline"}
		}
		return successfulTrace(ctx, raw)
	}
	m.CheckNow(t.Context())
	fresh, _ := auths.GetByID(a.ID)
	after, err := auths.ResolveProxyAuth(t.Context(), fresh)
	if err != nil || after.EffectiveProxyURL() != second.EffectiveProxyURL() {
		t.Fatalf("pool failure did not fall back to another allowed target: %v", err)
	}
	current, _ := auths.GetByID(a.ID)
	if coreauth.ReadProxyBindingMemory(current).NodeID != rememberedNodeID(after.EffectiveProxyURL()) {
		t.Fatal("fallback target was not persisted")
	}
}

func TestCredentialProxyMemoryPreservesDirectFallback(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyRules[0].Pool = ""
	cfg.ProxyRules[0].Targets = []config.ProxyRuleTargetConfig{{Pool: "residential", Priority: 1}, {Direct: true}}
	m, auths, a, _, root := credentialMemoryFixture(t, cfg, "direct.json", nil)
	if _, err := auths.ResolveProxyAuth(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	m.check = func(context.Context, string) TraceResult { return TraceResult{Error: "offline"} }
	m.CheckNow(t.Context())
	stale, _ := auths.GetByID(a.ID)
	for range 2 {
		resolved, err := auths.ResolveProxyAuth(t.Context(), stale)
		if err != nil || resolved.EffectiveProxyURL() != "direct" {
			t.Fatalf("direct fallback was blocked or an old node resurrected: %v", err)
		}
	}
	current, _ := auths.GetByID(a.ID)
	if memory := coreauth.ReadProxyBindingMemory(current); memory == nil || !memory.Direct {
		t.Fatal("direct fallback retained stale proxy metadata")
	}
	data, err := os.ReadFile(filepath.Join(root, a.FileName))
	if err != nil {
		t.Fatal(err)
	}
	if !authfileguard.ConsumeManagerPersistedGeneration(filepath.Join(root, a.FileName), coreauth.SourceHashFromBytes(data)) {
		t.Fatal("direct fallback write was not marked as manager-owned")
	}
	for _, allowed := range []bool{true, false} {
		if !allowed {
			cfg.ProxyRules[0].Targets = cfg.ProxyRules[0].Targets[:1]
		}
		_, movedAuths, moved, _, _ := credentialMemoryFixture(t, cfg, "moved.json", current.Metadata)
		resolved, err := movedAuths.ResolveProxyAuth(t.Context(), moved)
		if err != nil || (resolved.EffectiveProxyURL() == "direct") != allowed {
			t.Fatalf("migrated direct binding did not respect routing scope (allowed=%t): %v", allowed, err)
		}
	}
}

func TestCredentialProxyMemoryBackfillsExistingSidecarWithoutReallocation(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	m, auths, a, store, _ := credentialMemoryFixture(t, cfg, "existing.json", nil)
	binding, raw, err := m.randomBinding(m.snapshot().pools["residential"], a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.saveBinding(binding); err != nil {
		t.Fatal(err)
	}
	before := store.writes.Load()
	resolved, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil || resolved.EffectiveProxyURL() != raw || resolved.EffectiveProxyBindingID() != binding.ID {
		t.Fatalf("backfilling changed the existing sidecar binding: %v", err)
	}
	current, _ := auths.GetByID(a.ID)
	if coreauth.ReadProxyBindingMemory(current) == nil || store.writes.Load() != before+1 {
		t.Fatal("existing binding was not saved to the credential")
	}
}

func TestCredentialProxyMemoryWinsOverOtherServerSidecar(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
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

func TestCredentialProxyMemoryRespectsScope(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
	_, auths, a, _, _ := credentialMemoryFixture(t, cfg, "pin.json", nil)
	first, err := auths.ResolveProxyAuth(t.Context(), a)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _ := auths.GetByID(a.ID)
	for _, mode := range []string{"outside-rule", "removed-node"} {
		t.Run(mode, func(t *testing.T) {
			other := proxyPoolTestConfig("3334-3340")
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
				t.Fatal("nonmatching rule wrote metadata")
			}
		})
	}
}

func TestCredentialProxyMemoryConcurrentResolveAndRebind(t *testing.T) {
	cfg := proxyPoolTestConfig("3334-3340")
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
