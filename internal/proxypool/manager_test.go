package proxypool

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestResolvePersistsStableBindingWithoutExpandingPortRange(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := newTestManager(t, configPath, proxyPoolTestConfig("3334,3336-6000"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")

	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	if resolved.Source != "pool" || resolved.URL == "" || resolved.BindingID == "" {
		t.Fatalf("Resolve() = %+v", resolved)
	}
	snapshot := manager.snapshot()
	entry := snapshot.pools["residential"].entries[0]
	if got, want := entry.ports.Count(), 2666; got != want {
		t.Fatalf("stored port count = %d, want %d", got, want)
	}
	if got, want := entry.ports.String(), "3334,3336-6000"; got != want {
		t.Fatalf("stored port expression = %q, want %q", got, want)
	}
	stateInfo, errStat := os.Stat(manager.statePath)
	if errStat != nil {
		t.Fatalf("Stat(binding state) error = %v", errStat)
	}
	if got := stateInfo.Mode().Perm(); got != 0o600 {
		t.Fatalf("binding state mode = %o, want 600", got)
	}

	restored := newTestManager(t, configPath, proxyPoolTestConfig("3334,3336-6000"))
	restored.check = successfulTrace
	restoredResolved, errRestored := restored.Resolve(context.Background(), auth)
	if errRestored != nil {
		t.Fatalf("restored Resolve() error = %v", errRestored)
	}
	if restoredResolved != resolved {
		t.Fatalf("restored Resolve() = %+v, want %+v", restoredResolved, resolved)
	}
}

func TestResolveSpreadBindingsUsesDistinctPortsWithoutRangeProbing(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("10000-20000")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	var checks atomic.Int32
	manager.check = func(context.Context, string) TraceResult {
		checks.Add(1)
		return successfulTrace(context.Background(), "")
	}

	first, errFirst := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a"))
	if errFirst != nil {
		t.Fatalf("first Resolve() error = %v", errFirst)
	}
	second, errSecond := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-b"))
	if errSecond != nil {
		t.Fatalf("second Resolve() error = %v", errSecond)
	}
	firstURL, _ := url.Parse(first.URL)
	secondURL, _ := url.Parse(second.URL)
	if firstURL.Port() == "" || secondURL.Port() == "" || firstURL.Port() == secondURL.Port() {
		t.Fatalf("spread ports = %q/%q, want distinct", firstURL.Port(), secondURL.Port())
	}
	if got := checks.Load(); got != 2 {
		t.Fatalf("proxy checks = %d, want one bounded check per binding", got)
	}
	if got := manager.snapshot().pools["residential"].entries[0].ports.Count(); got != 10001 {
		t.Fatalf("compact port count = %d, want 10001", got)
	}
}

func TestResolveSpreadBindingsUsesDistinctPlaceholderInstances(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://user-session-{3}:password@proxy.example:18080"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.random = &incrementingReader{}
	manager.check = successfulTrace

	first, errFirst := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a"))
	if errFirst != nil {
		t.Fatalf("first Resolve() error = %v", errFirst)
	}
	second, errSecond := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-b"))
	if errSecond != nil {
		t.Fatalf("second Resolve() error = %v", errSecond)
	}
	if first.URL == second.URL {
		t.Fatalf("placeholder bindings share URL %q", first.URL)
	}
}

func TestResolveSpreadBindingsReservesPortsAcrossConcurrentAllocations(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("10000-10001")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	started := make(chan string, 2)
	release := make(chan struct{})
	manager.check = func(_ context.Context, proxyURL string) TraceResult {
		started <- proxyURL
		<-release
		return successfulTrace(context.Background(), proxyURL)
	}

	type resolveResult struct {
		proxy coreauth.ResolvedProxy
		err   error
	}
	results := make(chan resolveResult, 2)
	for _, authID := range []string{"auth-a", "auth-b"} {
		auth := proxyPoolTestAuth(authID)
		go func() {
			proxy, errResolve := manager.Resolve(context.Background(), auth)
			results <- resolveResult{proxy: proxy, err: errResolve}
		}()
	}
	waitStarted := func() string {
		select {
		case proxyURL := <-started:
			return proxyURL
		case <-time.After(time.Second):
			close(release)
			t.Fatal("concurrent allocation did not reach health check")
			return ""
		}
	}
	firstChecked := waitStarted()
	secondChecked := waitStarted()
	close(release)
	if firstChecked == secondChecked {
		t.Fatalf("concurrent allocations probed the same logical node %q", firstChecked)
	}
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatalf("concurrent Resolve() error = %v", result.err)
		}
	}
}

func TestResolveSpreadBindingsReusesLeastLoadedPortAfterExhaustion(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("10000-10001")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace

	for _, authID := range []string{"auth-a", "auth-b", "auth-c"} {
		if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth(authID)); errResolve != nil {
			t.Fatalf("Resolve(%s) error = %v", authID, errResolve)
		}
	}
	counts := make(map[int]int)
	for _, binding := range manager.SortedBindings() {
		counts[binding.Port]++
	}
	if len(counts) != 2 || counts[10000]+counts[10001] != 3 || counts[10000] > 2 || counts[10001] > 2 {
		t.Fatalf("spread counts after exhaustion = %#v, want balanced 2/1", counts)
	}
}

func TestEnablingSpreadBindingsPreservesExistingBindingsUntilRebind(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	cfg := proxyPoolTestConfig("10000")
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	manager := newTestManager(t, configPath, cfg)
	manager.check = successfulTrace
	authA := proxyPoolTestAuth("auth-a")
	authB := proxyPoolTestAuth("auth-b")
	authC := proxyPoolTestAuth("auth-c")
	for _, auth := range []*coreauth.Auth{authA, authB} {
		if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
			t.Fatalf("initial Resolve(%s) error = %v", auth.ID, errResolve)
		}
	}

	next := proxyPoolTestConfig("10000-10002")
	next.ProxyPools[0].SpreadBindings = true
	next.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	if errUpdate := manager.UpdateConfig(next); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	for _, auth := range []*coreauth.Auth{authA, authB} {
		resolved, errResolve := manager.Resolve(context.Background(), auth)
		if errResolve != nil {
			t.Fatalf("preserved Resolve(%s) error = %v", auth.ID, errResolve)
		}
		parsed, _ := url.Parse(resolved.URL)
		if parsed.Port() != "10000" {
			t.Fatalf("preserved Resolve(%s) port = %q, want 10000", auth.ID, parsed.Port())
		}
	}
	resolvedC, errC := manager.Resolve(context.Background(), authC)
	if errC != nil {
		t.Fatalf("new Resolve(auth-c) error = %v", errC)
	}
	parsedC, _ := url.Parse(resolvedC.URL)
	if parsedC.Port() == "10000" {
		t.Fatalf("new binding reused occupied port: %s", resolvedC.URL)
	}

	manager.SetAuthSource(staticAuthSource{authA.ID: authA, authB.ID: authB, authC.ID: authC})
	rebound := manager.Rebind(context.Background(), []string{authB.ID})
	if len(rebound) != 1 || !rebound[0].Updated || rebound[0].Binding == nil {
		t.Fatalf("Rebind(auth-b) = %#v", rebound)
	}
	if rebound[0].Binding.Port == 10000 || strconv.Itoa(rebound[0].Binding.Port) == parsedC.Port() {
		t.Fatalf("rebound port = %d, want remaining unoccupied port", rebound[0].Binding.Port)
	}
}

func TestSpreadRebindSkipsExcludedLeastLoadedStaticNode(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("10000-10001")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	current := proxyPoolTestAuth("auth-current")
	manager.SetAuthSource(staticAuthSource{current.ID: current})
	manager.mu.Lock()
	manager.bindings = map[string]Binding{
		current.ID: {ID: "current", AuthID: current.ID, Pool: "residential", Entry: "home", Port: 10000},
		"auth-b":   {ID: "other-b", AuthID: "auth-b", Pool: "residential", Entry: "home", Port: 10001},
		"auth-c":   {ID: "other-c", AuthID: "auth-c", Pool: "residential", Entry: "home", Port: 10001},
	}
	manager.mu.Unlock()

	results := manager.Rebind(context.Background(), []string{current.ID})
	if len(results) != 1 || !results[0].Updated || results[0].Binding == nil {
		t.Fatalf("Rebind() = %#v", results)
	}
	if results[0].Binding.Port != 10001 {
		t.Fatalf("rebound port = %d, want the non-excluded candidate 10001", results[0].Binding.Port)
	}
}

func TestSpreadAllocationWaitsForBindingPersistenceRollback(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("10000-10001")
	cfg.ProxyPools[0].SpreadBindings = true
	cfg.ProxyPools[0].BindAttempts = 1
	cfg.ProxyPools[0].Entries[0].URLTemplate = "http://proxy.example"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); errResolve != nil {
		t.Fatalf("initial Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()[0]
	replacement := before
	replacement.ID = "replacement"
	if before.Port == 10000 {
		replacement.Port = 10001
	} else {
		replacement.Port = 10000
	}

	persistStarted := make(chan struct{})
	releasePersist := make(chan struct{})
	var syncCalls atomic.Int32
	manager.syncDir = func(string) error {
		if syncCalls.Add(1) == 1 {
			close(persistStarted)
			<-releasePersist
			return errors.New("directory sync failed")
		}
		return nil
	}
	saveDone := make(chan error, 1)
	go func() { saveDone <- manager.saveBinding(replacement) }()
	select {
	case <-persistStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement persistence did not start")
	}

	type pendingResolve struct {
		proxy coreauth.ResolvedProxy
		err   error
	}
	resolveDone := make(chan pendingResolve, 1)
	go func() {
		proxy, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-b"))
		resolveDone <- pendingResolve{proxy: proxy, err: errResolve}
	}()
	select {
	case result := <-resolveDone:
		close(releasePersist)
		t.Fatalf("Resolve() completed against tentative binding state: %#v", result)
	case <-time.After(50 * time.Millisecond):
	}
	close(releasePersist)
	if errSave := <-saveDone; errSave == nil {
		t.Fatal("saveBinding() error = nil, want rollback")
	}
	result := <-resolveDone
	if result.err != nil {
		t.Fatalf("Resolve() after rollback error = %v", result.err)
	}
	parsed, _ := url.Parse(result.proxy.URL)
	if parsed.Port() == strconv.Itoa(before.Port) {
		t.Fatalf("Resolve() reused restored occupied port %d", before.Port)
	}
}

func TestResolveRollsBackBindingWhenDirectorySyncFails(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	wantErr := errors.New("directory sync failed")
	syncCalls := 0
	manager.syncDir = func(string) error {
		syncCalls++
		if syncCalls == 1 {
			return wantErr
		}
		return nil
	}

	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); !errors.Is(errResolve, wantErr) {
		t.Fatalf("Resolve() error = %v, want %v", errResolve, wantErr)
	}
	if got := manager.SortedBindings(); len(got) != 0 {
		t.Fatalf("bindings after directory sync failure = %+v, want none", got)
	}
	if _, errStat := os.Stat(manager.statePath); !errors.Is(errStat, os.ErrNotExist) {
		t.Fatalf("Stat(binding state) error = %v, want not exist", errStat)
	}
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want failed commit plus rollback sync", syncCalls)
	}
}

func TestSaveBindingRestoresPreviousStateWhenDirectorySyncFails(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := newTestManager(t, configPath, proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()[0]
	beforeData, errReadBefore := os.ReadFile(manager.statePath)
	if errReadBefore != nil {
		t.Fatalf("ReadFile(binding state) error = %v", errReadBefore)
	}
	wantErr := errors.New("directory sync failed")
	wantRollbackErr := errors.New("rollback directory sync failed")
	syncCalls := 0
	manager.syncDir = func(string) error {
		syncCalls++
		if syncCalls == 1 {
			return wantErr
		}
		return wantRollbackErr
	}
	replacement := cloneBinding(before)
	replacement.ID = "replacement-binding"

	errSave := manager.saveBinding(replacement)
	if !errors.Is(errSave, wantErr) || !errors.Is(errSave, wantRollbackErr) {
		t.Fatalf("saveBinding() error = %v, want joined %v and %v", errSave, wantErr, wantRollbackErr)
	}
	after := manager.SortedBindings()
	if len(after) != 1 || after[0].ID != before.ID {
		t.Fatalf("bindings after directory sync failure = %+v, want %+v", after, before)
	}
	afterData, errReadAfter := os.ReadFile(manager.statePath)
	if errReadAfter != nil {
		t.Fatalf("ReadFile(restored binding state) error = %v", errReadAfter)
	}
	if !bytes.Equal(afterData, beforeData) {
		t.Fatalf("binding state changed after rollback\nbefore: %s\nafter:  %s", beforeData, afterData)
	}
	restored := newTestManager(t, configPath, proxyPoolTestConfig("3334"))
	restoredBindings := restored.SortedBindings()
	if len(restoredBindings) != 1 || restoredBindings[0].ID != before.ID {
		t.Fatalf("restored bindings = %+v, want %+v", restoredBindings, before)
	}
	if syncCalls != 2 {
		t.Fatalf("directory sync calls = %d, want failed commit plus rollback sync", syncCalls)
	}
}

func TestResolveProxyPrecedence(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyURL = "http://global.example:8080"
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace

	explicitAuth := proxyPoolTestAuth("explicit")
	explicitAuth.ProxyURL = "socks5h://explicit.example:1080"
	explicit, errExplicit := manager.Resolve(context.Background(), explicitAuth)
	if errExplicit != nil || explicit.Source != "auth" || explicit.URL != explicitAuth.ProxyURL {
		t.Fatalf("explicit Resolve() = %+v, %v", explicit, errExplicit)
	}

	pooled, errPooled := manager.Resolve(context.Background(), proxyPoolTestAuth("pooled"))
	if errPooled != nil || pooled.Source != "pool" {
		t.Fatalf("pooled Resolve() = %+v, %v", pooled, errPooled)
	}

	unmatched := proxyPoolTestAuth("unmatched")
	unmatched.Provider = "xai"
	global, errGlobal := manager.Resolve(context.Background(), unmatched)
	if errGlobal != nil || global.Source != "global" || global.URL != cfg.ProxyURL {
		t.Fatalf("global Resolve() = %+v, %v", global, errGlobal)
	}

	cfg.ProxyURL = ""
	if errUpdate := manager.UpdateConfig(cfg); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	inherited, errInherited := manager.Resolve(context.Background(), unmatched)
	if errInherited != nil || inherited.Source != "inherit" || inherited.URL != "" {
		t.Fatalf("inherited Resolve() = %+v, %v", inherited, errInherited)
	}
}

func TestResolveInvalidExplicitProxyFailsClosed(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), &internalconfig.Config{})
	auth := proxyPoolTestAuth("invalid-explicit")
	auth.ProxyURL = "ftp://user:super-secret@proxy.example:21"
	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve == nil {
		t.Fatal("Resolve() error = nil, want proxy_unavailable")
	}
	if resolved.URL != "" || resolved.Source != "" {
		t.Fatalf("Resolve() = %+v, want no fallback proxy", resolved)
	}
	var unavailable *UnavailableError
	if !errors.As(errResolve, &unavailable) || unavailable.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("Resolve() error = %T %v, want UnavailableError", errResolve, errResolve)
	}
	if strings.Contains(errResolve.Error(), "super-secret") {
		t.Fatalf("Resolve() error leaked proxy credentials: %v", errResolve)
	}
}

func TestResolveAIStudioFailsClosedForMatchedPool(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyRules[0].Providers = []string{"aistudio"}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	auth := proxyPoolTestAuth("aistudio-1")
	auth.Provider = "aistudio"

	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve == nil {
		t.Fatal("Resolve() error = nil, want proxy_unavailable")
	}
	if resolved.URL != "" || len(manager.SortedBindings()) != 0 {
		t.Fatalf("Resolve() = %+v, bindings = %+v; relay must not claim proxy use", resolved, manager.SortedBindings())
	}
	var unavailable *UnavailableError
	if !errors.As(errResolve, &unavailable) || unavailable.StatusCode() != http.StatusServiceUnavailable {
		t.Fatalf("Resolve() error = %T %v, want UnavailableError", errResolve, errResolve)
	}
}

func TestResolveAIStudioAllowsExplicitDirect(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), &internalconfig.Config{})
	auth := &coreauth.Auth{ID: "aistudio-1", Provider: "aistudio", ProxyURL: "direct"}
	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	if resolved.URL != "direct" || resolved.Source != "auth" {
		t.Fatalf("Resolve() = %+v, want explicit direct", resolved)
	}
}

func TestUpdateConfigKeepsHealthForEquivalentProxyConfiguration(t *testing.T) {
	t.Parallel()

	cfg := proxyPoolTestConfig("3334")
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	beforeGeneration := manager.snapshot().generation

	equivalent := *cfg
	equivalent.Debug = !cfg.Debug
	if errUpdate := manager.UpdateConfig(&equivalent); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	if got := manager.snapshot().generation; got != beforeGeneration {
		t.Fatalf("generation = %d, want unchanged %d", got, beforeGeneration)
	}
	manager.mu.RLock()
	health := manager.health[binding.ID]
	manager.mu.RUnlock()
	if health.Generation != beforeGeneration || !health.OK {
		t.Fatalf("health after equivalent update = %+v", health)
	}
}

func TestResolveRetriesInsteadOfPersistingBindingFromStaleConfiguration(t *testing.T) {
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := newTestManager(t, configPath, proxyPoolTestConfig("3334"))
	checkStarted := make(chan struct{})
	releaseCheck := make(chan struct{})
	var startOnce sync.Once
	manager.check = func(context.Context, string) TraceResult {
		startOnce.Do(func() { close(checkStarted) })
		<-releaseCheck
		return successfulTrace(context.Background(), "")
	}
	auth := proxyPoolTestAuth("auth-a")
	type resolveResult struct {
		resolved coreauth.ResolvedProxy
		err      error
	}
	resultCh := make(chan resolveResult, 1)
	go func() {
		resolved, errResolve := manager.Resolve(context.Background(), auth)
		resultCh <- resolveResult{resolved: resolved, err: errResolve}
	}()
	<-checkStarted
	if errUpdate := manager.UpdateConfig(proxyPoolTestConfig("4444")); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	close(releaseCheck)
	result := <-resultCh
	if result.err != nil {
		t.Fatalf("Resolve() error = %v", result.err)
	}
	bindings := manager.SortedBindings()
	if len(bindings) != 1 || bindings[0].Port != 4444 {
		t.Fatalf("persisted bindings = %+v, want current port 4444", bindings)
	}
	if result.resolved.BindingID != bindings[0].ID {
		t.Fatalf("Resolve() binding = %q, persisted = %q", result.resolved.BindingID, bindings[0].ID)
	}

	restored := newTestManager(t, configPath, proxyPoolTestConfig("4444"))
	restoredBindings := restored.SortedBindings()
	if len(restoredBindings) != 1 || restoredBindings[0].Port != 4444 {
		t.Fatalf("restored bindings = %+v, want current port 4444", restoredBindings)
	}
}

func TestUpdateConfigWithPoolRenamePreservesCompatibleBinding(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := newTestManager(t, configPath, proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()[0]
	renamedConfig := proxyPoolTestConfig("3334")
	renamedConfig.ProxyPools[0].Name = "primary"
	renamedConfig.ProxyRules[0].Pool = "primary"

	if errUpdate := manager.UpdateConfigWithPoolRename(renamedConfig, "residential", "primary"); errUpdate != nil {
		t.Fatalf("UpdateConfigWithPoolRename() error = %v", errUpdate)
	}
	after := manager.SortedBindings()[0]
	if after.ID != before.ID || after.Pool != "primary" || after.Entry != before.Entry || after.Port != before.Port {
		t.Fatalf("binding after rename = %+v, want stable binding based on %+v", after, before)
	}
	restored := newTestManager(t, configPath, renamedConfig)
	restoredBindings := restored.SortedBindings()
	if len(restoredBindings) != 1 || restoredBindings[0].ID != before.ID || restoredBindings[0].Pool != "primary" {
		t.Fatalf("restored bindings = %+v", restoredBindings)
	}
}

func TestUpdateConfigInfersEquivalentPoolRename(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "config.yaml")
	manager := newTestManager(t, configPath, proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()[0]
	renamedConfig := proxyPoolTestConfig("3334")
	renamedConfig.ProxyPools[0].Name = "primary"
	renamedConfig.ProxyRules[0].Pool = "primary"

	if errUpdate := manager.UpdateConfig(renamedConfig); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	after := manager.SortedBindings()[0]
	if after.ID != before.ID || after.Pool != "primary" || after.Entry != before.Entry || after.Port != before.Port {
		t.Fatalf("binding after inferred rename = %+v, want stable binding based on %+v", after, before)
	}
	restored := newTestManager(t, configPath, renamedConfig)
	restoredBindings := restored.SortedBindings()
	if len(restoredBindings) != 1 || restoredBindings[0].ID != before.ID || restoredBindings[0].Pool != "primary" {
		t.Fatalf("restored bindings = %+v", restoredBindings)
	}
}

func TestUpdateConfigDoesNotInferUnrelatedPoolReplacement(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	replacement := proxyPoolTestConfig("4444")
	replacement.ProxyPools[0].Name = "primary"
	replacement.ProxyRules[0].Pool = "primary"

	if errUpdate := manager.UpdateConfig(replacement); errUpdate != nil {
		t.Fatalf("UpdateConfig() error = %v", errUpdate)
	}
	after := manager.SortedBindings()[0]
	if after.Pool != "residential" || after.Port != 3334 {
		t.Fatalf("unrelated replacement migrated binding: %+v", after)
	}
}

func TestBackgroundCleanupRevalidatesBindingAgainstLatestConfiguration(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	invalid := proxyPoolTestConfig("3334")
	invalid.ProxyRules[0].Providers = []string{"xai"}
	if errUpdate := manager.UpdateConfig(invalid); errUpdate != nil {
		t.Fatalf("UpdateConfig(invalid) error = %v", errUpdate)
	}
	source := &blockingAuthSource{
		auth:    auth,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager.SetAuthSource(source)
	done := make(chan struct{})
	go func() {
		manager.CheckNow(context.Background())
		close(done)
	}()
	<-source.started
	if errUpdate := manager.UpdateConfig(proxyPoolTestConfig("3334")); errUpdate != nil {
		t.Fatalf("UpdateConfig(valid) error = %v", errUpdate)
	}
	close(source.release)
	<-done
	bindings := manager.SortedBindings()
	if len(bindings) != 1 || bindings[0].AuthID != auth.ID {
		t.Fatalf("bindings after stale cleanup = %+v, want original binding", bindings)
	}
}

func TestBackgroundCleanupKeepsPendingBindingWhileLeaseIsHeld(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("pending-auth")
	release := manager.HoldBinding(auth.ID)
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		release()
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	manager.SetAuthSource(staticAuthSource{})

	manager.CheckNow(context.Background())
	if bindings := manager.SortedBindings(); len(bindings) != 1 || bindings[0].AuthID != auth.ID {
		release()
		t.Fatalf("bindings while lease is held = %+v, want pending binding", bindings)
	}

	release()
	manager.CheckNow(context.Background())
	if bindings := manager.SortedBindings(); len(bindings) != 0 {
		t.Fatalf("bindings after lease release = %+v, want none", bindings)
	}
}

func TestBackgroundCleanupKeepsDisabledBindingDuringManualReloginLease(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("disabled-auth")
	auth.Disabled = true
	auth.Status = coreauth.StatusDisabled
	manager.SetAuthSource(staticAuthSource{auth.ID: auth})
	release := manager.HoldBinding(auth.ID)
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		release()
		t.Fatalf("Resolve() error = %v", errResolve)
	}

	manager.CheckNow(context.Background())
	if bindings := manager.SortedBindings(); len(bindings) != 1 || bindings[0].AuthID != auth.ID {
		release()
		t.Fatalf("bindings during manual re-login lease = %+v, want disabled auth binding", bindings)
	}

	release()
	manager.CheckNow(context.Background())
	if bindings := manager.SortedBindings(); len(bindings) != 0 {
		t.Fatalf("bindings after manual re-login lease = %+v, want none", bindings)
	}
}

func TestBindingCleanupAllowsLeaseAcquisitionDuringAuthLookup(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("lease-delete-race")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	bindings := manager.SortedBindings()
	if len(bindings) != 1 {
		t.Fatalf("bindings before cleanup = %+v, want one", bindings)
	}
	source := &blockingGetAuthSource{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager.SetAuthSource(source)
	removeDone := make(chan error, 1)
	go func() {
		removeDone <- manager.removeBindings([]bindingRemovalCandidate{{AuthID: auth.ID, BindingID: bindings[0].ID}})
	}()
	select {
	case <-source.started:
	case <-time.After(time.Second):
		t.Fatal("binding cleanup did not reach final validation")
	}

	holdDone := make(chan func(), 1)
	go func() {
		holdDone <- manager.HoldBinding(auth.ID)
	}()
	var release func()
	select {
	case release = <-holdDone:
	case <-time.After(time.Second):
		close(source.release)
		<-removeDone
		t.Fatal("HoldBinding blocked behind auth source lookup")
	}
	close(source.release)
	if errRemove := <-removeDone; errRemove != nil {
		t.Fatalf("removeBindings() error = %v", errRemove)
	}
	if bindings = manager.SortedBindings(); len(bindings) != 1 || bindings[0].AuthID != auth.ID {
		release()
		t.Fatalf("bindings while lease was acquired = %+v, want original binding", bindings)
	}
	release()
	manager.SetAuthSource(staticAuthSource{})
	if errRemove := manager.removeBindings([]bindingRemovalCandidate{{AuthID: auth.ID, BindingID: bindings[0].ID}}); errRemove != nil {
		t.Fatalf("removeBindings() after lease release error = %v", errRemove)
	}
	if bindings = manager.SortedBindings(); len(bindings) != 0 {
		t.Fatalf("bindings after lease release = %+v, want none", bindings)
	}
}

func TestResolveStrictPoolFailureDoesNotPersistBinding(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = func(context.Context, string) TraceResult {
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}

	_, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a"))
	var unavailable *UnavailableError
	if !errors.As(errResolve, &unavailable) {
		t.Fatalf("Resolve() error = %v, want UnavailableError", errResolve)
	}
	if unavailable.StatusCode() != http.StatusServiceUnavailable || !unavailable.SkipAuthResult() {
		t.Fatalf("UnavailableError status/skip = %d/%t", unavailable.StatusCode(), unavailable.SkipAuthResult())
	}
	if got := manager.SortedBindings(); len(got) != 0 {
		t.Fatalf("bindings after failed Resolve() = %+v", got)
	}
	manager.mu.RLock()
	healthCount := len(manager.health)
	manager.mu.RUnlock()
	if healthCount != 0 {
		t.Fatalf("health entries after failed Resolve() = %d, want 0", healthCount)
	}
}

func TestAllocateBindingCountsDistinctProbesInsteadOfRepeatedDraws(t *testing.T) {
	cfg := &internalconfig.Config{}
	cfg.ProxyPools = []internalconfig.ProxyPoolConfig{{
		Name:         "residential",
		BindAttempts: 2,
		Entries: []internalconfig.ProxyPoolEntryConfig{
			{ID: "bad", URLTemplate: "http://bad-proxy.example:8080"},
			{ID: "good", URLTemplate: "http://good-proxy.example:8080"},
		},
	}}
	cfg.ProxyRules = []internalconfig.ProxyRuleConfig{{
		Name:      "codex",
		Pool:      "residential",
		Providers: []string{"codex"},
	}}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.random = bytes.NewReader(make([]byte, 256))
	var checked []string
	manager.check = func(_ context.Context, proxyURL string) TraceResult {
		checked = append(checked, proxyURL)
		if proxyURL == "http://good-proxy.example:8080" {
			return successfulTrace(context.Background(), proxyURL)
		}
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}
	resolved, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a"))
	if errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	if resolved.URL != "http://good-proxy.example:8080" {
		t.Fatalf("Resolve() URL = %q, want healthy second candidate", resolved.URL)
	}
	if len(checked) != 2 || checked[0] == checked[1] {
		t.Fatalf("checked candidates = %v, want two distinct probes", checked)
	}
}

func TestResolveCancellationDoesNotPersistOrPoisonBindingHealth(t *testing.T) {
	t.Run("allocation", func(t *testing.T) {
		manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
		manager.check = canceledTrace
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, errResolve := manager.Resolve(ctx, proxyPoolTestAuth("auth-a"))
		if !errors.Is(errResolve, context.DeadlineExceeded) {
			t.Fatalf("Resolve() error = %v, want context deadline", errResolve)
		}
		if len(manager.SortedBindings()) != 0 {
			t.Fatal("canceled allocation persisted a binding")
		}
		manager.mu.RLock()
		healthCount := len(manager.health)
		manager.mu.RUnlock()
		if healthCount != 0 {
			t.Fatalf("health entries = %d, want 0", healthCount)
		}
	})

	t.Run("existing_binding", func(t *testing.T) {
		manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
		manager.check = successfulTrace
		auth := proxyPoolTestAuth("auth-a")
		if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
			t.Fatalf("initial Resolve() error = %v", errResolve)
		}
		binding := manager.SortedBindings()[0]
		manager.mu.Lock()
		before := manager.health[binding.ID]
		before.RetryAfter = time.Time{}
		manager.health[binding.ID] = before
		manager.mu.Unlock()
		manager.check = canceledTrace
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, errResolve := manager.Resolve(ctx, auth)
		if !errors.Is(errResolve, context.DeadlineExceeded) {
			t.Fatalf("Resolve() error = %v, want context deadline", errResolve)
		}
		manager.mu.RLock()
		after := manager.health[binding.ID]
		manager.mu.RUnlock()
		if after != before {
			t.Fatalf("health changed after cancellation: before=%+v after=%+v", before, after)
		}
	})
}

func TestCheckPoolCancellationDoesNotOverwriteBoundHealth(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.mu.RLock()
	before := manager.health[binding.ID]
	manager.mu.RUnlock()
	manager.check = canceledTrace
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, errCheck := manager.CheckPool(ctx, "residential", 1)
	if !errors.Is(errCheck, context.DeadlineExceeded) {
		t.Fatalf("CheckPool() error = %v, want context deadline", errCheck)
	}
	manager.mu.RLock()
	after := manager.health[binding.ID]
	manager.mu.RUnlock()
	if after != before {
		t.Fatalf("health changed after canceled check: before=%+v after=%+v", before, after)
	}
}

func TestRemoveBindingsRestoresHealthWhenPersistenceFails(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.SetAuthSource(staticAuthSource{})
	manager.statePath = t.TempDir()

	if errRemove := manager.removeBindings([]bindingRemovalCandidate{{AuthID: auth.ID, BindingID: binding.ID}}); errRemove == nil {
		t.Fatal("removeBindings() error = nil, want persistence failure")
	}
	if got := manager.SortedBindings(); len(got) != 1 || got[0].ID != binding.ID {
		t.Fatalf("bindings after rollback = %+v, want binding %s", got, binding.ID)
	}
	manager.mu.RLock()
	_, hasHealth := manager.health[binding.ID]
	manager.mu.RUnlock()
	if !hasHealth {
		t.Fatal("binding health was not restored after persistence failure")
	}
}

func TestRebindKeepsCurrentBindingWhenNoHealthyAlternativeExists(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	manager.SetAuthSource(staticAuthSource{auth.ID: auth})
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()
	manager.check = func(context.Context, string) TraceResult {
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}

	results := manager.Rebind(context.Background(), []string{auth.ID})
	if len(results) != 1 || results[0].Updated || results[0].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("Rebind() = %+v", results)
	}
	after := manager.SortedBindings()
	if len(before) != 1 || len(after) != 1 || before[0].ID != after[0].ID {
		t.Fatalf("binding changed after failed rebind: before=%+v after=%+v", before, after)
	}
}

func TestRebindRejectsAIStudioRelayPool(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyRules[0].Providers = []string{"aistudio"}
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("aistudio-a")
	auth.Provider = "aistudio"
	manager.SetAuthSource(staticAuthSource{auth.ID: auth})

	results := manager.Rebind(context.Background(), []string{auth.ID})
	if len(results) != 1 || results[0].Updated || results[0].HTTPStatus != http.StatusServiceUnavailable {
		t.Fatalf("Rebind() = %+v, want AIStudio proxy unavailable", results)
	}
	if bindings := manager.SortedBindings(); len(bindings) != 0 {
		t.Fatalf("AIStudio rebind persisted unusable bindings: %+v", bindings)
	}
}

func TestOlderHealthProbeCannotOverwriteNewerFailure(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.mu.Lock()
	health := manager.health[binding.ID]
	health.RetryAfter = time.Time{}
	manager.health[binding.ID] = health
	manager.mu.Unlock()

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	manager.check = func(context.Context, string) TraceResult {
		close(probeStarted)
		<-releaseProbe
		return successfulTrace(context.Background(), "")
	}
	checkDone := make(chan struct{})
	go func() {
		manager.CheckNow(context.Background())
		close(checkDone)
	}()
	<-probeStarted

	executionAuth := auth.Clone()
	executionAuth.RuntimeProxyURL = resolved.URL
	executionAuth.RuntimeProxyBindingID = resolved.BindingID
	errFailure := manager.ReportFailure(context.Background(), executionAuth, testStatusError{status: http.StatusBadGateway, message: "proxy tunnel failed"})
	var unavailable *UnavailableError
	if !errors.As(errFailure, &unavailable) {
		t.Fatalf("ReportFailure() error = %v, want proxy unavailable", errFailure)
	}
	close(releaseProbe)
	<-checkDone

	manager.mu.RLock()
	finalHealth := manager.health[binding.ID]
	manager.mu.RUnlock()
	if finalHealth.OK || finalHealth.Error != "request_failed" {
		t.Fatalf("older successful probe overwrote newer failure: %+v", finalHealth)
	}
}

func TestNextBoundCheckDelayHonorsShortConfiguredInterval(t *testing.T) {
	cfg := proxyPoolTestConfig("3334")
	cfg.ProxyPools[0].CheckIntervalSeconds = 1
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), cfg)
	fixedNow := time.Now().UTC()
	manager.now = func() time.Time { return fixedNow }
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	if delay := manager.nextBoundCheckDelay(); delay != time.Second {
		t.Fatalf("nextBoundCheckDelay() = %v, want 1s", delay)
	}
}

func TestSuccessfulRebindRemovesPreviousBindingHealth(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334-3335"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	manager.SetAuthSource(staticAuthSource{auth.ID: auth})
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	before := manager.SortedBindings()[0]

	results := manager.Rebind(context.Background(), []string{auth.ID})
	if len(results) != 1 || !results[0].Updated {
		t.Fatalf("Rebind() = %+v", results)
	}
	after := manager.SortedBindings()[0]
	if after.ID == before.ID {
		t.Fatalf("binding ID did not change: before=%s after=%s", before.ID, after.ID)
	}
	manager.mu.RLock()
	_, hasPrevious := manager.health[before.ID]
	_, hasCurrent := manager.health[after.ID]
	healthCount := len(manager.health)
	manager.mu.RUnlock()
	if hasPrevious || !hasCurrent || healthCount != 1 {
		t.Fatalf("health after rebind: previous=%t current=%t count=%d", hasPrevious, hasCurrent, healthCount)
	}
}

func TestStoreBoundHealthRejectsReplacedBinding(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	snapshot := manager.snapshot()
	oldBinding := Binding{ID: "old", AuthID: "auth-a", Pool: "residential", Entry: "home", Port: 3334}
	newBinding := oldBinding
	newBinding.ID = "new"
	manager.mu.Lock()
	manager.bindings[oldBinding.AuthID] = newBinding
	manager.mu.Unlock()

	_, stored := manager.storeBoundHealth(snapshot, oldBinding, "socks5h://user:pass@127.0.0.1:3334", successfulTrace(context.Background(), ""), manager.nextProbeEpoch())
	if stored {
		t.Fatal("storeBoundHealth() stored a replaced binding")
	}
	manager.mu.RLock()
	_, exists := manager.health[oldBinding.ID]
	manager.mu.RUnlock()
	if exists {
		t.Fatal("replaced binding health was restored")
	}
}

func TestCheckNowChecksOnlyBoundNodes(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334-6000"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	if _, errResolve := manager.Resolve(context.Background(), auth); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.mu.Lock()
	health := manager.health[binding.ID]
	health.RetryAfter = time.Time{}
	manager.health[binding.ID] = health
	manager.mu.Unlock()

	var checks atomic.Int64
	manager.check = func(context.Context, string) TraceResult {
		checks.Add(1)
		return successfulTrace(context.Background(), "")
	}
	manager.CheckNow(context.Background())
	if got := checks.Load(); got != 1 {
		t.Fatalf("background checks = %d, want one bound node", got)
	}
}

func TestStopSerializesConcurrentRestart(t *testing.T) {
	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	binding := manager.SortedBindings()[0]
	manager.mu.Lock()
	health := manager.health[binding.ID]
	health.RetryAfter = time.Time{}
	manager.health[binding.ID] = health
	manager.mu.Unlock()

	firstStarted := make(chan struct{})
	firstCanceled := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int64
	manager.check = func(ctx context.Context, _ string) TraceResult {
		if calls.Add(1) == 1 {
			close(firstStarted)
			<-ctx.Done()
			close(firstCanceled)
			<-releaseFirst
			return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
		}
		<-ctx.Done()
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}

	manager.Start(context.Background())
	<-firstStarted
	stopDone := make(chan struct{})
	go func() {
		manager.Stop()
		close(stopDone)
	}()
	<-firstCanceled

	startDone := make(chan struct{})
	go func() {
		manager.Start(context.Background())
		close(startDone)
	}()
	select {
	case <-startDone:
		t.Fatal("Start() returned before the previous health loop stopped")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	<-stopDone
	<-startDone
	manager.Stop()
}

func TestCheckPoolLimitsUnboundSamples(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334-6000"))
	manager.check = successfulTrace
	if _, errResolve := manager.Resolve(context.Background(), proxyPoolTestAuth("auth-a")); errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}

	results, errCheck := manager.CheckPool(context.Background(), "residential", 2)
	if errCheck != nil {
		t.Fatalf("CheckPool() error = %v", errCheck)
	}
	if got, wantMax := len(results), 3; got > wantMax {
		t.Fatalf("CheckPool() result count = %d, want at most %d", got, wantMax)
	}
}

func TestReportFailureMarksProxyInfrastructureWithoutChangingProviderErrors(t *testing.T) {
	t.Parallel()

	manager := newTestManager(t, filepath.Join(t.TempDir(), "config.yaml"), proxyPoolTestConfig("3334"))
	manager.check = successfulTrace
	auth := proxyPoolTestAuth("auth-a")
	resolved, errResolve := manager.Resolve(context.Background(), auth)
	if errResolve != nil {
		t.Fatalf("Resolve() error = %v", errResolve)
	}
	runtimeAuth := auth.Clone()
	runtimeAuth.RuntimeProxyURL = resolved.URL
	runtimeAuth.RuntimeProxyBindingID = resolved.BindingID

	providerErr := errors.New("provider rejected request")
	if got := manager.ReportFailure(context.Background(), runtimeAuth, providerErr); got != providerErr {
		t.Fatalf("provider error changed to %v", got)
	}
	proxyErr := &net.DNSError{Err: "temporary failure", Name: "proxy.example"}
	if got := manager.ReportFailure(context.Background(), runtimeAuth, proxyErr); got != proxyErr {
		t.Fatalf("healthy proxy changed ambiguous provider error to %v", got)
	}
	manager.check = func(context.Context, string) TraceResult {
		return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed"}
	}
	var unavailable *UnavailableError
	if got := manager.ReportFailure(context.Background(), runtimeAuth, proxyErr); !errors.As(got, &unavailable) {
		t.Fatalf("failed proxy recheck error = %v, want UnavailableError", got)
	}
}

func TestProxyInfrastructureStatusRequiresProxyEvidence(t *testing.T) {
	t.Parallel()

	if isProxyInfrastructureError(testStatusError{status: http.StatusServiceUnavailable, message: "upstream service unavailable"}) {
		t.Fatal("generic upstream 503 was classified as proxy infrastructure")
	}
	if !isProxyInfrastructureError(testStatusError{status: http.StatusBadGateway, message: "proxy tunnel failed"}) {
		t.Fatal("proxy tunnel 502 was not classified as proxy infrastructure")
	}
	if !isProxyInfrastructureError(testStatusError{status: http.StatusProxyAuthRequired, message: "authentication required"}) {
		t.Fatal("407 was not classified as proxy infrastructure")
	}
	providerNetworkErr := &url.Error{Op: "Post", URL: "https://provider.example", Err: io.ErrUnexpectedEOF}
	if isProxyInfrastructureError(providerNetworkErr) || !isAmbiguousProxyInfrastructureError(providerNetworkErr) {
		t.Fatal("provider network error did not require proxy recheck")
	}
	if !isAmbiguousProxyInfrastructureError(errors.New("Bad Gateway")) {
		t.Fatal("plain CONNECT Bad Gateway did not require proxy recheck")
	}
}

func newTestManager(t *testing.T, configPath string, cfg *internalconfig.Config) *Manager {
	t.Helper()
	manager, errNew := NewManager(configPath, cfg)
	if errNew != nil {
		t.Fatalf("NewManager() error = %v", errNew)
	}
	return manager
}

func proxyPoolTestConfig(ports string) *internalconfig.Config {
	cfg := &internalconfig.Config{}
	cfg.ProxyPools = []internalconfig.ProxyPoolConfig{{
		Name:                 "residential",
		PlaceholderCharset:   "abc123",
		CheckIntervalSeconds: 300,
		BindAttempts:         8,
		Entries: []internalconfig.ProxyPoolEntryConfig{{
			ID:          "home",
			URLTemplate: "http://user-session-{3}:password@proxy.example",
			Ports:       ports,
		}},
	}}
	cfg.ProxyRules = []internalconfig.ProxyRuleConfig{{
		Name:       "codex",
		Pool:       "residential",
		Providers:  []string{"codex"},
		Priorities: []int{0},
	}}
	return cfg
}

func proxyPoolTestAuth(id string) *coreauth.Auth {
	return &coreauth.Auth{ID: id, Provider: "codex", Attributes: map[string]string{"priority": "0"}}
}

func successfulTrace(context.Context, string) TraceResult {
	return TraceResult{OK: true, IP: "203.0.113.8", Location: "US", CheckedAt: time.Now().UTC()}
}

func canceledTrace(ctx context.Context, _ string) TraceResult {
	<-ctx.Done()
	return TraceResult{CheckedAt: time.Now().UTC(), Error: "request_failed", Message: ctx.Err().Error()}
}

type staticAuthSource map[string]*coreauth.Auth

type blockingAuthSource struct {
	auth    *coreauth.Auth
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type blockingGetAuthSource struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type incrementingReader struct {
	mu    sync.Mutex
	value byte
}

func (r *incrementingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index := range p {
		p[index] = r.value
		r.value++
	}
	return len(p), nil
}

type testStatusError struct {
	status  int
	message string
}

func (e testStatusError) Error() string   { return e.message }
func (e testStatusError) StatusCode() int { return e.status }

func (s staticAuthSource) List() []*coreauth.Auth {
	auths := make([]*coreauth.Auth, 0, len(s))
	for _, auth := range s {
		auths = append(auths, auth)
	}
	return auths
}

func (s staticAuthSource) GetByID(id string) (*coreauth.Auth, bool) {
	auth, ok := s[id]
	return auth, ok
}

func (s *blockingAuthSource) List() []*coreauth.Auth {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return []*coreauth.Auth{s.auth}
}

func (s *blockingAuthSource) GetByID(id string) (*coreauth.Auth, bool) {
	if s == nil || s.auth == nil || s.auth.ID != id {
		return nil, false
	}
	return s.auth, true
}

func (*blockingGetAuthSource) List() []*coreauth.Auth { return nil }

func (s *blockingGetAuthSource) GetByID(string) (*coreauth.Auth, bool) {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return nil, false
}
