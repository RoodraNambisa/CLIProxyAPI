package watcher

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/authfileguard"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/diff"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/watcher/synthesizer"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"gopkg.in/yaml.v3"
)

func TestApplyAuthExcludedModelsMeta_APIKey(t *testing.T) {
	auth := &coreauth.Auth{Attributes: map[string]string{}}
	cfg := &config.Config{}
	perKey := []string{" Model-1 ", "model-2"}

	synthesizer.ApplyAuthExcludedModelsMeta(auth, cfg, perKey, "apikey")

	expected := diff.ComputeExcludedModelsHash([]string{"model-1", "model-2"})
	if got := auth.Attributes["excluded_models_hash"]; got != expected {
		t.Fatalf("expected hash %s, got %s", expected, got)
	}
	if got := auth.Attributes["auth_kind"]; got != "apikey" {
		t.Fatalf("expected auth_kind=apikey, got %s", got)
	}
}

func TestApplyAuthExcludedModelsMeta_OAuthProvider(t *testing.T) {
	auth := &coreauth.Auth{
		Provider:   "TestProv",
		Attributes: map[string]string{},
	}
	cfg := &config.Config{
		OAuthExcludedModels: map[string][]string{
			"testprov": {"A", "b"},
		},
	}

	synthesizer.ApplyAuthExcludedModelsMeta(auth, cfg, nil, "oauth")

	expected := diff.ComputeExcludedModelsHash([]string{"a", "b"})
	if got := auth.Attributes["excluded_models_hash"]; got != expected {
		t.Fatalf("expected hash %s, got %s", expected, got)
	}
	if got := auth.Attributes["auth_kind"]; got != "oauth" {
		t.Fatalf("expected auth_kind=oauth, got %s", got)
	}
}

func TestBuildAPIKeyClientsCounts(t *testing.T) {
	cfg := &config.Config{
		GeminiKey:       []config.GeminiKey{{APIKey: "g1"}, {APIKey: "g2"}},
		InteractionsKey: []config.GeminiKey{{APIKey: "i1"}},
		VertexCompatAPIKey: []config.VertexCompatKey{
			{APIKey: "v1"},
		},
		ClaudeKey: []config.ClaudeKey{{APIKey: "c1"}},
		CodexKey:  []config.CodexKey{{APIKey: "x1"}, {APIKey: "x2"}},
		OpenAICompatibility: []config.OpenAICompatibility{
			{APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "o1"}, {APIKey: "o2"}}},
			{Disabled: true, APIKeyEntries: []config.OpenAICompatibilityAPIKey{{APIKey: "disabled"}}},
		},
	}

	gemini, vertex, claude, codex, compat := BuildAPIKeyClients(cfg)
	if gemini != 3 || vertex != 1 || claude != 1 || codex != 2 || compat != 2 {
		t.Fatalf("unexpected counts: %d %d %d %d %d", gemini, vertex, claude, codex, compat)
	}
}

func TestNormalizeAuthStripsTemporalFields(t *testing.T) {
	now := time.Now()
	auth := &coreauth.Auth{
		CreatedAt:        now,
		UpdatedAt:        now,
		LastRefreshedAt:  now,
		NextRefreshAfter: now,
		Quota: coreauth.QuotaState{
			NextRecoverAt: now,
		},
		Runtime: map[string]any{"k": "v"},
	}

	normalized := normalizeAuth(auth)
	if !normalized.CreatedAt.IsZero() || !normalized.UpdatedAt.IsZero() || !normalized.LastRefreshedAt.IsZero() || !normalized.NextRefreshAfter.IsZero() {
		t.Fatal("expected time fields to be zeroed")
	}
	if normalized.Runtime != nil {
		t.Fatal("expected runtime to be nil")
	}
	if !normalized.Quota.NextRecoverAt.IsZero() {
		t.Fatal("expected quota.NextRecoverAt to be zeroed")
	}
}

func TestMatchProvider(t *testing.T) {
	if _, ok := matchProvider("OpenAI", []string{"openai", "claude"}); !ok {
		t.Fatal("expected match to succeed ignoring case")
	}
	if _, ok := matchProvider("missing", []string{"openai"}); ok {
		t.Fatal("expected match to fail for unknown provider")
	}
}

func TestSnapshotCoreAuths_ConfigAndAuthFiles(t *testing.T) {
	authDir := t.TempDir()
	metadata := map[string]any{
		"type":       "gemini",
		"email":      "user@example.com",
		"project_id": "proj-a, proj-b",
		"proxy_url":  "https://proxy",
	}
	authFile := filepath.Join(authDir, "gemini.json")
	data, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("failed to marshal metadata: %v", err)
	}
	if err = os.WriteFile(authFile, data, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	cfg := &config.Config{
		AuthDir: authDir,
		GeminiKey: []config.GeminiKey{
			{
				APIKey:         "g-key",
				BaseURL:        "https://gemini",
				ExcludedModels: []string{"Model-A", "model-b"},
				Headers:        map[string]string{"X-Req": "1"},
			},
		},
	}

	w := &Watcher{authDir: authDir}
	w.SetConfig(cfg)

	auths := w.SnapshotCoreAuths()
	if len(auths) != 1 {
		t.Fatalf("expected 1 config auth entry, got %d", len(auths))
	}

	var geminiAPIKeyAuth *coreauth.Auth
	for _, a := range auths {
		if a.Provider == "gemini" && a.Attributes["api_key"] == "g-key" {
			geminiAPIKeyAuth = a
		}
	}
	if geminiAPIKeyAuth == nil {
		t.Fatal("expected synthesized Gemini API key auth")
	}
	expectedAPIKeyHash := diff.ComputeExcludedModelsHash([]string{"Model-A", "model-b"})
	if geminiAPIKeyAuth.Attributes["excluded_models_hash"] != expectedAPIKeyHash {
		t.Fatalf("expected API key excluded hash %s, got %s", expectedAPIKeyHash, geminiAPIKeyAuth.Attributes["excluded_models_hash"])
	}
	if geminiAPIKeyAuth.Attributes["auth_kind"] != "apikey" {
		t.Fatalf("expected auth_kind=apikey, got %s", geminiAPIKeyAuth.Attributes["auth_kind"])
	}
}

func TestReloadConfigIfChanged_TriggersOnChangeAndSkipsUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	configPath := filepath.Join(tmpDir, "config.yaml")
	writeConfig := func(port int, allowRemote bool) {
		cfg := &config.Config{
			Port:    port,
			AuthDir: authDir,
			RemoteManagement: config.RemoteManagement{
				AllowRemote: allowRemote,
			},
		}
		data, err := yaml.Marshal(cfg)
		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}
		if err = os.WriteFile(configPath, data, 0o644); err != nil {
			t.Fatalf("failed to write config: %v", err)
		}
	}

	writeConfig(8080, false)

	reloads := 0
	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		reloadCallback: func(*config.Config) { reloads++ },
	}

	w.reloadConfigIfChanged()
	if reloads != 1 {
		t.Fatalf("expected first reload to trigger callback once, got %d", reloads)
	}

	// Same content should be skipped by hash check.
	w.reloadConfigIfChanged()
	if reloads != 1 {
		t.Fatalf("expected unchanged config to be skipped, callback count %d", reloads)
	}

	writeConfig(9090, true)
	w.reloadConfigIfChanged()
	if reloads != 2 {
		t.Fatalf("expected changed config to trigger reload, callback count %d", reloads)
	}
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.Port != 9090 || !w.config.RemoteManagement.AllowRemote {
		t.Fatalf("expected config to be updated after reload, got %+v", w.config)
	}
}

func TestStartAndStopSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir), 0o644); err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}

	var reloads int32
	w, err := NewWatcher(configPath, authDir, func(*config.Config) {
		atomic.AddInt32(&reloads, 1)
	})
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	if err := w.Start(context.Background()); err != nil {
		t.Fatalf("expected Start to succeed: %v", err)
	}
	w.eventMu.Lock()
	eventDone := w.eventDone
	w.eventMu.Unlock()
	if eventDone == nil {
		t.Fatal("Start() did not create an event-loop completion signal")
	}
	if err := w.Stop(); err != nil {
		t.Fatalf("expected Stop to succeed: %v", err)
	}
	select {
	case <-eventDone:
	default:
		t.Fatal("Stop() returned before the event loop exited")
	}
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected one reload callback, got %d", got)
	}
}

func TestWatcherCanRestartEventLoopAfterContextCancellation(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if errMkdir := os.MkdirAll(authDir, 0o755); errMkdir != nil {
		t.Fatalf("create auth dir: %v", errMkdir)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if errWrite := os.WriteFile(configPath, []byte("auth_dir: "+authDir), 0o644); errWrite != nil {
		t.Fatalf("create config file: %v", errWrite)
	}
	w, errWatcher := NewWatcher(configPath, authDir, nil)
	if errWatcher != nil {
		t.Fatalf("NewWatcher() error = %v", errWatcher)
	}
	t.Cleanup(func() { _ = w.Stop() })
	w.SetConfig(&config.Config{AuthDir: authDir})

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	if errStart := w.Start(firstCtx); errStart != nil {
		t.Fatalf("first Start() error = %v", errStart)
	}
	w.eventMu.Lock()
	firstDone := w.eventDone
	w.eventMu.Unlock()
	cancelFirst()
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first event loop did not stop after context cancellation")
	}

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	if errStart := w.Start(secondCtx); errStart != nil {
		cancelSecond()
		t.Fatalf("second Start() error = %v", errStart)
	}
	w.eventMu.Lock()
	secondDone := w.eventDone
	w.eventMu.Unlock()
	cancelSecond()
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second event loop did not stop after context cancellation")
	}
}

func TestStartFailsWhenConfigMissing(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "missing-config.yaml")

	w, err := NewWatcher(configPath, authDir, nil)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err == nil {
		t.Fatal("expected Start to fail for missing config file")
	}
}

func TestDispatchRuntimeAuthUpdateEnqueuesAndUpdatesState(t *testing.T) {
	queue := make(chan AuthUpdate, 4)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	auth := &coreauth.Auth{ID: "auth-1", Provider: "test"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: auth}); !ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate to enqueue")
	}

	select {
	case update := <-queue:
		if update.Action != AuthUpdateActionAdd || update.Auth.ID != "auth-1" {
			t.Fatalf("unexpected update: %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for auth update")
	}

	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionDelete, ID: "auth-1"}); !ok {
		t.Fatal("expected delete update to enqueue")
	}
	select {
	case update := <-queue:
		if update.Action != AuthUpdateActionDelete || update.ID != "auth-1" {
			t.Fatalf("unexpected delete update: %+v", update)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delete update")
	}
	w.clientsMutex.RLock()
	if _, exists := w.runtimeAuths["auth-1"]; exists {
		w.clientsMutex.RUnlock()
		t.Fatal("expected runtime auth to be cleared after delete")
	}
	w.clientsMutex.RUnlock()
}

func TestDispatchRuntimeAuthUpdateRejectsRetiredGeminiCLI(t *testing.T) {
	queue := make(chan AuthUpdate, 1)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	retired := &coreauth.Auth{ID: "legacy", Provider: "gemini-cli"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: retired}); ok {
		t.Fatal("retired Gemini CLI runtime auth was accepted")
	}
	w.clientsMutex.RLock()
	_, stored := w.runtimeAuths[retired.ID]
	w.clientsMutex.RUnlock()
	if stored {
		t.Fatal("retired Gemini CLI runtime auth was retained")
	}
	select {
	case update := <-queue:
		t.Fatalf("retired Gemini CLI runtime update was enqueued: %+v", update)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDispatchRuntimeAuthUpdateRetiresExistingRuntimeAuth(t *testing.T) {
	queue := make(chan AuthUpdate, 2)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	active := &coreauth.Auth{ID: "legacy", Provider: "codex"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: active}); !ok {
		t.Fatal("active runtime auth was not accepted")
	}
	select {
	case update := <-queue:
		if update.Action != AuthUpdateActionAdd {
			t.Fatalf("initial update action = %s, want add", update.Action)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial runtime auth update")
	}

	retired := &coreauth.Auth{ID: active.ID, Provider: "gemini-cli"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionModify, Auth: retired}); !ok {
		t.Fatal("retired replacement did not enqueue deletion")
	}
	select {
	case update := <-queue:
		if update.Action != AuthUpdateActionDelete || update.ID != active.ID {
			t.Fatalf("replacement update = %#v, want delete for %s", update, active.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for retired runtime auth deletion")
	}
	w.clientsMutex.RLock()
	_, runtimeStored := w.runtimeAuths[active.ID]
	_, currentStored := w.currentAuths[active.ID]
	w.clientsMutex.RUnlock()
	if runtimeStored || currentStored {
		t.Fatalf("retired runtime auth remained in watcher state: runtime=%t current=%t", runtimeStored, currentStored)
	}
}

func TestDispatchRuntimeAuthUpdateRetiredCollisionPreservesFileSnapshot(t *testing.T) {
	queue := make(chan AuthUpdate, 1)
	fileAuth := &coreauth.Auth{ID: "shared", Provider: "codex", FileName: "shared.json"}
	w := &Watcher{currentAuths: map[string]*coreauth.Auth{fileAuth.ID: fileAuth.Clone()}}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	retired := &coreauth.Auth{ID: fileAuth.ID, Provider: "gemini-cli"}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionModify, Auth: retired}); ok {
		t.Fatal("retired runtime collision was accepted")
	}
	w.clientsMutex.RLock()
	current := w.currentAuths[fileAuth.ID]
	_, runtimeStored := w.runtimeAuths[fileAuth.ID]
	w.clientsMutex.RUnlock()
	if current == nil || current.Provider != fileAuth.Provider || runtimeStored {
		t.Fatalf("file snapshot changed after rejected runtime collision: current=%#v runtime=%t", current, runtimeStored)
	}
	select {
	case update := <-queue:
		t.Fatalf("rejected runtime collision enqueued update: %#v", update)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDispatchRuntimeAuthUpdateResultReturnsDeleteFallbackWithoutQueue(t *testing.T) {
	active := &coreauth.Auth{ID: "legacy", Provider: "codex"}
	w := &Watcher{
		runtimeAuths: map[string]*coreauth.Auth{active.ID: active.Clone()},
		currentAuths: map[string]*coreauth.Auth{active.ID: active.Clone()},
	}
	retired := &coreauth.Auth{ID: active.ID, Provider: "gemini-cli"}
	result := w.DispatchRuntimeAuthUpdateResult(AuthUpdate{Action: AuthUpdateActionModify, Auth: retired})
	if result.Enqueued || result.Consumed || result.Fallback == nil {
		t.Fatalf("dispatch result = %#v, want delete fallback", result)
	}
	if result.Fallback.Action != AuthUpdateActionDelete || result.Fallback.ID != active.ID || result.Fallback.Auth == nil {
		t.Fatalf("fallback update = %#v, want delete for %s", result.Fallback, active.ID)
	}
	if _, exists := w.runtimeAuths[active.ID]; exists {
		t.Fatal("retired runtime auth remained after fallback creation")
	}
	if _, exists := w.currentAuths[active.ID]; exists {
		t.Fatal("retired current auth remained after fallback creation")
	}
}

func TestRetiredPathBlocksRuntimeAuthAdmissionAndRefresh(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "retired.json")
	auth := &coreauth.Auth{
		ID: "retired", Provider: "codex",
		Attributes: map[string]string{"source": path},
	}
	w := &Watcher{
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		authQueue:        make(chan AuthUpdate, 1),
		retiredAuthPaths: map[string]struct{}{},
		runtimeAuths:     make(map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
	}
	w.retiredAuthPaths[w.normalizeAuthPath(path)] = struct{}{}

	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: auth}); ok {
		t.Fatal("runtime auth for a retired path was accepted")
	}
	if _, exists := w.runtimeAuths[auth.ID]; exists {
		t.Fatal("rejected runtime auth was retained")
	}

	w.runtimeAuths[auth.ID] = auth.Clone()
	w.currentAuths[auth.ID] = auth.Clone()
	w.refreshAuthState(false)
	if _, exists := w.runtimeAuths[auth.ID]; exists {
		t.Fatal("refresh retained runtime auth for a retired path")
	}
	if _, exists := w.currentAuths[auth.ID]; exists {
		t.Fatal("refresh reintroduced runtime auth for a retired path")
	}
	w.dispatchMu.Lock()
	defer w.dispatchMu.Unlock()
	if len(w.pendingUpdates) != 1 {
		t.Fatalf("pending updates = %#v, want one delete", w.pendingUpdates)
	}
	for _, update := range w.pendingUpdates {
		if update.Action != AuthUpdateActionDelete || update.ID != auth.ID {
			t.Fatalf("pending update = %#v, want delete for %s", update, auth.ID)
		}
	}
}

func TestRetiredPathRuntimeAdmissionShapes(t *testing.T) {
	authDir := t.TempDir()
	retiredPath := filepath.Join(authDir, "retired.json")
	tests := []struct {
		name         string
		auth         *coreauth.Auth
		wantAccepted bool
	}{
		{
			name: "relative source",
			auth: &coreauth.Auth{ID: "relative", Provider: "codex", Attributes: map[string]string{
				"source": "retired.json",
			}},
		},
		{
			name: "file name only",
			auth: &coreauth.Auth{ID: "filename", Provider: "codex", FileName: "retired.json"},
		},
		{
			name: "relative path attribute",
			auth: &coreauth.Auth{ID: "path", Provider: "codex", Attributes: map[string]string{
				"path": "retired.json",
			}},
		},
		{
			name: "unrelated path attribute",
			auth: &coreauth.Auth{ID: "active-path", Provider: "codex", Attributes: map[string]string{
				"path": "active.json",
			}},
			wantAccepted: true,
		},
		{
			name: "config source",
			auth: &coreauth.Auth{ID: "config", Provider: "codex", Attributes: map[string]string{
				"source": "config:codex",
			}},
			wantAccepted: true,
		},
		{
			name: "runtime only",
			auth: &coreauth.Auth{ID: "runtime", Provider: "codex", FileName: "retired.json", Attributes: map[string]string{
				"runtime_only": "true",
			}},
			wantAccepted: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &Watcher{
				authDir:          authDir,
				authQueue:        make(chan AuthUpdate, 1),
				retiredAuthPaths: map[string]struct{}{},
			}
			w.retiredAuthPaths[w.normalizeAuthPath(retiredPath)] = struct{}{}
			accepted := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: tt.auth})
			if accepted != tt.wantAccepted {
				t.Fatalf("DispatchRuntimeAuthUpdate() = %t, want %t", accepted, tt.wantAccepted)
			}
			w.clientsMutex.RLock()
			_, stored := w.runtimeAuths[tt.auth.ID]
			w.clientsMutex.RUnlock()
			if stored != tt.wantAccepted {
				t.Fatalf("runtime auth stored = %t, want %t", stored, tt.wantAccepted)
			}
		})
	}
}

func TestAddOrUpdateClientSkipsUnchanged(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}
	data, _ := os.ReadFile(authFile)
	sum := sha256.Sum256(data)

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})
	// Use normalizeAuthPath to match how addOrUpdateClient stores the key
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	w.addOrUpdateClient(authFile)
	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload for unchanged file, got %d", got)
	}
}

func TestAddOrUpdateClientTriggersReloadAndHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo","api_key":"k"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})

	w.addOrUpdateClient(authFile)

	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload callback for auth update, got %d", got)
	}
	// Use normalizeAuthPath to match how addOrUpdateClient stores the key
	normalized := w.normalizeAuthPath(authFile)
	if _, ok := w.lastAuthHashes[normalized]; !ok {
		t.Fatalf("expected hash to be stored for %s", normalized)
	}
}

func TestRemoveClientRemovesHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	var reloads int32

	w := &Watcher{
		authDir:        tmpDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})
	// Use normalizeAuthPath to set up the hash with the correct key format
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.removeClient(authFile)
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected hash to be removed after deletion")
	}
	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no reload callback for auth removal, got %d", got)
	}
}

func TestAuthFileEventsDoNotInvokeSnapshotCoreAuths(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"codex","email":"u@example.com"}`), 0o644); err != nil {
		t.Fatalf("failed to create auth file: %v", err)
	}

	origSnapshot := snapshotCoreAuthsFunc
	var snapshotCalls int32
	snapshotCoreAuthsFunc = func(cfg *config.Config, authDir string) []*coreauth.Auth {
		atomic.AddInt32(&snapshotCalls, 1)
		return origSnapshot(cfg, authDir)
	}
	defer func() { snapshotCoreAuthsFunc = origSnapshot }()

	w := &Watcher{
		authDir:          tmpDir,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
	}
	w.SetConfig(&config.Config{AuthDir: tmpDir})

	w.addOrUpdateClient(authFile)
	w.removeClient(authFile)

	if got := atomic.LoadInt32(&snapshotCalls); got != 0 {
		t.Fatalf("expected auth file events to avoid full snapshot, got %d calls", got)
	}
}

func TestAuthSliceToMap(t *testing.T) {
	t.Parallel()

	valid1 := &coreauth.Auth{ID: "a"}
	valid2 := &coreauth.Auth{ID: "b"}
	dupOld := &coreauth.Auth{ID: "dup", Label: "old"}
	dupNew := &coreauth.Auth{ID: "dup", Label: "new"}
	empty := &coreauth.Auth{ID: "  "}

	tests := []struct {
		name string
		in   []*coreauth.Auth
		want map[string]*coreauth.Auth
	}{
		{
			name: "nil input",
			in:   nil,
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "empty input",
			in:   []*coreauth.Auth{},
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "filters invalid auths",
			in:   []*coreauth.Auth{nil, empty},
			want: map[string]*coreauth.Auth{},
		},
		{
			name: "keeps valid auths",
			in:   []*coreauth.Auth{valid1, nil, valid2},
			want: map[string]*coreauth.Auth{"a": valid1, "b": valid2},
		},
		{
			name: "last duplicate wins",
			in:   []*coreauth.Auth{dupOld, dupNew},
			want: map[string]*coreauth.Auth{"dup": dupNew},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := authSliceToMap(tc.in)
			if len(tc.want) == 0 {
				if got == nil {
					t.Fatal("expected empty map, got nil")
				}
				if len(got) != 0 {
					t.Fatalf("expected empty map, got %#v", got)
				}
				return
			}
			if len(got) != len(tc.want) {
				t.Fatalf("unexpected map length: got %d, want %d", len(got), len(tc.want))
			}
			for id, wantAuth := range tc.want {
				gotAuth, ok := got[id]
				if !ok {
					t.Fatalf("missing id %q in result map", id)
				}
				if !authEqual(gotAuth, wantAuth) {
					t.Fatalf("unexpected auth for id %q: got %#v, want %#v", id, gotAuth, wantAuth)
				}
			}
		})
	}
}

func TestTriggerServerUpdateCancelsPendingTimerOnImmediate(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := &config.Config{AuthDir: tmpDir}

	var reloads int32
	w := &Watcher{
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	w.SetConfig(cfg)

	w.serverUpdateMu.Lock()
	w.serverUpdateLast = time.Now().Add(-(serverUpdateDebounce - 100*time.Millisecond))
	w.serverUpdateMu.Unlock()
	w.triggerServerUpdate(cfg)

	if got := atomic.LoadInt32(&reloads); got != 0 {
		t.Fatalf("expected no immediate reload, got %d", got)
	}

	w.serverUpdateMu.Lock()
	if !w.serverUpdatePend || w.serverUpdateTimer == nil {
		w.serverUpdateMu.Unlock()
		t.Fatal("expected a pending server update timer")
	}
	w.serverUpdateLast = time.Now().Add(-(serverUpdateDebounce + 10*time.Millisecond))
	w.serverUpdateMu.Unlock()

	w.triggerServerUpdate(cfg)
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected immediate reload once, got %d", got)
	}

	time.Sleep(250 * time.Millisecond)
	if got := atomic.LoadInt32(&reloads); got != 1 {
		t.Fatalf("expected pending timer to be cancelled, got %d reloads", got)
	}
}

func TestShouldDebounceRemove(t *testing.T) {
	w := &Watcher{}
	path := filepath.Clean("test.json")

	if w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("first call should not debounce")
	}
	if !w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("second call within window should debounce")
	}

	w.clientsMutex.Lock()
	w.lastRemoveTimes = map[string]time.Time{path: time.Now().Add(-2 * authRemoveDebounceWindow)}
	w.clientsMutex.Unlock()

	if w.shouldDebounceRemove(path, time.Now()) {
		t.Fatal("call after window should not debounce")
	}
}

func TestAuthFileUnchangedUsesHash(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "sample.json")
	content := []byte(`{"type":"demo"}`)
	if err := os.WriteFile(authFile, content, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	w := &Watcher{lastAuthHashes: make(map[string]string)}
	unchanged, err := w.authFileUnchanged(authFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if unchanged {
		t.Fatal("expected first check to report changed")
	}

	sum := sha256.Sum256(content)
	// Use normalizeAuthPath to match how authFileUnchanged looks up the key
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	unchanged, err = w.authFileUnchanged(authFile)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !unchanged {
		t.Fatal("expected hash match to report unchanged")
	}
}

func TestAuthFileUnchangedEmptyAndMissing(t *testing.T) {
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty auth file: %v", err)
	}

	w := &Watcher{lastAuthHashes: make(map[string]string)}
	unchanged, err := w.authFileUnchanged(emptyFile)
	if err != nil {
		t.Fatalf("unexpected error for empty file: %v", err)
	}
	if unchanged {
		t.Fatal("expected empty file to be treated as changed")
	}

	_, err = w.authFileUnchanged(filepath.Join(tmpDir, "missing.json"))
	if err == nil {
		t.Fatal("expected error for missing auth file")
	}
}

func TestReloadClientsCachesAuthHashes(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "one.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	w := &Watcher{
		authDir: tmpDir,
		config:  &config.Config{AuthDir: tmpDir},
	}

	w.reloadClients(true, nil, false)

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if len(w.lastAuthHashes) != 1 {
		t.Fatalf("expected hash cache for one auth file, got %d", len(w.lastAuthHashes))
	}
}

func TestReloadClientsDoesNotHoldClientsMutexWhileWaitingForPathLock(t *testing.T) {
	authDir := t.TempDir()
	authFile := filepath.Join(authDir, "one.json")
	if errWrite := os.WriteFile(authFile, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth file: %v", errWrite)
	}
	w := &Watcher{
		authDir: authDir,
		config:  &config.Config{AuthDir: authDir},
	}

	unlockPath := authfileguard.Lock(authFile)
	reloadDone := make(chan struct{})
	go func() {
		w.reloadClients(true, nil, false)
		close(reloadDone)
	}()

	// Give reloadClients time to reach the held path lock. The clients mutex
	// must remain available while the reload waits there.
	time.Sleep(100 * time.Millisecond)
	clientsLockAcquired := make(chan struct{})
	go func() {
		w.clientsMutex.Lock()
		close(clientsLockAcquired)
		w.clientsMutex.Unlock()
	}()

	clientsLockBlocked := false
	select {
	case <-clientsLockAcquired:
	case <-time.After(500 * time.Millisecond):
		clientsLockBlocked = true
	}
	unlockPath()

	select {
	case <-reloadDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reloadClients did not finish after the path lock was released")
	}
	if clientsLockBlocked {
		select {
		case <-clientsLockAcquired:
		case <-time.After(2 * time.Second):
			t.Fatal("clients mutex remained blocked after reloadClients finished")
		}
		t.Fatal("reloadClients held clientsMutex while waiting for an auth path lock")
	}
}

func TestReloadClientsLogsConfigDiffs(t *testing.T) {
	tmpDir := t.TempDir()
	oldCfg := &config.Config{AuthDir: tmpDir, Port: 1, Debug: false}
	newCfg := &config.Config{AuthDir: tmpDir, Port: 2, Debug: true}

	w := &Watcher{
		authDir: tmpDir,
		config:  oldCfg,
	}
	w.SetConfig(oldCfg)
	w.oldConfigYaml, _ = yaml.Marshal(oldCfg)

	w.clientsMutex.Lock()
	w.config = newCfg
	w.clientsMutex.Unlock()

	w.reloadClients(false, nil, false)
}

func TestReloadClientsHandlesNilConfig(t *testing.T) {
	w := &Watcher{}
	w.reloadClients(true, nil, false)
}

func TestReloadClientsFiltersProvidersWithNilCurrentAuths(t *testing.T) {
	tmp := t.TempDir()
	w := &Watcher{
		authDir: tmp,
		config:  &config.Config{AuthDir: tmp},
	}
	w.reloadClients(false, []string{"match"}, false)
	if w.currentAuths != nil && len(w.currentAuths) != 0 {
		t.Fatalf("expected currentAuths to be nil or empty, got %d", len(w.currentAuths))
	}
}

func TestSetAuthUpdateQueueNilResetsDispatch(t *testing.T) {
	w := &Watcher{}
	queue := make(chan AuthUpdate, 1)
	w.SetAuthUpdateQueue(queue)
	if w.dispatchCond == nil || w.dispatchCancel == nil {
		t.Fatal("expected dispatch to be initialized")
	}
	w.SetAuthUpdateQueue(nil)
	if w.dispatchCancel != nil {
		t.Fatal("expected dispatch cancel to be cleared when queue nil")
	}
	if w.dispatchDone != nil {
		t.Fatal("expected dispatch completion to be cleared when queue nil")
	}
}

func TestSetAuthUpdateQueueWaitsForOldDispatcherAndClearsPending(t *testing.T) {
	oldCanceled := make(chan struct{})
	oldDone := make(chan struct{})
	w := &Watcher{
		authQueue: make(chan AuthUpdate),
		pendingUpdates: map[string]AuthUpdate{
			"stale": {Action: AuthUpdateActionModify, ID: "stale"},
		},
		pendingOrder:   []string{"stale"},
		dispatchCancel: func() { close(oldCanceled) },
		dispatchDone:   oldDone,
	}
	newQueue := make(chan AuthUpdate, 1)
	setDone := make(chan struct{})
	go func() {
		w.SetAuthUpdateQueue(newQueue)
		close(setDone)
	}()

	select {
	case <-oldCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("old dispatcher was not canceled")
	}
	select {
	case <-setDone:
		t.Fatal("queue replacement completed before the old dispatcher exited")
	default:
	}
	freshDispatchDone := make(chan struct{})
	go func() {
		w.dispatchAuthUpdates([]AuthUpdate{{Action: AuthUpdateActionAdd, ID: "fresh"}})
		close(freshDispatchDone)
	}()
	select {
	case <-freshDispatchDone:
		t.Fatal("new update was dispatched before queue replacement completed")
	default:
	}
	close(oldDone)
	select {
	case <-setDone:
	case <-time.After(2 * time.Second):
		t.Fatal("queue replacement did not finish after the old dispatcher exited")
	}
	t.Cleanup(w.stopDispatch)

	w.dispatchMu.Lock()
	_, stalePending := w.pendingUpdates["stale"]
	for _, key := range w.pendingOrder {
		stalePending = stalePending || key == "stale"
	}
	w.dispatchMu.Unlock()
	if stalePending {
		t.Fatal("stale pending update survived queue replacement")
	}
	if got := w.getAuthQueue(); got != newQueue {
		t.Fatal("new auth update queue was not installed")
	}
	select {
	case <-freshDispatchDone:
	case <-time.After(2 * time.Second):
		t.Fatal("new update did not resume after queue replacement")
	}
	select {
	case update := <-newQueue:
		if update.ID != "fresh" {
			t.Fatalf("replacement queue received update %q, want fresh", update.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement queue did not receive fresh update")
	}
}

func TestDispatchRuntimeAuthUpdateWaitsForQueueReplacement(t *testing.T) {
	oldCanceled := make(chan struct{})
	oldDone := make(chan struct{})
	w := &Watcher{
		authQueue:      make(chan AuthUpdate),
		dispatchCancel: func() { close(oldCanceled) },
		dispatchDone:   oldDone,
	}
	newQueue := make(chan AuthUpdate, 1)
	setDone := make(chan struct{})
	go func() {
		w.SetAuthUpdateQueue(newQueue)
		close(setDone)
	}()
	select {
	case <-oldCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("old dispatcher was not canceled")
	}

	accepted := make(chan bool, 1)
	go func() {
		accepted <- w.DispatchRuntimeAuthUpdate(AuthUpdate{
			Action: AuthUpdateActionAdd,
			ID:     "runtime",
			Auth:   &coreauth.Auth{ID: "runtime", Provider: "codex"},
		})
	}()
	select {
	case result := <-accepted:
		t.Fatalf("runtime update returned %t before queue replacement completed", result)
	default:
	}
	close(oldDone)
	select {
	case <-setDone:
	case <-time.After(2 * time.Second):
		t.Fatal("queue replacement did not finish")
	}
	t.Cleanup(w.stopDispatch)
	select {
	case result := <-accepted:
		if !result {
			t.Fatal("runtime update was rejected after replacement queue became available")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime update did not resume after queue replacement")
	}
	select {
	case update := <-newQueue:
		if update.ID != "runtime" {
			t.Fatalf("replacement queue received update %q, want runtime", update.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement queue did not receive runtime update")
	}
}

func TestNextPendingBatchStopsBeforeCanceledPendingUpdates(t *testing.T) {
	w := &Watcher{
		pendingUpdates: map[string]AuthUpdate{
			"stale": {Action: AuthUpdateActionModify, ID: "stale"},
		},
		pendingOrder: []string{"stale"},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if batch, ok := w.nextPendingBatch(ctx); ok || batch != nil {
		t.Fatalf("canceled dispatcher returned pending batch: %#v", batch)
	}
}

func TestPersistAsyncEarlyReturns(t *testing.T) {
	var nilWatcher *Watcher
	nilWatcher.persistConfigAsync()
	nilWatcher.persistAuthAsync("msg", "a")

	w := &Watcher{}
	w.persistConfigAsync()
	w.persistAuthAsync("msg", "   ", "")
}

type errorPersister struct {
	configCalls int32
	authCalls   int32
}

func (p *errorPersister) PersistConfig(context.Context) error {
	atomic.AddInt32(&p.configCalls, 1)
	return fmt.Errorf("persist config error")
}

func (p *errorPersister) PersistAuthFiles(context.Context, string, ...string) error {
	atomic.AddInt32(&p.authCalls, 1)
	return fmt.Errorf("persist auth error")
}

func TestPersistAsyncErrorPaths(t *testing.T) {
	p := &errorPersister{}
	w := &Watcher{storePersister: p}
	w.persistConfigAsync()
	w.persistAuthAsync("msg", "a")
	time.Sleep(30 * time.Millisecond)
	if atomic.LoadInt32(&p.configCalls) != 1 {
		t.Fatalf("expected PersistConfig to be called once, got %d", p.configCalls)
	}
	if atomic.LoadInt32(&p.authCalls) != 1 {
		t.Fatalf("expected PersistAuthFiles to be called once, got %d", p.authCalls)
	}
}

func TestStopConfigReloadTimerSafeWhenNil(t *testing.T) {
	w := &Watcher{}
	w.stopConfigReloadTimer()
	w.configReloadMu.Lock()
	w.configReloadTimer = time.AfterFunc(10*time.Millisecond, func() {})
	w.configReloadMu.Unlock()
	time.Sleep(1 * time.Millisecond)
	w.stopConfigReloadTimer()
}

func TestHandleEventRemovesAuthFile(t *testing.T) {
	tmpDir := t.TempDir()
	authFile := filepath.Join(tmpDir, "remove.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	if err := os.Remove(authFile); err != nil {
		t.Fatalf("failed to remove auth file pre-check: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        tmpDir,
		config:         &config.Config{AuthDir: tmpDir},
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) {
			atomic.AddInt32(&reloads, 1)
		},
	}
	// Use normalizeAuthPath to set up the hash with the correct key format
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})

	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reload callback for auth removal, got %d", reloads)
	}
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected hash entry to be removed")
	}
}

func TestDispatchAuthUpdatesFlushesQueue(t *testing.T) {
	queue := make(chan AuthUpdate, 4)
	w := &Watcher{}
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	w.dispatchAuthUpdates([]AuthUpdate{
		{Action: AuthUpdateActionAdd, ID: "a"},
		{Action: AuthUpdateActionModify, ID: "b"},
	})

	got := make([]AuthUpdate, 0, 2)
	for i := 0; i < 2; i++ {
		select {
		case u := <-queue:
			got = append(got, u)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for update %d", i)
		}
	}
	if len(got) != 2 || got[0].ID != "a" || got[1].ID != "b" {
		t.Fatalf("unexpected updates order/content: %+v", got)
	}
}

func TestDispatchLoopExitsOnContextDoneWhileSending(t *testing.T) {
	queue := make(chan AuthUpdate) // unbuffered to block sends
	w := &Watcher{
		authQueue: queue,
		pendingUpdates: map[string]AuthUpdate{
			"k": {Action: AuthUpdateActionAdd, ID: "k"},
		},
		pendingOrder: []string{"k"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.dispatchLoop(ctx)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected dispatchLoop to exit after ctx canceled while blocked on send")
	}
}

func TestProcessEventsHandlesEventErrorAndChannelClose(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: make(chan fsnotify.Event, 2),
			Errors: make(chan error, 2),
		},
		configPath: "config.yaml",
		authDir:    "auth",
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	w.watcher.Events <- fsnotify.Event{Name: "unrelated.txt", Op: fsnotify.Write}
	w.watcher.Errors <- fmt.Errorf("watcher error")

	time.Sleep(20 * time.Millisecond)
	close(w.watcher.Events)
	close(w.watcher.Errors)

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit after channels closed")
	}
}

func TestProcessEventsReturnsWhenErrorsChannelClosed(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: nil,
			Errors: make(chan error),
		},
	}

	close(w.watcher.Errors)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit after errors channel closed")
	}
}

func TestHandleEventIgnoresUnrelatedFiles(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: filepath.Join(tmpDir, "note.txt"), Op: fsnotify.Write})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reloads for unrelated file, got %d", reloads)
	}
}

func TestHandleEventConfigChangeSchedulesReload(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: configPath, Op: fsnotify.Write})

	time.Sleep(400 * time.Millisecond)
	if atomic.LoadInt32(&reloads) != 1 {
		t.Fatalf("expected config change to trigger reload once, got %d", reloads)
	}
}

func TestHandleEventAuthWriteTriggersUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "a.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Write})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected auth write to avoid global reload callback, got %d", reloads)
	}
}

func TestHandleEventRemoveDebounceSkips(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "remove.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		lastRemoveTimes: map[string]time.Time{
			filepath.Clean(authFile): time.Now(),
		},
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected remove to be debounced, got %d", reloads)
	}
}

func TestHandleEventRecreateClearsRemoveDebounce(t *testing.T) {
	authDir := t.TempDir()
	authFile := filepath.Join(authDir, "recreated.json")
	content := []byte(`{"type":"codex","access_token":"test"}`)
	if errWrite := os.WriteFile(authFile, content, 0o600); errWrite != nil {
		t.Fatalf("write initial auth file: %v", errWrite)
	}
	w := &Watcher{
		authDir:         authDir,
		config:          &config.Config{AuthDir: authDir},
		lastAuthHashes:  make(map[string]string),
		fileAuthsByPath: make(map[string]map[string]*coreauth.Auth),
		currentAuths:    make(map[string]*coreauth.Auth),
	}
	w.addOrUpdateClient(authFile)

	if errRemove := os.Remove(authFile); errRemove != nil {
		t.Fatalf("remove initial auth file: %v", errRemove)
	}
	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})

	if errWrite := os.WriteFile(authFile, content, 0o600); errWrite != nil {
		t.Fatalf("recreate auth file: %v", errWrite)
	}
	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Create})

	normalizedPath := w.normalizeAuthPath(authFile)
	w.clientsMutex.RLock()
	_, debouncePresent := w.lastRemoveTimes[normalizedPath]
	_, recreatedKnown := w.lastAuthHashes[normalizedPath]
	w.clientsMutex.RUnlock()
	if debouncePresent {
		t.Fatal("create event did not clear the previous remove debounce record")
	}
	if !recreatedKnown {
		t.Fatal("recreated auth file was not loaded")
	}

	if errRemove := os.Remove(authFile); errRemove != nil {
		t.Fatalf("remove recreated auth file: %v", errRemove)
	}
	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if w.isKnownAuthFile(authFile) {
		t.Fatal("second remove was incorrectly debounced after file recreation")
	}
}

func TestHandleEventAtomicReplaceUnchangedSkips(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "same.json")
	content := []byte(`{"type":"demo"}`)
	if err := os.WriteFile(authFile, content, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	sum := sha256.Sum256(content)

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(sum[:])

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Rename})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected unchanged atomic replace to be skipped, got %d", reloads)
	}
}

func TestHandleEventAtomicReplaceChangedTriggersUpdate(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "change.json")
	oldContent := []byte(`{"type":"demo","v":1}`)
	newContent := []byte(`{"type":"demo","v":2}`)
	if err := os.WriteFile(authFile, newContent, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	oldSum := sha256.Sum256(oldContent)

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = hexString(oldSum[:])

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Rename})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected changed atomic replace to avoid global reload, got %d", reloads)
	}
}

func TestHandleEventRemoveUnknownFileIgnored(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "unknown.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected unknown remove to be ignored, got %d", reloads)
	}
}

func TestHandleEventRemoveKnownFileDeletes(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authFile := filepath.Join(authDir, "known.json")

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		configPath:     configPath,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})
	w.lastAuthHashes[w.normalizeAuthPath(authFile)] = "hash"

	w.handleEvent(fsnotify.Event{Name: authFile, Op: fsnotify.Remove})
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected known remove to avoid global reload, got %d", reloads)
	}
	if _, ok := w.lastAuthHashes[w.normalizeAuthPath(authFile)]; ok {
		t.Fatal("expected known auth hash to be deleted")
	}
}

func TestNormalizeAuthPathAndDebounceCleanup(t *testing.T) {
	w := &Watcher{}
	if got := w.normalizeAuthPath("   "); got != "" {
		t.Fatalf("expected empty normalize result, got %q", got)
	}
	if got := w.normalizeAuthPath("  a/../b  "); got != filepath.Clean("a/../b") {
		t.Fatalf("unexpected normalize result: %q", got)
	}

	w.clientsMutex.Lock()
	w.lastRemoveTimes = make(map[string]time.Time, 140)
	old := time.Now().Add(-3 * authRemoveDebounceWindow)
	for i := 0; i < 129; i++ {
		w.lastRemoveTimes[fmt.Sprintf("old-%d", i)] = old
	}
	w.clientsMutex.Unlock()

	w.shouldDebounceRemove("new-path", time.Now())

	w.clientsMutex.Lock()
	gotLen := len(w.lastRemoveTimes)
	w.clientsMutex.Unlock()
	if gotLen >= 129 {
		t.Fatalf("expected debounce cleanup to shrink map, got %d", gotLen)
	}
}

func TestRefreshAuthStateDispatchesRuntimeAuths(t *testing.T) {
	queue := make(chan AuthUpdate, 8)
	w := &Watcher{
		authDir:        t.TempDir(),
		lastAuthHashes: make(map[string]string),
	}
	w.SetConfig(&config.Config{AuthDir: w.authDir})
	w.SetAuthUpdateQueue(queue)
	defer w.stopDispatch()

	w.clientsMutex.Lock()
	w.runtimeAuths = map[string]*coreauth.Auth{
		"nil":    nil,
		"r1":     {ID: "r1", Provider: "runtime"},
		"legacy": {ID: "legacy", Provider: "gemini-cli"},
	}
	w.clientsMutex.Unlock()

	w.refreshAuthState(false)

	select {
	case u := <-queue:
		if u.Action != AuthUpdateActionAdd || u.ID != "r1" {
			t.Fatalf("unexpected auth update: %+v", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for runtime auth update")
	}
	w.clientsMutex.RLock()
	_, retained := w.runtimeAuths["legacy"]
	w.clientsMutex.RUnlock()
	if retained {
		t.Fatal("refresh retained a retired Gemini CLI runtime auth")
	}
}

func TestSeedCurrentFileAuthsAllowsFirstSnapshotToDeleteMissingAuth(t *testing.T) {
	authDir := t.TempDir()
	fileAuth := &coreauth.Auth{
		ID:       "removed.json",
		Provider: "codex",
		FileName: "removed.json",
	}
	runtimeAuth := &coreauth.Auth{
		ID:       "runtime",
		Provider: "codex",
		Attributes: map[string]string{
			"runtime_only": "true",
			"path":         filepath.Join(authDir, "removed.json"),
		},
	}
	w := &Watcher{
		authDir:   authDir,
		authQueue: make(chan AuthUpdate),
	}
	w.SeedCurrentFileAuths([]*coreauth.Auth{fileAuth, runtimeAuth})

	w.clientsMutex.Lock()
	updates := w.prepareAuthUpdatesLocked(nil, false)
	w.clientsMutex.Unlock()
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionDelete || updates[0].ID != fileAuth.ID {
		t.Fatalf("initial reconciliation updates = %+v, want delete for %s", updates, fileAuth.ID)
	}
}

func TestSeedCurrentFileAuthsResolvesSymlinkedAuthDirectory(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errLink := os.Symlink(realDir, linkDir); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	fileAuth := &coreauth.Auth{
		ID:       "removed.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(realDir, "removed.json"),
		},
	}
	w := &Watcher{authDir: linkDir, authQueue: make(chan AuthUpdate)}
	w.SeedCurrentFileAuths([]*coreauth.Auth{fileAuth})
	w.clientsMutex.Lock()
	updates := w.prepareAuthUpdatesLocked(nil, false)
	w.clientsMutex.Unlock()
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionDelete || updates[0].ID != fileAuth.ID {
		t.Fatalf("initial reconciliation updates = %+v, want delete for %s", updates, fileAuth.ID)
	}
}

func TestAuthUsesRetiredPathResolvesSymlinkAlias(t *testing.T) {
	realDir := t.TempDir()
	linkDir := filepath.Join(t.TempDir(), "auths")
	if errLink := os.Symlink(realDir, linkDir); errLink != nil {
		t.Skipf("symlink is unavailable: %v", errLink)
	}
	w := &Watcher{
		authDir:          linkDir,
		retiredAuthPaths: make(map[string]struct{}),
	}
	w.retiredAuthPaths[w.normalizeAuthPath(filepath.Join(realDir, "retired.json"))] = struct{}{}
	auth := &coreauth.Auth{
		ID:       "retired.json",
		Provider: "codex",
		Attributes: map[string]string{
			"path": filepath.Join(linkDir, "retired.json"),
		},
	}
	if !w.authUsesRetiredPathLocked(auth) {
		t.Fatal("retired physical path did not match symlinked auth path")
	}
}

func TestAddOrUpdateClientEdgeCases(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := tmpDir
	authFile := filepath.Join(tmpDir, "edge.json")
	if err := os.WriteFile(authFile, []byte(`{"type":"demo"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}
	emptyFile := filepath.Join(tmpDir, "empty.json")
	if err := os.WriteFile(emptyFile, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty auth file: %v", err)
	}

	var reloads int32
	w := &Watcher{
		authDir:        authDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}

	w.addOrUpdateClient(filepath.Join(tmpDir, "missing.json"))
	w.addOrUpdateClient(emptyFile)
	if atomic.LoadInt32(&reloads) != 0 {
		t.Fatalf("expected no reloads for missing/empty file, got %d", reloads)
	}

	w.addOrUpdateClient(authFile) // config nil -> should not panic or update
	if len(w.lastAuthHashes) != 0 {
		t.Fatalf("expected no hash entries without config, got %d", len(w.lastAuthHashes))
	}
}

func TestLoadFileClientsWalkError(t *testing.T) {
	tmpDir := t.TempDir()
	noAccessDir := filepath.Join(tmpDir, "0noaccess")
	if err := os.MkdirAll(noAccessDir, 0o755); err != nil {
		t.Fatalf("failed to create noaccess dir: %v", err)
	}
	if err := os.Chmod(noAccessDir, 0); err != nil {
		t.Skipf("chmod not supported: %v", err)
	}
	defer func() { _ = os.Chmod(noAccessDir, 0o755) }()

	cfg := &config.Config{AuthDir: tmpDir}
	w := &Watcher{}
	w.SetConfig(cfg)

	count := w.loadFileClients(cfg)
	if count != 0 {
		t.Fatalf("expected count 0 due to walk error, got %d", count)
	}
}

func TestReloadConfigIfChangedHandlesMissingAndEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	w := &Watcher{
		configPath: filepath.Join(tmpDir, "missing.yaml"),
		authDir:    authDir,
	}
	w.reloadConfigIfChanged() // missing file -> log + return

	emptyPath := filepath.Join(tmpDir, "empty.yaml")
	if err := os.WriteFile(emptyPath, []byte(""), 0o644); err != nil {
		t.Fatalf("failed to write empty config: %v", err)
	}
	w.configPath = emptyPath
	w.reloadConfigIfChanged() // empty file -> early return
}

func TestReloadConfigUsesMirroredAuthDir(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}

	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+filepath.Join(tmpDir, "other")+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	w := &Watcher{
		configPath:      configPath,
		authDir:         authDir,
		mirroredAuthDir: authDir,
		lastAuthHashes:  make(map[string]string),
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.AuthDir != authDir {
		t.Fatalf("expected AuthDir to be overridden by mirroredAuthDir %s, got %+v", authDir, w.config)
	}
}

func TestReloadConfigFiltersAffectedOAuthProviders(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")

	// Ensure SnapshotCoreAuths yields a provider that is NOT affected, so we can assert it survives.
	if err := os.WriteFile(filepath.Join(authDir, "provider-b.json"), []byte(`{"type":"provider-b","email":"b@example.com"}`), 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	oldCfg := &config.Config{
		AuthDir: authDir,
		OAuthExcludedModels: map[string][]string{
			"provider-a": {"m1"},
		},
	}
	newCfg := &config.Config{
		AuthDir: authDir,
		OAuthExcludedModels: map[string][]string{
			"provider-a": {"m2"},
		},
	}
	data, err := yaml.Marshal(newCfg)
	if err != nil {
		t.Fatalf("failed to marshal config: %v", err)
	}
	if err = os.WriteFile(configPath, data, 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		lastAuthHashes: make(map[string]string),
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "provider-a"},
		},
	}
	w.SetConfig(oldCfg)

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	for _, auth := range w.currentAuths {
		if auth != nil && auth.Provider == "provider-a" {
			t.Fatal("expected affected provider auth to be filtered")
		}
	}
	foundB := false
	for _, auth := range w.currentAuths {
		if auth != nil && auth.Provider == "provider-b" {
			foundB = true
			break
		}
	}
	if !foundB {
		t.Fatal("expected unaffected provider auth to remain")
	}
}

func TestReloadConfigTriggersCallbackForMaxRetryCredentialsChange(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")

	oldCfg := &config.Config{
		AuthDir:             authDir,
		MaxRetryCredentials: 0,
		RequestRetry:        1,
		MaxRetryInterval:    5,
	}
	newCfg := &config.Config{
		AuthDir:             authDir,
		MaxRetryCredentials: 2,
		RequestRetry:        1,
		MaxRetryInterval:    5,
	}
	data, errMarshal := yaml.Marshal(newCfg)
	if errMarshal != nil {
		t.Fatalf("failed to marshal config: %v", errMarshal)
	}
	if errWrite := os.WriteFile(configPath, data, 0o644); errWrite != nil {
		t.Fatalf("failed to write config: %v", errWrite)
	}

	callbackCalls := 0
	callbackMaxRetryCredentials := -1
	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		lastAuthHashes: make(map[string]string),
		reloadCallback: func(cfg *config.Config) {
			callbackCalls++
			if cfg != nil {
				callbackMaxRetryCredentials = cfg.MaxRetryCredentials
			}
		},
	}
	w.SetConfig(oldCfg)

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}

	if callbackCalls != 1 {
		t.Fatalf("expected reload callback to be called once, got %d", callbackCalls)
	}
	if callbackMaxRetryCredentials != 2 {
		t.Fatalf("expected callback MaxRetryCredentials=2, got %d", callbackMaxRetryCredentials)
	}

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if w.config == nil || w.config.MaxRetryCredentials != 2 {
		t.Fatalf("expected watcher config MaxRetryCredentials=2, got %+v", w.config)
	}
}

func TestReloadConfigForcesAuthRefreshForAuthModelExclusionsChange(t *testing.T) {
	tmpDir := t.TempDir()
	authDir := filepath.Join(tmpDir, "auth")
	if err := os.MkdirAll(authDir, 0o755); err != nil {
		t.Fatalf("failed to create auth dir: %v", err)
	}
	configPath := filepath.Join(tmpDir, "config.yaml")

	oldCfg := &config.Config{AuthDir: authDir}
	newCfg := &config.Config{
		AuthDir: authDir,
		AuthModelExclusions: []config.AuthModelExclusionRule{
			{Models: []string{"gpt-image-2"}, Priorities: []int{-1}},
		},
	}
	data, errMarshal := yaml.Marshal(newCfg)
	if errMarshal != nil {
		t.Fatalf("failed to marshal config: %v", errMarshal)
	}
	if errWrite := os.WriteFile(configPath, data, 0o644); errWrite != nil {
		t.Fatalf("failed to write config: %v", errWrite)
	}

	origSnapshot := snapshotCoreAuthsFunc
	snapshotCoreAuthsFunc = func(*config.Config, string) []*coreauth.Auth {
		return []*coreauth.Auth{{ID: "auth-1", Provider: "codex"}}
	}
	defer func() { snapshotCoreAuthsFunc = origSnapshot }()

	w := &Watcher{
		configPath:     configPath,
		authDir:        authDir,
		authQueue:      make(chan AuthUpdate, 4),
		lastAuthHashes: make(map[string]string),
	}
	w.SetConfig(oldCfg)
	w.clientsMutex.Lock()
	w.currentAuths = map[string]*coreauth.Auth{
		"auth-1": {ID: "auth-1", Provider: "codex"},
	}
	w.clientsMutex.Unlock()

	if ok := w.reloadConfig(); !ok {
		t.Fatal("expected reloadConfig to succeed")
	}

	w.dispatchMu.Lock()
	defer w.dispatchMu.Unlock()
	if len(w.pendingUpdates) != 1 {
		t.Fatalf("pending updates = %+v, want one forced modify", w.pendingUpdates)
	}
	for _, update := range w.pendingUpdates {
		if update.Action != AuthUpdateActionModify || update.ID != "auth-1" {
			t.Fatalf("update = %+v, want forced modify for auth-1", update)
		}
	}
}

func TestStartFailsWhenAuthDirMissing(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("auth_dir: "+filepath.Join(tmpDir, "missing-auth")+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config file: %v", err)
	}
	authDir := filepath.Join(tmpDir, "missing-auth")

	w, err := NewWatcher(configPath, authDir, nil)
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Stop()
	w.SetConfig(&config.Config{AuthDir: authDir})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := w.Start(ctx); err == nil {
		t.Fatal("expected Start to fail for missing auth dir")
	}
	if errMkdir := os.MkdirAll(authDir, 0o755); errMkdir != nil {
		t.Fatalf("create auth dir after failed Start(): %v", errMkdir)
	}
	if errStart := w.Start(ctx); errStart != nil {
		t.Fatalf("Start() after repairing auth dir error = %v", errStart)
	}
}

func TestDispatchRuntimeAuthUpdateReturnsFalseWithoutQueue(t *testing.T) {
	w := &Watcher{}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionAdd, Auth: &coreauth.Auth{ID: "a"}}); ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate to return false when no queue configured")
	}
	if ok := w.DispatchRuntimeAuthUpdate(AuthUpdate{Action: AuthUpdateActionDelete, Auth: &coreauth.Auth{ID: "a"}}); ok {
		t.Fatal("expected DispatchRuntimeAuthUpdate delete to return false when no queue configured")
	}
}

func TestNormalizeAuthNil(t *testing.T) {
	if normalizeAuth(nil) != nil {
		t.Fatal("expected normalizeAuth(nil) to return nil")
	}
}

// stubStore implements coreauth.Store plus watcher-specific persistence helpers.
type stubStore struct {
	mu              sync.Mutex
	authDir         string
	cfgPersisted    int32
	authPersisted   int32
	authDeleted     int32
	lastAuthMessage string
	lastAuthPaths   []string
}

func (s *stubStore) List(context.Context) ([]*coreauth.Auth, error) { return nil, nil }
func (s *stubStore) Save(context.Context, *coreauth.Auth) (string, error) {
	return "", nil
}
func (s *stubStore) Delete(context.Context, string) error {
	atomic.AddInt32(&s.authDeleted, 1)
	return nil
}
func (s *stubStore) PersistConfig(context.Context) error {
	atomic.AddInt32(&s.cfgPersisted, 1)
	return nil
}
func (s *stubStore) PersistAuthFiles(_ context.Context, message string, paths ...string) error {
	s.mu.Lock()
	s.lastAuthMessage = message
	s.lastAuthPaths = append([]string(nil), paths...)
	s.mu.Unlock()
	atomic.AddInt32(&s.authPersisted, 1)
	return nil
}
func (s *stubStore) AuthDir() string { return s.authDir }

func (s *stubStore) authPersistenceSnapshot() (string, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastAuthMessage, append([]string(nil), s.lastAuthPaths...)
}

type controlledAuthPersister struct {
	started     chan struct{}
	result      chan error
	releaseOnce sync.Once
	calls       int32
	hasDeadline atomic.Bool
	hasDone     atomic.Bool
	resultMu    sync.Mutex
	releasedErr error
}

func newControlledAuthPersister(t *testing.T) *controlledAuthPersister {
	t.Helper()
	p := &controlledAuthPersister{
		started: make(chan struct{}),
		result:  make(chan error, 1),
	}
	t.Cleanup(func() {
		p.release(errors.New("test cleanup"))
	})
	return p
}

func (p *controlledAuthPersister) PersistConfig(context.Context) error { return nil }

func (p *controlledAuthPersister) PersistAuthFiles(ctx context.Context, _ string, _ ...string) error {
	return nil
}

func (p *controlledAuthPersister) FinalizeAuthFileDeletion(ctx context.Context, _ string) error {
	if _, ok := ctx.Deadline(); ok {
		p.hasDeadline.Store(true)
	}
	if ctx.Done() != nil {
		p.hasDone.Store(true)
	}
	if atomic.AddInt32(&p.calls, 1) != 1 {
		p.resultMu.Lock()
		errPersist := p.releasedErr
		p.resultMu.Unlock()
		return errPersist
	}
	close(p.started)
	select {
	case errPersist := <-p.result:
		return errPersist
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *controlledAuthPersister) release(errPersist error) {
	p.releaseOnce.Do(func() {
		p.resultMu.Lock()
		p.releasedErr = errPersist
		p.resultMu.Unlock()
		p.result <- errPersist
	})
}

type controlledPersistCall struct {
	message             string
	paths               []string
	payload             []byte
	expectedDeleteHash  string
	expectedPersistHash string
	deleteGeneration    *authfileguard.DeleteGeneration
	deleteAttempt       int
	identityBinding     bool
	result              chan error
	once                sync.Once
}

func (c *controlledPersistCall) complete(errPersist error) {
	c.once.Do(func() {
		c.result <- errPersist
	})
}

type controlledFilePersister struct {
	calls     chan *controlledPersistCall
	mu        sync.Mutex
	allCalls  []*controlledPersistCall
	callCount int32
}

type contextBlockingFilePersister struct {
	started  chan struct{}
	finished chan struct{}
}

type nonContextBlockingFilePersister struct {
	started chan struct{}
	release chan struct{}
}

func (*contextBlockingFilePersister) PersistConfig(context.Context) error { return nil }

func (p *contextBlockingFilePersister) PersistAuthFiles(ctx context.Context, _ string, _ ...string) error {
	close(p.started)
	<-ctx.Done()
	close(p.finished)
	return ctx.Err()
}

func (*nonContextBlockingFilePersister) PersistConfig(context.Context) error { return nil }

func (p *nonContextBlockingFilePersister) PersistAuthFiles(context.Context, string, ...string) error {
	close(p.started)
	<-p.release
	return nil
}

func newControlledFilePersister(t *testing.T) *controlledFilePersister {
	t.Helper()
	p := &controlledFilePersister{calls: make(chan *controlledPersistCall, 8)}
	t.Cleanup(func() {
		p.mu.Lock()
		calls := append([]*controlledPersistCall(nil), p.allCalls...)
		p.mu.Unlock()
		for _, call := range calls {
			call.complete(errors.New("test cleanup"))
		}
	})
	return p
}

func TestWatcherStopCancelsAndWaitsForAuthPersistence(t *testing.T) {
	authDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	w, errWatcher := NewWatcher(configPath, authDir, nil)
	if errWatcher != nil {
		t.Fatalf("NewWatcher() error = %v", errWatcher)
	}
	persister := &contextBlockingFilePersister{
		started:  make(chan struct{}),
		finished: make(chan struct{}),
	}
	w.storePersister = persister
	if !w.persistAuthAsyncWithCompletion("test", nil, filepath.Join(authDir, "auth.json")) {
		t.Fatal("persistAuthAsyncWithCompletion() = false")
	}
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("auth persistence did not start")
	}
	if errStop := w.Stop(); errStop != nil {
		t.Fatalf("Stop() error = %v", errStop)
	}
	select {
	case <-persister.finished:
	default:
		t.Fatal("Stop() returned before auth persistence exited")
	}
}

func TestWatcherStopDoesNotWaitForeverForNonCooperativePersister(t *testing.T) {
	authDir := t.TempDir()
	configPath := filepath.Join(t.TempDir(), "config.yaml")
	w, errWatcher := NewWatcher(configPath, authDir, nil)
	if errWatcher != nil {
		t.Fatalf("NewWatcher() error = %v", errWatcher)
	}
	persister := &nonContextBlockingFilePersister{started: make(chan struct{}), release: make(chan struct{})}
	w.storePersister = persister
	previousWait := authPersistenceShutdownWait
	authPersistenceShutdownWait = 25 * time.Millisecond
	defer func() { authPersistenceShutdownWait = previousWait }()
	if !w.persistAuthAsyncWithCompletion("test", nil, filepath.Join(authDir, "auth.json")) {
		t.Fatal("persistAuthAsyncWithCompletion() = false")
	}
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("auth persistence did not start")
	}
	started := time.Now()
	if errStop := w.Stop(); errStop != nil {
		t.Fatalf("Stop() error = %v", errStop)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Stop() waited %s for non-cooperative persistence", elapsed)
	}
	close(persister.release)
	w.authWorkWG.Wait()
}

func (p *controlledFilePersister) PersistConfig(context.Context) error { return nil }

func (p *controlledFilePersister) PersistAuthFiles(ctx context.Context, message string, paths ...string) error {
	call := &controlledPersistCall{
		message:             message,
		paths:               append([]string(nil), paths...),
		expectedDeleteHash:  authfileguard.ExpectedDeleteHash(ctx),
		expectedPersistHash: authfileguard.ExpectedPersistHash(ctx),
		deleteGeneration:    authfileguard.DeleteGenerationFromContext(ctx),
		deleteAttempt:       authfileguard.DeleteAttempt(ctx),
		identityBinding:     authfileguard.DeleteIdentityBindingAllowed(ctx),
		result:              make(chan error, 1),
	}
	if len(paths) > 0 {
		var errRead error
		call.payload, errRead = os.ReadFile(paths[0])
		if errValidate := authfileguard.ValidatePersistSnapshot(ctx, call.payload, errRead == nil); errValidate != nil {
			return errValidate
		}
	}
	p.mu.Lock()
	p.allCalls = append(p.allCalls, call)
	p.mu.Unlock()
	atomic.AddInt32(&p.callCount, 1)
	select {
	case p.calls <- call:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case errPersist := <-call.result:
		return errPersist
	case <-ctx.Done():
		return ctx.Err()
	}
}

func nextControlledPersistCall(t *testing.T, p *controlledFilePersister) *controlledPersistCall {
	t.Helper()
	select {
	case call := <-p.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("PersistAuthFiles was not started")
		return nil
	}
}

func failControlledPersistRetries(t *testing.T, p *controlledFilePersister, errPersist error) {
	t.Helper()
	for range 3 {
		nextControlledPersistCall(t, p).complete(errPersist)
	}
}

func newFileAdmissionTestWatcher(authDir string, persister storePersister) *Watcher {
	return &Watcher{
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		storePersister:   persister,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
		retiredAuthPaths: make(map[string]struct{}),
		authRetryBase:    time.Millisecond,
	}
}

func watcherRuntimeAccessToken(w *Watcher) (string, int) {
	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	for _, auth := range w.currentAuths {
		if auth == nil || auth.Metadata == nil {
			continue
		}
		token, _ := auth.Metadata["access_token"].(string)
		return token, len(w.currentAuths)
	}
	return "", len(w.currentAuths)
}

func newRetiredDeletionTestWatcher(t *testing.T, persister storePersister) (*Watcher, string) {
	t.Helper()
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy-gemini.json")
	w := &Watcher{
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		storePersister:   persister,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
		retiredAuthPaths: make(map[string]struct{}),
		authRetryBase:    time.Millisecond,
	}
	t.Cleanup(func() {
		w.stopped.Store(true)
		w.stopAuthPersistenceRetryTimers()
		w.stopAuthPersistenceTasks()
	})
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	if !authfileguard.IsRetired(path) {
		t.Fatal("retired auth path was not marked")
	}
	t.Cleanup(func() {
		authfileguard.ClearRetired(path)
	})
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove retired auth: %v", errRemove)
	}
	return w, path
}

func startRetiredDeletion(t *testing.T, w *Watcher, path string, persister *controlledAuthPersister) {
	t.Helper()
	returned := make(chan struct{})
	go func() {
		w.removeClientState(path, true)
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("removeClientState blocked on remote persistence")
	}
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("PersistAuthFiles was not started")
	}
}

func waitForWatcherCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func TestNewWatcherDetectsPersisterAndAuthDir(t *testing.T) {
	tmp := t.TempDir()
	store := &stubStore{authDir: tmp}
	orig := sdkAuth.GetTokenStore()
	sdkAuth.RegisterTokenStore(store)
	defer sdkAuth.RegisterTokenStore(orig)

	w, err := NewWatcher("config.yaml", "auth", nil)
	if err != nil {
		t.Fatalf("NewWatcher failed: %v", err)
	}
	if w.storePersister == nil {
		t.Fatal("expected storePersister to be set from token store")
	}
	if w.mirroredAuthDir != tmp {
		t.Fatalf("expected mirroredAuthDir %s, got %s", tmp, w.mirroredAuthDir)
	}
}

func TestPersistConfigAndAuthAsyncInvokePersister(t *testing.T) {
	w := &Watcher{
		storePersister: &stubStore{},
	}

	w.persistConfigAsync()
	w.persistAuthAsync("msg", " a ", "", "b ")

	time.Sleep(30 * time.Millisecond)
	store := w.storePersister.(*stubStore)
	if got := atomic.LoadInt32(&store.cfgPersisted); got != 1 {
		t.Fatalf("expected PersistConfig to be called once, got %d", got)
	}
	if got := atomic.LoadInt32(&store.authPersisted); got != 1 {
		t.Fatalf("expected PersistAuthFiles to be called once, got %d", got)
	}
	lastAuthMessage, lastAuthPaths := store.authPersistenceSnapshot()
	if lastAuthMessage != "msg" {
		t.Fatalf("unexpected auth message: %s", lastAuthMessage)
	}
	if len(lastAuthPaths) != 2 || lastAuthPaths[0] != "a" || lastAuthPaths[1] != "b" {
		t.Fatalf("unexpected filtered paths: %#v", lastAuthPaths)
	}
}

func TestFileAuthAdmissionWaitsForPersistenceSuccess(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"persisted"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)

	w.addOrUpdateClient(path)
	call := nextControlledPersistCall(t, persister)
	if string(call.payload) != string(data) {
		t.Fatalf("persisted payload = %s, want %s", call.payload, data)
	}

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, quarantined := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	pathAuthCount := len(w.fileAuthsByPath[normalized])
	w.clientsMutex.RUnlock()
	if !quarantined || !pending {
		t.Fatal("auth path was not quarantined while persistence was pending")
	}
	if runtimeCount != 0 || pathAuthCount != 0 {
		t.Fatalf("auth admitted before persistence: runtime=%d path=%d", runtimeCount, pathAuthCount)
	}

	call.complete(nil)
	waitForWatcherCondition(t, "persisted auth admission", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "persisted"
	})
	w.clientsMutex.RLock()
	_, quarantined = w.retiredAuthPaths[normalized]
	_, pending = w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if quarantined || pending {
		t.Fatal("successful persistence left the auth path quarantined")
	}
}

func TestFileAuthPersistenceCompletionRejectsSameContentReplacement(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"persisted"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)

	w.addOrUpdateClient(path)
	firstCall := nextControlledPersistCall(t, persister)
	replacement := filepath.Join(authDir, "replacement.json")
	if errWrite := os.WriteFile(replacement, data, 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	if errRename := os.Rename(replacement, path); errRename != nil {
		t.Fatalf("replace auth: %v", errRename)
	}
	firstCall.complete(nil)

	secondCall := nextControlledPersistCall(t, persister)
	w.clientsMutex.RLock()
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if runtimeCount != 0 {
		t.Fatalf("same-content replacement admitted by stale persistence: %d auths", runtimeCount)
	}
	if string(secondCall.payload) != string(data) {
		t.Fatalf("replacement payload = %s, want %s", secondCall.payload, data)
	}
	secondCall.complete(nil)
	waitForWatcherCondition(t, "same-content replacement persistence", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "persisted"
	})
}

func TestFileAuthPersistenceFailureKeepsPathQuarantined(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"blocked"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)

	w.addOrUpdateClient(path)
	call := nextControlledPersistCall(t, persister)
	errPersist := fmt.Errorf("mirror rejected auth: %w", coreauth.ErrRetiredGeminiCLIAuthReadOnly)
	call.complete(errPersist)
	failControlledPersistRetries(t, persister, errPersist)

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, quarantined := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	pathAuthCount := len(w.fileAuthsByPath[normalized])
	w.clientsMutex.RUnlock()
	if !quarantined || !pending {
		t.Fatal("failed persistence cleared the auth path quarantine or retry")
	}
	if runtimeCount != 0 || pathAuthCount != 0 {
		t.Fatalf("failed persistence admitted auth: runtime=%d path=%d", runtimeCount, pathAuthCount)
	}
	if unchanged, errSame := w.authFileUnchanged(path); errSame != nil || !unchanged {
		t.Fatalf("quarantined file unchanged check = (%t, %v), want active retry", unchanged, errSame)
	}
	w.stopped.Store(true)
}

func TestFileAuthPersistenceRetriesTransientFailure(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"persisted"}`), 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)

	w.addOrUpdateClient(path)
	nextControlledPersistCall(t, persister).complete(errors.New("temporary backend failure"))
	retry := nextControlledPersistCall(t, persister)
	if !strings.Contains(retry.message, "Retry auth") {
		t.Fatalf("retry message = %q", retry.message)
	}
	retry.complete(nil)

	waitForWatcherCondition(t, "auth admission after persistence retry", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "persisted"
	})
	if got := atomic.LoadInt32(&persister.callCount); got != 2 {
		t.Fatalf("PersistAuthFiles calls = %d, want 2", got)
	}
}

func TestAuthRemovalRetriesKeepExpectedDeleteHash(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"removed"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	t.Cleanup(func() {
		w.stopped.Store(true)
		w.stopAuthPersistenceRetryTimers()
		w.stopAuthPersistenceTasks()
	})
	normalized := w.normalizeAuthPath(path)
	w.lastAuthHashes[normalized] = coreauth.SourceHashFromBytes(data)
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove auth: %v", errRemove)
	}

	w.removeClientState(path, true)
	first := nextControlledPersistCall(t, persister)
	if first.expectedDeleteHash != coreauth.SourceHashFromBytes(data) {
		t.Fatalf("first expected delete hash = %q", first.expectedDeleteHash)
	}
	first.complete(errors.New("temporary conflict"))
	retry := nextControlledPersistCall(t, persister)
	if retry.expectedDeleteHash != first.expectedDeleteHash {
		t.Fatalf("retry expected delete hash = %q, want %q", retry.expectedDeleteHash, first.expectedDeleteHash)
	}
	if retry.deleteGeneration == nil || retry.deleteGeneration != first.deleteGeneration {
		t.Fatal("retry did not preserve the deletion generation instance")
	}
	retry.complete(nil)
	waitForWatcherCondition(t, "auth removal completion", func() bool {
		w.clientsMutex.RLock()
		_, pending := w.retiredDeletes[normalized]
		w.clientsMutex.RUnlock()
		return !pending
	})
}

func TestAuthRemovalUncertainGenerationStaysQuarantinedWithoutRetry(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"removed"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	t.Cleanup(func() { w.stopped.Store(true) })
	normalized := w.normalizeAuthPath(path)
	w.lastAuthHashes[normalized] = coreauth.SourceHashFromBytes(data)
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove auth: %v", errRemove)
	}

	w.removeClientState(path, true)
	metadataRoot, authRoot, _, errContext := w.authDeleteTombstoneContext()
	if errContext != nil {
		t.Fatalf("authDeleteTombstoneContext() error = %v", errContext)
	}
	tombstonePath := filepath.Join(metadataRoot, authDeleteTombstoneDirectory, authDeleteTombstoneName(authRoot, "codex.json"))
	call := nextControlledPersistCall(t, persister)
	if _, errStat := os.Stat(tombstonePath); errStat != nil {
		t.Fatalf("deletion started before tombstone was durable: %v", errStat)
	}
	call.complete(authfileguard.ErrDeleteGenerationUncertain)
	time.Sleep(5 * w.authRetryBase)

	w.clientsMutex.RLock()
	_, quarantined := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if !quarantined || !pending {
		t.Fatal("uncertain deletion cleared path quarantine")
	}
	waitForWatcherCondition(t, "uncertain deletion tombstone", func() bool {
		_, errStat := os.Stat(tombstonePath)
		return errStat == nil
	})
	select {
	case retry := <-persister.calls:
		t.Fatalf("uncertain deletion scheduled retry: %q", retry.message)
	default:
	}
}

func TestFileAuthPersistenceCompletionSurvivesHashCacheRebuild(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	data := []byte(`{"type":"codex","access_token":"persisted"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)
	w.addOrUpdateClient(path)
	call := nextControlledPersistCall(t, persister)
	w.clientsMutex.Lock()
	w.lastAuthHashes = make(map[string]string)
	w.clientsMutex.Unlock()
	call.complete(nil)
	waitForWatcherCondition(t, "admission after hash cache rebuild", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "persisted"
	})
}

func TestFileAuthPersistenceCompletionIsGenerationSafe(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "codex.json")
	firstData := []byte(`{"type":"codex","access_token":"first"}`)
	secondData := []byte(`{"type":"codex","access_token":"second"}`)
	persister := newControlledFilePersister(t)
	w := newFileAdmissionTestWatcher(authDir, persister)

	if errWrite := os.WriteFile(path, firstData, 0o600); errWrite != nil {
		t.Fatalf("write first auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	firstCall := nextControlledPersistCall(t, persister)
	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	firstGeneration := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()

	if errWrite := os.WriteFile(path, secondData, 0o600); errWrite != nil {
		t.Fatalf("write second auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	secondCall := nextControlledPersistCall(t, persister)
	w.clientsMutex.RLock()
	secondGeneration := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if secondGeneration == firstGeneration {
		t.Fatalf("replacement generation = %d, want newer than %d", secondGeneration, firstGeneration)
	}

	firstHash := coreauth.SourceHashFromBytes(firstData)
	w.completeAuthPersistence(path, normalized, firstHash, firstGeneration, nil)
	w.completeAuthPersistence(path, normalized, firstHash, firstGeneration, errors.New("stale failure"))
	w.clientsMutex.RLock()
	currentGeneration := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if currentGeneration != secondGeneration || runtimeCount != 0 {
		t.Fatalf("stale completion changed replacement state: generation=%d runtime=%d", currentGeneration, runtimeCount)
	}

	secondCall.complete(nil)
	waitForWatcherCondition(t, "replacement generation admission", func() bool {
		token, count := watcherRuntimeAccessToken(w)
		return count == 1 && token == "second"
	})
	w.completeAuthPersistence(path, normalized, firstHash, firstGeneration, nil)
	w.completeAuthPersistence(path, normalized, firstHash, firstGeneration, errors.New("late stale failure"))
	if token, count := watcherRuntimeAccessToken(w); count != 1 || token != "second" {
		t.Fatalf("stale completion replaced newer auth: count=%d token=%q", count, token)
	}

	firstCall.complete(errors.New("stale persisted generation"))
}

func TestRetiredGeminiCLIFileChangeIsNotPersisted(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy-gemini.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	store := &stubStore{}
	w := &Watcher{
		authDir:          authDir,
		config:           &config.Config{},
		storePersister:   store,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
	}
	w.addOrUpdateClient(path)
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt32(&store.authPersisted); got != 0 {
		t.Fatalf("PersistAuthFiles calls = %d, want 0", got)
	}
}

func TestRetiredGeminiCLIPathRemainsIgnoredUntilDeleted(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "legacy-gemini.json")
	w := &Watcher{
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
		retiredAuthPaths: make(map[string]struct{}),
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"legacy"}`), 0o600); errWrite != nil {
		t.Fatalf("write retired auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"rewritten"}`), 0o600); errWrite != nil {
		t.Fatalf("rewrite retired auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	if len(w.currentAuths) != 0 || len(w.fileAuthsByPath) != 0 {
		t.Fatalf("rewritten retired path synthesized auths: current=%d files=%d", len(w.currentAuths), len(w.fileAuthsByPath))
	}

	w.removeClientState(path, true)
	w.addOrUpdateClient(path)
	if len(w.currentAuths) != 1 || len(w.fileAuthsByPath) != 1 {
		t.Fatalf("recreated path did not synthesize after delete: current=%d files=%d", len(w.currentAuths), len(w.fileAuthsByPath))
	}
}

func TestRetiredDeletionFailureKeepsReplacementBlocked(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, persister)

	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, retiredWhileBlocked := w.retiredAuthPaths[normalized]
	_, deletePending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !retiredWhileBlocked || !deletePending || !authfileguard.IsRetired(path) {
		t.Fatal("retired marker was cleared before persistence completed")
	}
	if runtimeCount != 0 {
		t.Fatalf("replacement entered runtime while delete was pending: %d auths", runtimeCount)
	}

	persister.release(errors.New("remote delete outcome uncertain"))
	waitForWatcherCondition(t, "retired deletion retries", func() bool {
		return atomic.LoadInt32(&persister.calls) >= 4
	})

	w.clientsMutex.RLock()
	_, retiredAfterFailure := w.retiredAuthPaths[normalized]
	_, deletePending = w.retiredDeletes[normalized]
	runtimeCount = len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !retiredAfterFailure || !deletePending || !authfileguard.IsRetired(path) {
		t.Fatal("failed deletion cleared the retired marker or retry")
	}
	if runtimeCount != 0 {
		t.Fatalf("replacement entered runtime after failed deletion: %d auths", runtimeCount)
	}
	if got := atomic.LoadInt32(&persister.calls); got < 4 {
		t.Fatalf("PersistAuthFiles calls = %d, want at least 4", got)
	}
}

func TestRetiredDeletionUncertainGenerationStaysQuarantinedWithoutRetry(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, persister)
	persister.release(authfileguard.ErrDeleteGenerationUncertain)
	time.Sleep(5 * w.authRetryBase)

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, quarantined := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if !quarantined || !pending || !authfileguard.IsRetired(path) {
		t.Fatal("uncertain retired deletion cleared path quarantine")
	}
	if got := atomic.LoadInt32(&persister.calls); got != 1 {
		t.Fatalf("retired deletion calls = %d, want no retry", got)
	}
}

func TestRetiredDeletionSuccessLoadsBlockedReplacement(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, persister)

	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}
	w.addOrUpdateClient(path)
	persister.release(nil)

	normalized := w.normalizeAuthPath(path)
	waitForWatcherCondition(t, "successful delete completion", func() bool {
		w.clientsMutex.RLock()
		_, retired := w.retiredAuthPaths[normalized]
		_, pending := w.retiredDeletes[normalized]
		runtimeCount := len(w.currentAuths)
		pathAuthCount := len(w.fileAuthsByPath[normalized])
		w.clientsMutex.RUnlock()
		return !retired && !pending && runtimeCount == 1 && pathAuthCount == 1
	})
	if authfileguard.IsRetired(path) {
		t.Fatal("successful deletion did not clear the retired path guard")
	}
}

func TestRetiredDeletionCommittedErrorDoesNotRetry(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, persister)

	persister.release(coreauth.NewDeleteOutcomeError(coreauth.DeleteOutcomeCommitted, errors.New("response lost after delete")))
	normalized := w.normalizeAuthPath(path)
	waitForWatcherCondition(t, "committed delete completion", func() bool {
		w.clientsMutex.RLock()
		_, retired := w.retiredAuthPaths[normalized]
		_, pending := w.retiredDeletes[normalized]
		w.clientsMutex.RUnlock()
		return !retired && !pending
	})
	time.Sleep(5 * time.Millisecond)
	if got := atomic.LoadInt32(&persister.calls); got != 1 {
		t.Fatalf("delete persistence calls = %d, want 1", got)
	}
}

func TestHandleEventRetiredAtomicReplacementFinalizesBeforeLoading(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("retired deletion finalizer was not started")
	}

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !pending {
		t.Fatal("retired deletion generation was not recorded")
	}
	if runtimeCount != 0 {
		t.Fatalf("replacement loaded before retired deletion finalized: %d auths", runtimeCount)
	}

	persister.release(nil)
	waitForWatcherCondition(t, "atomic replacement load", func() bool {
		w.clientsMutex.RLock()
		_, retired := w.retiredAuthPaths[normalized]
		_, deletePending := w.retiredDeletes[normalized]
		runtimeCount := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		return !retired && !deletePending && runtimeCount == 1
	})
	if authfileguard.IsRetired(path) {
		t.Fatal("successful atomic replacement left the path retired")
	}
}

func TestHandleEventRetiredCreateOrWriteReplacementFinalizesBeforeLoading(t *testing.T) {
	for _, operation := range []fsnotify.Op{fsnotify.Create, fsnotify.Write} {
		t.Run(operation.String(), func(t *testing.T) {
			persister := newControlledAuthPersister(t)
			w, path := newRetiredDeletionTestWatcher(t, persister)
			if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
				t.Fatalf("write replacement auth: %v", errWrite)
			}

			w.handleEvent(fsnotify.Event{Name: path, Op: operation})
			select {
			case <-persister.started:
			case <-time.After(time.Second):
				t.Fatal("retired deletion finalizer was not started")
			}

			normalized := w.normalizeAuthPath(path)
			w.clientsMutex.RLock()
			_, pending := w.retiredDeletes[normalized]
			runtimeCount := len(w.currentAuths)
			w.clientsMutex.RUnlock()
			if !pending || runtimeCount != 0 {
				t.Fatalf("replacement state before finalization: pending=%t runtime=%d", pending, runtimeCount)
			}

			persister.release(nil)
			waitForWatcherCondition(t, "create/write replacement load", func() bool {
				w.clientsMutex.RLock()
				_, retired := w.retiredAuthPaths[normalized]
				_, deletePending := w.retiredDeletes[normalized]
				runtimeCount = len(w.currentAuths)
				w.clientsMutex.RUnlock()
				return !retired && !deletePending && runtimeCount == 1
			})
		})
	}
}

func TestHandleEventRetiredReplacementDeduplicatesCreateWriteSequence(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement auth: %v", errWrite)
	}

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Create})
	select {
	case <-persister.started:
	case <-time.After(time.Second):
		t.Fatal("retired deletion finalizer was not started")
	}
	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	if got := atomic.LoadInt32(&persister.calls); got != 1 {
		t.Fatalf("retired deletion calls after create/write = %d, want 1", got)
	}

	persister.release(nil)
	normalized := w.normalizeAuthPath(path)
	waitForWatcherCondition(t, "deduplicated replacement load", func() bool {
		w.clientsMutex.RLock()
		_, retired := w.retiredAuthPaths[normalized]
		_, pending := w.retiredDeletes[normalized]
		runtimeCount := len(w.currentAuths)
		w.clientsMutex.RUnlock()
		return !retired && !pending && runtimeCount == 1
	})
}

func TestHandleEventRetiredWriteDoesNotDeleteRetiredContent(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	if errWrite := os.WriteFile(path, []byte(`{"type":"gemini","access_token":"still-retired"}`), 0o600); errWrite != nil {
		t.Fatalf("rewrite retired auth: %v", errWrite)
	}

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Write})
	select {
	case <-persister.started:
		t.Fatal("retired content started automatic deletion")
	case <-time.After(20 * time.Millisecond):
	}
	if !authfileguard.IsRetired(path) {
		t.Fatal("retired content lost its retired marker")
	}
}

func TestRetiredDeletionLegacyPersisterConfirmsAbsentPath(t *testing.T) {
	persister := newControlledFilePersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)

	w.removeClientState(path, true)
	call := nextControlledPersistCall(t, persister)
	if len(call.paths) != 1 || call.paths[0] != path {
		t.Fatalf("legacy persisted paths = %#v, want [%q]", call.paths, path)
	}
	if len(call.payload) != 0 {
		t.Fatalf("legacy persisted payload = %s, want absent file", call.payload)
	}
	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, retired := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !retired || !pending || !authfileguard.IsRetired(path) {
		t.Fatal("legacy persister cleared retired quarantine before persistence completed")
	}
	if runtimeCount != 0 {
		t.Fatalf("legacy deletion admitted auth before persistence: %d auths", runtimeCount)
	}

	call.complete(nil)
	waitForWatcherCondition(t, "legacy deletion confirmation", func() bool {
		w.clientsMutex.RLock()
		_, retired = w.retiredAuthPaths[normalized]
		_, pending = w.retiredDeletes[normalized]
		w.clientsMutex.RUnlock()
		return !retired && !pending && !authfileguard.IsRetired(path)
	})
	w.clientsMutex.RLock()
	runtimeCount = len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if runtimeCount != 0 {
		t.Fatalf("legacy deletion confirmation admitted auth: %d auths", runtimeCount)
	}
	if got := atomic.LoadInt32(&persister.callCount); got != 1 {
		t.Fatalf("PersistAuthFiles calls = %d, want 1", got)
	}
}

func TestHandleEventRetiredAtomicReplacementStaysBlockedWithLegacyPersister(t *testing.T) {
	persister := newControlledFilePersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement: %v", errWrite)
	}

	w.handleEvent(fsnotify.Event{Name: path, Op: fsnotify.Rename})

	normalized := w.normalizeAuthPath(path)
	time.Sleep(20 * time.Millisecond)
	w.clientsMutex.RLock()
	_, retired := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	pathAuthCount := len(w.fileAuthsByPath[normalized])
	w.clientsMutex.RUnlock()
	if !retired || !pending || !authfileguard.IsRetired(path) {
		t.Fatal("legacy replacement refusal cleared retired quarantine or retry")
	}
	if runtimeCount != 0 || pathAuthCount != 0 {
		t.Fatalf("legacy replacement refusal admitted replacement: runtime=%d path=%d", runtimeCount, pathAuthCount)
	}
	if got := atomic.LoadInt32(&persister.callCount); got != 0 {
		t.Fatalf("PersistAuthFiles calls = %d, want 0", got)
	}
}

func TestRetiredDeletionLegacySuccessPersistsNewReplacement(t *testing.T) {
	persister := newControlledFilePersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)

	w.removeClientState(path, true)
	call := nextControlledPersistCall(t, persister)
	if len(call.payload) != 0 {
		t.Fatalf("legacy persisted payload = %s, want absent file", call.payload)
	}
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex","access_token":"replacement"}`), 0o600); errWrite != nil {
		t.Fatalf("write replacement: %v", errWrite)
	}
	call.complete(nil)
	replacementCall := nextControlledPersistCall(t, persister)
	if string(replacementCall.payload) != `{"type":"codex","access_token":"replacement"}` {
		t.Fatalf("replacement persistence payload = %q", replacementCall.payload)
	}

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, retired := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !retired || !pending || !authfileguard.IsQuarantined(path) || runtimeCount != 0 {
		t.Fatal("replacement was admitted before legacy persistence completed")
	}

	replacementCall.complete(nil)
	waitForWatcherCondition(t, "legacy replacement persistence", func() bool {
		w.clientsMutex.RLock()
		_, retired = w.retiredAuthPaths[normalized]
		_, pending = w.retiredDeletes[normalized]
		_, tombstoned := w.retiredDeleteHashes[normalized]
		runtimeCount = len(w.currentAuths)
		w.clientsMutex.RUnlock()
		return !retired && !pending && !tombstoned && runtimeCount == 1
	})
	if authfileguard.IsRetired(path) || authfileguard.IsQuarantined(path) {
		t.Fatal("replacement remained guarded after persistence")
	}
}

func TestRetiredDeletionLegacyPersisterFailureKeepsPathQuarantined(t *testing.T) {
	persister := newControlledFilePersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)

	w.removeClientState(path, true)
	call := nextControlledPersistCall(t, persister)
	errPersist := errors.New("legacy deletion failed")
	call.complete(errPersist)
	failControlledPersistRetries(t, persister, errPersist)

	normalized := w.normalizeAuthPath(path)
	w.clientsMutex.RLock()
	_, retired := w.retiredAuthPaths[normalized]
	_, pending := w.retiredDeletes[normalized]
	runtimeCount := len(w.currentAuths)
	pathAuthCount := len(w.fileAuthsByPath[normalized])
	w.clientsMutex.RUnlock()
	if !retired || !pending || !authfileguard.IsRetired(path) {
		t.Fatal("legacy persistence failure cleared retired quarantine or retry")
	}
	if runtimeCount != 0 || pathAuthCount != 0 {
		t.Fatalf("legacy persistence failure admitted auth: runtime=%d path=%d", runtimeCount, pathAuthCount)
	}
	if got := atomic.LoadInt32(&persister.callCount); got != 4 {
		t.Fatalf("PersistAuthFiles calls = %d, want 4", got)
	}
	w.stopped.Store(true)
}

func TestRetiredDeletionLegacyCompletionIsGenerationSafe(t *testing.T) {
	persister := newControlledFilePersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	normalized := w.normalizeAuthPath(path)

	w.removeClientState(path, true)
	firstCall := nextControlledPersistCall(t, persister)
	w.clientsMutex.RLock()
	firstGeneration := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()

	w.removeClientState(path, true)
	secondCall := nextControlledPersistCall(t, persister)
	w.clientsMutex.RLock()
	secondGeneration := w.retiredDeletes[normalized]
	w.clientsMutex.RUnlock()
	if secondGeneration <= firstGeneration {
		t.Fatalf("deletion generation = %d, want newer than %d", secondGeneration, firstGeneration)
	}

	w.completeRetiredDelete(path, normalized, firstGeneration, authfileguard.CaptureRetired(path), nil)
	w.clientsMutex.RLock()
	currentGeneration := w.retiredDeletes[normalized]
	_, retired := w.retiredAuthPaths[normalized]
	w.clientsMutex.RUnlock()
	if currentGeneration != secondGeneration || !retired || !authfileguard.IsRetired(path) {
		t.Fatalf("stale completion changed quarantine: generation=%d retired=%t guarded=%t", currentGeneration, retired, authfileguard.IsRetired(path))
	}

	secondCall.complete(nil)
	waitForWatcherCondition(t, "current legacy deletion completion", func() bool {
		w.clientsMutex.RLock()
		_, retired = w.retiredAuthPaths[normalized]
		_, pending := w.retiredDeletes[normalized]
		w.clientsMutex.RUnlock()
		return !retired && !pending && !authfileguard.IsRetired(path)
	})
	firstCall.complete(nil)
	if got := atomic.LoadInt32(&persister.callCount); got != 2 {
		t.Fatalf("PersistAuthFiles calls = %d, want 2", got)
	}
}

func TestRetiredDeletionFinalizerContextCanBeCanceledOnWatcherStop(t *testing.T) {
	persister := newControlledAuthPersister(t)
	w, path := newRetiredDeletionTestWatcher(t, persister)
	startRetiredDeletion(t, w, path, persister)
	if persister.hasDeadline.Load() {
		t.Fatal("retired deletion finalizer received a deadline")
	}
	if !persister.hasDone.Load() {
		t.Fatal("retired deletion finalizer context cannot be canceled")
	}
	persister.release(nil)
}

func TestFilterRetiredPathAuthsBlocksFullRefreshBypass(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.json")
	w := &Watcher{retiredAuthPaths: map[string]struct{}{}, lastAuthHashes: map[string]string{}}
	w.retiredAuthPaths[w.normalizeAuthPath(path)] = struct{}{}
	auth := &coreauth.Auth{ID: "legacy", Provider: "codex", Attributes: map[string]string{"source": path}}
	filtered := w.filterRetiredPathAuthsLocked([]*coreauth.Auth{auth})
	if len(filtered) != 0 {
		t.Fatalf("filtered auths = %#v, want none", filtered)
	}
}

func TestHandleEvent_SymlinkRemovesCachedAuthWithoutMutatingStore(t *testing.T) {
	externalPath := filepath.Join(t.TempDir(), "external.json")
	externalData := []byte(`{"type":"codex"}`)
	if errWrite := os.WriteFile(externalPath, externalData, 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	authDir := t.TempDir()
	aliasPath := filepath.Join(authDir, "alias.json")
	if errSymlink := os.Symlink(externalPath, aliasPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	store := &stubStore{}
	oldAuth := &coreauth.Auth{ID: "cached-auth", Provider: "codex"}
	w := &Watcher{
		authDir:          authDir,
		storePersister:   store,
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     map[string]*coreauth.Auth{oldAuth.ID: oldAuth.Clone()},
	}
	normalized := w.normalizeAuthPath(aliasPath)
	sum := sha256.Sum256(externalData)
	w.lastAuthHashes[normalized] = fmt.Sprintf("%x", sum[:])
	w.lastAuthContents[normalized] = oldAuth.Clone()
	w.fileAuthsByPath[normalized] = map[string]*coreauth.Auth{oldAuth.ID: oldAuth.Clone()}

	w.handleEvent(fsnotify.Event{Name: aliasPath, Op: fsnotify.Write})

	if got := atomic.LoadInt32(&store.authPersisted); got != 0 {
		t.Fatalf("PersistAuthFiles calls = %d, want 0", got)
	}
	if _, ok := w.lastAuthHashes[normalized]; ok {
		t.Fatal("expected symlink hash cache to be removed")
	}
	if _, ok := w.fileAuthsByPath[normalized]; ok {
		t.Fatal("expected symlink file auth cache to be removed")
	}
	if _, ok := w.currentAuths[oldAuth.ID]; ok {
		t.Fatal("expected cached runtime auth to be removed")
	}
	if got := atomic.LoadInt32(&store.authDeleted); got != 0 {
		t.Fatalf("Delete calls = %d, want 0", got)
	}
}

func TestHandleEventSymlinkPreservesDeletionTombstone(t *testing.T) {
	externalPath := filepath.Join(t.TempDir(), "external.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	authDir := t.TempDir()
	aliasPath := filepath.Join(authDir, "alias.json")
	if errSymlink := os.Symlink(externalPath, aliasPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	normalized := filepath.Clean(aliasPath)
	w := &Watcher{
		authDir:             authDir,
		lastAuthHashes:      map[string]string{normalized: "cached"},
		fileAuthsByPath:     map[string]map[string]*coreauth.Auth{normalized: {"cached": {ID: "cached", Provider: "codex"}}},
		currentAuths:        map[string]*coreauth.Auth{"cached": {ID: "cached", Provider: "codex"}},
		retiredAuthPaths:    map[string]struct{}{normalized: {}},
		retiredDeletes:      map[string]uint64{normalized: 7},
		retiredDeleteHashes: map[string]string{normalized: strings.Repeat("a", sha256.Size*2)},
	}

	w.handleEvent(fsnotify.Event{Name: aliasPath, Op: fsnotify.Write})

	w.clientsMutex.RLock()
	_, quarantined := w.retiredAuthPaths[normalized]
	generation := w.retiredDeletes[normalized]
	deleteHash := w.retiredDeleteHashes[normalized]
	runtimeCount := len(w.currentAuths)
	w.clientsMutex.RUnlock()
	if !quarantined || generation != 7 || deleteHash == "" {
		t.Fatalf("symlink event changed quarantine: quarantined=%t generation=%d hash=%q", quarantined, generation, deleteHash)
	}
	if runtimeCount != 0 {
		t.Fatalf("symlink event retained %d runtime auths", runtimeCount)
	}
}

func TestAddOrUpdateClient_SymlinkRaceDoesNotRelockPath(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "auth.json")
	data := []byte(`{"type":"codex","access_token":"cached"}`)
	if errWrite := os.WriteFile(path, data, 0o600); errWrite != nil {
		t.Fatalf("write auth: %v", errWrite)
	}
	w := &Watcher{
		authDir:          authDir,
		config:           &config.Config{AuthDir: authDir},
		lastAuthHashes:   make(map[string]string),
		lastAuthContents: make(map[string]*coreauth.Auth),
		fileAuthsByPath:  make(map[string]map[string]*coreauth.Auth),
		currentAuths:     make(map[string]*coreauth.Auth),
		retiredAuthPaths: make(map[string]struct{}),
	}
	w.addOrUpdateClient(path)
	if len(w.currentAuths) != 1 {
		t.Fatalf("initial runtime auth count = %d, want 1", len(w.currentAuths))
	}

	externalPath := filepath.Join(t.TempDir(), "external.json")
	if errWrite := os.WriteFile(externalPath, data, 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatalf("remove regular auth: %v", errRemove)
	}
	if errSymlink := os.Symlink(externalPath, path); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}

	done := make(chan struct{})
	go func() {
		w.addOrUpdateClient(path)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("addOrUpdateClient deadlocked while removing symlink state")
	}
	if len(w.currentAuths) != 0 {
		t.Fatalf("cached runtime auth count = %d, want 0", len(w.currentAuths))
	}

	lockReleased := make(chan struct{})
	go func() {
		unlockPath := authfileguard.Lock(path)
		unlockPath()
		close(lockReleased)
	}()
	select {
	case <-lockReleased:
	case <-time.After(time.Second):
		t.Fatal("addOrUpdateClient did not release the path lock")
	}
}

func TestLoadFileClients_SymlinkDoesNotMutateMirroredAuth(t *testing.T) {
	externalPath := filepath.Join(t.TempDir(), "external.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	authDir := t.TempDir()
	if errSymlink := os.Symlink(externalPath, filepath.Join(authDir, "alias.json")); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	store := &stubStore{}
	w := &Watcher{storePersister: store}
	if got := w.loadFileClients(&config.Config{AuthDir: authDir}); got != 0 {
		t.Fatalf("auth file count = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&store.authDeleted); got != 0 {
		t.Fatalf("Delete calls = %d, want 0", got)
	}
}

func TestReadAuthFileUnderRootRejectsIntermediateSymlink(t *testing.T) {
	authDir := t.TempDir()
	externalDir := t.TempDir()
	externalPath := filepath.Join(externalDir, "auth.json")
	if errWrite := os.WriteFile(externalPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	linkPath := filepath.Join(authDir, "nested")
	if errSymlink := os.Symlink(externalDir, linkPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}
	if data, errRead := readAuthFileUnderRoot(authDir, filepath.Join(linkPath, "auth.json")); errRead == nil {
		t.Fatalf("readAuthFileUnderRoot() followed intermediate symlink: %s", data)
	}
}

func TestScheduleConfigReloadDebounces(t *testing.T) {
	tmp := t.TempDir()
	authDir := tmp
	cfgPath := tmp + "/config.yaml"
	if err := os.WriteFile(cfgPath, []byte("auth_dir: "+authDir+"\n"), 0o644); err != nil {
		t.Fatalf("failed to write config: %v", err)
	}

	var reloads int32
	w := &Watcher{
		configPath:     cfgPath,
		authDir:        authDir,
		reloadCallback: func(*config.Config) { atomic.AddInt32(&reloads, 1) },
	}
	w.SetConfig(&config.Config{AuthDir: authDir})

	w.scheduleConfigReload()
	w.scheduleConfigReload()

	time.Sleep(400 * time.Millisecond)

	if atomic.LoadInt32(&reloads) != 1 {
		t.Fatalf("expected single debounced reload, got %d", reloads)
	}
	w.clientsMutex.RLock()
	lastConfigHash := w.lastConfigHash
	w.clientsMutex.RUnlock()
	if lastConfigHash == "" {
		t.Fatal("expected lastConfigHash to be set after reload")
	}
}

func TestPrepareAuthUpdatesLockedForceAndDelete(t *testing.T) {
	w := &Watcher{
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "p1"},
		},
		authQueue: make(chan AuthUpdate, 4),
	}

	updates := w.prepareAuthUpdatesLocked([]*coreauth.Auth{{ID: "a", Provider: "p2"}}, false)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionModify || updates[0].ID != "a" {
		t.Fatalf("unexpected modify updates: %+v", updates)
	}

	updates = w.prepareAuthUpdatesLocked([]*coreauth.Auth{{ID: "a", Provider: "p2"}}, true)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionModify {
		t.Fatalf("expected force modify, got %+v", updates)
	}

	updates = w.prepareAuthUpdatesLocked([]*coreauth.Auth{}, false)
	if len(updates) != 1 || updates[0].Action != AuthUpdateActionDelete || updates[0].ID != "a" {
		t.Fatalf("expected delete for missing auth, got %+v", updates)
	}
}

func TestAuthEqualIgnoresTemporalFields(t *testing.T) {
	now := time.Now()
	a := &coreauth.Auth{ID: "x", CreatedAt: now}
	b := &coreauth.Auth{ID: "x", CreatedAt: now.Add(5 * time.Second)}
	if !authEqual(a, b) {
		t.Fatal("expected authEqual to ignore temporal differences")
	}
}

func TestAuthEqualIgnoresManagerRuntimeInstance(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "managed.json",
		FileName: "managed.json",
		Provider: "codex",
	}
	registered, errRegister := manager.Register(t.Context(), auth)
	if errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	if registered.RuntimeInstanceID() == "" {
		t.Fatal("registered auth is missing runtime instance identity")
	}
	if !authEqual(registered, auth) {
		t.Fatal("manager runtime instance changed watcher equality")
	}
	w := &Watcher{authDir: t.TempDir()}
	w.SeedCurrentFileAuths([]*coreauth.Auth{registered})
	w.clientsMutex.Lock()
	updates := w.prepareAuthUpdatesLocked([]*coreauth.Auth{auth}, false)
	w.clientsMutex.Unlock()
	if len(updates) != 0 {
		t.Fatalf("unchanged seeded auth produced updates: %+v", updates)
	}
}

func TestDispatchLoopExitsWhenQueueNilAndContextCanceled(t *testing.T) {
	w := &Watcher{
		dispatchCond:   nil,
		pendingUpdates: map[string]AuthUpdate{"k": {ID: "k"}},
		pendingOrder:   []string{"k"},
	}
	w.dispatchCond = sync.NewCond(&w.dispatchMu)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.dispatchLoop(ctx)
		close(done)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	w.dispatchMu.Lock()
	w.dispatchCond.Broadcast()
	w.dispatchMu.Unlock()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dispatchLoop did not exit after context cancel")
	}
}

func TestReloadClientsFiltersOAuthProvidersWithoutRescan(t *testing.T) {
	tmp := t.TempDir()
	w := &Watcher{
		authDir: tmp,
		config:  &config.Config{AuthDir: tmp},
		currentAuths: map[string]*coreauth.Auth{
			"a": {ID: "a", Provider: "Match"},
			"b": {ID: "b", Provider: "other"},
		},
		lastAuthHashes: map[string]string{"cached": "hash"},
	}

	w.reloadClients(false, []string{"match"}, false)

	w.clientsMutex.RLock()
	defer w.clientsMutex.RUnlock()
	if _, ok := w.currentAuths["a"]; ok {
		t.Fatal("expected filtered provider to be removed")
	}
	if len(w.lastAuthHashes) != 1 {
		t.Fatalf("expected existing hash cache to be retained, got %d", len(w.lastAuthHashes))
	}
}

func TestScheduleProcessEventsStopsOnContextDone(t *testing.T) {
	w := &Watcher{
		watcher: &fsnotify.Watcher{
			Events: make(chan fsnotify.Event, 1),
			Errors: make(chan error, 1),
		},
		configPath: "config.yaml",
		authDir:    "auth",
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.processEvents(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("processEvents did not exit on context cancel")
	}
}

func hexString(data []byte) string {
	return strings.ToLower(fmt.Sprintf("%x", data))
}
