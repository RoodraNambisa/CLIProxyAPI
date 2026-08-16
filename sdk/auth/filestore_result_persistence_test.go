package auth

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

var (
	_ cliproxyauth.RefreshPersistenceConcurrencyStore = (*FileTokenStore)(nil)
	_ cliproxyauth.SourceConditionalSaveStore         = (*FileTokenStore)(nil)
)

type fileTokenStorePersistenceExecutor struct {
	executeCalls atomic.Int64
	streamCalls  atomic.Int64
	refreshCalls atomic.Int64

	executeStarted chan struct{}
	releaseExecute <-chan struct{}
	executeOnce    sync.Once
}

func (*fileTokenStorePersistenceExecutor) Identifier() string { return "chatgpt-web" }

func (executor *fileTokenStorePersistenceExecutor) Execute(
	ctx context.Context,
	auth *cliproxyauth.Auth,
	_ cliproxyexecutor.Request,
	opts cliproxyexecutor.Options,
) (cliproxyexecutor.Response, error) {
	executor.executeCalls.Add(1)
	if executor.executeStarted != nil {
		executor.executeOnce.Do(func() { close(executor.executeStarted) })
	}
	if executor.releaseExecute != nil {
		select {
		case <-executor.releaseExecute:
		case <-ctx.Done():
			return cliproxyexecutor.Response{}, ctx.Err()
		}
	}
	if fileTokenStoreTestMetadataString(auth, "access_token") == "stale-access-token" {
		return cliproxyexecutor.Response{}, &cliproxyauth.Error{
			HTTPStatus: http.StatusUnauthorized,
			Message:    "invalid access token",
		}
	}
	state, _ := opts.Metadata[cliproxyexecutor.ImageGenerationResultStateMetadataKey].(*cliproxyexecutor.ImageGenerationResultState)
	if state != nil {
		state.AddProduced(1)
	}
	return cliproxyexecutor.Response{Payload: []byte("successful-image-response")}, nil
}

func (executor *fileTokenStorePersistenceExecutor) ExecuteStream(
	_ context.Context,
	auth *cliproxyauth.Auth,
	_ cliproxyexecutor.Request,
	_ cliproxyexecutor.Options,
) (*cliproxyexecutor.StreamResult, error) {
	executor.streamCalls.Add(1)
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	if fileTokenStoreTestMetadataString(auth, "access_token") == "stream-stale-access-token" {
		chunks <- cliproxyexecutor.BootstrapCommitStreamChunk()
		chunks <- cliproxyexecutor.StreamChunk{Err: &cliproxyauth.Error{
			HTTPStatus: http.StatusUnauthorized,
			Message:    "invalid access token",
		}}
	} else {
		chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("successful-stream-response")}
	}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (*fileTokenStorePersistenceExecutor) CountTokens(
	context.Context,
	*cliproxyauth.Auth,
	cliproxyexecutor.Request,
	cliproxyexecutor.Options,
) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (executor *fileTokenStorePersistenceExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	executor.refreshCalls.Add(1)
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh-access-token"
	updated.Metadata["refresh_token"] = "fresh-refresh-token"
	updated.Metadata["session_id"] = "fresh-session"
	updated.Metadata["cookies"] = []any{map[string]any{"name": "session", "value": "fresh-cookie"}}
	return updated, nil
}

func (*fileTokenStorePersistenceExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func (*fileTokenStorePersistenceExecutor) Close() error { return nil }

type fileTokenStoreSaveGate struct {
	release chan struct{}
	started chan string
	once    sync.Once
	active  atomic.Int64
	peak    atomic.Int64
}

func newFileTokenStoreSaveGate() *fileTokenStoreSaveGate {
	return &fileTokenStoreSaveGate{
		release: make(chan struct{}),
		started: make(chan string, 32),
	}
}

func (gate *fileTokenStoreSaveGate) lockTarget(
	ctx context.Context,
	root *os.Root,
	relativePath string,
) (func() error, error) {
	active := gate.active.Add(1)
	for {
		peak := gate.peak.Load()
		if active <= peak || gate.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	select {
	case gate.started <- relativePath:
	default:
	}
	select {
	case <-gate.release:
	case <-ctx.Done():
		gate.active.Add(-1)
		return nil, ctx.Err()
	}
	unlockTarget, errLock := lockRootAuthTarget(ctx, root, relativePath)
	if errLock != nil {
		gate.active.Add(-1)
		return nil, errLock
	}
	var unlockOnce sync.Once
	var unlockErr error
	return func() error {
		unlockOnce.Do(func() {
			unlockErr = unlockTarget()
			gate.active.Add(-1)
		})
		return unlockErr
	}, nil
}

func (gate *fileTokenStoreSaveGate) releaseSaves() {
	gate.once.Do(func() { close(gate.release) })
}

func registerFileTokenStorePersistenceAuth(
	t *testing.T,
	manager *cliproxyauth.Manager,
	id, email, accessToken string,
) *cliproxyauth.Auth {
	t.Helper()
	registered, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
		ID:       id,
		FileName: id,
		Provider: "chatgpt-web",
		Status:   cliproxyauth.StatusActive,
		Metadata: map[string]any{
			"type":                  "chatgpt-web",
			"email":                 email,
			"access_token":          accessToken,
			"refresh_token":         "old-refresh-token",
			"session_id":            "old-session",
			"cookies":               []any{map[string]any{"name": "session", "value": "old-cookie"}},
			"lifecycle_state":       cliproxyauth.LifecycleStateActive,
			"refresh_strategy":      "web_oauth_rt",
			"image_quota_remaining": 10,
		},
	})
	if errRegister != nil {
		t.Fatalf("Register(%s) error: %v", id, errRegister)
	}
	if registered.Attributes == nil || registered.Attributes[cliproxyauth.SourceHashAttributeKey] == "" {
		t.Fatalf("registered FileTokenStore credential %s has no source hash", id)
	}
	return registered
}

func registerFileTokenStoreModel(t *testing.T, auth *cliproxyauth.Auth, model string) {
	t.Helper()
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
}

func waitForFileTokenStorePersistedAuth(
	t *testing.T,
	store *FileTokenStore,
	authID string,
	condition func(*cliproxyauth.Auth) bool,
) *cliproxyauth.Auth {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		auths, errList := store.List(t.Context())
		if errList != nil {
			t.Fatalf("list FileTokenStore credentials: %v", errList)
		}
		for _, auth := range auths {
			if auth != nil && auth.ID == authID && condition(auth) {
				return auth
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("FileTokenStore credential %s did not reach expected state", authID)
		}
		time.Sleep(time.Millisecond)
	}
}

func fileTokenStoreImageQuota(auth *cliproxyauth.Auth) int {
	if auth == nil || auth.Metadata == nil {
		return -1
	}
	switch value := auth.Metadata["image_quota_remaining"].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	default:
		return -1
	}
}

func fileTokenStoreCookieValue(auth *cliproxyauth.Auth) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	encoded, errMarshal := json.Marshal(auth.Metadata["cookies"])
	if errMarshal != nil {
		return ""
	}
	var cookies []struct {
		Value string `json:"value"`
	}
	if errUnmarshal := json.Unmarshal(encoded, &cookies); errUnmarshal != nil {
		return ""
	}
	for _, cookie := range cookies {
		if cookie.Value != "" {
			return cookie.Value
		}
	}
	return ""
}

func waitForFileTokenStorePersistenceMetrics(
	t *testing.T,
	manager *cliproxyauth.Manager,
	condition func(cliproxyauth.RefreshPersistenceMetricsSnapshot) bool,
) cliproxyauth.RefreshPersistenceMetricsSnapshot {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := manager.RefreshPersistenceMetrics()
		if condition(metrics) {
			return metrics
		}
		if time.Now().After(deadline) {
			t.Fatalf("FileTokenStore persistence metrics did not reach expected state: %#v", metrics)
		}
		time.Sleep(time.Millisecond)
	}
}

func fileTokenStoreErrorStatus(err error) int {
	type statusCoder interface{ StatusCode() int }
	var target statusCoder
	if errors.As(err, &target) {
		return target.StatusCode()
	}
	return 0
}

func executeFileTokenStoreImage(
	ctx context.Context,
	manager *cliproxyauth.Manager,
	model string,
) (cliproxyexecutor.Response, error) {
	return manager.Execute(
		ctx,
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{Metadata: map[string]any{
			cliproxyexecutor.ImageGenerationMaxResultsMetadataKey: 1,
		}},
	)
}

func TestFileTokenStoreSuccessfulResponseDoesNotWaitForResultSave(t *testing.T) {
	const (
		authID = "file-result-success.json"
		model  = "file-result-success-model"
	)
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	executor := &fileTokenStorePersistenceExecutor{}
	manager.RegisterExecutor(executor)
	registered := registerFileTokenStorePersistenceAuth(t, manager, authID, "file-result-success@example.com", "ready-access-token")
	registerFileTokenStoreModel(t, registered, model)
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	t.Cleanup(func() {
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	type executeResult struct {
		response cliproxyexecutor.Response
		err      error
	}
	executeDone := make(chan executeResult, 1)
	go func() {
		response, errExecute := executeFileTokenStoreImage(t.Context(), manager, model)
		executeDone <- executeResult{response: response, err: errExecute}
	}()
	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background FileTokenStore result save did not start")
	}
	select {
	case result := <-executeDone:
		if result.err != nil || string(result.response.Payload) != "successful-image-response" {
			t.Fatalf("Execute() = %q, %v", result.response.Payload, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("successful response waited for FileTokenStore Save")
	}

	gate.releaseSaves()
	waitForFileTokenStorePersistedAuth(t, store, registered.ID, func(auth *cliproxyauth.Auth) bool {
		return fileTokenStoreImageQuota(auth) == 9
	})
}

func TestFileTokenStoreUnauthorizedResponseDoesNotWaitForRefreshSave(t *testing.T) {
	const model = "file-result-401-model"
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	executor := &fileTokenStorePersistenceExecutor{}
	manager.RegisterExecutor(executor)
	primary := registerFileTokenStorePersistenceAuth(t, manager, "aa-file-result-401.json", "file-result-401@example.com", "stale-access-token")
	backup := registerFileTokenStorePersistenceAuth(t, manager, "zz-file-result-401-backup.json", "file-result-401-backup@example.com", "ready-access-token")
	registerFileTokenStoreModel(t, primary, model)
	registerFileTokenStoreModel(t, backup, model)
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	t.Cleanup(func() {
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	requestDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(t.Context(), []string{"chatgpt-web"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		requestDone <- errExecute
	}()
	select {
	case startedID := <-gate.started:
		if startedID != primary.ID {
			t.Fatalf("background refresh persisted %q, want %q", startedID, primary.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("background FileTokenStore refresh save did not start")
	}
	select {
	case errExecute := <-requestDone:
		if fileTokenStoreErrorStatus(errExecute) != http.StatusUnauthorized || errExecute.Error() != "invalid access token" {
			t.Fatalf("Execute() error = %T %v, want original 401", errExecute, errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("401 response waited for FileTokenStore refresh Save")
	}
	if calls := executor.executeCalls.Load(); calls != 1 {
		t.Fatalf("Execute() calls = %d, want one request without retry or fallback", calls)
	}
	if calls := executor.refreshCalls.Load(); calls != 1 {
		t.Fatalf("Refresh() calls = %d, want one background refresh", calls)
	}

	gate.releaseSaves()
	waitForFileTokenStorePersistedAuth(t, store, primary.ID, func(auth *cliproxyauth.Auth) bool {
		return fileTokenStoreTestMetadataString(auth, "access_token") == "fresh-access-token"
	})
}

func TestFileTokenStoreMidstreamUnauthorizedDoesNotWaitForRefreshSave(t *testing.T) {
	const model = "file-result-stream-401-model"
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	executor := &fileTokenStorePersistenceExecutor{}
	manager.RegisterExecutor(executor)
	primary := registerFileTokenStorePersistenceAuth(t, manager, "aa-file-result-stream-401.json", "file-result-stream-401@example.com", "stream-stale-access-token")
	backup := registerFileTokenStorePersistenceAuth(t, manager, "zz-file-result-stream-401-backup.json", "file-result-stream-401-backup@example.com", "ready-access-token")
	registerFileTokenStoreModel(t, primary, model)
	registerFileTokenStoreModel(t, backup, model)
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	t.Cleanup(func() {
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	stream, errStream := manager.ExecuteStream(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errStream != nil || stream == nil {
		t.Fatalf("ExecuteStream() = %#v, %v", stream, errStream)
	}
	var unauthorized error
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			unauthorized = chunk.Err
		}
	}
	if fileTokenStoreErrorStatus(unauthorized) != http.StatusUnauthorized || unauthorized.Error() != "invalid access token" {
		t.Fatalf("stream error = %T %v, want original 401", unauthorized, unauthorized)
	}
	select {
	case startedID := <-gate.started:
		if startedID != primary.ID {
			t.Fatalf("background stream refresh persisted %q, want %q", startedID, primary.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stream 401 background FileTokenStore refresh save did not start")
	}
	if calls := executor.streamCalls.Load(); calls != 1 {
		t.Fatalf("ExecuteStream() calls = %d, want one stream without fallback", calls)
	}
	if calls := executor.refreshCalls.Load(); calls != 1 {
		t.Fatalf("Refresh() calls = %d, want one background refresh", calls)
	}

	gate.releaseSaves()
	waitForFileTokenStorePersistedAuth(t, store, primary.ID, func(auth *cliproxyauth.Auth) bool {
		return fileTokenStoreTestMetadataString(auth, "access_token") == "fresh-access-token"
	})
}

func TestFileTokenStoreCoalescesResultsWithoutOverwritingConcurrentRefresh(t *testing.T) {
	const (
		authID = "file-result-refresh-interleave.json"
		model  = "file-result-refresh-interleave-model"
	)
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	executor := &fileTokenStorePersistenceExecutor{}
	manager.RegisterExecutor(executor)
	registered := registerFileTokenStorePersistenceAuth(t, manager, authID, "file-result-refresh-interleave@example.com", "ready-access-token")
	registerFileTokenStoreModel(t, registered, model)
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	t.Cleanup(func() {
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	response, errExecute := executeFileTokenStoreImage(t.Context(), manager, model)
	if errExecute != nil || string(response.Payload) != "successful-image-response" {
		t.Fatalf("first Execute() = %q, %v", response.Payload, errExecute)
	}
	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first coalesced FileTokenStore result save did not start")
	}
	for attempt := 0; attempt < 3; attempt++ {
		response, errExecute = executeFileTokenStoreImage(t.Context(), manager, model)
		if errExecute != nil || string(response.Payload) != "successful-image-response" {
			t.Fatalf("coalesced Execute(%d) = %q, %v", attempt, response.Payload, errExecute)
		}
	}
	expected, ok := manager.GetByID(registered.ID)
	if !ok || expected == nil || fileTokenStoreImageQuota(expected) != 6 {
		t.Fatalf("runtime result state before refresh = %#v", expected)
	}
	type refreshResult struct {
		auth *cliproxyauth.Auth
		err  error
	}
	refreshDone := make(chan refreshResult, 1)
	go func() {
		refreshed, errRefresh := manager.RefreshChatGPTWebForRequest(context.Background(), expected)
		refreshDone <- refreshResult{auth: refreshed, err: errRefresh}
	}()
	waitForFileTokenStorePersistenceMetrics(t, manager, func(metrics cliproxyauth.RefreshPersistenceMetricsSnapshot) bool {
		return metrics.Active == 1 && metrics.Queued == 1
	})
	if calls := executor.refreshCalls.Load(); calls != 0 {
		t.Fatalf("Refresh() entered FileTokenStore while result Save held admission: %d", calls)
	}

	gate.releaseSaves()
	select {
	case result := <-refreshDone:
		if result.err != nil || result.auth == nil {
			t.Fatalf("RefreshChatGPTWebForRequest() = %#v, %v", result.auth, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FileTokenStore credential refresh did not finish")
	}
	persisted := waitForFileTokenStorePersistedAuth(t, store, registered.ID, func(auth *cliproxyauth.Auth) bool {
		return fileTokenStoreTestMetadataString(auth, "access_token") == "fresh-access-token" &&
			fileTokenStoreTestMetadataString(auth, "refresh_token") == "fresh-refresh-token" &&
			fileTokenStoreTestMetadataString(auth, "session_id") == "fresh-session" &&
			fileTokenStoreCookieValue(auth) == "fresh-cookie" &&
			fileTokenStoreImageQuota(auth) == 6
	})
	if persisted == nil {
		t.Fatal("fresh credential and coalesced result state were not persisted")
	}
	metrics := manager.RefreshPersistenceMetrics()
	if metrics.Concurrency != 1 || metrics.PeakActive != 1 || metrics.ResultPersistence.Coalesced < 3 {
		t.Fatalf("FileTokenStore persistence metrics = %#v", metrics)
	}
	if peak := gate.peak.Load(); peak != 1 {
		t.Fatalf("FileTokenStore concurrent Save peak = %d, want 1", peak)
	}
}

func TestFileTokenStoreSerializesConcurrentRefreshPersistence(t *testing.T) {
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	executor := &fileTokenStorePersistenceExecutor{}
	manager.RegisterExecutor(executor)
	first := registerFileTokenStorePersistenceAuth(t, manager, "file-refresh-serial-first.json", "file-refresh-serial-first@example.com", "first-access-token")
	second := registerFileTokenStorePersistenceAuth(t, manager, "file-refresh-serial-second.json", "file-refresh-serial-second@example.com", "second-access-token")
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	t.Cleanup(func() {
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	type refreshResult struct{ err error }
	results := make(chan refreshResult, 2)
	for _, auth := range []*cliproxyauth.Auth{first, second} {
		auth := auth
		go func() {
			_, errRefresh := manager.RefreshChatGPTWebForRequest(context.Background(), auth)
			results <- refreshResult{err: errRefresh}
		}()
	}
	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first serialized FileTokenStore refresh did not start")
	}
	metrics := waitForFileTokenStorePersistenceMetrics(t, manager, func(metrics cliproxyauth.RefreshPersistenceMetricsSnapshot) bool {
		return metrics.Active == 1 && metrics.Queued == 1
	})
	if metrics.Concurrency != 1 || metrics.Active != 1 || metrics.Queued != 1 {
		t.Fatalf("FileTokenStore refresh persistence pressure = %#v", metrics)
	}
	if peak := gate.peak.Load(); peak != 1 {
		t.Fatalf("FileTokenStore concurrent Save peak while blocked = %d, want 1", peak)
	}

	gate.releaseSaves()
	for range 2 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("RefreshChatGPTWebForRequest() error: %v", result.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("serialized FileTokenStore refresh did not finish")
		}
	}
	if peak := gate.peak.Load(); peak != 1 {
		t.Fatalf("FileTokenStore concurrent Save peak = %d, want 1", peak)
	}
}

func TestFileTokenStoreCloseDrainsInFlightResultThroughSharedAdmission(t *testing.T) {
	const (
		authID = "file-result-close-drain.json"
		model  = "file-result-close-drain-model"
	)
	releaseExecute := make(chan struct{})
	executor := &fileTokenStorePersistenceExecutor{
		executeStarted: make(chan struct{}),
		releaseExecute: releaseExecute,
	}
	store := NewFileTokenStore()
	store.SetBaseDir(t.TempDir())
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	registered := registerFileTokenStorePersistenceAuth(t, manager, authID, "file-result-close-drain@example.com", "ready-access-token")
	registerFileTokenStoreModel(t, registered, model)
	gate := newFileTokenStoreSaveGate()
	store.lockTarget = gate.lockTarget
	var releaseExecuteOnce sync.Once
	releaseExecution := func() { releaseExecuteOnce.Do(func() { close(releaseExecute) }) }
	t.Cleanup(func() {
		releaseExecution()
		gate.releaseSaves()
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("CloseExecutors() error: %v", errClose)
		}
	})

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := executeFileTokenStoreImage(t.Context(), manager, model)
		executeDone <- errExecute
	}()
	select {
	case <-executor.executeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight FileTokenStore execution did not start")
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- manager.CloseExecutors() }()
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before producer completed: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}

	releaseExecution()
	select {
	case <-gate.started:
	case <-time.After(2 * time.Second):
		t.Fatal("result produced during close did not reach FileTokenStore Save")
	}
	select {
	case errClose := <-closeDone:
		t.Fatalf("CloseExecutors() returned before result persistence drained: %v", errClose)
	case <-time.After(50 * time.Millisecond):
	}

	gate.releaseSaves()
	select {
	case errExecute := <-executeDone:
		if errExecute != nil {
			t.Fatalf("in-flight Execute() error: %v", errExecute)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight FileTokenStore execution did not finish")
	}
	select {
	case errClose := <-closeDone:
		if errClose != nil {
			t.Fatalf("CloseExecutors() error: %v", errClose)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("CloseExecutors() did not finish after FileTokenStore result drain")
	}
	waitForFileTokenStorePersistedAuth(t, store, registered.ID, func(auth *cliproxyauth.Auth) bool {
		return fileTokenStoreImageQuota(auth) == 9
	})
	metrics := manager.RefreshPersistenceMetrics()
	if metrics.Concurrency != 1 || metrics.PeakActive != 1 || metrics.ResultPersistence.ProducerActive != 0 {
		t.Fatalf("FileTokenStore close persistence metrics = %#v", metrics)
	}
	if peak := gate.peak.Load(); peak != 1 {
		t.Fatalf("FileTokenStore concurrent Save peak during close = %d, want 1", peak)
	}
}
