package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type fileTokenStoreRefreshExecutor struct{}

func (*fileTokenStoreRefreshExecutor) Identifier() string { return "chatgpt-web" }

func (*fileTokenStoreRefreshExecutor) Execute(_ context.Context, auth *cliproxyauth.Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if fileTokenStoreTestMetadataString(auth, "access_token") == "stale-access-token" {
		return cliproxyexecutor.Response{}, &cliproxyauth.Error{
			HTTPStatus: http.StatusUnauthorized,
			Message:    "access token expired",
		}
	}
	return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
}

func (*fileTokenStoreRefreshExecutor) ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, errors.New("not implemented")
}

func (*fileTokenStoreRefreshExecutor) Refresh(_ context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	updated := auth.Clone()
	updated.Metadata["access_token"] = "fresh-access-token"
	updated.Metadata["refresh_token"] = "rotated-refresh-token"
	return updated, nil
}

func (*fileTokenStoreRefreshExecutor) CountTokens(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, errors.New("not implemented")
}

func (*fileTokenStoreRefreshExecutor) HttpRequest(context.Context, *cliproxyauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("not implemented")
}

func fileTokenStoreTestMetadataString(auth *cliproxyauth.Auth, key string) string {
	if auth == nil || auth.Metadata == nil {
		return ""
	}
	value, _ := auth.Metadata[key].(string)
	return value
}

func waitForFileTokenStoreAuth(t *testing.T, manager *cliproxyauth.Manager, authID string, ready func(*cliproxyauth.Auth) bool) *cliproxyauth.Auth {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		current, ok := manager.GetByID(authID)
		if ok && current != nil && ready(current) {
			return current
		}
		if time.Now().After(deadline) {
			t.Fatalf("credential %q did not reach the expected background refresh state: %#v", authID, current)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestFileTokenStoreEnablesSerializedRefreshPersistence(t *testing.T) {
	store := NewFileTokenStore()
	if concurrency := store.RefreshPersistenceConcurrency(); concurrency != 1 {
		t.Fatalf("refresh persistence concurrency = %d, want 1", concurrency)
	}
	manager := cliproxyauth.NewManager(store, nil, nil)
	snapshot := manager.RefreshPersistenceMetrics()
	if !snapshot.Enabled || snapshot.Concurrency != 1 || snapshot.QueueLimit <= 0 {
		t.Fatalf("refresh persistence metrics = %+v, want enabled serialized queue", snapshot)
	}
}

func TestFileTokenStorePersistsRotatedChatGPTWebRefreshTokens(t *testing.T) {
	const (
		authID = "chatgpt-web-file-refresh.json"
		model  = "chatgpt-web-file-refresh-model"
	)
	authDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(&fileTokenStoreRefreshExecutor{})
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("close auth manager executors: %v", errClose)
		}
	})

	registered, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
		ID:       authID,
		FileName: authID,
		Provider: "chatgpt-web",
		Status:   cliproxyauth.StatusActive,
		Metadata: map[string]any{
			"type":             "chatgpt-web",
			"email":            "file-refresh@example.com",
			"access_token":     "stale-access-token",
			"refresh_token":    "initial-refresh-token",
			"lifecycle_state":  cliproxyauth.LifecycleStateActive,
			"refresh_strategy": "web_oauth_rt",
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })

	_, errExecute := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	var unauthorized *cliproxyauth.Error
	if !errors.As(errExecute, &unauthorized) || unauthorized.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("first request error = %v, want original 401", errExecute)
	}
	waitForFileTokenStoreAuth(t, manager, registered.ID, func(current *cliproxyauth.Auth) bool {
		return fileTokenStoreTestMetadataString(current, "access_token") == "fresh-access-token" && !current.Unavailable
	})
	response, errExecute := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	if string(response.Payload) != "ok" {
		t.Fatalf("response payload = %q, want ok", response.Payload)
	}

	loaded, errList := store.List(t.Context())
	if errList != nil {
		t.Fatal(errList)
	}
	var persisted *cliproxyauth.Auth
	for _, candidate := range loaded {
		if candidate != nil && candidate.ID == registered.ID {
			persisted = candidate
			break
		}
	}
	if persisted == nil {
		t.Fatalf("persisted credential %q was not reloaded", registered.ID)
	}
	if got := fileTokenStoreTestMetadataString(persisted, "access_token"); got != "fresh-access-token" {
		t.Fatalf("persisted access token = %q, want fresh-access-token", got)
	}
	if got := fileTokenStoreTestMetadataString(persisted, "refresh_token"); got != "rotated-refresh-token" {
		t.Fatalf("persisted refresh token = %q, want rotated-refresh-token", got)
	}
	if persisted.Attributes == nil || persisted.Attributes[cliproxyauth.SourceHashAttributeKey] == "" {
		t.Fatal("reloaded credential is missing the persisted source hash")
	}
	info, errStat := os.Stat(filepath.Join(authDir, registered.ID))
	if errStat != nil {
		t.Fatal(errStat)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("persisted credential mode = %o, want 600", info.Mode().Perm())
	}
}

func TestFileTokenStoreRefreshAcceptsLegacyPayloadWithoutDisabled(t *testing.T) {
	const (
		authID = "chatgpt-web-file-refresh-legacy.json"
		model  = "chatgpt-web-file-refresh-legacy-model"
	)
	authDir := t.TempDir()
	path := filepath.Join(authDir, authID)
	legacy := []byte(`{"type":"chatgpt-web","email":"legacy-refresh@example.com","access_token":"stale-access-token","refresh_token":"initial-refresh-token","lifecycle_state":"active","refresh_strategy":"web_oauth_rt"}`)
	if errWrite := os.WriteFile(path, legacy, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(&fileTokenStoreRefreshExecutor{})
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("close auth manager executors: %v", errClose)
		}
	})
	if errLoad := manager.Load(t.Context()); errLoad != nil {
		t.Fatal(errLoad)
	}
	loaded, ok := manager.GetByID(authID)
	if !ok || loaded == nil {
		t.Fatalf("legacy credential %q was not loaded", authID)
	}
	registry.GetGlobalRegistry().RegisterClient(loaded.ID, loaded.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(loaded.ID) })

	_, errExecute := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	var unauthorized *cliproxyauth.Error
	if !errors.As(errExecute, &unauthorized) || unauthorized.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("first request error = %v, want original 401", errExecute)
	}
	waitForFileTokenStoreAuth(t, manager, loaded.ID, func(current *cliproxyauth.Auth) bool {
		return fileTokenStoreTestMetadataString(current, "access_token") == "fresh-access-token" && !current.Unavailable
	})
	persisted, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var metadata map[string]any
	if errDecode := json.Unmarshal(persisted, &metadata); errDecode != nil {
		t.Fatal(errDecode)
	}
	if metadata["disabled"] != false {
		t.Fatalf("persisted disabled = %#v, want false", metadata["disabled"])
	}
	if metadata["access_token"] != "fresh-access-token" || metadata["refresh_token"] != "rotated-refresh-token" {
		t.Fatalf("legacy credential refresh was not persisted: %#v", metadata)
	}
}

func TestFileTokenStoreRefreshDoesNotOverwriteExternalReplacement(t *testing.T) {
	const (
		authID = "chatgpt-web-file-refresh-conflict.json"
		model  = "chatgpt-web-file-refresh-conflict-model"
	)
	authDir := t.TempDir()
	store := NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := cliproxyauth.NewManager(store, &cliproxyauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(&fileTokenStoreRefreshExecutor{})
	t.Cleanup(func() {
		if errClose := manager.CloseExecutors(); errClose != nil {
			t.Errorf("close auth manager executors: %v", errClose)
		}
	})

	registered, errRegister := manager.Register(t.Context(), &cliproxyauth.Auth{
		ID:       authID,
		FileName: authID,
		Provider: "chatgpt-web",
		Status:   cliproxyauth.StatusActive,
		Metadata: map[string]any{
			"type":             "chatgpt-web",
			"email":            "file-refresh-conflict@example.com",
			"access_token":     "stale-access-token",
			"refresh_token":    "initial-refresh-token",
			"lifecycle_state":  cliproxyauth.LifecycleStateActive,
			"refresh_strategy": "web_oauth_rt",
		},
	})
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(registered.ID, registered.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(registered.ID) })

	replacement := []byte(`{"type":"chatgpt-web","email":"external@example.com","access_token":"external-access-token","refresh_token":"external-refresh-token","lifecycle_state":"active","refresh_strategy":"web_oauth_rt"}`)
	path := filepath.Join(authDir, registered.ID)
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	_, errExecute := manager.Execute(
		t.Context(),
		[]string{"chatgpt-web"},
		cliproxyexecutor.Request{Model: model},
		cliproxyexecutor.Options{},
	)
	var authErr *cliproxyauth.Error
	if !errors.As(errExecute, &authErr) || authErr.StatusCode() != http.StatusUnauthorized {
		t.Fatalf("request error = %v, want original 401", errExecute)
	}
	current := waitForFileTokenStoreAuth(t, manager, registered.ID, func(candidate *cliproxyauth.Auth) bool {
		return candidate.LifecycleState() == cliproxyauth.LifecycleStateReauthRequired &&
			candidate.LastError != nil && candidate.LastError.Code == "refresh_persist_failed"
	})
	persisted, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	if !bytes.Equal(persisted, replacement) {
		t.Fatalf("external replacement was overwritten:\n got: %s\nwant: %s", persisted, replacement)
	}
	if fileTokenStoreTestMetadataString(current, "access_token") != "stale-access-token" ||
		current.LifecycleState() != cliproxyauth.LifecycleStateReauthRequired {
		t.Fatalf("runtime credential changed after rejected refresh: %#v", current)
	}
}
