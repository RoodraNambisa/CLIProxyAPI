package auth

import (
	"context"
	"sync"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	core "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
)

type xaiRefreshProbeExecutor struct {
	ProviderExecutor
	calls, refreshes int
}

func TestXAIManagementRefreshDeduplicatesAndRejectsReplacement(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	exec := &xaiRefreshProbeExecutor{}
	manager.RegisterExecutor(exec)
	auth, err := manager.Register(t.Context(), &Auth{ID: "management-refresh", Provider: "xai", Metadata: map[string]any{"access_token": "old-token", "refresh_token": "refresh-token"}})
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			refreshed, errRefresh := manager.RefreshXAIAfterUnauthorized(t.Context(), auth)
			if errRefresh != nil || refreshed == nil || refreshed.Metadata["access_token"] != "new-token" {
				t.Errorf("shared refresh failed: %v", errRefresh)
			}
		})
	}
	workers.Wait()
	if exec.refreshes != 1 {
		t.Fatalf("concurrent queries rotated the token %d times", exec.refreshes)
	}
	if err = manager.Delete(t.Context(), auth.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.Register(t.Context(), &Auth{ID: auth.ID, Provider: "xai", Metadata: map[string]any{"access_token": "replacement", "refresh_token": "replacement-refresh"}}); err != nil {
		t.Fatal(err)
	}
	if _, err = manager.RefreshXAIAfterUnauthorized(t.Context(), auth); err == nil || exec.refreshes != 1 {
		t.Fatal("stale query refreshed a replacement credential")
	}
}

func (e *xaiRefreshProbeExecutor) Identifier() string { return "xai" }
func (e *xaiRefreshProbeExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	e.refreshes++
	updated := auth.Clone()
	updated.Metadata["access_token"] = "new-token"
	return updated, nil
}
func (e *xaiRefreshProbeExecutor) Execute(_ context.Context, auth *Auth, _ core.Request, _ core.Options) (core.Response, error) {
	e.calls++
	if auth.Metadata["access_token"] != "new-token" {
		return core.Response{}, &Error{HTTPStatus: 401, Code: "bad-credentials", Message: "invalid access token"}
	}
	return core.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func TestXAIUnauthorizedRefreshRetriesSameCredentialAndPreservesMetadata(t *testing.T) {
	manager := NewManager(nil, &FillFirstSelector{}, nil)
	manager.SetRetryConfig(0, 1, 0)
	exec := &xaiRefreshProbeExecutor{}
	manager.RegisterExecutor(exec)
	auth := &Auth{ID: "xai-refresh", Provider: "xai", Metadata: map[string]any{"access_token": "old-token", "refresh_token": "refresh-token", "xai_identity_seed": "stable", "headers": map[string]any{"X-Keep": "yes"}}}
	registerFallbackAuthForModel(t, manager, auth, "grok-refresh")
	response, err := manager.Execute(t.Context(), []string{"xai"}, core.Request{Model: "grok-refresh"}, core.Options{Metadata: map[string]any{core.PinnedAuthMetadataKey: auth.ID}})
	if err != nil || exec.calls != 2 || exec.refreshes != 1 || len(response.Payload) == 0 {
		t.Fatalf("calls=%d refreshes=%d err=%v", exec.calls, exec.refreshes, err)
	}
	current, _ := manager.GetByID(auth.ID)
	if current.Metadata["xai_identity_seed"] != "stable" || current.Metadata["access_token"] != "new-token" || current.Metadata["headers"] == nil {
		t.Fatal("refresh discarded identity or custom configuration")
	}
	for _, tc := range []struct {
		status  int
		apiKey  bool
		already bool
	}{{403, false, false}, {401, true, false}, {401, false, true}} {
		candidate := current.Clone()
		if tc.apiKey {
			candidate.Attributes = map[string]string{"api_key": "native"}
		}
		_, attempted, err := manager.tryRefreshAfterUnauthorized(t.Context(), exec, candidate, &Error{HTTPStatus: tc.status, Message: "denied"}, tc.already)
		if attempted || err != nil || exec.refreshes != 1 {
			t.Fatal("generic 403, native key, or repeated refresh was retried")
		}
	}
}

func TestXAINativeModelAliasAndSeedSurviveConfigurationReload(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{XAIKey: []internalconfig.XAIKey{{APIKey: "key", BaseURL: "https://api.x.ai/v1", Models: []internalconfig.CodexModel{{Name: "grok-4.6", Alias: "fast", IsCompat: true}}}}})
	exec := &xaiRefreshProbeExecutor{}
	auth, err := manager.Register(t.Context(), &Auth{ID: "native-key", Provider: "xai", Attributes: map[string]string{"api_key": "key", "base_url": "https://api.x.ai/v1", "runtime_only": "true", "source": "config:xai[fixture]"}, Metadata: map[string]any{"xai_identity_seed": "kept"}})
	if err != nil {
		t.Fatal(err)
	}
	err = manager.ProbeCredential(t.Context(), auth, exec, core.Request{Model: "fast"}, core.Options{}, func(_ context.Context, _ *Auth, request core.Request, _ core.Options) error {
		info, ok := ResolvedAPIKeyModelInfo(request)
		if request.Model != "grok-4.6" || !ok || !info.IsCompat {
			t.Fatal("native alias or model capability lost")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	next := auth.Clone()
	next.Metadata = nil
	prepareAuthReplacement(auth, next, false)
	if next.Metadata["xai_identity_seed"] != "kept" {
		t.Fatal("same config key lost its identity")
	}
	next.Metadata = nil
	next.Attributes["api_key"] = "replacement"
	prepareAuthReplacement(auth, next, false)
	if next.Metadata["xai_identity_seed"] != nil {
		t.Fatal("replacement key inherited another credential identity")
	}
}
