package cliproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v6/internal/runtime/executor"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

type chatGPTWebCatalogTestExecutor struct {
	mu       sync.Mutex
	models   []chatgptwebauth.CatalogModel
	err      error
	execute  func(*coreauth.Auth, cliproxyexecutor.Request) (cliproxyexecutor.Response, error)
	refresh  func(*coreauth.Auth) (*coreauth.Auth, error)
	calls    int
	active   int
	maxSeen  int
	started  chan struct{}
	release  chan struct{}
	executed chan string
	once     sync.Once
}

func (*chatGPTWebCatalogTestExecutor) Identifier() string { return chatgptwebauth.Provider }

func (executor *chatGPTWebCatalogTestExecutor) Execute(_ context.Context, auth *coreauth.Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if executor.executed != nil {
		executor.executed <- req.Model
	}
	if executor.execute != nil {
		return executor.execute(auth, req)
	}
	return cliproxyexecutor.Response{}, nil
}

func (*chatGPTWebCatalogTestExecutor) ExecuteStream(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	return nil, nil
}

func (executor *chatGPTWebCatalogTestExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	if executor.refresh != nil {
		return executor.refresh(auth)
	}
	return auth, nil
}

func (*chatGPTWebCatalogTestExecutor) CountTokens(context.Context, *coreauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (*chatGPTWebCatalogTestExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (executor *chatGPTWebCatalogTestExecutor) FetchModels(ctx context.Context, _ *coreauth.Auth) ([]chatgptwebauth.CatalogModel, error) {
	executor.mu.Lock()
	executor.calls++
	executor.active++
	if executor.active > executor.maxSeen {
		executor.maxSeen = executor.active
	}
	models := append([]chatgptwebauth.CatalogModel(nil), executor.models...)
	err := executor.err
	executor.mu.Unlock()
	defer func() {
		executor.mu.Lock()
		executor.active--
		executor.mu.Unlock()
	}()
	if executor.started != nil {
		executor.once.Do(func() { close(executor.started) })
	}
	if executor.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-executor.release:
		}
	}
	return models, err
}

func (executor *chatGPTWebCatalogTestExecutor) set(models []chatgptwebauth.CatalogModel, err error) {
	executor.mu.Lock()
	executor.models = append([]chatgptwebauth.CatalogModel(nil), models...)
	executor.err = err
	executor.mu.Unlock()
}

func (executor *chatGPTWebCatalogTestExecutor) callCount() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.calls
}

func (executor *chatGPTWebCatalogTestExecutor) maxConcurrent() int {
	executor.mu.Lock()
	defer executor.mu.Unlock()
	return executor.maxSeen
}

type chatGPTWebCatalogBlockingStore struct {
	mu      sync.Mutex
	block   bool
	started chan struct{}
	release chan struct{}
	once    sync.Once
	auths   []*coreauth.Auth
}

func (store *chatGPTWebCatalogBlockingStore) List(context.Context) ([]*coreauth.Auth, error) {
	auths := make([]*coreauth.Auth, 0, len(store.auths))
	for _, auth := range store.auths {
		auths = append(auths, auth.Clone())
	}
	return auths, nil
}

func (store *chatGPTWebCatalogBlockingStore) Save(ctx context.Context, _ *coreauth.Auth) (string, error) {
	store.mu.Lock()
	block := store.block
	started := store.started
	release := store.release
	store.mu.Unlock()
	if !block {
		return "", nil
	}
	store.once.Do(func() { close(started) })
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-release:
		return "", nil
	}
}

func (*chatGPTWebCatalogBlockingStore) Delete(context.Context, string) error { return nil }

func (store *chatGPTWebCatalogBlockingStore) enable() {
	store.mu.Lock()
	store.block = true
	store.mu.Unlock()
}

func TestChatGPTWebBuiltinModelsAlwaysIncludeImageModel(t *testing.T) {
	models := chatGPTWebBuiltinModels()
	var image *registry.ModelInfo
	for _, model := range models {
		if model != nil && model.ID == "gpt-image-2" {
			image = model
		}
	}
	if image == nil || image.Type != registry.OpenAIImageModelType {
		t.Fatalf("image model = %#v", image)
	}
}

func TestServiceChatGPTWebModelsUsesLastCatalog(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-catalog")
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: chatGPTWebCatalogCredentialIdentity(auth),
		Models:             []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})
	models := service.chatGPTWebModelsForAuth(auth)
	if len(models) != 1 || models[0].ID != "remote-model" {
		t.Fatalf("models = %#v", models)
	}
	models[0].ID = "mutated"
	again := service.chatGPTWebModelsForAuth(auth)
	if again[0].ID != "remote-model" {
		t.Fatal("cached catalog was returned without cloning")
	}
}

func TestServiceChatGPTWebModelsRejectsDifferentRuntimeInstance(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-runtime-cache")
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("old-account-model", "", 0, "")},
	})

	replacement, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("replace auth: %v", err)
	}
	if replacement.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		t.Fatal("replacement auth reused old runtime instance")
	}

	models := service.chatGPTWebModelsForAuth(replacement)
	if containsRegisteredModel(models, "old-account-model") {
		t.Fatalf("old runtime catalog leaked into replacement: %v", registeredModelIDs(models))
	}
	if !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("fallback catalog missing image model: %v", registeredModelIDs(models))
	}
	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("stale runtime catalog was not removed")
	}
}

func TestServiceChatGPTWebCatalogDoesNotMigrateEmptyIdentityAcrossRuntimeReplacement(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-empty-identity",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("old-account-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("old-account-model", "", 0, ""),
	})

	replacement, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	})
	if err != nil {
		t.Fatalf("replace auth: %v", err)
	}
	if replacement.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		t.Fatal("replacement auth reused old runtime instance")
	}

	service.reconcileChatGPTWebAuthState(context.Background(), replacement)

	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("empty-identity catalog migrated across runtime replacement")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "old-account-model") ||
		!containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("replacement models = %v", registeredModelIDs(models))
	}
}

func TestServiceChatGPTWebCatalogMigratesOnSameEmailRefresh(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-source-refresh",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "old-source",
		},
		Metadata: map[string]any{
			"email":           "Person@Example.com",
			"access_token":    "old-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: chatGPTWebCatalogCredentialIdentity(auth),
		Models:             []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})

	updated, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "new-source",
		},
		Metadata: map[string]any{
			"email":           "person@example.com",
			"access_token":    "new-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("refresh auth: %v", err)
	}
	if updated.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		t.Fatal("source refresh unexpectedly reused runtime instance")
	}

	service.reconcileChatGPTWebAuthState(context.Background(), updated)

	cached, ok := service.chatGPTWebModelCatalog.Load(auth.ID)
	if !ok {
		t.Fatal("same-email source refresh discarded the model catalog")
	}
	entry, ok := cached.(*chatGPTWebModelCatalogCacheEntry)
	if !ok || entry == nil || entry.RuntimeInstanceID != updated.RuntimeInstanceID() {
		t.Fatalf("migrated catalog = %#v, want runtime %q", cached, updated.RuntimeInstanceID())
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "remote-model") {
		t.Fatalf("same-email replacement models = %v", registeredModelIDs(models))
	}
}

func TestChatGPTWebCatalogCredentialIdentityUsesStableJWTClaims(t *testing.T) {
	oldAuth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "old-jti"),
		},
	}
	refreshedAuth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "new-jti"),
		},
	}
	replacementAuth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": chatGPTWebTestJWT(t, "account-two", "user-two", "subject-two", "replacement-jti"),
		},
	}
	oldIdentity := chatGPTWebCatalogCredentialIdentity(oldAuth)
	if oldIdentity == "" || oldIdentity != chatGPTWebCatalogCredentialIdentity(refreshedAuth) {
		t.Fatalf("same-account JWT identities differ: %q / %q", oldIdentity, chatGPTWebCatalogCredentialIdentity(refreshedAuth))
	}
	if oldIdentity == chatGPTWebCatalogCredentialIdentity(replacementAuth) {
		t.Fatal("different JWT account reused the old credential identity")
	}
	sharedAccountAuth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": chatGPTWebTestJWT(t, "account-one", "user-two", "subject-two", "shared-account-jti"),
		},
	}
	if oldIdentity == chatGPTWebCatalogCredentialIdentity(sharedAccountAuth) {
		t.Fatal("different user in the same account reused the old credential identity")
	}
}

func TestChatGPTWebCatalogCredentialIdentityPrefersAccessToken(t *testing.T) {
	oldAccountToken := chatGPTWebTestJWT(t, "account-old", "user-old", "subject-old", "old-jti")
	newAccountToken := chatGPTWebTestJWT(t, "account-new", "user-new", "subject-new", "new-jti")
	auth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": newAccountToken,
			"id_token":     oldAccountToken,
		},
	}
	newAccount := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{"access_token": newAccountToken},
	}
	oldAccount := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{"access_token": oldAccountToken},
	}
	identity := chatGPTWebCatalogCredentialIdentity(auth)
	if identity == "" || identity != chatGPTWebCatalogCredentialIdentity(newAccount) {
		t.Fatalf("identity = %q, want access token identity", identity)
	}
	if identity == chatGPTWebCatalogCredentialIdentity(oldAccount) {
		t.Fatal("stale id token overrode the current access token identity")
	}
}

func TestChatGPTWebCatalogCredentialIdentityUsesIDTokenAccountWhenAccessTokenLacksOne(t *testing.T) {
	accessWithoutAccount := chatGPTWebTestJWT(t, "", "user-new", "subject-new", "access-jti")
	idWithAccount := chatGPTWebTestJWT(t, "account-new", "user-new", "subject-new", "id-jti")
	auth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{
			"access_token": accessWithoutAccount,
			"id_token":     idWithAccount,
		},
	}
	accountAuth := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Metadata: map[string]any{"access_token": idWithAccount},
	}
	if got, want := chatGPTWebCatalogCredentialIdentity(auth), chatGPTWebCatalogCredentialIdentity(accountAuth); got == "" || got != want {
		t.Fatalf("identity = %q, want %q", got, want)
	}
}

func TestChatGPTWebCatalogCredentialIdentityUsesStableClaimsBeforeSourceHash(t *testing.T) {
	first := &coreauth.Auth{
		Provider: chatgptwebauth.Provider,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "first-source",
		},
		Metadata: map[string]any{
			"access_token": chatGPTWebTestJWT(t, "", "same-user", "same-subject", "first-jti"),
			"email":        "same@example.com",
		},
	}
	second := first.Clone()
	second.Attributes[coreauth.SourceHashAttributeKey] = "second-source"
	firstIdentity := chatGPTWebCatalogCredentialIdentity(first)
	secondIdentity := chatGPTWebCatalogCredentialIdentity(second)
	if firstIdentity == "" || firstIdentity != secondIdentity {
		t.Fatalf("source identities = %q / %q", firstIdentity, secondIdentity)
	}
}

func TestServiceChatGPTWebModelsRejectsSameRuntimeDifferentCredential(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-same-runtime-cache",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-old", "user-old", "subject-old", "old-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: chatGPTWebCatalogCredentialIdentity(auth),
		Models:             []*registry.ModelInfo{chatGPTWebTextModelInfo("old-account-model", "", 0, "")},
	})

	replacement := auth.Clone()
	replacement.Metadata = map[string]any{
		"access_token":    chatGPTWebTestJWT(t, "account-new", "user-new", "subject-new", "new-jti"),
		"lifecycle_state": coreauth.LifecycleStateActive,
	}
	if replacement.RuntimeInstanceID() != auth.RuntimeInstanceID() {
		t.Fatal("test replacement did not preserve the runtime instance")
	}
	models := service.chatGPTWebModelsForAuth(replacement)
	if containsRegisteredModel(models, "old-account-model") {
		t.Fatalf("old account catalog leaked into replacement: %v", registeredModelIDs(models))
	}
	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("different credential retained the cached catalog")
	}
}

func TestServiceChatGPTWebModelsRejectsOpaqueCredentialReplacement(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-opaque-cache",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-old-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: chatGPTWebCatalogCredentialIdentity(auth),
		Models:             []*registry.ModelInfo{chatGPTWebTextModelInfo("old-account-model", "", 0, "")},
	})

	replacement := auth.Clone()
	replacement.Metadata["access_token"] = "opaque-new-token"
	installed, err := manager.Update(context.Background(), replacement)
	if err != nil {
		t.Fatalf("replace opaque credential: %v", err)
	}
	models := service.chatGPTWebModelsForAuth(installed)
	for _, model := range models {
		if model != nil && model.ID == "old-account-model" {
			t.Fatal("opaque replacement reused the previous credential catalog")
		}
	}
}

func TestServiceCurrentChatGPTWebCatalogReplacesSameSourceDifferentCredential(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{coreManager: manager}
	sourceHash := "stable-source"
	source, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-same-runtime-fetch",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: sourceHash,
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-old", "user-old", "subject-old", "old-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	replacement, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       source.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: sourceHash,
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-new", "user-new", "subject-new", "new-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("replace auth: %v", err)
	}
	if replacement.RuntimeInstanceID() == source.RuntimeInstanceID() {
		t.Fatal("different credential reused the old runtime instance")
	}
	if _, release, active := source.BeginRuntimeExecution(context.Background()); active {
		release()
		t.Fatal("old credential runtime remained active after replacement")
	}
	if _, current := service.currentAuthForChatGPTWebCatalog(source); current {
		t.Fatal("stale fetch source was accepted after the credential identity changed")
	}
}

func TestServiceChatGPTWebCatalogPreservesNoEmailJWTRefresh(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-jwt-refresh",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "old-source",
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "old-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:  auth.RuntimeInstanceID(),
		CredentialIdentity: chatGPTWebCatalogCredentialIdentity(auth),
		Models:             []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})

	updated, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "new-source",
		},
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-one", "user-one", "subject-one", "new-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("refresh auth: %v", err)
	}
	service.reconcileChatGPTWebAuthState(context.Background(), updated)

	cached, ok := service.chatGPTWebModelCatalog.Load(auth.ID)
	entry, okEntry := cached.(*chatGPTWebModelCatalogCacheEntry)
	if !ok || !okEntry || entry == nil || !containsRegisteredModel(entry.Models, "remote-model") {
		t.Fatalf("same-account JWT refresh dropped catalog: %#v", cached)
	}
}

func TestServiceChatGPTWebCatalogPreservesOptionalClaimDrift(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-optional-claim-refresh",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    chatGPTWebTestJWT(t, "account-one", "user-one", "", "old-jti"),
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:   auth.RuntimeInstanceID(),
		CredentialIdentity:  chatGPTWebCatalogCredentialIdentity(auth),
		CredentialReference: coreauth.NewChatGPTWebCredentialReference(auth),
		Models:              []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})

	updated := auth.Clone()
	updated.Metadata["access_token"] = chatGPTWebTestJWT(t, "account-one", "", "", "new-jti")
	models := service.chatGPTWebModelsForAuth(updated)
	if !containsRegisteredModel(models, "remote-model") {
		t.Fatalf("optional claim drift dropped catalog: %v", registeredModelIDs(models))
	}

	differentUser := auth.Clone()
	differentUser.Metadata["access_token"] = chatGPTWebTestJWT(t, "account-one", "user-two", "", "replacement-jti")
	if models = service.chatGPTWebModelsForAuth(differentUser); containsRegisteredModel(models, "remote-model") {
		t.Fatalf("different user reused catalog: %v", registeredModelIDs(models))
	}
}

func chatGPTWebTestJWT(t *testing.T, accountID, userID, subject, tokenID string) string {
	t.Helper()
	header, err := json.Marshal(map[string]any{"alg": "none", "typ": "JWT"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"sub": subject,
		"jti": tokenID,
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": accountID,
			"chatgpt_user_id":    userID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(header) + "." +
		base64.RawURLEncoding.EncodeToString(payload) + ".signature"
}

func TestChatGPTWebCatalogModelInfosKeepsBuiltinImageOnEmptyCatalog(t *testing.T) {
	models := chatGPTWebCatalogModelInfos(nil)
	if len(models) != 1 || models[0].ID != "gpt-image-2" {
		t.Fatalf("models = %#v", models)
	}
}

func TestChatGPTWebCatalogModelInfosDeduplicatesNormalizedModelIDs(t *testing.T) {
	models := chatGPTWebCatalogModelInfos([]chatgptwebauth.CatalogModel{
		{Slug: " remote-model ", DisplayName: "Remote"},
		{Slug: "REMOTE-MODEL", DisplayName: "Duplicate"},
	})
	if got := registeredModelIDs(models); len(got) != 2 || got[0] != "remote-model" || got[1] != "gpt-image-2" {
		t.Fatalf("models = %v, want one remote model and the builtin image model", got)
	}
}

func TestServiceReconcileChatGPTWebStatusDisabledUnregistersModels(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-status-disabled")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	GlobalModelRegistry().RegisterClient(auth.ID, auth.Provider, chatGPTWebBuiltinModels())

	disabled := auth.Clone()
	disabled.Status = coreauth.StatusDisabled
	disabled.Disabled = false
	installed, err := manager.Update(context.Background(), disabled)
	if err != nil {
		t.Fatalf("disable auth: %v", err)
	}
	service.reconcileChatGPTWebAuthState(context.Background(), installed)

	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("disabled auth models = %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogReplacesFallbackAndKeepsImage(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model", DisplayName: "Remote"}}, nil)

	service.syncAuthModels(context.Background(), auth.ID)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "remote-model") || !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("registered models = %v", registeredModelIDs(models))
	}
	if containsRegisteredModel(models, chatGPTWebFallbackModelIDs[0]) {
		t.Fatalf("fallback model remained after successful catalog refresh: %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogKeepsLastSuccessOnFailure(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}}, nil)
	service.syncAuthModels(context.Background(), auth.ID)

	executor.set(nil, errors.New("catalog unavailable"))
	service.syncAuthModels(context.Background(), auth.ID)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "remote-model") || !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("last successful catalog was not preserved: %v", registeredModelIDs(models))
	}
}

func TestServiceChatGPTWebControlledOpaqueRefreshKeepsLastCatalogOnFetchFailure(t *testing.T) {
	executor := &chatGPTWebCatalogTestExecutor{}
	manager := coreauth.NewManager(nil, &coreauth.FillFirstSelector{}, nil)
	manager.RegisterExecutor(executor)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-opaque-refresh-catalog",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "stale",
			"refresh_token":   "opaque-refresh-one",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-only-model"}}, nil)
	service.syncChatGPTWebModels(context.Background(), auth)
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); !containsRegisteredModel(models, "remote-only-model") {
		t.Fatalf("initial catalog models = %v", registeredModelIDs(models))
	}

	executor.execute = func(current *coreauth.Auth, _ cliproxyexecutor.Request) (cliproxyexecutor.Response, error) {
		if token, _ := current.Metadata["access_token"].(string); token == "stale" {
			return cliproxyexecutor.Response{}, &coreauth.Error{
				HTTPStatus: http.StatusUnauthorized,
				Message:    "invalid access token",
			}
		}
		return cliproxyexecutor.Response{Payload: []byte("ok")}, nil
	}
	executor.refresh = func(current *coreauth.Auth) (*coreauth.Auth, error) {
		updated := current.Clone()
		updated.Metadata["access_token"] = "fresh"
		updated.Metadata["refresh_token"] = "opaque-refresh-two"
		return updated, nil
	}
	executor.set(nil, errors.New("catalog unavailable"))
	manager.AddHook(authMaintenanceHook{service: service})

	response, err := manager.Execute(context.Background(), []string{chatgptwebauth.Provider}, cliproxyexecutor.Request{
		Model: "remote-only-model",
	}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("execute after controlled refresh: %v", err)
	}
	if string(response.Payload) != "ok" {
		t.Fatalf("response payload = %q", response.Payload)
	}

	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	cached, ok := service.chatGPTWebModelCatalog.Load(auth.ID)
	entry, okEntry := cached.(*chatGPTWebModelCatalogCacheEntry)
	if !ok || !okEntry || entry == nil {
		t.Fatalf("catalog entry = %#v", cached)
	}
	if entry.RuntimeInstanceID != current.RuntimeInstanceID() ||
		entry.CredentialIdentity != chatGPTWebCatalogCredentialIdentity(current) {
		t.Fatalf("migrated catalog entry = %#v", entry)
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); !containsRegisteredModel(models, "remote-only-model") {
		t.Fatalf("catalog after failed refresh = %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogFailurePreservesModelCooldown(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}}, nil)
	service.syncAuthModels(context.Background(), auth.ID)

	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current, _ := service.coreManager.GetByID(auth.ID)
	current.ModelStates = map[string]*coreauth.ModelState{
		"remote-model": {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed model cooldown: %v", err)
	}
	executor.set(nil, errors.New("catalog unavailable"))

	service.syncAuthModels(context.Background(), auth.ID)

	updated, _ := service.coreManager.GetByID(auth.ID)
	state := updated.ModelStates["remote-model"]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("model cooldown after failed refresh = %#v", state)
	}
}

func TestServiceSyncChatGPTWebCatalogSuccessPreservesOverlappingModelCooldown(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}, {Slug: "removed-model"}}, nil)
	service.syncAuthModels(context.Background(), auth.ID)

	cooldownUntil := time.Now().Add(time.Hour).UTC()
	current, _ := service.coreManager.GetByID(auth.ID)
	current.ModelStates = map[string]*coreauth.ModelState{
		"remote-model": {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
		"removed-model": {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: cooldownUntil,
		},
	}
	if _, err := service.coreManager.Update(context.Background(), current); err != nil {
		t.Fatalf("seed model cooldowns: %v", err)
	}
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}, {Slug: "new-model"}}, nil)

	service.syncAuthModels(context.Background(), auth.ID)

	updated, _ := service.coreManager.GetByID(auth.ID)
	state := updated.ModelStates["remote-model"]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(cooldownUntil) {
		t.Fatalf("overlapping model cooldown = %#v", state)
	}
	if _, exists := updated.ModelStates["removed-model"]; exists {
		t.Fatalf("removed model cooldown remained: %#v", updated.ModelStates)
	}
}

func TestServiceSyncChatGPTWebCatalogRefreshesSchedulerBeforeStatePersistence(t *testing.T) {
	store := &chatGPTWebCatalogBlockingStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	executor := &chatGPTWebCatalogTestExecutor{executed: make(chan string, 1)}
	manager := coreauth.NewManager(store, nil, nil)
	manager.RegisterExecutor(executor)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-scheduler-before-persist")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	executor.set([]chatgptwebauth.CatalogModel{{Slug: "removed-model"}}, nil)
	service.syncAuthModels(context.Background(), auth.ID)

	current, ok := manager.GetByID(auth.ID)
	if !ok {
		t.Fatal("registered auth missing")
	}
	current.ModelStates = map[string]*coreauth.ModelState{
		"removed-model": {
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: time.Now().Add(time.Hour),
		},
	}
	if _, errUpdate := manager.Update(context.Background(), current); errUpdate != nil {
		t.Fatalf("seed removed model state: %v", errUpdate)
	}

	store.enable()
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "new-model"}}, nil)
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		service.syncAuthModels(context.Background(), auth.ID)
	}()
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("model state persistence did not block")
	}

	transitionLockDone := make(chan struct{})
	go func() {
		unlockTransition := service.lockAuthModelTransition(auth.ID)
		unlockTransition()
		close(transitionLockDone)
	}()
	select {
	case <-transitionLockDone:
	case <-time.After(time.Second):
		close(store.release)
		<-syncDone
		t.Fatal("model state persistence held the auth model transition lock")
	}

	executeDone := make(chan error, 1)
	go func() {
		_, errExecute := manager.Execute(
			context.Background(),
			[]string{chatgptwebauth.Provider},
			cliproxyexecutor.Request{Model: "new-model"},
			cliproxyexecutor.Options{},
		)
		executeDone <- errExecute
	}()
	select {
	case model := <-executor.executed:
		if model != "new-model" {
			t.Fatalf("executed model = %q", model)
		}
	case <-time.After(time.Second):
		close(store.release)
		<-syncDone
		t.Fatal("new model was not schedulable before state persistence completed")
	}

	close(store.release)
	select {
	case <-syncDone:
	case <-time.After(time.Second):
		t.Fatal("catalog sync did not finish after persistence resumed")
	}
	select {
	case errExecute := <-executeDone:
		if errExecute != nil {
			t.Fatalf("execute new model: %v", errExecute)
		}
	case <-time.After(time.Second):
		t.Fatal("new model execution did not finish")
	}
}

func TestServiceSyncChatGPTWebCatalogDoesNotReuseReplacedRuntimeOnFailure(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "old-account-model"}}, nil)
	service.syncAuthModels(context.Background(), auth.ID)

	replacement, err := service.coreManager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("replace auth: %v", err)
	}
	if replacement.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		t.Fatal("replacement auth reused old runtime instance")
	}
	executor.set(nil, errors.New("catalog unavailable"))

	service.syncAuthModels(context.Background(), replacement.ID)

	models := registry.GetGlobalRegistry().GetModelsForClient(replacement.ID)
	if containsRegisteredModel(models, "old-account-model") {
		t.Fatalf("old runtime catalog remained registered: %v", registeredModelIDs(models))
	}
	if !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("replacement fallback catalog missing image model: %v", registeredModelIDs(models))
	}
	if _, exists := service.chatGPTWebModelCatalog.Load(replacement.ID); exists {
		t.Fatal("old runtime catalog remained cached")
	}
}

func TestServiceSyncChatGPTWebCatalogAcceptsAuthoritativeEmptyCatalog(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{}, nil)

	service.syncAuthModels(context.Background(), auth.ID)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if len(models) != 1 || models[0].ID != "gpt-image-2" {
		t.Fatalf("registered models = %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogRejectsStaleAuthGeneration(t *testing.T) {
	executor := &chatGPTWebCatalogTestExecutor{
		models:  []chatgptwebauth.CatalogModel{{Slug: "stale-model"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-stale-catalog")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("old-account-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("old-account-model", "", 0, ""),
	})
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.syncAuthModels(context.Background(), auth.ID)
	}()
	<-executor.started

	replacement := &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}
	service.applyCoreAuthAddOrUpdate(context.Background(), replacement)
	current, ok := manager.GetByID(auth.ID)
	if !ok {
		close(executor.release)
		<-done
		t.Fatal("replacement auth was not installed")
	}
	if current.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		close(executor.release)
		<-done
		t.Fatal("replacement auth reused the old runtime instance")
	}
	modelsBeforeRelease := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(modelsBeforeRelease, "old-account-model") {
		close(executor.release)
		<-done
		t.Fatalf("old model remained while stale fetch was blocked: %v", registeredModelIDs(modelsBeforeRelease))
	}
	if !containsRegisteredModel(modelsBeforeRelease, "gpt-image-2") {
		close(executor.release)
		<-done
		t.Fatalf("replacement fallback missing while stale fetch was blocked: %v", registeredModelIDs(modelsBeforeRelease))
	}
	close(executor.release)
	<-done

	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("stale catalog result populated the cache")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); containsRegisteredModel(models, "stale-model") {
		t.Fatalf("stale model was registered: %v", registeredModelIDs(models))
	}
}

func TestServiceAuthReplacementWaitsForValidatedChatGPTWebCatalogCommit(t *testing.T) {
	executor := &chatGPTWebCatalogTestExecutor{
		models: []chatgptwebauth.CatalogModel{{Slug: "old-account-model"}},
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncCancel:  cancel,
		modelSyncQueue:   make(chan string, 1),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-atomic-catalog-replacement")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	commitValidated := make(chan struct{})
	releaseCommit := make(chan struct{})
	var observeOnce sync.Once
	service.chatGPTWebCatalogCommitObserved = func(*coreauth.Auth) {
		observeOnce.Do(func() {
			close(commitValidated)
			<-releaseCommit
		})
	}
	syncDone := make(chan struct{})
	go func() {
		service.syncAuthModels(ctx, auth.ID)
		close(syncDone)
	}()
	select {
	case <-commitValidated:
	case <-time.After(time.Second):
		t.Fatal("catalog commit was not observed")
	}

	replacementDone := make(chan struct{})
	go func() {
		service.applyCoreAuthAddOrUpdate(ctx, &coreauth.Auth{
			ID:       auth.ID,
			Provider: chatgptwebauth.Provider,
			Status:   coreauth.StatusActive,
			Metadata: map[string]any{
				"access_token":    "replacement-token",
				"account_id":      "replacement-account",
				"lifecycle_state": coreauth.LifecycleStateActive,
			},
		})
		close(replacementDone)
	}()

	select {
	case <-replacementDone:
		close(releaseCommit)
		<-syncDone
		t.Fatal("replacement completed before the validated catalog commit")
	case <-time.After(25 * time.Millisecond):
	}
	currentBeforeRelease, ok := manager.GetByID(auth.ID)
	if !ok || currentBeforeRelease.RuntimeInstanceID() != auth.RuntimeInstanceID() {
		close(releaseCommit)
		<-syncDone
		t.Fatal("replacement entered the manager before the catalog commit was released")
	}

	close(releaseCommit)
	select {
	case <-syncDone:
	case <-time.After(time.Second):
		t.Fatal("catalog sync did not complete")
	}
	select {
	case <-replacementDone:
	case <-time.After(time.Second):
		t.Fatal("replacement did not resume after the catalog commit")
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current.RuntimeInstanceID() == auth.RuntimeInstanceID() {
		t.Fatal("replacement auth was not installed")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "old-account-model") {
		t.Fatalf("old account model remained after replacement: %v", registeredModelIDs(models))
	}
	if !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("replacement fallback model is missing: %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogCancelsRetiredRuntimeFetch(t *testing.T) {
	executor := &chatGPTWebCatalogTestExecutor{
		models:  []chatgptwebauth.CatalogModel{{Slug: "stale-model"}},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-retired-fetch")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.syncAuthModels(context.Background(), auth.ID)
	}()
	<-executor.started

	if _, err := manager.Update(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}); err != nil {
		t.Fatalf("replace auth: %v", err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		close(executor.release)
		t.Fatal("retired auth did not cancel its model catalog request")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); containsRegisteredModel(models, "stale-model") {
		t.Fatalf("retired catalog result was registered: %v", registeredModelIDs(models))
	}
}

func TestServiceSyncChatGPTWebCatalogSerializesSameAuthFetches(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.started = make(chan struct{})
	executor.release = make(chan struct{})
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}}, nil)

	var workers sync.WaitGroup
	workers.Add(2)
	for range 2 {
		go func() {
			defer workers.Done()
			service.syncAuthModels(context.Background(), auth.ID)
		}()
	}
	<-executor.started
	time.Sleep(20 * time.Millisecond)
	if got := executor.maxConcurrent(); got != 1 {
		close(executor.release)
		workers.Wait()
		t.Fatalf("concurrent catalog fetches = %d, want 1", got)
	}
	close(executor.release)
	workers.Wait()
	if calls := executor.callCount(); calls != 2 {
		t.Fatalf("catalog calls = %d, want 2 serialized calls", calls)
	}
	if got := executor.maxConcurrent(); got != 1 {
		t.Fatalf("maximum concurrent catalog fetches = %d, want 1", got)
	}
}

func TestServiceRetiredChatGPTWebFetchLockSerializesImmediateReuse(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.started = make(chan struct{})
	executor.release = make(chan struct{})
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-model"}}, nil)

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		service.syncAuthModels(context.Background(), auth.ID)
	}()
	<-executor.started
	service.retireChatGPTWebModelFetchLock(auth.ID)
	go func() {
		defer workers.Done()
		service.syncAuthModels(context.Background(), auth.ID)
	}()
	time.Sleep(20 * time.Millisecond)
	if got := executor.maxConcurrent(); got != 1 {
		close(executor.release)
		workers.Wait()
		t.Fatalf("concurrent catalog fetches after lock retirement = %d", got)
	}
	close(executor.release)
	workers.Wait()

	service.chatGPTWebModelFetchMu.Lock()
	_, retained := service.chatGPTWebModelFetchLocks[auth.ID]
	service.chatGPTWebModelFetchMu.Unlock()
	if retained {
		t.Fatal("retired idle fetch lock remained cached")
	}
}

func TestServiceRetiredChatGPTWebFetchLockCreatedAfterRetirementIsRemoved(t *testing.T) {
	service := &Service{}
	const authID = "chatgpt-web-retired-before-lock"

	service.retireChatGPTWebModelFetchLock(authID)
	unlock := service.lockChatGPTWebModelFetch(authID)
	unlock()

	service.chatGPTWebModelFetchMu.Lock()
	_, retained := service.chatGPTWebModelFetchLocks[authID]
	service.chatGPTWebModelFetchMu.Unlock()
	if retained {
		t.Fatal("fetch lock created after retirement remained cached")
	}
}

func TestServiceReconcileStaleChatGPTWebCatalogRemovesPostDeleteRegistration(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-post-commit-delete")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("stale-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("stale-model", "", 0, ""),
	})
	if err := manager.Delete(coreauth.WithSkipPersist(context.Background()), auth.ID); err != nil {
		t.Fatalf("delete auth: %v", err)
	}

	unlockTransition := service.lockAuthModelTransition(auth.ID)
	service.reconcileStaleChatGPTWebCatalogLocked(context.Background(), auth.ID, nil)
	unlockTransition()

	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("post-delete catalog remained cached")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("post-delete models = %v", registeredModelIDs(models))
	}
}

func TestServiceReconcileStaleChatGPTWebCatalogFallsBackInlineWhenQueueFull(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-after-stale"}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncQueue <- "occupied"
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	unlockTransition := service.lockAuthModelTransition(auth.ID)
	expected, action, syncInline := service.reconcileStaleChatGPTWebCatalogLocked(ctx, auth.ID, auth)
	unlockTransition()
	if !syncInline {
		t.Fatal("full model sync queue did not request inline fallback")
	}
	service.applyChatGPTWebRegistryState(ctx, expected, action)
	service.runChatGPTWebCatalogSyncInline(ctx, expected, syncInline)

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "remote-after-stale") {
		t.Fatalf("models after inline fallback = %v", registeredModelIDs(models))
	}
	if queuedID := <-service.modelSyncQueue; queuedID != "occupied" {
		t.Fatalf("queued auth ID = %q, want occupied", queuedID)
	}
}

func TestServiceStaleChatGPTWebFetchFallsBackInlineWithoutFetchLockDeadlock(t *testing.T) {
	service, auth, executor := newChatGPTWebCatalogTestService(t)
	executor.started = make(chan struct{})
	executor.release = make(chan struct{})
	executor.set([]chatgptwebauth.CatalogModel{{Slug: "remote-after-replacement"}}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncQueue <- "occupied"
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	syncDone := make(chan struct{})
	go func() {
		service.syncAuthModels(ctx, auth.ID)
		close(syncDone)
	}()
	<-executor.started

	replacement := auth.Clone()
	replacement.Metadata["access_token"] = "replacement-token"
	installed, current, errUpdate := service.coreManager.UpdateIfCurrent(context.Background(), auth, replacement)
	if errUpdate != nil || !current || installed == nil {
		close(executor.release)
		t.Fatalf("install same-account refresh = (%v, %v, %v)", installed, current, errUpdate)
	}
	if installed.RuntimeInstanceID() != auth.RuntimeInstanceID() {
		close(executor.release)
		t.Fatal("same-account refresh replaced the runtime instance")
	}
	close(executor.release)

	select {
	case <-syncDone:
	case <-time.After(time.Second):
		t.Fatal("stale catalog inline fallback deadlocked on the fetch lock")
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "remote-after-replacement") {
		t.Fatalf("models after stale fetch fallback = %v", registeredModelIDs(models))
	}
	if queuedID := <-service.modelSyncQueue; queuedID != "occupied" {
		t.Fatalf("queued auth ID = %q, want occupied", queuedID)
	}
}

func TestServiceStaleRegistryActionDoesNotClearReplacementModelState(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	original := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-stale-registry-action")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(original.ID) })

	retryAt := time.Now().Add(time.Hour)
	replacement := original.CloneWithoutRuntimeInstance()
	if replacement.Attributes == nil {
		replacement.Attributes = make(map[string]string)
	}
	replacement.Attributes[coreauth.SourceHashAttributeKey] = "replacement-source"
	replacement.ModelStates = map[string]*coreauth.ModelState{
		"gpt-5": {
			Unavailable:    true,
			NextRetryAfter: retryAt,
		},
	}
	installed, err := manager.Update(
		coreauth.WithSkipPersist(coreauth.WithSkipStateCarryForward(context.Background())),
		replacement,
	)
	if err != nil {
		t.Fatalf("update replacement: %v", err)
	}
	GlobalModelRegistry().RegisterClient(installed.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("gpt-5", "", 0, ""),
	})

	service.applyChatGPTWebRegistryState(context.Background(), original, chatGPTWebRegistryStateReconcile)

	current, _ := manager.GetByID(installed.ID)
	state := current.ModelStates["gpt-5"]
	if state == nil || !state.Unavailable || !state.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("replacement model state was cleared by stale action: %#v", state)
	}
}

func TestServiceChatGPTWebDeleteCleanupKeepsReplacementGenerationResources(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-delete-replacement")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	removedRuntimeInstanceID := auth.RuntimeInstanceID()

	if err := manager.Delete(coreauth.WithSkipPersist(context.Background()), auth.ID); err != nil {
		t.Fatalf("delete old auth: %v", err)
	}
	replacement, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			coreauth.SourceHashAttributeKey: "replacement-generation",
		},
		Metadata: map[string]any{
			"access_token":    "replacement-token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register replacement auth: %v", err)
	}
	if replacement.RuntimeInstanceID() == removedRuntimeInstanceID {
		t.Fatal("replacement reused the deleted runtime instance")
	}
	replacementEntry := &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: replacement.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("replacement-model", "", 0, "")},
	}
	service.chatGPTWebModelCatalog.Store(auth.ID, replacementEntry)
	service.chatGPTWebModelFetchLocks = map[string]*chatGPTWebModelFetchLockEntry{
		auth.ID: {},
	}
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, replacementEntry.Models)

	service.cleanupChatGPTWebModelResourcesAfterDelete(auth.ID, removedRuntimeInstanceID)

	cached, exists := service.chatGPTWebModelCatalog.Load(auth.ID)
	if !exists || cached != replacementEntry {
		t.Fatalf("replacement model catalog was removed: %#v", cached)
	}
	if _, exists := service.chatGPTWebModelFetchLocks[auth.ID]; !exists {
		t.Fatal("replacement model fetch lock was retired")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); !containsRegisteredModel(models, "replacement-model") {
		t.Fatalf("replacement model registration was removed: %v", registeredModelIDs(models))
	}
}

func TestServiceMissingAuthSyncKeepsConcurrentReplacementResources(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "chatgpt-web-missing-sync-replacement"
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	unlockTransition := service.lockAuthModelTransition(authID)
	syncDone := make(chan struct{})
	go func() {
		defer close(syncDone)
		service.syncAuthModels(context.Background(), authID)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		service.authModelTransitionMu.Lock()
		entry := service.authModelTransitionLocks[authID]
		waiting := entry != nil && entry.references >= 2
		service.authModelTransitionMu.Unlock()
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			unlockTransition()
			t.Fatal("missing-auth sync did not wait on the model transition lock")
		}
		time.Sleep(time.Millisecond)
	}

	replacement, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"email":           "replacement@example.com",
			"access_token":    "replacement-token",
			"account_id":      "replacement-account",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		unlockTransition()
		t.Fatalf("register replacement: %v", err)
	}
	replacementEntry := &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID:   replacement.RuntimeInstanceID(),
		CredentialIdentity:  chatGPTWebCatalogCredentialIdentity(replacement),
		CredentialReference: coreauth.NewChatGPTWebCredentialReference(replacement),
		Models:              []*registry.ModelInfo{chatGPTWebTextModelInfo("replacement-model", "", 0, "")},
	}
	service.chatGPTWebModelCatalog.Store(authID, replacementEntry)
	GlobalModelRegistry().RegisterClient(authID, chatgptwebauth.Provider, replacementEntry.Models)
	unlockTransition()

	select {
	case <-syncDone:
	case <-time.After(5 * time.Second):
		t.Fatal("missing-auth sync did not finish")
	}
	cached, exists := service.chatGPTWebModelCatalog.Load(authID)
	if !exists || cached == nil {
		t.Fatal("concurrent replacement catalog was removed")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); !containsRegisteredModel(models, "replacement-model") {
		t.Fatalf("concurrent replacement models = %v", registeredModelIDs(models))
	}
}

func TestServiceChatGPTWebDeleteCleanupRemovesStaleGenerationWithoutReplacement(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	authID := "chatgpt-web-delete-stale-generation"
	entry := &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: "older-runtime-generation",
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("stale-model", "", 0, "")},
	}
	service.chatGPTWebModelCatalog.Store(authID, entry)
	service.chatGPTWebModelFetchLocks = map[string]*chatGPTWebModelFetchLockEntry{
		authID: {},
	}
	GlobalModelRegistry().RegisterClient(authID, chatgptwebauth.Provider, entry.Models)
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	service.cleanupChatGPTWebModelResourcesAfterDelete(authID, "newer-deleted-generation")

	if _, exists := service.chatGPTWebModelCatalog.Load(authID); exists {
		t.Fatal("stale model catalog was retained after auth deletion")
	}
	if _, exists := service.chatGPTWebModelFetchLocks[authID]; exists {
		t.Fatal("stale model fetch lock was retained after auth deletion")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("stale model registration was retained: %v", registeredModelIDs(models))
	}
}

func TestApplyCoreAuthUpdateRebuildsModelsOnChatGPTWebProviderChange(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncCancel:  cancel,
		modelSyncQueue:   make(chan string, 1),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-provider-change")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("old-web-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("old-web-model", "", 0, ""),
	})

	service.applyCoreAuthAddOrUpdate(context.Background(), &coreauth.Auth{
		ID:       auth.ID,
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})

	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "old-web-model") {
		t.Fatalf("old ChatGPT Web model remained after provider change: %v", registeredModelIDs(models))
	}
	if len(models) == 0 || !containsRegisteredModel(models, registry.GetClaudeModels()[0].ID) {
		t.Fatalf("replacement provider models were not registered immediately: %v", registeredModelIDs(models))
	}
}

func TestApplyCoreAuthUpdateClearsDisabledChatGPTWebCatalog(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncQueue:   make(chan string, 1),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-watcher-disabled")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("remote-model", "", 0, ""),
	})

	disabled := auth.Clone()
	disabled.Disabled = true
	disabled.Status = coreauth.StatusDisabled
	service.applyCoreAuthAddOrUpdate(context.Background(), disabled)

	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("watcher-disabled auth retained its model catalog")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("watcher-disabled auth retained models: %v", registeredModelIDs(models))
	}
}

func TestAuthMaintenanceHookQueuesChatGPTWebModelSync(t *testing.T) {
	service := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 1)
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	authMaintenanceHook{service: service}.OnAuthUpdated(ctx, &coreauth.Auth{
		ID:       "chatgpt-web-model-sync-hook",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{"lifecycle_state": coreauth.LifecycleStateActive},
	})

	select {
	case authID := <-service.modelSyncQueue:
		if authID != "chatgpt-web-model-sync-hook" {
			t.Fatalf("queued auth ID = %q", authID)
		}
	default:
		t.Fatal("expected ChatGPT Web auth update to queue model sync")
	}
}

func TestInstallAuthMaintenanceHookSyncsLoadedChatGPTWebModels(t *testing.T) {
	const authID = "chatgpt-web-loaded-model-sync"
	store := &chatGPTWebCatalogBlockingStore{auths: []*coreauth.Auth{{
		ID:       authID,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"access_token":    "opaque-token",
			"refresh_token":   "opaque-refresh",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}}}
	manager := coreauth.NewManager(store, nil, nil)
	if err := manager.Load(t.Context()); err != nil {
		t.Fatalf("load auths: %v", err)
	}
	_, cancel := context.WithCancel(t.Context())
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncCancel:  cancel,
		modelSyncQueue:   make(chan string, 1),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	t.Cleanup(func() {
		cancel()
		_ = manager.CloseExecutors()
	})
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(authID) })

	if models := registry.GetGlobalRegistry().GetModelsForClient(authID); len(models) != 0 {
		t.Fatalf("models before hook installation = %v", registeredModelIDs(models))
	}
	service.installAuthMaintenanceHook(t.Context())

	models := registry.GetGlobalRegistry().GetModelsForClient(authID)
	if !containsRegisteredModel(models, chatGPTWebFallbackModelIDs[0]) ||
		!containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("loaded auth models = %v", registeredModelIDs(models))
	}
	select {
	case queuedID := <-service.modelSyncQueue:
		if queuedID != authID {
			t.Fatalf("queued auth ID = %q, want %q", queuedID, authID)
		}
	default:
		t.Fatal("loaded auth did not queue a remote catalog refresh")
	}
}

func TestAuthMaintenanceHookDisablesChatGPTWebModelsAndCatalog(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	manager.AddHook(authMaintenanceHook{service: service})
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-disabled-models")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	service.chatGPTWebModelCatalog.Store(auth.ID, &chatGPTWebModelCatalogCacheEntry{
		RuntimeInstanceID: auth.RuntimeInstanceID(),
		Models:            []*registry.ModelInfo{chatGPTWebTextModelInfo("remote-model", "", 0, "")},
	})
	GlobalModelRegistry().RegisterClient(auth.ID, chatgptwebauth.Provider, []*registry.ModelInfo{
		chatGPTWebTextModelInfo("remote-model", "", 0, ""),
	})

	disabled := auth.Clone()
	disabled.Disabled = true
	disabled.Status = coreauth.StatusDisabled
	if _, err := manager.Update(context.Background(), disabled); err != nil {
		t.Fatalf("disable auth: %v", err)
	}

	if _, exists := service.chatGPTWebModelCatalog.Load(auth.ID); exists {
		t.Fatal("disabled auth retained its model catalog")
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); len(models) != 0 {
		t.Fatalf("disabled auth retained registered models: %v", registeredModelIDs(models))
	}
}

func TestAuthMaintenanceHookRebuildsDirectProviderChanges(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		cfg:              &config.Config{},
		coreManager:      manager,
		modelSyncCancel:  cancel,
		modelSyncQueue:   make(chan string, 2),
		modelSyncPending: make(map[string]modelSyncTaskState),
	}
	manager.AddHook(authMaintenanceHook{service: service})
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "direct-provider-change",
		Provider: "claude",
		Status:   coreauth.StatusActive,
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	web := auth.Clone()
	web.Provider = chatgptwebauth.Provider
	web.Metadata = map[string]any{
		"access_token":    "token",
		"lifecycle_state": coreauth.LifecycleStateActive,
	}
	if _, err = manager.Update(context.Background(), web); err != nil {
		t.Fatalf("switch to ChatGPT Web: %v", err)
	}
	if registered, ok := manager.Executor(chatgptwebauth.Provider); !ok {
		t.Fatal("ChatGPT Web executor was not registered")
	} else if _, ok = registered.(*runtimeexecutor.ChatGPTWebExecutor); !ok {
		t.Fatalf("ChatGPT Web executor type = %T", registered)
	}
	if models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID); !containsRegisteredModel(models, "gpt-image-2") {
		t.Fatalf("ChatGPT Web fallback models = %v", registeredModelIDs(models))
	}

	claude := web.Clone()
	claude.Provider = "claude"
	claude.Metadata = nil
	if _, err = manager.Update(context.Background(), claude); err != nil {
		t.Fatalf("switch to Claude: %v", err)
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if containsRegisteredModel(models, "gpt-image-2") ||
		!containsRegisteredModel(models, registry.GetClaudeModels()[0].ID) {
		t.Fatalf("Claude models after direct switch = %v", registeredModelIDs(models))
	}
}

func TestAuthMaintenanceHookResetsOverlappingModelStateOnProviderChange(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{
		cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
			Name:    "new-provider",
			BaseURL: "https://new-provider.example",
			Models: []config.OpenAICompatibilityModel{{
				Name:  "shared-model",
				Alias: "shared-model",
			}},
		}}},
		coreManager: manager,
	}
	cooldownUntil := time.Now().Add(time.Hour)
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "direct-overlapping-provider-change",
		Provider: "old-provider",
		Status:   coreauth.StatusActive,
		ModelStates: map[string]*coreauth.ModelState{
			"shared-model": {
				Status:         coreauth.StatusError,
				Unavailable:    true,
				NextRetryAfter: cooldownUntil,
			},
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	GlobalModelRegistry().RegisterClient(auth.ID, "old-provider", []*registry.ModelInfo{{ID: "shared-model"}})
	manager.AddHook(authMaintenanceHook{service: service})

	replacement := auth.Clone()
	replacement.Provider = "new-provider"
	if _, err = manager.Update(context.Background(), replacement); err != nil {
		t.Fatalf("change provider: %v", err)
	}

	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("updated auth missing")
	}
	if state := current.ModelStates["shared-model"]; state != nil &&
		(state.Unavailable || !state.NextRetryAfter.IsZero() || state.Status == coreauth.StatusError) {
		t.Fatalf("overlapping model state survived provider change: %#v", state)
	}
	if got := registry.GetGlobalRegistry().GetProviderForClient(auth.ID); got != "new-provider" {
		t.Fatalf("registered provider = %q", got)
	}
}

func TestServiceSyncChatGPTWebModelsSkipsLegacyCompatibilityCatalog(t *testing.T) {
	executor := &chatGPTWebCatalogTestExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	service := &Service{
		cfg: &config.Config{OpenAICompatibility: []config.OpenAICompatibility{{
			Name: "chatgpt-web",
			Models: []config.OpenAICompatibilityModel{{
				Name:  "legacy-upstream-model",
				Alias: "legacy-client-model",
			}},
		}}},
		coreManager: manager,
	}
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "legacy-chatgpt-web-catalog",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"compat_name":  "chatgpt-web",
			"provider_key": "openai-compatibility-chatgpt-web",
		},
	})
	if err != nil {
		t.Fatalf("register legacy auth: %v", err)
	}
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })

	service.syncAuthModels(context.Background(), auth.ID)

	if calls := executor.callCount(); calls != 0 {
		t.Fatalf("legacy compatibility auth fetched native catalog %d time(s)", calls)
	}
	models := registry.GetGlobalRegistry().GetModelsForClient(auth.ID)
	if !containsRegisteredModel(models, "legacy-client-model") {
		t.Fatalf("legacy compatibility models were not registered: %v", registeredModelIDs(models))
	}
}

func TestServiceRefreshChatGPTWebModelCatalogsQueuesOnlyActiveNativeAuths(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.modelSyncCancel = cancel
	service.modelSyncQueue = make(chan string, 4)
	service.modelSyncPending = make(map[string]modelSyncTaskState)

	active := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-config-reload")
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-config-reload-compat",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"compat_name":  "chatgpt-web",
			"provider_key": "openai-compatibility-chatgpt-web",
		},
	}); err != nil {
		t.Fatalf("register legacy auth: %v", err)
	}
	if _, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       "chatgpt-web-config-reload-disabled",
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusDisabled,
		Disabled: true,
		Metadata: map[string]any{
			"access_token":    "token",
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	}); err != nil {
		t.Fatalf("register disabled auth: %v", err)
	}

	service.refreshChatGPTWebModelCatalogs(context.Background())

	select {
	case authID := <-service.modelSyncQueue:
		if authID != active.ID {
			t.Fatalf("queued auth ID = %q, want %q", authID, active.ID)
		}
	default:
		t.Fatal("active native ChatGPT Web auth was not queued after config reload")
	}
	select {
	case authID := <-service.modelSyncQueue:
		t.Fatalf("unexpected extra catalog sync for %q", authID)
	default:
	}
}

func TestServiceRefreshChatGPTWebModelRegistrationUsesAuthTransitionLock(t *testing.T) {
	service, auth, _ := newChatGPTWebCatalogTestService(t)
	unlockTransition := service.lockAuthModelTransition(auth.ID)
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		service.refreshChatGPTWebModelRegistration(context.Background(), auth)
		close(done)
	}()
	<-started
	select {
	case <-done:
		unlockTransition()
		t.Fatal("model registration bypassed the auth transition lock")
	case <-time.After(25 * time.Millisecond):
	}
	unlockTransition()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("model registration did not resume after auth transition lock release")
	}
}

func newChatGPTWebCatalogTestService(t *testing.T) (*Service, *coreauth.Auth, *chatGPTWebCatalogTestExecutor) {
	t.Helper()
	executor := &chatGPTWebCatalogTestExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	service := &Service{cfg: &config.Config{}, coreManager: manager}
	auth := registerChatGPTWebCatalogTestAuth(t, manager, "chatgpt-web-catalog-sync")
	t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(auth.ID) })
	return service, auth, executor
}

func registerChatGPTWebCatalogTestAuth(t *testing.T, manager *coreauth.Manager, id string) *coreauth.Auth {
	t.Helper()
	auth, err := manager.Register(context.Background(), &coreauth.Auth{
		ID:       id,
		Provider: chatgptwebauth.Provider,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"email":           id + "@example.com",
			"access_token":    "token",
			"account_id":      id,
			"lifecycle_state": coreauth.LifecycleStateActive,
		},
	})
	if err != nil {
		t.Fatalf("register auth: %v", err)
	}
	return auth
}
