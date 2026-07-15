package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
	"golang.org/x/net/context"
)

func TestRestrictExecutionProviders(t *testing.T) {
	ctx := providerRestrictedContext(t, "codex,xai")
	providers, errRestricted := restrictExecutionProviders(ctx, []string{"claude", "xai", "codex"})
	if errRestricted != nil {
		t.Fatalf("restrictExecutionProviders() error = %v", errRestricted.Error)
	}
	if got := strings.Join(providers, ","); got != "xai,codex" {
		t.Fatalf("providers = %q, want xai,codex", got)
	}

	providers, errRestricted = restrictExecutionProviders(ctx, []string{"claude"})
	if len(providers) != 0 || errRestricted == nil || errRestricted.StatusCode != http.StatusForbidden {
		t.Fatalf("restricted result = %#v, %#v", providers, errRestricted)
	}
	body := string(BuildErrorResponseBody(errRestricted.StatusCode, errRestricted.Error.Error()))
	if !strings.Contains(body, `"code":"provider_not_allowed"`) {
		t.Fatalf("error body = %s, want provider_not_allowed", body)
	}
}

func TestProviderRestrictionRunsBeforeAllAuthManagerExecutionPaths(t *testing.T) {
	modelRegistry := registry.GetGlobalRegistry()
	clientID := "provider-group-xai-only"
	modelID := "provider-group-xai-model"
	modelRegistry.RegisterClient(clientID, "xai", []*registry.ModelInfo{{ID: modelID}})
	defer modelRegistry.UnregisterClient(clientID)

	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	ctx := providerRestrictedContext(t, "codex")
	payload := []byte(`{"model":"provider-group-xai-model"}`)

	_, _, nonStreamErr := handler.ExecuteWithAuthManager(ctx, "openai", modelID, payload, "")
	assertProviderNotAllowed(t, nonStreamErr)
	_, _, explicitErr := handler.ExecuteWithProviders(ctx, []string{"xai"}, "openai", modelID, payload, "")
	assertProviderNotAllowed(t, explicitErr)
	_, _, countErr := handler.ExecuteCountWithAuthManager(ctx, "gemini", modelID, payload, "")
	assertProviderNotAllowed(t, countErr)
	_, _, streamErrors := handler.ExecuteStreamWithAuthManager(ctx, "openai", modelID, payload, "")
	assertProviderNotAllowed(t, <-streamErrors)
	_, _, explicitStreamErrors := handler.ExecuteStreamWithProviders(ctx, []string{"xai"}, "openai", modelID, payload, "")
	assertProviderNotAllowed(t, <-explicitStreamErrors)
}

func TestFilterModelsByProviderAccess(t *testing.T) {
	modelRegistry := registry.GetGlobalRegistry()
	registrations := []struct {
		client   string
		provider string
		model    string
	}{
		{client: "provider-group-codex", provider: "codex", model: "provider-group-codex-model"},
		{client: "provider-group-xai", provider: "xai", model: "provider-group-xai-model-list"},
		{client: "provider-group-shared-codex", provider: "codex", model: "provider-group-shared-model"},
		{client: "provider-group-shared-xai", provider: "xai", model: "provider-group-shared-model"},
	}
	for _, registration := range registrations {
		modelRegistry.RegisterClient(registration.client, registration.provider, []*registry.ModelInfo{{ID: registration.model}})
		defer modelRegistry.UnregisterClient(registration.client)
	}

	ginContext := restrictedGinContext(t, "codex")
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	models := []map[string]any{
		{"id": "provider-group-codex-model"},
		{"id": "provider-group-xai-model-list"},
		{"name": "models/provider-group-shared-model"},
		{"id": "provider-group-unknown-model"},
	}
	filtered := handler.FilterModelsByProviderAccess(ginContext, models)
	if len(filtered) != 2 || filtered[0]["id"] != "provider-group-codex-model" || filtered[1]["name"] != "models/provider-group-shared-model" {
		t.Fatalf("filtered models = %#v", filtered)
	}
}

func providerRestrictedContext(t *testing.T, providers string) context.Context {
	t.Helper()
	ginContext := restrictedGinContext(t, providers)
	return context.WithValue(context.Background(), "gin", ginContext)
}

func restrictedGinContext(t *testing.T, providers string) *gin.Context {
	t.Helper()
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Set("accessMetadata", map[string]string{sdkaccess.MetadataAllowedProviders: providers})
	ginContext.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return ginContext
}

func assertProviderNotAllowed(t *testing.T, errMessage *interfaces.ErrorMessage) {
	t.Helper()
	if errMessage == nil || errMessage.StatusCode != http.StatusForbidden {
		t.Fatalf("error = %#v, want provider_not_allowed 403", errMessage)
	}
}
