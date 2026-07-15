package configaccess

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	sdkaccess "github.com/router-for-me/CLIProxyAPI/v6/sdk/access"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestProviderAuthenticateIncludesAllowedProviders(t *testing.T) {
	provider := newProvider("", []string{"restricted", "unrestricted"}, []sdkconfig.APIKeyGroup{
		{APIKey: "restricted", Providers: []string{"Codex", "xAI", "codex"}},
	})

	request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	request.Header.Set("Authorization", "Bearer restricted")
	result, authErr := provider.Authenticate(context.Background(), request)
	if authErr != nil {
		t.Fatalf("Authenticate() error = %v", authErr)
	}
	if got := result.Metadata[sdkaccess.MetadataAllowedProviders]; got != "codex,xai" {
		t.Fatalf("allowed providers = %q, want codex,xai", got)
	}

	request.Header.Set("Authorization", "Bearer unrestricted")
	result, authErr = provider.Authenticate(context.Background(), request)
	if authErr != nil {
		t.Fatalf("Authenticate() unrestricted error = %v", authErr)
	}
	if _, exists := result.Metadata[sdkaccess.MetadataAllowedProviders]; exists {
		t.Fatalf("unrestricted key has provider restriction: %#v", result.Metadata)
	}
}

func TestProviderAuthenticateTreatsAllAsUnrestricted(t *testing.T) {
	provider := newProvider("", []string{"key"}, []sdkconfig.APIKeyGroup{
		{APIKey: "key", Providers: []string{"codex", "all"}},
	})
	request := httptest.NewRequest(http.MethodGet, "/v1/models", nil)
	request.Header.Set("X-Api-Key", "key")
	result, authErr := provider.Authenticate(context.Background(), request)
	if authErr != nil {
		t.Fatalf("Authenticate() error = %v", authErr)
	}
	if _, exists := result.Metadata[sdkaccess.MetadataAllowedProviders]; exists {
		t.Fatalf("all provider marker did not clear restriction: %#v", result.Metadata)
	}
}
