package auth

import (
	"context"
	"testing"
)

func TestUsageAuthInfosExcludeCredentialSecrets(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	auth := &Auth{
		ID:       "api-key-auth",
		Provider: "openai",
		FileName: "api-key.json",
		Status:   StatusActive,
		Attributes: map[string]string{
			"api_key":       "secret-api-key",
			"session_token": "secret-session",
		},
		Metadata: map[string]any{
			"access_token": "secret-access-token",
		},
	}
	authIndex := auth.EnsureIndex()
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatal(err)
	}

	items := manager.ListUsageAuthInfos()
	if len(items) != 1 {
		t.Fatalf("items len = %d, want 1", len(items))
	}
	if items[0].AuthIndex != authIndex || items[0].AccountType != "api_key" || items[0].Account != "" {
		t.Fatalf("usage auth info = %+v, want index and redacted API key", items[0])
	}
	if got, ok := manager.UsageAuthInfoByIndex(authIndex); !ok || got != items[0] {
		t.Fatalf("UsageAuthInfoByIndex() = %+v, %v; want %+v", got, ok, items[0])
	}
}
