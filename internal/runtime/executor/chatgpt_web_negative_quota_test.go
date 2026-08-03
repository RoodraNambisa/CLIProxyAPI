package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestChatGPTWebAccountInfoRefreshPreservesAuthoritativeNegativeQuota(t *testing.T) {
	resetAt := time.Now().UTC().Add(6 * time.Hour).Truncate(time.Second)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case chatgptwebauth.AccountCheckPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"accounts": map[string]any{
				"default": map[string]any{"account": map[string]any{
					"account_id": "account-negative",
					"plan_type":  "free",
				}},
			}})
		case chatgptwebauth.ConversationInitPath:
			_ = json.NewEncoder(w).Encode(map[string]any{"limits_progress": []any{
				map[string]any{
					"feature_name": "image_gen",
					"remaining":    -2,
					"reset_after":  resetAt.Format(time.RFC3339),
				},
			}})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	manager := cliproxyauth.NewManager(nil, nil, nil)
	auth := chatGPTWebTestAuth("negative-authoritative-quota")
	auth.Metadata["account_id"] = "account-negative"
	auth.Metadata["image_quota_remaining"] = 3
	auth.Metadata["quota_state"] = string(chatgptwebauth.QuotaStateAvailable)
	auth.Metadata["quota_stale"] = true
	auth.Metadata["quota_last_error"] = "invalid_response"
	if _, errRegister := manager.Register(cliproxyauth.WithSkipPersist(context.Background()), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	cfg := &config.Config{ChatGPTWeb: config.ChatGPTWebConfig{AccountInfo: config.ChatGPTWebAccountInfoConfig{
		MaxRetries: accountInfoTestInt(0),
	}}}
	executor := NewChatGPTWebExecutor(cfg, manager)
	executor.runtimeBaseURL = server.URL
	t.Cleanup(func() { _ = executor.Close() })

	outcome := executor.refreshChatGPTWebAccountInfo(context.Background(), auth.ID, true)
	if outcome.status != chatgptwebauth.AccountInfoResultUpdated || outcome.errorCode != "" || outcome.retryable {
		t.Fatalf("account info outcome = %+v", outcome)
	}
	current, ok := manager.GetByID(auth.ID)
	if !ok || current == nil {
		t.Fatal("refreshed auth missing")
	}
	credential, errCredential := chatgptwebauth.ParseCredential(current.Metadata)
	if errCredential != nil {
		t.Fatalf("ParseCredential() error = %v", errCredential)
	}
	if credential.ImageQuotaRemaining == nil || *credential.ImageQuotaRemaining != -2 {
		t.Fatalf("ImageQuotaRemaining = %#v, want -2", credential.ImageQuotaRemaining)
	}
	if credential.QuotaState != chatgptwebauth.QuotaStateExhausted || credential.QuotaStale ||
		credential.QuotaLastError != "" || credential.QuotaUpdatedAt == "" {
		t.Fatalf("authoritative quota = %+v", credential)
	}
	parsedResetAt, errReset := time.Parse(time.RFC3339Nano, credential.ImageQuotaResetAt)
	if errReset != nil || !parsedResetAt.Equal(resetAt) {
		t.Fatalf("image quota reset = %q (%v), want %s", credential.ImageQuotaResetAt, errReset, resetAt)
	}
	if blocked, retryAt := cliproxyauth.ChatGPTWebImageCapabilityUnavailable(current, time.Now()); !blocked || !retryAt.Equal(resetAt) {
		t.Fatalf("image capability = blocked %v retry %s, want true/%s", blocked, retryAt, resetAt)
	}
}
