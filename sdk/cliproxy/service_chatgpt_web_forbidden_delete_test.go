package cliproxy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v6/sdk/config"
)

func TestChatGPTWebDeadDeleteRequiresPreservedFailureEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, state, reason, original string
		wantQueue                     bool
	}{
		{"CF recovery", "reauth_required", "auto_relogin_exhausted", "cloudflare_challenge", false},
		{"network recovery", "reauth_required", "auto_relogin_exhausted", "authentication_network_error", false},
		{"session refresh failure", "reauth_required", "session_refresh_failed", "session_refresh_failed", false},
		{"erroneous CF dead", "dead", "cloudflare_challenge", "cloudflare_challenge", false},
		{"erroneous network dead", "dead", "authentication_network_error", "authentication_network_error", false},
		{"unknown forbidden dead", "dead", "session_refresh_forbidden", "session_refresh_forbidden", false},
		{"legacy ambiguous session", "dead", "session_expired", "", false},
		{"wrapped forbidden", "dead", "session_expired", "session_refresh_forbidden", false},
		{"wrapped CF", "dead", "session_expired", "cloudflare_challenge", false},
		{"missing reason", "dead", "", "", false},
		{"explicit deleted", "dead", "account_deleted", "account_deleted", true},
		{"explicit deactivated", "dead", "account_deactivated", "account_deactivated", true},
		{"legacy explicit deleted", "dead", "account_deleted", "", true},
		{"configured passkey rule", "dead", "invalid_passkey_response", "invalid_passkey_response", true},
		{"confirmed session expiry", "dead", "session_expired", "session_expired", true},
		{"confirmed missing token", "dead", "access_token_missing", "access_token_missing", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authDir := t.TempDir()
			store := sdkauth.NewFileTokenStore()
			store.SetBaseDir(authDir)
			auth := chatGPTWebAutoDeleteTestAuth("fixture.json", 0, tc.state)
			auth.Metadata["lifecycle_reason"] = tc.reason
			if tc.original != "" {
				chatgptwebauth.RecordLifecycleFailure(auth.Metadata, &chatgptwebauth.AuthError{Code: tc.original, StatusCode: 403})
			}
			if _, err := store.Save(t.Context(), auth); err != nil {
				t.Fatal(err)
			}
			auths, err := store.List(t.Context())
			if err != nil || len(auths) != 1 {
				t.Fatalf("reload persisted auth: %v / %d", err, len(auths))
			}
			auth = auths[0]
			cfg := &config.Config{AuthDir: authDir}
			cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
			service := &Service{cfg: cfg, coreManager: coreauth.NewManager(store, nil, nil)}
			if _, err := service.coreManager.Register(coreauth.WithSkipPersist(t.Context()), auth); err != nil {
				t.Fatal(err)
			}
			if cfg.AuthMaintenance.Enable {
				t.Fatal("fixture unexpectedly enabled generic maintenance")
			}
			candidates := service.scanAuthMaintenanceCandidatesWithPolicy(
				cfg.AuthMaintenance, service.snapshotChatGPTWebDeadAuthDeletePolicy(), authDir,
			)
			if (len(candidates) == 1) != tc.wantQueue {
				t.Fatalf("scan candidates = %d, want queue=%v", len(candidates), tc.wantQueue)
			}
			service.handleAuthMaintenanceResult(t.Context(), coreauth.Result{
				AuthID: auth.ID, Provider: auth.Provider, Error: &coreauth.Error{Code: tc.original, HTTPStatus: 403},
			})
			if (len(service.maintenanceQueue) == 1) != tc.wantQueue {
				t.Fatalf("event queue = %d, want queue=%v", len(service.maintenanceQueue), tc.wantQueue)
			}
			current, _ := service.coreManager.GetByID(auth.ID)
			if current.LifecycleSelectable() {
				t.Fatal("delete safety check restored an unrecovered credential to routing")
			}
			if _, err := os.Stat(filepath.Join(authDir, "fixture.json")); err != nil {
				t.Fatal("queue-only test removed a credential")
			}
		})
	}
}

func TestChatGPTWebLegacyAmbiguousQueuedDeletionIsCanceledWithoutReactivation(t *testing.T) {
	authDir := t.TempDir()
	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	cfg := &config.Config{AuthDir: authDir}
	cfg.ChatGPTWeb.AutoDeleteDeadAuths = true
	service := &Service{cfg: cfg, coreManager: coreauth.NewManager(store, nil, nil)}
	auth := chatGPTWebAutoDeleteTestAuth("legacy.json", 0, coreauth.LifecycleStateDead)
	auth.Metadata["lifecycle_reason"] = "session_expired"
	auth.FileName = filepath.Join(authDir, auth.ID)
	auth.Attributes["path"] = auth.FileName
	current, err := service.coreManager.Register(t.Context(), auth)
	if err != nil {
		t.Fatal(err)
	}
	candidate, ok := service.authMaintenanceCandidateForAuth(current, authDir, "chatgpt_web_dead_session_expired")
	if !ok || !service.disableAuthMaintenanceCandidate(t.Context(), candidate, true) {
		t.Fatal("could not simulate the old pending-delete marker")
	}
	candidate = snapshotChatGPTWebAutoDeleteCandidate(t, service, candidate, authDir)
	deleted, err := service.deleteAuthMaintenanceCandidate(t.Context(), candidate)
	if err != nil || deleted {
		t.Fatalf("ambiguous queued deletion: deleted=%v err=%v", deleted, err)
	}
	current, _ = service.coreManager.GetByID(auth.ID)
	if current.LifecycleState() != coreauth.LifecycleStateDead || current.LifecycleSelectable() || authMaintenancePendingDelete(current) {
		t.Fatal("canceling deletion removed quarantine or retained the deletion marker")
	}
	raw, err := os.ReadFile(auth.FileName)
	if err != nil {
		t.Fatal(err)
	}
	var metadata map[string]any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		t.Fatal(err)
	}
	if metadata["lifecycle_state"] != "dead" || chatgptwebauth.DeadLifecycleDeletionAllowed(metadata) {
		t.Fatal("persisted quarantine was not preserved")
	}
}
