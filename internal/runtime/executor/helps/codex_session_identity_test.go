package helps

import (
	"testing"

	"github.com/tidwall/gjson"
)

func TestProjectCodexSessionIdentityFillsMissingFieldsAndPreservesMetadata(t *testing.T) {
	payload := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":"hello"}],"client_metadata":{"x-codex-installation-id":"install-1","x-codex-turn-metadata":"{\"workspace\":\"/tmp/project\"}"}}`)
	defaults := CodexSessionIdentity{
		SessionID: "session-default", ThreadID: "thread-default", TurnID: "turn-default",
		WindowID: "thread-default:0", RequestKind: "turn",
	}

	projected, identity, turnMetadata, err := ProjectCodexSessionIdentity(payload, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, defaults)
	if err != nil {
		t.Fatalf("ProjectCodexSessionIdentity() error = %v", err)
	}
	if identity != defaults {
		t.Fatalf("identity = %#v, want %#v", identity, defaults)
	}
	if got := gjson.GetBytes(projected, "input.0.content").String(); got != "hello" {
		t.Fatalf("input content = %q, want hello", got)
	}
	if got := gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String(); got != "install-1" {
		t.Fatalf("installation ID = %q, want install-1", got)
	}
	if got := gjson.Get(turnMetadata, "workspace").String(); got != "/tmp/project" {
		t.Fatalf("workspace = %q, want /tmp/project", got)
	}
	for path, want := range map[string]string{
		"session_id":   "session-default",
		"thread_id":    "thread-default",
		"turn_id":      "turn-default",
		"window_id":    "thread-default:0",
		"request_kind": "turn",
	} {
		if got := gjson.Get(turnMetadata, path).String(); got != want {
			t.Fatalf("turn metadata %s = %q, want %q", path, got, want)
		}
	}
}

func TestProjectCodexSessionIdentityUsesDocumentedPriority(t *testing.T) {
	payload := []byte(`{"client_metadata":{"session_id":"body-flat-session","thread_id":"body-flat-thread","turn_id":"body-flat-turn","x-codex-window-id":"body-flat-window","x-codex-turn-metadata":"{\"session_id\":\"body-turn-session\",\"thread_id\":\"body-turn-thread\",\"turn_id\":\"body-turn-turn\",\"window_id\":\"body-turn-window\",\"body_unknown\":true}"}}`)
	admin := CodexSessionIdentityHeaderSource{
		SessionID:    "admin-session",
		TurnMetadata: `{"turn_id":"admin-turn","admin_unknown":true}`,
	}
	client := CodexSessionIdentityHeaderSource{
		SessionID: "client-session", ThreadID: "client-thread", WindowID: "client-window",
		TurnMetadata: `{"turn_id":"client-turn","client_unknown":true}`,
	}
	defaults := CodexSessionIdentity{
		SessionID: "default-session", ThreadID: "default-thread", TurnID: "default-turn",
		WindowID: "default-window", RequestKind: "turn",
	}

	_, identity, turnMetadata, err := ProjectCodexSessionIdentity(payload, admin, CodexSessionIdentityHeaderSource{}, client, defaults)
	if err != nil {
		t.Fatalf("ProjectCodexSessionIdentity() error = %v", err)
	}
	if identity.SessionID != "admin-session" {
		t.Fatalf("SessionID = %q, want admin-session", identity.SessionID)
	}
	if identity.ThreadID != "body-turn-thread" {
		t.Fatalf("ThreadID = %q, want body-turn-thread", identity.ThreadID)
	}
	if identity.TurnID != "admin-turn" {
		t.Fatalf("TurnID = %q, want admin-turn", identity.TurnID)
	}
	if identity.WindowID != "body-turn-window" {
		t.Fatalf("WindowID = %q, want body-turn-window", identity.WindowID)
	}
	for _, key := range []string{"admin_unknown", "body_unknown", "client_unknown"} {
		if !gjson.Get(turnMetadata, key).Bool() {
			t.Fatalf("turn metadata lost %s: %s", key, turnMetadata)
		}
	}
}

func TestProjectCodexSessionIdentityPairsPartialSessionAndThreadIdentity(t *testing.T) {
	defaults := CodexSessionIdentity{
		SessionID: "default-session", ThreadID: "default-thread", TurnID: "default-turn",
		WindowID: "default-thread:0", RequestKind: "turn",
	}
	tests := []struct {
		name    string
		payload string
		wantID  string
	}{
		{name: "thread only", payload: `{"client_metadata":{"thread_id":"explicit-thread"}}`, wantID: "explicit-thread"},
		{name: "session only", payload: `{"client_metadata":{"session_id":"explicit-session"}}`, wantID: "explicit-session"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, identity, _, err := ProjectCodexSessionIdentity([]byte(tt.payload), CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, defaults)
			if err != nil {
				t.Fatalf("ProjectCodexSessionIdentity() error = %v", err)
			}
			if identity.SessionID != tt.wantID || identity.ThreadID != tt.wantID {
				t.Fatalf("session/thread = %q/%q, want %q/%q", identity.SessionID, identity.ThreadID, tt.wantID, tt.wantID)
			}
			if wantWindow := tt.wantID + ":0"; identity.WindowID != wantWindow {
				t.Fatalf("WindowID = %q, want %q", identity.WindowID, wantWindow)
			}
		})
	}
}

func TestProjectCodexSessionIdentityRejectsMalformedMetadata(t *testing.T) {
	defaults := CodexSessionIdentity{SessionID: "s", ThreadID: "t", TurnID: "u", WindowID: "w", RequestKind: "turn"}
	tests := []struct {
		name    string
		payload string
		client  CodexSessionIdentityHeaderSource
	}{
		{name: "client metadata array", payload: `{"client_metadata":[]}`},
		{name: "body turn metadata primitive", payload: `{"client_metadata":{"x-codex-turn-metadata":"false"}}`},
		{name: "client turn metadata invalid", payload: `{}`, client: CodexSessionIdentityHeaderSource{TurnMetadata: `{"turn_id":`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := ProjectCodexSessionIdentity([]byte(tt.payload), CodexSessionIdentityHeaderSource{}, CodexSessionIdentityHeaderSource{}, tt.client, defaults); err == nil {
				t.Fatal("ProjectCodexSessionIdentity() error = nil, want malformed metadata error")
			}
		})
	}
}

func TestProjectCodexSessionIdentityDeviceProjectionOnlyRewritesInstallation(t *testing.T) {
	payload := []byte(`{"model":"gpt-5.4","client_metadata":{"session_id":"client-session","thread_id":"client-thread","x-codex-installation-id":"old-install","x-codex-turn-metadata":"{\"workspace\":\"/tmp/project\",\"installation_id\":\"old-install\"}"}}`)
	projected, identity, turnMetadata, err := ProjectCodexSessionIdentityWithProjection(
		payload,
		CodexSessionIdentityHeaderSource{},
		CodexSessionIdentityHeaderSource{},
		CodexSessionIdentityHeaderSource{},
		CodexSessionIdentity{},
		CodexSessionIdentityProjection{InstallationID: "stable-install", ProjectSession: false},
	)
	if err != nil {
		t.Fatalf("ProjectCodexSessionIdentityWithProjection() error = %v", err)
	}
	if identity.InstallationID != "stable-install" || identity.SessionID != "" || identity.ThreadID != "" {
		t.Fatalf("identity = %#v, want installation-only projection", identity)
	}
	if got := gjson.GetBytes(projected, "client_metadata.x-codex-installation-id").String(); got != "stable-install" {
		t.Fatalf("installation ID = %q, want stable-install", got)
	}
	if got := gjson.GetBytes(projected, "client_metadata.session_id").String(); got != "client-session" {
		t.Fatalf("session ID = %q, want original client-session", got)
	}
	if got := gjson.Get(turnMetadata, "installation_id").String(); got != "stable-install" {
		t.Fatalf("turn installation ID = %q, want stable-install", got)
	}
	if got := gjson.Get(turnMetadata, "workspace").String(); got != "/tmp/project" {
		t.Fatalf("workspace = %q, want preserved value", got)
	}
}

func TestProjectCodexSessionIdentityForcedValuesOverrideClientButNotAdmin(t *testing.T) {
	payload := []byte(`{"client_metadata":{"session_id":"body-session","thread_id":"body-thread"}}`)
	admin := CodexSessionIdentityHeaderSource{SessionID: "admin-session"}
	forced := CodexSessionIdentity{SessionID: "forced-session", ThreadID: "forced-thread", TurnID: "forced-turn", WindowID: "forced-window"}
	_, identity, _, err := ProjectCodexSessionIdentityWithProjection(
		payload,
		admin,
		CodexSessionIdentityHeaderSource{},
		CodexSessionIdentityHeaderSource{SessionID: "client-session", ThreadID: "client-thread"},
		CodexSessionIdentity{RequestKind: "turn"},
		CodexSessionIdentityProjection{ForcedIdentity: forced, ProjectSession: true},
	)
	if err != nil {
		t.Fatalf("ProjectCodexSessionIdentityWithProjection() error = %v", err)
	}
	if identity.SessionID != "admin-session" || identity.ThreadID != "forced-thread" || identity.TurnID != "forced-turn" || identity.WindowID != "forced-window" {
		t.Fatalf("identity = %#v, want admin session plus forced identity", identity)
	}
}
