package synthesizer

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestNewFileSynthesizer(t *testing.T) {
	synth := NewFileSynthesizer()
	if synth == nil {
		t.Fatal("expected non-nil synthesizer")
	}
}
func TestFileSynthesizer_Synthesize_NilContext(t *testing.T) {
	synth := NewFileSynthesizer()
	auths, err := synth.Synthesize(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 0 {
		t.Fatalf("expected empty auths, got %d", len(auths))
	}
}

func TestFileSynthesizer_Synthesize_EmptyAuthDir(t *testing.T) {
	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     "",
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}
	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 0 {
		t.Fatalf("expected empty auths, got %d", len(auths))
	}
}

func TestFileSynthesizer_Synthesize_NonExistentDir(t *testing.T) {
	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     "/non/existent/path",
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}
	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 0 {
		t.Fatalf("expected empty auths, got %d", len(auths))
	}
}

func TestFileSynthesizer_Synthesize_ValidAuthFile(t *testing.T) {
	tempDir := t.TempDir()

	// Create a valid auth file
	authData := map[string]any{
		"type":      "claude",
		"email":     "test@example.com",
		"proxy_url": "http://proxy.local",
		"prefix":    "test-prefix",
		"headers": map[string]string{
			" X-Test ": " value ",
			"X-Empty":  "  ",
		},
		"disable_cooling": true,
		"request_retry":   2,
	}
	data, _ := json.Marshal(authData)
	err := os.WriteFile(filepath.Join(tempDir, "claude-auth.json"), data, 0644)
	if err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}

	if auths[0].Provider != "claude" {
		t.Errorf("expected provider claude, got %s", auths[0].Provider)
	}
	if auths[0].Label != "test@example.com" {
		t.Errorf("expected label test@example.com, got %s", auths[0].Label)
	}
	if auths[0].Prefix != "test-prefix" {
		t.Errorf("expected prefix test-prefix, got %s", auths[0].Prefix)
	}
	if auths[0].ProxyURL != "http://proxy.local" {
		t.Errorf("expected proxy_url http://proxy.local, got %s", auths[0].ProxyURL)
	}
	if got := auths[0].Attributes["header:X-Test"]; got != "value" {
		t.Errorf("expected header:X-Test value, got %q", got)
	}
	if _, ok := auths[0].Attributes["header:X-Empty"]; ok {
		t.Errorf("expected header:X-Empty to be absent, got %q", auths[0].Attributes["header:X-Empty"])
	}
	if v, ok := auths[0].Metadata["disable_cooling"].(bool); !ok || !v {
		t.Errorf("expected disable_cooling true, got %v", auths[0].Metadata["disable_cooling"])
	}
	if v, ok := auths[0].Metadata["request_retry"].(float64); !ok || int(v) != 2 {
		t.Errorf("expected request_retry 2, got %v", auths[0].Metadata["request_retry"])
	}
	if auths[0].Status != coreauth.StatusActive {
		t.Errorf("expected status active, got %s", auths[0].Status)
	}
	wantRaw, err := coreauth.CanonicalMetadataBytes(auths[0])
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := coreauth.SourceHashFromBytes(wantRaw)
	if got := auths[0].Attributes[coreauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
	if rawHash := coreauth.SourceHashFromBytes(data); rawHash == wantHash {
		t.Fatal("expected canonical source hash to differ from raw file hash")
	}
}

func TestFileSynthesizer_Synthesize_SkipsRetiredGeminiCLICredentials(t *testing.T) {
	tempDir := t.TempDir()
	for name, provider := range map[string]string{
		"legacy-gemini.json":     "gemini",
		"legacy-gemini-cli.json": "gemini-cli",
	} {
		data, errMarshal := json.Marshal(map[string]any{
			"type":         provider,
			"email":        "legacy@example.com",
			"access_token": "secret-token",
		})
		if errMarshal != nil {
			t.Fatalf("marshal %s: %v", name, errMarshal)
		}
		if errWrite := os.WriteFile(filepath.Join(tempDir, name), data, 0o600); errWrite != nil {
			t.Fatalf("write %s: %v", name, errWrite)
		}
	}

	synth := NewFileSynthesizer()
	auths, err := synth.Synthesize(&SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	})
	if err != nil {
		t.Fatalf("Synthesize() error = %v", err)
	}
	if len(auths) != 0 {
		t.Fatalf("Synthesize() returned %d retired Gemini CLI auths, want 0", len(auths))
	}
}

func TestSynthesizeAuthFileAcceptsGeminiAPIKeyFile(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "gemini-key.json")
	data := []byte(`{"type":"gemini","api_key":"active-key","email":"key@example.com","project_id":"project"}`)
	ctx := &SynthesisContext{
		Config:      &config.Config{AuthDir: authDir},
		AuthDir:     authDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}
	auths := SynthesizeAuthFile(ctx, path, data)
	if len(auths) != 1 || auths[0].Attributes["api_key"] != "active-key" || auths[0].Attributes["auth_kind"] != "apikey" || coreauth.IsRetiredGeminiCLIAuth(auths[0]) {
		if len(auths) == 0 {
			t.Fatal("Gemini API key file was not synthesized")
		}
		t.Fatalf("synthesized Gemini API key attributes = %#v, metadata = %#v, retired = %t", auths[0].Attributes, auths[0].Metadata, coreauth.IsRetiredGeminiCLIAuth(auths[0]))
	}
}

func TestSynthesizeAuthFileRejectsMixedGeminiCLICredential(t *testing.T) {
	authDir := t.TempDir()
	for _, test := range []struct {
		name string
		data string
	}{
		{name: "access token", data: `{"type":"gemini","api_key":"misleading-key","access_token":"legacy-token"}`},
		{name: "refresh token", data: `{"type":"gemini","api_key":"misleading-key","refresh_token":"legacy-token"}`},
		{name: "token object", data: `{"type":"gemini","api_key":"misleading-key","token":{"access_token":"legacy-token"}}`},
		{name: "oauth auth kind", data: `{"type":"gemini","api_key":"misleading-key","auth_kind":"oauth"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(authDir, strings.ReplaceAll(test.name, " ", "-")+".json")
			ctx := &SynthesisContext{
				Config:      &config.Config{AuthDir: authDir},
				AuthDir:     authDir,
				Now:         time.Now(),
				IDGenerator: NewStableIDGenerator(),
			}
			if auths := SynthesizeAuthFile(ctx, path, []byte(test.data)); len(auths) != 0 {
				t.Fatalf("mixed Gemini CLI credential was synthesized: %#v", auths)
			}
		})
	}
}

func TestFileSynthesizer_Synthesize_SkipsSymlinkCredentials(t *testing.T) {
	tempDir := t.TempDir()
	externalPath := filepath.Join(t.TempDir(), "external.json")
	data := []byte(`{"type":"codex","email":"external@example.com"}`)
	if errWrite := os.WriteFile(externalPath, data, 0o600); errWrite != nil {
		t.Fatalf("write external auth: %v", errWrite)
	}
	aliasPath := filepath.Join(tempDir, "alias.json")
	if errSymlink := os.Symlink(externalPath, aliasPath); errSymlink != nil {
		t.Skipf("symlink is unavailable: %v", errSymlink)
	}

	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}
	synth := NewFileSynthesizer()
	auths, errSynthesize := synth.Synthesize(ctx)
	if errSynthesize != nil {
		t.Fatalf("Synthesize() error = %v", errSynthesize)
	}
	if len(auths) != 0 {
		t.Fatalf("Synthesize() returned %d symlink auths, want 0", len(auths))
	}
	if auths := SynthesizeAuthFile(ctx, aliasPath, data); len(auths) != 0 {
		t.Fatalf("SynthesizeAuthFile() returned %d symlink auths, want 0", len(auths))
	}
}

func TestFileSynthesizer_Synthesize_CodexPrefersMetadataPlanType(t *testing.T) {
	tempDir := t.TempDir()

	authData := map[string]any{
		"type":      "codex",
		"email":     "codex@example.com",
		"plan_type": "team",
		"id_token":  testCodexJWT("acct-1", "pro"),
	}
	data, _ := json.Marshal(authData)
	if err := os.WriteFile(filepath.Join(tempDir, "codex-auth.json"), data, 0o644); err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}
	if got := auths[0].Attributes["plan_type"]; got != "team" {
		t.Fatalf("plan_type = %q, want %q", got, "team")
	}
}

func TestFileSynthesizer_Synthesize_SkipsInvalidFiles(t *testing.T) {
	tempDir := t.TempDir()

	// Create various invalid files
	_ = os.WriteFile(filepath.Join(tempDir, "not-json.txt"), []byte("text content"), 0644)
	_ = os.WriteFile(filepath.Join(tempDir, "invalid.json"), []byte("not valid json"), 0644)
	_ = os.WriteFile(filepath.Join(tempDir, "empty.json"), []byte(""), 0644)
	_ = os.WriteFile(filepath.Join(tempDir, "no-type.json"), []byte(`{"email": "test@example.com"}`), 0644)

	// Create one valid file
	validData, _ := json.Marshal(map[string]any{"type": "claude", "email": "valid@example.com"})
	_ = os.WriteFile(filepath.Join(tempDir, "valid.json"), validData, 0644)

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("only valid auth file should be processed, got %d", len(auths))
	}
	if auths[0].Label != "valid@example.com" {
		t.Errorf("expected label valid@example.com, got %s", auths[0].Label)
	}
}

func testCodexJWT(accountID string, planType string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload, _ := json.Marshal(map[string]any{
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_account_id": accountID,
			"chatgpt_plan_type":  planType,
		},
	})
	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".sig"
}

func TestFileSynthesizer_Synthesize_SkipsDirectories(t *testing.T) {
	tempDir := t.TempDir()

	// Create a subdirectory with a json file inside
	subDir := filepath.Join(tempDir, "subdir.json")
	err := os.Mkdir(subDir, 0755)
	if err != nil {
		t.Fatalf("failed to create subdir: %v", err)
	}

	// Create a valid file in root
	validData, _ := json.Marshal(map[string]any{"type": "claude"})
	_ = os.WriteFile(filepath.Join(tempDir, "valid.json"), validData, 0644)

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}
}

func TestFileSynthesizer_Synthesize_RelativeID(t *testing.T) {
	tempDir := t.TempDir()

	authData := map[string]any{"type": "claude"}
	data, _ := json.Marshal(authData)
	err := os.WriteFile(filepath.Join(tempDir, "my-auth.json"), data, 0644)
	if err != nil {
		t.Fatalf("failed to write auth file: %v", err)
	}

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config:      &config.Config{},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, err := synth.Synthesize(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}

	// ID should be relative path
	if auths[0].ID != "my-auth.json" {
		t.Errorf("expected ID my-auth.json, got %s", auths[0].ID)
	}
}

func TestFileSynthesizer_Synthesize_PrefixValidation(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		wantPrefix string
	}{
		{"valid prefix", "myprefix", "myprefix"},
		{"prefix with slashes trimmed", "/myprefix/", "myprefix"},
		{"prefix with spaces trimmed", "  myprefix  ", "myprefix"},
		{"prefix with internal slash rejected", "my/prefix", ""},
		{"empty prefix", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			authData := map[string]any{
				"type":   "claude",
				"prefix": tt.prefix,
			}
			data, _ := json.Marshal(authData)
			_ = os.WriteFile(filepath.Join(tempDir, "auth.json"), data, 0644)

			synth := NewFileSynthesizer()
			ctx := &SynthesisContext{
				Config:      &config.Config{},
				AuthDir:     tempDir,
				Now:         time.Now(),
				IDGenerator: NewStableIDGenerator(),
			}

			auths, err := synth.Synthesize(ctx)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(auths) != 1 {
				t.Fatalf("expected 1 auth, got %d", len(auths))
			}
			if auths[0].Prefix != tt.wantPrefix {
				t.Errorf("expected prefix %q, got %q", tt.wantPrefix, auths[0].Prefix)
			}
		})
	}
}

func TestFileSynthesizer_Synthesize_PriorityParsing(t *testing.T) {
	tests := []struct {
		name     string
		priority any
		want     string
		hasValue bool
	}{
		{
			name:     "string with spaces",
			priority: " 10 ",
			want:     "10",
			hasValue: true,
		},
		{
			name:     "number",
			priority: 8,
			want:     "8",
			hasValue: true,
		},
		{
			name:     "invalid string",
			priority: "1x",
			hasValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			authData := map[string]any{
				"type":     "claude",
				"priority": tt.priority,
			}
			data, _ := json.Marshal(authData)
			errWriteFile := os.WriteFile(filepath.Join(tempDir, "auth.json"), data, 0644)
			if errWriteFile != nil {
				t.Fatalf("failed to write auth file: %v", errWriteFile)
			}

			synth := NewFileSynthesizer()
			ctx := &SynthesisContext{
				Config:      &config.Config{},
				AuthDir:     tempDir,
				Now:         time.Now(),
				IDGenerator: NewStableIDGenerator(),
			}

			auths, errSynthesize := synth.Synthesize(ctx)
			if errSynthesize != nil {
				t.Fatalf("unexpected error: %v", errSynthesize)
			}
			if len(auths) != 1 {
				t.Fatalf("expected 1 auth, got %d", len(auths))
			}

			value, ok := auths[0].Attributes["priority"]
			if tt.hasValue {
				if !ok {
					t.Fatal("expected priority attribute to be set")
				}
				if value != tt.want {
					t.Fatalf("expected priority %q, got %q", tt.want, value)
				}
				return
			}
			if ok {
				t.Fatalf("expected priority attribute to be absent, got %q", value)
			}
		})
	}
}

func TestFileSynthesizer_Synthesize_OAuthExcludedModelsMerged(t *testing.T) {
	tempDir := t.TempDir()
	authData := map[string]any{
		"type":            "claude",
		"excluded_models": []string{"custom-model", "MODEL-B"},
	}
	data, _ := json.Marshal(authData)
	errWriteFile := os.WriteFile(filepath.Join(tempDir, "auth.json"), data, 0644)
	if errWriteFile != nil {
		t.Fatalf("failed to write auth file: %v", errWriteFile)
	}

	synth := NewFileSynthesizer()
	ctx := &SynthesisContext{
		Config: &config.Config{
			OAuthExcludedModels: map[string][]string{
				"claude": {"shared", "model-b"},
			},
		},
		AuthDir:     tempDir,
		Now:         time.Now(),
		IDGenerator: NewStableIDGenerator(),
	}

	auths, errSynthesize := synth.Synthesize(ctx)
	if errSynthesize != nil {
		t.Fatalf("unexpected error: %v", errSynthesize)
	}
	if len(auths) != 1 {
		t.Fatalf("expected 1 auth, got %d", len(auths))
	}

	got := auths[0].Attributes["excluded_models"]
	want := "custom-model,model-b,shared"
	if got != want {
		t.Fatalf("expected excluded_models %q, got %q", want, got)
	}
}

func TestFileSynthesizer_Synthesize_NoteParsing(t *testing.T) {
	tests := []struct {
		name     string
		note     any
		want     string
		hasValue bool
	}{
		{
			name:     "valid string note",
			note:     "hello world",
			want:     "hello world",
			hasValue: true,
		},
		{
			name:     "string note with whitespace",
			note:     "  trimmed note  ",
			want:     "trimmed note",
			hasValue: true,
		},
		{
			name:     "empty string note",
			note:     "",
			hasValue: false,
		},
		{
			name:     "whitespace only note",
			note:     "   ",
			hasValue: false,
		},
		{
			name:     "non-string note ignored",
			note:     12345,
			hasValue: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			authData := map[string]any{
				"type": "claude",
				"note": tt.note,
			}
			data, _ := json.Marshal(authData)
			errWriteFile := os.WriteFile(filepath.Join(tempDir, "auth.json"), data, 0644)
			if errWriteFile != nil {
				t.Fatalf("failed to write auth file: %v", errWriteFile)
			}

			synth := NewFileSynthesizer()
			ctx := &SynthesisContext{
				Config:      &config.Config{},
				AuthDir:     tempDir,
				Now:         time.Now(),
				IDGenerator: NewStableIDGenerator(),
			}

			auths, errSynthesize := synth.Synthesize(ctx)
			if errSynthesize != nil {
				t.Fatalf("unexpected error: %v", errSynthesize)
			}
			if len(auths) != 1 {
				t.Fatalf("expected 1 auth, got %d", len(auths))
			}

			value, ok := auths[0].Attributes["note"]
			if tt.hasValue {
				if !ok {
					t.Fatal("expected note attribute to be set")
				}
				if value != tt.want {
					t.Fatalf("expected note %q, got %q", tt.want, value)
				}
				return
			}
			if ok {
				t.Fatalf("expected note attribute to be absent, got %q", value)
			}
		})
	}
}

func TestSynthesizeAuthFileMapsChatGPTWebLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		payload      string
		wantState    string
		wantStatus   coreauth.Status
		wantMessage  string
		wantDisabled bool
	}{
		{
			name:       "legacy token defaults active",
			payload:    `{"type":"chatgpt-web","access_token":"token"}`,
			wantState:  coreauth.LifecycleStateActive,
			wantStatus: coreauth.StatusActive,
		},
		{
			name:       "missing token defaults pending",
			payload:    `{"type":"chatgpt-web"}`,
			wantState:  coreauth.LifecycleStateLoginPending,
			wantStatus: coreauth.StatusPending,
		},
		{
			name:        "explicit active",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"active","lifecycle_reason":"ready"}`,
			wantState:   coreauth.LifecycleStateActive,
			wantStatus:  coreauth.StatusActive,
			wantMessage: "ready",
		},
		{
			name:        "explicit login pending",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"login_pending","lifecycle_reason":"awaiting_login"}`,
			wantState:   coreauth.LifecycleStateLoginPending,
			wantStatus:  coreauth.StatusPending,
			wantMessage: "awaiting_login",
		},
		{
			name:        "dead stays distinct from disabled",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"dead","lifecycle_reason":"account_deactivated"}`,
			wantState:   coreauth.LifecycleStateDead,
			wantStatus:  coreauth.StatusError,
			wantMessage: "account_deactivated",
		},
		{
			name:        "interaction required",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"interaction_required","lifecycle_reason":"passkey_required"}`,
			wantState:   coreauth.LifecycleStateInteractionRequired,
			wantStatus:  coreauth.StatusError,
			wantMessage: "passkey_required",
		},
		{
			name:        "unknown reason is sanitized",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"reauth_required","lifecycle_reason":"secret-shaped-reason"}`,
			wantState:   coreauth.LifecycleStateReauthRequired,
			wantStatus:  coreauth.StatusError,
			wantMessage: "authentication_failed",
		},
		{
			name:        "unknown state fails closed",
			payload:     `{"type":"chatgpt-web","lifecycle_state":"secret-shaped-state","lifecycle_reason":"secret-shaped-reason"}`,
			wantState:   coreauth.LifecycleStateReauthRequired,
			wantStatus:  coreauth.StatusError,
			wantMessage: "authentication_failed",
		},
		{
			name:         "disabled overrides lifecycle",
			payload:      `{"type":"chatgpt-web","lifecycle_state":"dead","lifecycle_reason":"account_deactivated","disabled":true}`,
			wantState:    coreauth.LifecycleStateDead,
			wantStatus:   coreauth.StatusDisabled,
			wantDisabled: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "chatgpt-web.json")
			ctx := &SynthesisContext{AuthDir: dir, Now: time.Now()}
			auths := SynthesizeAuthFile(ctx, path, []byte(test.payload))
			if len(auths) != 1 {
				t.Fatalf("SynthesizeAuthFile() returned %d auths, want 1", len(auths))
			}
			auth := auths[0]
			if auth.Provider != "chatgpt-web" {
				t.Fatalf("provider = %q, want chatgpt-web", auth.Provider)
			}
			if auth.LifecycleState() != test.wantState {
				t.Fatalf("lifecycle state = %q, want %q", auth.LifecycleState(), test.wantState)
			}
			if auth.Status != test.wantStatus {
				t.Fatalf("status = %q, want %q", auth.Status, test.wantStatus)
			}
			if auth.StatusMessage != test.wantMessage {
				t.Fatalf("status message = %q, want %q", auth.StatusMessage, test.wantMessage)
			}
			if auth.Disabled != test.wantDisabled {
				t.Fatalf("disabled = %v, want %v", auth.Disabled, test.wantDisabled)
			}
			var original map[string]any
			if errUnmarshal := json.Unmarshal([]byte(test.payload), &original); errUnmarshal != nil {
				t.Fatalf("unmarshal test payload: %v", errUnmarshal)
			}
			if !reflect.DeepEqual(auth.Metadata, original) {
				t.Fatalf("derived lifecycle state mutated metadata: got %v want %v", auth.Metadata, original)
			}
			canonical, errCanonical := coreauth.CanonicalMetadataBytes(&coreauth.Auth{
				Metadata: original,
				Disabled: test.wantDisabled,
			})
			if errCanonical != nil {
				t.Fatalf("canonicalize original metadata: %v", errCanonical)
			}
			if got, want := auth.Attributes[coreauth.SourceHashAttributeKey], coreauth.SourceHashFromBytes(canonical); got != want {
				t.Fatalf("source hash = %q, want %q", got, want)
			}
		})
	}
}
