package management

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestClearAllAuthCooldownsConfigBackedAuthBypassesUnavailableAuthDir(t *testing.T) {
	tests := []struct {
		name       string
		provider   string
		source     string
		apiKey     string
		mutate     func(*coreauth.Auth, string)
		wantStatus int
	}{
		{name: "gemini", provider: "gemini", source: "config:gemini[gemini-token]", apiKey: "gemini-key", wantStatus: http.StatusOK},
		{name: "claude", provider: "claude", source: "config:claude[claude-token]", apiKey: "claude-key", wantStatus: http.StatusOK},
		{name: "codex", provider: "codex", source: "config:codex[codex-token]", apiKey: "codex-key", wantStatus: http.StatusOK},
		{name: "openai compatibility", provider: "openrouter", source: "config:openrouter[compat-token]", apiKey: "compat-key", wantStatus: http.StatusOK},
		{name: "openai compatibility without api key", provider: "openrouter", source: "config:openrouter[keyless-token]", wantStatus: http.StatusOK},
		{name: "vertex api key", provider: "vertex", source: "config:vertex-apikey[vertex-token]", apiKey: "vertex-key", wantStatus: http.StatusOK},
		{
			name:     "config source with file name",
			provider: "codex",
			source:   "config:codex[file-name-token]",
			apiKey:   "file-name-key",
			mutate: func(auth *coreauth.Auth, _ string) {
				auth.FileName = "missing.json"
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:     "config source with path",
			provider: "claude",
			source:   "config:claude[path-token]",
			apiKey:   "path-key",
			mutate: func(auth *coreauth.Auth, authDir string) {
				auth.Attributes["path"] = filepath.Join(authDir, "missing.json")
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name:     "managed file source",
			provider: "vertex",
			apiKey:   "source-key",
			mutate: func(auth *coreauth.Auth, authDir string) {
				auth.Attributes["source"] = filepath.Join(authDir, "missing.json")
			},
			wantStatus: http.StatusServiceUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authDir := filepath.Join(t.TempDir(), "auths")
			if errMkdir := os.MkdirAll(authDir, 0o700); errMkdir != nil {
				t.Fatalf("create auth dir: %v", errMkdir)
			}
			nextRetry := time.Now().Add(time.Hour).Round(time.Second)
			auth := &coreauth.Auth{
				ID:             "test-auth",
				Provider:       test.provider,
				Status:         coreauth.StatusError,
				Unavailable:    true,
				NextRetryAfter: nextRetry,
				CooldownScope:  "auth",
				Attributes: map[string]string{
					"source": test.source,
				},
			}
			if test.apiKey != "" {
				auth.Attributes["api_key"] = test.apiKey
			}
			if test.mutate != nil {
				test.mutate(auth, authDir)
			}

			manager := coreauth.NewManager(nil, nil, nil)
			if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
				t.Fatalf("Register() error = %v", errRegister)
			}
			if errRemove := os.RemoveAll(authDir); errRemove != nil {
				t.Fatalf("remove auth dir: %v", errRemove)
			}

			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-all", nil)
			h.ClearAllAuthCooldowns(ctx)
			if recorder.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}

			current, ok := manager.GetByID(auth.ID)
			if !ok || current == nil {
				t.Fatal("auth disappeared")
			}
			if test.wantStatus == http.StatusOK {
				if current.Unavailable || current.Status != coreauth.StatusActive || current.CooldownScope != "" || !current.NextRetryAfter.IsZero() {
					t.Fatalf("cooldown was not cleared: %#v", current)
				}
				return
			}
			if !current.Unavailable || current.Status != coreauth.StatusError || current.CooldownScope != "auth" || !current.NextRetryAfter.Equal(nextRetry) {
				t.Fatalf("file-backed cooldown changed after failed verification: %#v", current)
			}
		})
	}
}

func TestClearAllAuthCooldownsVerificationFailureIsAtomic(t *testing.T) {
	authDir := t.TempDir()
	validPath := filepath.Join(authDir, "a-valid.json")
	if errWrite := os.WriteFile(validPath, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write valid auth file: %v", errWrite)
	}

	nextRetry := time.Now().Add(time.Hour).Round(time.Second)
	manager := coreauth.NewManager(nil, nil, nil)
	for _, auth := range []*coreauth.Auth{
		{
			ID:             "a-valid.json",
			FileName:       "a-valid.json",
			Provider:       "codex",
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: nextRetry,
			CooldownScope:  "auth",
			Attributes:     map[string]string{"path": validPath},
			Metadata:       map[string]any{"type": "codex"},
		},
		{
			ID:             "z-missing.json",
			FileName:       "z-missing.json",
			Provider:       "codex",
			Status:         coreauth.StatusError,
			Unavailable:    true,
			NextRetryAfter: nextRetry,
			CooldownScope:  "auth",
			Attributes:     map[string]string{"path": filepath.Join(authDir, "z-missing.json")},
			Metadata:       map[string]any{"type": "codex"},
		},
	} {
		if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-all", nil)
	h.ClearAllAuthCooldowns(ctx)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusServiceUnavailable, recorder.Body.String())
	}
	current, ok := manager.GetByID("a-valid.json")
	if !ok || current == nil {
		t.Fatal("valid auth disappeared")
	}
	if !current.Unavailable || current.Status != coreauth.StatusError || current.CooldownScope != "auth" || !current.NextRetryAfter.Equal(nextRetry) {
		t.Fatalf("valid auth mutated before verification completed: %#v", current)
	}
}

func TestClearSelectedAuthCooldownsKeepsMissingItemsPartial(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "valid.json")
	if errWrite := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); errWrite != nil {
		t.Fatalf("write valid auth file: %v", errWrite)
	}
	nextRetry := time.Now().Add(time.Hour).Round(time.Second)
	manager := coreauth.NewManager(nil, nil, nil)
	_, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), &coreauth.Auth{
		ID:             "valid.json",
		FileName:       "valid.json",
		Provider:       "codex",
		Status:         coreauth.StatusError,
		Unavailable:    true,
		NextRetryAfter: nextRetry,
		CooldownScope:  "auth",
		Attributes:     map[string]string{"path": path},
		Metadata:       map[string]any{"type": "codex"},
	})
	if errRegister != nil {
		t.Fatalf("Register() error = %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-selected", strings.NewReader(`{"names":["valid.json","missing.json"]}`))
	ctx.Request.Header.Set("Content-Type", "application/json")
	h.ClearSelectedAuthCooldowns(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"missing":["missing.json"]`) {
		t.Fatalf("response does not preserve missing item: %s", recorder.Body.String())
	}
	current, ok := manager.GetByID("valid.json")
	if !ok || current == nil {
		t.Fatal("valid auth disappeared")
	}
	if current.Unavailable || current.Status != coreauth.StatusActive || current.CooldownScope != "" || !current.NextRetryAfter.IsZero() {
		t.Fatalf("valid auth cooldown was not cleared: %#v", current)
	}
}
