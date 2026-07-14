package logging

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestGinLogrusLoggerSkipsRetiredAmpOAuthPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)

	engine := gin.New()
	engine.Use(GinLogrusLogger())
	req := httptest.NewRequest(http.MethodGet, "/auth/callback?code=sensitive-code&state=sensitive-state", nil)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	entries := hook.AllEntries()
	for _, entry := range entries {
		message := entry.Message
		if strings.Contains(message, "sensitive-code") || strings.Contains(message, "sensitive-state") {
			t.Fatalf("retired Amp OAuth query was logged: %s", message)
		}
	}
	if len(entries) != 0 {
		t.Fatalf("retired Amp OAuth path emitted %d access log entries", len(entries))
	}
}

func TestGinLogrusLoggerSkipsRetiredGeminiCLICallbackQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)

	engine := gin.New()
	engine.Use(GinLogrusLogger())
	req := httptest.NewRequest(http.MethodGet, "/google/callback?code=gemini-code-sentinel&state=gemini-state-sentinel", nil)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	entries := hook.AllEntries()
	for _, entry := range entries {
		if strings.Contains(entry.Message, "gemini-code-sentinel") || strings.Contains(entry.Message, "gemini-state-sentinel") {
			t.Fatalf("retired Gemini CLI callback query was logged: %s", entry.Message)
		}
	}
	if len(entries) != 0 {
		t.Fatalf("retired Gemini CLI callback emitted %d access log entries", len(entries))
	}
}

func TestGinLogrusLoggerSkipsPrefixedRetiredGeminiCLICallbackQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)

	engine := gin.New()
	engine.Use(GinLogrusLogger())
	req := httptest.NewRequest(http.MethodGet, "/secret-token/google/callback?code=google-code-1234&state=google-state-5678", nil)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	if entries := hook.AllEntries(); len(entries) != 0 {
		t.Fatalf("prefixed retired Gemini CLI callback emitted %d access log entries", len(entries))
	}
}

func TestGinLogrusLoggerMasksPrefixedOAuthCallbackQuery(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)

	engine := gin.New()
	engine.Use(GinLogrusLogger())
	req := httptest.NewRequest(http.MethodGet, "/nested-access/codex/callback?CODE[]=alpha-code-9876&State%5B%5D=bravo-state-5432", nil)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)

	entries := hook.AllEntries()
	if len(entries) != 1 {
		t.Fatalf("callback emitted %d access log entries, want 1", len(entries))
	}
	message := entries[0].Message
	for _, secret := range []string{"alpha-code-9876", "bravo-state-5432"} {
		if strings.Contains(message, secret) {
			t.Fatalf("OAuth callback query value %q was logged: %s", secret, message)
		}
	}
	wantPath := "/nested-access/codex/callback?CODE[]=alph...9876&State%5B%5D=brav...5432"
	if !strings.Contains(message, wantPath) {
		t.Fatalf("access log %q does not contain masked path %q", message, wantPath)
	}
}

func TestGinLogrusLoggerKeepsCustomRouteUnderRetiredPrefix(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)

	engine := gin.New()
	engine.Use(GinLogrusLogger())
	engine.GET("/auth/custom", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	req := httptest.NewRequest(http.MethodGet, "/auth/custom", nil)
	recorder := httptest.NewRecorder()
	engine.ServeHTTP(recorder, req)
	if len(hook.AllEntries()) != 1 {
		t.Fatalf("custom route emitted %d access log entries, want 1", len(hook.AllEntries()))
	}
}

func TestGinLogrusRecoveryRepanicsErrAbortHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(GinLogrusRecovery())
	engine.GET("/abort", func(c *gin.Context) {
		panic(http.ErrAbortHandler)
	})

	req := httptest.NewRequest(http.MethodGet, "/abort", nil)
	recorder := httptest.NewRecorder()

	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatalf("expected panic, got nil")
		}
		err, ok := recovered.(error)
		if !ok {
			t.Fatalf("expected error panic, got %T", recovered)
		}
		if !errors.Is(err, http.ErrAbortHandler) {
			t.Fatalf("expected ErrAbortHandler, got %v", err)
		}
		if err != http.ErrAbortHandler {
			t.Fatalf("expected exact ErrAbortHandler sentinel, got %v", err)
		}
	}()

	engine.ServeHTTP(recorder, req)
}

func TestGinLogrusRecoveryHandlesRegularPanic(t *testing.T) {
	gin.SetMode(gin.TestMode)

	engine := gin.New()
	engine.Use(GinLogrusRecovery())
	engine.GET("/panic", func(c *gin.Context) {
		panic("boom")
	})

	req := httptest.NewRequest(http.MethodGet, "/panic", nil)
	recorder := httptest.NewRecorder()

	engine.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", recorder.Code)
	}
}
