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

func TestGinAccessLogCapturesRequestCredentialAndKeepsRequestsIsolated(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	engine := gin.New()
	engine.Use(GinLogrusLogger())
	engine.POST("/v1/responses", func(c *gin.Context) {
		if c.Query("selected") != "" {
			ctx := c.Request.Context()
			SetRequestCredential(ctx, CredentialIdentity{Provider: "codex", Index: "first", Name: "first.json"})
			SetRequestCredential(ctx, CredentialIdentity{Provider: "codex", Index: "second", Name: "second.json"})
		}
		c.JSON(500, gin.H{"error": gin.H{"message": "fixture overload"}})
	})
	for _, query := range []string{"?selected=yes", ""} {
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/responses"+query, nil))
	}
	entries := hook.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("entries = %d", len(entries))
	}
	if entries[0].Data["auth_name"] != "second.json" || entries[0].Data["auth_index"] != "second" || entries[0].Data["request_id"] == "" {
		t.Fatal("final access log lost credential or request identity")
	}
	if _, exists := entries[1].Data["auth_index"]; exists {
		t.Fatal("credential leaked into unrelated request")
	}
	if entries[0].Data["request_id"] == entries[1].Data["request_id"] {
		t.Fatal("request IDs not isolated")
	}
}

func TestSearchAccessLogsReceiveRequestIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	hook := logtest.NewGlobal()
	defer hook.Reset()
	previousLevel := log.GetLevel()
	log.SetLevel(log.InfoLevel)
	defer log.SetLevel(previousLevel)
	engine := gin.New()
	engine.Use(GinLogrusLogger())
	for _, path := range []string{"/v1/alpha/search", "/backend-api/codex/alpha/search"} {
		engine.POST(path, func(c *gin.Context) {
			if GetGinRequestID(c) == "" || GetRequestID(c.Request.Context()) == "" {
				t.Error("search context has no request ID")
			}
			c.Status(http.StatusOK)
		})
		engine.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, path, nil))
		entry := hook.LastEntry()
		if entry == nil || entry.Data["path"] != path || entry.Data["method"] != "POST" || entry.Data["request_id"] == "--------" {
			t.Fatalf("search access log cannot be traced: %+v", entry)
		}
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
