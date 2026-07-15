package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestSummarizeAuthCooldown(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	authUntil := now.Add(10 * time.Minute)
	modelUntil := now.Add(30 * time.Minute)
	quotaUntil := now.Add(45 * time.Minute)

	authWide := &coreauth.Auth{
		Unavailable:    true,
		CooldownScope:  "auth",
		NextRetryAfter: authUntil,
		ModelStates: map[string]*coreauth.ModelState{
			"model-a": {Unavailable: true, NextRetryAfter: modelUntil},
		},
	}
	if got := summarizeAuthCooldown(authWide, now); !got.Active || got.Scope != "auth" || !got.Until.Equal(authUntil) || got.ModelCount != 0 {
		t.Fatalf("auth-wide summary = %#v", got)
	}
	if got := modelCooldownForAuth(authWide, now, "any-model"); !got.Active || got.Scope != "auth" || !got.Until.Equal(authUntil) {
		t.Fatalf("auth-wide model status = %#v", got)
	}

	modelScoped := &coreauth.Auth{ModelStates: map[string]*coreauth.ModelState{
		"active-a":   {Unavailable: true, NextRetryAfter: now.Add(5 * time.Minute)},
		"active-b":   {Unavailable: true, NextRetryAfter: modelUntil},
		"expired":    {Unavailable: true, NextRetryAfter: now.Add(-time.Minute)},
		"indefinite": {Unavailable: true},
		"available":  {NextRetryAfter: modelUntil},
		"disabled":   {Status: coreauth.StatusDisabled, Unavailable: true, NextRetryAfter: modelUntil},
	}}
	if got := summarizeAuthCooldown(modelScoped, now); !got.Active || got.Scope != "model" || !got.Until.Equal(modelUntil) || got.ModelCount != 2 {
		t.Fatalf("model summary = %#v", got)
	}
	if got := modelCooldownForAuth(&coreauth.Auth{ModelStates: map[string]*coreauth.ModelState{
		"gpt-5": {Unavailable: true, NextRetryAfter: modelUntil},
	}}, now, "gpt-5(high)"); !got.Active || got.Scope != "model" || !got.Until.Equal(modelUntil) {
		t.Fatalf("suffixed model status = %#v", got)
	}

	expiredAuthWithModel := &coreauth.Auth{
		Unavailable:    true,
		CooldownScope:  "auth",
		NextRetryAfter: now.Add(-time.Minute),
		ModelStates: map[string]*coreauth.ModelState{
			"model-a": {Unavailable: true, NextRetryAfter: modelUntil},
		},
	}
	if got := summarizeAuthCooldown(expiredAuthWithModel, now); !got.Active || got.Scope != "model" || got.ModelCount != 1 {
		t.Fatalf("expired auth-wide summary = %#v", got)
	}

	disabled := &coreauth.Auth{Disabled: true, Unavailable: true, CooldownScope: "auth", NextRetryAfter: authUntil}
	if got := summarizeAuthCooldown(disabled, now); got.Active {
		t.Fatalf("disabled summary = %#v, want inactive", got)
	}
	if got := modelCooldownForAuth(disabled, now, "any-model"); got.Active {
		t.Fatalf("disabled model status = %#v, want inactive", got)
	}

	quotaExtended := &coreauth.Auth{
		Unavailable:    true,
		CooldownScope:  "auth",
		NextRetryAfter: authUntil,
		Quota:          coreauth.QuotaState{NextRecoverAt: quotaUntil},
		ModelStates: map[string]*coreauth.ModelState{
			"model-a": {
				Unavailable:    true,
				NextRetryAfter: modelUntil,
				Quota:          coreauth.QuotaState{NextRecoverAt: quotaUntil},
			},
		},
	}
	if got := summarizeAuthCooldown(quotaExtended, now); !got.Active || got.Scope != "auth" || !got.Until.Equal(quotaUntil) {
		t.Fatalf("quota-extended auth summary = %#v", got)
	}
	quotaExtended.CooldownScope = "model"
	if got := summarizeAuthCooldown(quotaExtended, now); !got.Active || got.Scope != "model" || !got.Until.Equal(quotaUntil) {
		t.Fatalf("quota-extended model summary = %#v", got)
	}
	if got := modelCooldownForAuth(quotaExtended, now, "model-a"); !got.Active || !got.Until.Equal(quotaUntil) {
		t.Fatalf("quota-extended model status = %#v", got)
	}

	retryLater := &coreauth.Auth{
		Unavailable:    true,
		CooldownScope:  "auth",
		NextRetryAfter: quotaUntil,
		Quota:          coreauth.QuotaState{NextRecoverAt: authUntil},
	}
	if got := summarizeAuthCooldown(retryLater, now); !got.Active || !got.Until.Equal(quotaUntil) {
		t.Fatalf("retry-later auth summary = %#v", got)
	}

	quotaOnly := &coreauth.Auth{
		Unavailable:   true,
		CooldownScope: "auth",
		Quota:         coreauth.QuotaState{NextRecoverAt: quotaUntil},
		ModelStates: map[string]*coreauth.ModelState{
			"model-a": {Unavailable: true, Quota: coreauth.QuotaState{NextRecoverAt: quotaUntil}},
		},
	}
	if got := summarizeAuthCooldown(quotaOnly, now); got.Active {
		t.Fatalf("quota-only summary = %#v, want inactive", got)
	}
}

func TestAuthFilesExposeCooldownStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Now().UTC()
	retryAt := now.Add(20 * time.Minute)
	manager := coreauth.NewManager(nil, nil, nil)
	auth := &coreauth.Auth{
		ID:       "cooldown-runtime",
		FileName: "cooldown-runtime",
		Provider: "codex",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
		Runtime:        map[string]any{"source": "runtime"},
		Unavailable:    true,
		CooldownScope:  "model",
		NextRetryAfter: retryAt,
		ModelStates: map[string]*coreauth.ModelState{
			"upstream-model": {Unavailable: true, NextRetryAfter: retryAt},
			"gpt-5":          {Unavailable: true, NextRetryAfter: retryAt},
			"name-upstream":  {Unavailable: true, NextRetryAfter: retryAt},
		},
	}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{
		{ID: "prefixed-model", UpstreamID: "upstream-model"},
		{ID: "team/gpt-5(high)", UpstreamID: "gpt-5(high)"},
		{ID: "name-alias", Name: "models/name-upstream"},
		{ID: "available-model", UpstreamID: "available-model"},
	})
	authWideRetryAt := now.Add(10 * time.Minute)
	authWide := &coreauth.Auth{
		ID:             "auth-wide-runtime",
		FileName:       "auth-wide-runtime",
		Provider:       "codex",
		Status:         coreauth.StatusActive,
		Attributes:     map[string]string{"runtime_only": "true"},
		Runtime:        map[string]any{"source": "runtime"},
		Unavailable:    true,
		CooldownScope:  "auth",
		NextRetryAfter: authWideRetryAt,
	}
	if _, errRegister := manager.Register(context.Background(), authWide); errRegister != nil {
		t.Fatalf("register auth-wide auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(authWide.ID, authWide.Provider, []*registry.ModelInfo{{ID: "wide-a"}, {ID: "wide-b"}})
	t.Cleanup(func() {
		registry.GetGlobalRegistry().UnregisterClient(auth.ID)
		registry.GetGlobalRegistry().UnregisterClient(authWide.ID)
	})

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	listRecorder := httptest.NewRecorder()
	listContext, _ := gin.CreateTestContext(listRecorder)
	listContext.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files", nil)
	h.ListAuthFiles(listContext)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("list status = %d; body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	var listBody struct {
		Files []map[string]any `json:"files"`
	}
	if errDecode := json.Unmarshal(listRecorder.Body.Bytes(), &listBody); errDecode != nil || len(listBody.Files) != 2 {
		t.Fatalf("decode list: %v; body=%s", errDecode, listRecorder.Body.String())
	}
	entriesByID := make(map[string]map[string]any, len(listBody.Files))
	for _, item := range listBody.Files {
		entriesByID[item["id"].(string)] = item
	}
	entry := entriesByID[auth.ID]
	if entry["cooldown_active"] != true || entry["cooldown_scope"] != "model" || entry["cooldown_model_count"] != float64(3) {
		t.Fatalf("list cooldown fields = %#v", entry)
	}
	if _, exists := entry["next_retry_after"]; !exists {
		t.Fatalf("next_retry_after compatibility field missing: %#v", entry)
	}
	assertCooldownTime(t, entry["cooldown_until"], retryAt)
	authWideEntry := entriesByID[authWide.ID]
	if authWideEntry["cooldown_active"] != true || authWideEntry["cooldown_scope"] != "auth" || authWideEntry["cooldown_model_count"] != float64(0) {
		t.Fatalf("auth-wide list fields = %#v", authWideEntry)
	}
	assertCooldownTime(t, authWideEntry["cooldown_until"], authWideRetryAt)

	modelsByID := requestAuthModels(t, h, auth.ID, 4)
	cooled := modelsByID["prefixed-model"]
	if cooled["cooldown_active"] != true || cooled["scope"] != "model" {
		t.Fatalf("cooled model fields = %#v", cooled)
	}
	assertCooldownTime(t, cooled["until"], retryAt)
	for _, modelID := range []string{"team/gpt-5(high)", "name-alias"} {
		model := modelsByID[modelID]
		if model["cooldown_active"] != true || model["scope"] != "model" {
			t.Fatalf("mapped model %q fields = %#v", modelID, model)
		}
	}
	if available := modelsByID["available-model"]; available["cooldown_active"] != false {
		t.Fatalf("available model fields = %#v", available)
	}

	for modelID, model := range requestAuthModels(t, h, authWide.ID, 2) {
		if model["cooldown_active"] != true || model["scope"] != "auth" {
			t.Fatalf("auth-wide model %q fields = %#v", modelID, model)
		}
		assertCooldownTime(t, model["until"], authWideRetryAt)
	}
}

func requestAuthModels(t *testing.T, h *Handler, name string, want int) map[string]map[string]any {
	t.Helper()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v0/management/auth-files/models?name="+name, nil)
	h.GetAuthFileModels(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("models status = %d; body=%s", recorder.Code, recorder.Body.String())
	}
	var body struct {
		Models []map[string]any `json:"models"`
	}
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), &body); errDecode != nil || len(body.Models) != want {
		t.Fatalf("decode models: %v; body=%s", errDecode, recorder.Body.String())
	}
	modelsByID := make(map[string]map[string]any, len(body.Models))
	for _, model := range body.Models {
		modelsByID[model["id"].(string)] = model
	}
	return modelsByID
}

func assertCooldownTime(t *testing.T, raw any, want time.Time) {
	t.Helper()
	text, ok := raw.(string)
	if !ok {
		t.Fatalf("cooldown time = %#v, want string", raw)
	}
	got, errParse := time.Parse(time.RFC3339Nano, text)
	if errParse != nil || !got.Equal(want) {
		t.Fatalf("cooldown time = %q (%v), want %s", text, errParse, want.Format(time.RFC3339Nano))
	}
}
