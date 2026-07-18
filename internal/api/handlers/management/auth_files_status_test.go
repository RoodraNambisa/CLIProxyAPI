package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/auth/vertex"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestPatchAuthFileStatus_InvokesAuthStatusHook(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       "status-hook.json",
		FileName: "status-hook.json",
		Provider: "claude",
		Status:   coreauth.StatusDisabled,
		Disabled: true,
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	var (
		called bool
		gotID  string
	)
	h.SetAuthStatusHook(func(_ context.Context, auth *coreauth.Auth) {
		called = true
		if auth != nil {
			gotID = auth.ID
		}
	})

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"status-hook.json","disabled":false}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if !called {
		t.Fatal("expected auth status hook to be called")
	}
	if gotID != "status-hook.json" {
		t.Fatalf("hook auth ID = %q, want %q", gotID, "status-hook.json")
	}
}

func TestPatchAuthFileStatus_RuntimeOnlyIDDoesNotRequireJSONSuffix(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       "aistudio-runtime-id",
		FileName: "aistudio-runtime-display",
		Provider: "aistudio",
		Status:   coreauth.StatusActive,
		Attributes: map[string]string{
			"runtime_only": "true",
		},
		Runtime: map[string]any{"source": "runtime"},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register runtime-only auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"aistudio-runtime-display","disabled":true}`))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request

	h.PatchAuthFileStatus(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	current, ok := manager.GetByID(record.ID)
	if !ok || current == nil || !current.Disabled {
		t.Fatalf("runtime-only auth after status update = %#v, want disabled", current)
	}
}

func TestPatchAuthFileStatus_UnknownRuntimeIDReturnsNotFound(t *testing.T) {
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"missing-runtime-id","disabled":true}`))
	request.Header.Set("Content-Type", "application/json")
	ctx.Request = request
	h.PatchAuthFileStatus(ctx)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}

func TestPatchAuthFileStatus_DisableSetsMetadataDisabled(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:       "status-disable.json",
		FileName: "status-disable.json",
		Provider: "vertex",
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"type":  "vertex",
			"label": "vertex-label",
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"status-disable.json","disabled":true}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	current, ok := manager.GetByID("status-disable.json")
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if !current.Disabled {
		t.Fatal("expected auth to be disabled")
	}
	if got, ok := current.Metadata["disabled"].(bool); !ok || !got {
		t.Fatalf("metadata disabled = %#v, want true", current.Metadata["disabled"])
	}
}

func TestPatchAuthFileStatus_EnablePreservesChatGPTWebLifecycle(t *testing.T) {
	for _, test := range []struct {
		name   string
		state  string
		reason string
	}{
		{name: "dead", state: coreauth.LifecycleStateDead, reason: "account_deactivated"},
		{name: "interaction required", state: coreauth.LifecycleStateInteractionRequired, reason: "passkey_required"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			record := &coreauth.Auth{
				ID:       "chatgpt-web-status.json",
				FileName: "chatgpt-web-status.json",
				Provider: "chatgpt-web",
				Status:   coreauth.StatusDisabled,
				Disabled: true,
				Metadata: map[string]any{
					"type":             "chatgpt-web",
					"disabled":         true,
					"lifecycle_state":  test.state,
					"lifecycle_reason": test.reason,
				},
			}
			if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
				t.Fatal(errRegister)
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			request := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"chatgpt-web-status.json","disabled":false}`))
			request.Header.Set("Content-Type", "application/json")
			ctx.Request = request

			h.PatchAuthFileStatus(ctx)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, body=%s", recorder.Code, recorder.Body.String())
			}
			current, ok := manager.GetByID(record.ID)
			if !ok || current == nil {
				t.Fatal("enabled auth is not registered")
			}
			if current.Disabled || current.Status != coreauth.StatusError || current.StatusMessage != test.reason {
				t.Fatalf("enabled lifecycle = disabled:%v status:%q message:%q", current.Disabled, current.Status, current.StatusMessage)
			}
			if _, exists := current.Metadata["disabled"]; exists {
				t.Fatalf("disabled metadata was not cleared: %#v", current.Metadata)
			}
		})
	}
}

func TestBuildAuthFromFileData_SetsSourceHashAttribute(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	path := filepath.Join(authDir, "managed.json")
	data := []byte(`{"type":"antigravity","email":"same@example.com"}`)

	auth, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", err)
	}

	wantRaw, err := coreauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := coreauth.SourceHashFromBytes(wantRaw)
	if rawHash := coreauth.SourceHashFromBytes(data); rawHash == wantHash {
		t.Fatal("expected canonical metadata hash to differ from raw upload hash")
	}
	if got := auth.Attributes[coreauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
}

func TestBuildAuthFromFileData_PreservesDisabledState(t *testing.T) {
	authDir := t.TempDir()
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)

	path := filepath.Join(authDir, "managed-disabled.json")
	data := []byte(`{"type":"antigravity","email":"same@example.com","disabled":true}`)

	auth, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", err)
	}
	if !auth.Disabled {
		t.Fatal("expected auth to remain disabled")
	}
	if auth.Status != coreauth.StatusDisabled {
		t.Fatalf("status = %q, want %q", auth.Status, coreauth.StatusDisabled)
	}
	wantRaw, err := coreauth.CanonicalMetadataBytes(auth)
	if err != nil {
		t.Fatalf("CanonicalMetadataBytes() error = %v", err)
	}
	wantHash := coreauth.SourceHashFromBytes(wantRaw)
	if got := auth.Attributes[coreauth.SourceHashAttributeKey]; got != wantHash {
		t.Fatalf("source hash = %q, want %q", got, wantHash)
	}
}

func TestBuildAuthFromFileData_MapsChatGPTWebLifecycle(t *testing.T) {
	tests := []struct {
		name         string
		state        string
		reason       string
		disabled     bool
		wantStatus   coreauth.Status
		wantMessage  string
		wantDisabled bool
	}{
		{
			name:        "active",
			state:       coreauth.LifecycleStateActive,
			reason:      "ready",
			wantStatus:  coreauth.StatusActive,
			wantMessage: "ready",
		},
		{
			name:        "login pending",
			state:       coreauth.LifecycleStateLoginPending,
			reason:      "awaiting_login",
			wantStatus:  coreauth.StatusPending,
			wantMessage: "awaiting_login",
		},
		{
			name:        "interaction required",
			state:       coreauth.LifecycleStateInteractionRequired,
			reason:      "passkey_required",
			wantStatus:  coreauth.StatusError,
			wantMessage: "passkey_required",
		},
		{
			name:        "dead",
			state:       coreauth.LifecycleStateDead,
			reason:      "account_deactivated",
			wantStatus:  coreauth.StatusError,
			wantMessage: "account_deactivated",
		},
		{
			name:        "invalid state fails closed",
			state:       "secret-shaped-invalid-state",
			reason:      "secret-shaped-invalid-reason",
			wantStatus:  coreauth.StatusError,
			wantMessage: "authentication_failed",
		},
		{
			name:         "disabled overrides dead",
			state:        coreauth.LifecycleStateDead,
			reason:       "account_deactivated",
			disabled:     true,
			wantStatus:   coreauth.StatusDisabled,
			wantDisabled: true,
		},
	}

	authDir := t.TempDir()
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metadata := map[string]any{
				"type":             "chatgpt-web",
				"lifecycle_state":  test.state,
				"lifecycle_reason": test.reason,
			}
			if test.disabled {
				metadata["disabled"] = true
			}
			data, errMarshal := json.Marshal(metadata)
			if errMarshal != nil {
				t.Fatalf("marshal metadata: %v", errMarshal)
			}
			path := filepath.Join(authDir, strings.ReplaceAll(test.name, " ", "-")+".json")
			auth, errBuild := h.buildAuthFromFileData(path, data)
			if errBuild != nil {
				t.Fatalf("buildAuthFromFileData() error = %v", errBuild)
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
		})
	}
}

func TestBuildAuthFileEntry_ExposesLastErrorStatusCode(t *testing.T) {
	authDir := t.TempDir()
	path := filepath.Join(authDir, "error-status.json")
	if err := os.WriteFile(path, []byte(`{"type":"codex"}`), 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, nil)
	auth := &coreauth.Auth{
		ID:       "error-status.json",
		Provider: "codex",
		FileName: "error-status.json",
		Attributes: map[string]string{
			"path": path,
		},
		LastError: &coreauth.Error{
			HTTPStatus: http.StatusUnauthorized,
			Message:    "authentication token has been invalidated",
		},
		StatusMessage: "authentication token has been invalidated",
	}

	entry := h.buildAuthFileEntry(auth)
	if entry == nil {
		t.Fatal("expected auth file entry")
	}
	if got, ok := entry["last_error_status_code"].(int); !ok || got != http.StatusUnauthorized {
		t.Fatalf("last_error_status_code = %#v, want %d", entry["last_error_status_code"], http.StatusUnauthorized)
	}
	lastError, ok := entry["last_error"].(*coreauth.Error)
	if !ok || lastError == nil || lastError.HTTPStatus != http.StatusUnauthorized {
		t.Fatalf("last_error = %#v, want unauthorized error", entry["last_error"])
	}
}

func TestUpsertAuthRecord_PreservesCooldownStateForSameSourceRewrite(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	path := filepath.Join(authDir, "managed.json")
	data := []byte(`{"type":"antigravity","email":"same@example.com"}`)
	authID := h.authIDForPath(path)
	retryAt := time.Now().Add(5 * time.Minute).UTC()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write auth file: %v", err)
	}

	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	loaded, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(loaded) != 1 || loaded[0] == nil {
		t.Fatalf("expected 1 loaded auth, got %d", len(loaded))
	}
	existing := loaded[0]
	if existing.ID != authID {
		t.Fatalf("loaded auth ID = %q, want %q", existing.ID, authID)
	}
	existing.Status = coreauth.StatusError
	existing.Unavailable = true
	existing.LastError = &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"}
	existing.StatusMessage = "quota exhausted"
	existing.NextRetryAfter = retryAt
	existing.Quota = coreauth.QuotaState{
		Exceeded:      true,
		NextRecoverAt: retryAt,
	}
	existing.ModelStates = map[string]*coreauth.ModelState{
		"gemini-2.5-pro": {
			Unavailable:    true,
			NextRetryAfter: retryAt,
			Quota: coreauth.QuotaState{
				Exceeded:      true,
				NextRecoverAt: retryAt,
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), existing); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	rebuilt, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", err)
	}
	if err := h.upsertAuthRecord(context.Background(), rebuilt); err != nil {
		t.Fatalf("upsertAuthRecord() error = %v", err)
	}

	current, ok := manager.GetByID(authID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if !current.Unavailable {
		t.Fatal("expected unavailable flag to be preserved")
	}
	if current.LastError == nil || current.LastError.HTTPStatus != 429 {
		t.Fatalf("last error = %#v, want preserved 429 error", current.LastError)
	}
	if current.Status != coreauth.StatusError {
		t.Fatalf("status = %q, want %q", current.Status, coreauth.StatusError)
	}
	if current.StatusMessage != "quota exhausted" {
		t.Fatalf("status message = %q, want %q", current.StatusMessage, "quota exhausted")
	}
	if !current.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("next retry after = %v, want %v", current.NextRetryAfter, retryAt)
	}
	if !current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want exceeded state preserved", current.Quota)
	}
	state, ok := current.ModelStates["gemini-2.5-pro"]
	if !ok || state == nil {
		t.Fatal("expected model state to be preserved")
	}
	if !state.Unavailable {
		t.Fatal("expected model unavailable state to be preserved")
	}
	if !state.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("model next retry after = %v, want %v", state.NextRetryAfter, retryAt)
	}
	if !state.Quota.Exceeded {
		t.Fatalf("model quota = %#v, want exceeded state preserved", state.Quota)
	}
}

func TestUpsertAuthRecord_PreservesCooldownStateForStorageBackedSameSourceRewrite(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)

	record := &coreauth.Auth{
		ID:       "managed-token.json",
		FileName: "managed-token.json",
		Provider: "antigravity",
		Storage:  &managementTestTokenStorage{},
		Metadata: map[string]any{
			"type":  "antigravity",
			"email": "same@example.com",
		},
	}
	path, err := store.Save(context.Background(), record)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if got, ok := record.Metadata["access_token"].(string); !ok || got != "tok-storage" {
		t.Fatalf("metadata access_token = %#v, want %q", record.Metadata["access_token"], "tok-storage")
	}

	retryAt := time.Now().Add(5 * time.Minute).UTC()
	record.Status = coreauth.StatusError
	record.Unavailable = true
	record.LastError = &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"}
	record.StatusMessage = "quota exhausted"
	record.NextRetryAfter = retryAt
	record.Quota = coreauth.QuotaState{
		Exceeded:      true,
		NextRecoverAt: retryAt,
	}
	record.ModelStates = map[string]*coreauth.ModelState{
		"gemini-2.5-pro": {
			Unavailable:    true,
			NextRetryAfter: retryAt,
			Quota: coreauth.QuotaState{
				Exceeded:      true,
				NextRecoverAt: retryAt,
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	rebuilt, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", err)
	}
	if err := h.upsertAuthRecord(context.Background(), rebuilt); err != nil {
		t.Fatalf("upsertAuthRecord() error = %v", err)
	}

	current, ok := manager.GetByID(record.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if !current.Unavailable {
		t.Fatal("expected unavailable flag to be preserved")
	}
	if current.LastError == nil || current.LastError.HTTPStatus != 429 {
		t.Fatalf("last error = %#v, want preserved 429 error", current.LastError)
	}
	if current.Status != coreauth.StatusError {
		t.Fatalf("status = %q, want %q", current.Status, coreauth.StatusError)
	}
	if current.StatusMessage != "quota exhausted" {
		t.Fatalf("status message = %q, want %q", current.StatusMessage, "quota exhausted")
	}
	if !current.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("next retry after = %v, want %v", current.NextRetryAfter, retryAt)
	}
	if !current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want exceeded state preserved", current.Quota)
	}
	state, ok := current.ModelStates["gemini-2.5-pro"]
	if !ok || state == nil {
		t.Fatal("expected model state to be preserved")
	}
	if !state.Unavailable {
		t.Fatal("expected model unavailable state to be preserved")
	}
	if !state.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("model next retry after = %v, want %v", state.NextRetryAfter, retryAt)
	}
	if !state.Quota.Exceeded {
		t.Fatalf("model quota = %#v, want exceeded state preserved", state.Quota)
	}
}

func TestUpsertAuthRecord_PreservesVertexMetadataAndCooldownStateForSameSourceRewrite(t *testing.T) {
	authDir := t.TempDir()
	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: authDir}, manager)

	store := sdkauth.NewFileTokenStore()
	store.SetBaseDir(authDir)

	record := &coreauth.Auth{
		ID:       "vertex.json",
		FileName: "vertex.json",
		Provider: "vertex",
		Storage: &vertex.VertexCredentialStorage{
			ServiceAccount: map[string]any{
				"type":         "service_account",
				"project_id":   "vertex-project",
				"client_email": "vertex@example.com",
			},
			ProjectID: "vertex-project",
			Email:     "vertex@example.com",
			Location:  "us-central1",
		},
		Metadata: map[string]any{
			"type":                 "vertex",
			"email":                "vertex@example.com",
			"label":                "vertex-label",
			"tool_prefix_disabled": true,
		},
	}
	path, err := store.Save(context.Background(), record)
	if err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	retryAt := time.Now().Add(5 * time.Minute).UTC()
	record.Status = coreauth.StatusError
	record.Unavailable = true
	record.LastError = &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"}
	record.StatusMessage = "quota exhausted"
	record.NextRetryAfter = retryAt
	record.Quota = coreauth.QuotaState{
		Exceeded:      true,
		NextRecoverAt: retryAt,
	}
	record.ModelStates = map[string]*coreauth.ModelState{
		"gemini-2.5-pro": {
			Unavailable:    true,
			NextRetryAfter: retryAt,
			Quota: coreauth.QuotaState{
				Exceeded:      true,
				NextRecoverAt: retryAt,
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	rebuilt, err := h.buildAuthFromFileData(path, data)
	if err != nil {
		t.Fatalf("buildAuthFromFileData() error = %v", err)
	}
	if err := h.upsertAuthRecord(context.Background(), rebuilt); err != nil {
		t.Fatalf("upsertAuthRecord() error = %v", err)
	}

	current, ok := manager.GetByID(record.ID)
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if got, ok := current.Metadata["label"].(string); !ok || got != "vertex-label" {
		t.Fatalf("metadata label = %#v, want %q", current.Metadata["label"], "vertex-label")
	}
	if got, ok := current.Metadata["tool_prefix_disabled"].(bool); !ok || !got {
		t.Fatalf("metadata tool_prefix_disabled = %#v, want true", current.Metadata["tool_prefix_disabled"])
	}
	if !current.Unavailable {
		t.Fatal("expected unavailable flag to be preserved")
	}
	if current.LastError == nil || current.LastError.HTTPStatus != 429 {
		t.Fatalf("last error = %#v, want preserved 429 error", current.LastError)
	}
	if current.Status != coreauth.StatusError {
		t.Fatalf("status = %q, want %q", current.Status, coreauth.StatusError)
	}
	if !current.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("next retry after = %v, want %v", current.NextRetryAfter, retryAt)
	}
	if !current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want exceeded state preserved", current.Quota)
	}
	state, ok := current.ModelStates["gemini-2.5-pro"]
	if !ok || state == nil {
		t.Fatal("expected model state to be preserved")
	}
	if !state.Unavailable {
		t.Fatal("expected model unavailable state to be preserved")
	}
	if !state.NextRetryAfter.Equal(retryAt) {
		t.Fatalf("model next retry after = %v, want %v", state.NextRetryAfter, retryAt)
	}
	if !state.Quota.Exceeded {
		t.Fatalf("model quota = %#v, want exceeded state preserved", state.Quota)
	}
}

func TestPatchAuthFileStatus_ReenableClearsCooldownState(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	record := &coreauth.Auth{
		ID:          "status-reset.json",
		FileName:    "status-reset.json",
		Provider:    "antigravity",
		Status:      coreauth.StatusError,
		Unavailable: true,
		Metadata: map[string]any{
			"type": "antigravity",
		},
		LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
		StatusMessage:  "quota exhausted",
		NextRetryAfter: time.Now().Add(5 * time.Minute).UTC(),
		Quota: coreauth.QuotaState{
			Exceeded: true,
		},
		ModelStates: map[string]*coreauth.ModelState{
			"gemini-2.5-pro": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
				NextRetryAfter: time.Now().Add(5 * time.Minute).UTC(),
				Quota: coreauth.QuotaState{
					Exceeded: true,
				},
			},
		},
	}
	if err := coreauth.SetCanonicalSourceHashAttribute(record); err != nil {
		t.Fatalf("SetCanonicalSourceHashAttribute() error = %v", err)
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPatch, "/v0/management/auth-files/status", bytes.NewBufferString(`{"name":"status-reset.json","disabled":false}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.PatchAuthFileStatus(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}

	current, ok := manager.GetByID("status-reset.json")
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if current.Disabled {
		t.Fatal("expected auth to be enabled")
	}
	if current.Status != coreauth.StatusActive {
		t.Fatalf("status = %q, want %q", current.Status, coreauth.StatusActive)
	}
	if current.Unavailable {
		t.Fatal("expected auth unavailable flag to be cleared")
	}
	if current.LastError != nil {
		t.Fatalf("last error = %#v, want nil", current.LastError)
	}
	if current.StatusMessage != "" {
		t.Fatalf("status message = %q, want empty", current.StatusMessage)
	}
	if !current.NextRetryAfter.IsZero() {
		t.Fatalf("next retry after = %v, want zero", current.NextRetryAfter)
	}
	if current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want zero state", current.Quota)
	}
	state := current.ModelStates["gemini-2.5-pro"]
	if state == nil {
		t.Fatal("expected model state to remain present")
	}
	if state.Status != coreauth.StatusActive {
		t.Fatalf("model status = %q, want %q", state.Status, coreauth.StatusActive)
	}
	if state.StatusMessage != "" {
		t.Fatalf("model status message = %q, want empty", state.StatusMessage)
	}
	if state.Unavailable {
		t.Fatal("expected model unavailable flag to be cleared")
	}
	if state.LastError != nil {
		t.Fatalf("model last error = %#v, want nil", state.LastError)
	}
	if !state.NextRetryAfter.IsZero() {
		t.Fatalf("model next retry after = %v, want zero", state.NextRetryAfter)
	}
	if state.Quota.Exceeded {
		t.Fatalf("model quota = %#v, want zero state", state.Quota)
	}
}

func TestClearAllAuthCooldowns_ClearsCooldownState(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	retryAt := time.Now().Add(5 * time.Minute).UTC()
	record := &coreauth.Auth{
		ID:             "cooldown-all.json",
		FileName:       "cooldown-all.json",
		Provider:       "gemini",
		Status:         coreauth.StatusError,
		Unavailable:    true,
		CooldownScope:  "auth",
		StatusMessage:  "quota exhausted",
		LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
		NextRetryAfter: retryAt,
		Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryAt},
		ModelStates: map[string]*coreauth.ModelState{
			"gemini-2.5-pro": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
				NextRetryAfter: retryAt,
				Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryAt},
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	disabled := &coreauth.Auth{
		ID:             "cooldown-disabled.json",
		FileName:       "cooldown-disabled.json",
		Provider:       "gemini",
		Status:         coreauth.StatusDisabled,
		Disabled:       true,
		Unavailable:    true,
		CooldownScope:  "auth",
		StatusMessage:  "disabled via management API",
		NextRetryAfter: retryAt,
		Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryAt},
	}
	if _, errRegister := manager.Register(context.Background(), disabled); errRegister != nil {
		t.Fatalf("register disabled auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear", nil)

	h.ClearAllAuthCooldowns(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	current, ok := manager.GetByID("cooldown-all.json")
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if current.Status != coreauth.StatusActive {
		t.Fatalf("status = %q, want %q", current.Status, coreauth.StatusActive)
	}
	if current.Unavailable {
		t.Fatal("expected auth unavailable flag to be cleared")
	}
	if !current.NextRetryAfter.IsZero() {
		t.Fatalf("next retry after = %v, want zero", current.NextRetryAfter)
	}
	if current.CooldownScope != "" {
		t.Fatalf("cooldown scope = %q, want empty", current.CooldownScope)
	}
	if current.LastError != nil {
		t.Fatalf("last error = %#v, want nil", current.LastError)
	}
	if current.Quota.Exceeded {
		t.Fatalf("quota = %#v, want zero state", current.Quota)
	}
	state := current.ModelStates["gemini-2.5-pro"]
	if state == nil {
		t.Fatal("expected model state to remain present")
	}
	if state.Status != coreauth.StatusActive {
		t.Fatalf("model status = %q, want %q", state.Status, coreauth.StatusActive)
	}
	if state.Unavailable || !state.NextRetryAfter.IsZero() || state.LastError != nil || state.Quota.Exceeded {
		t.Fatalf("model state = %#v, want cooldown cleared", state)
	}

	currentDisabled, ok := manager.GetByID("cooldown-disabled.json")
	if !ok || currentDisabled == nil {
		t.Fatal("expected disabled auth to remain registered")
	}
	if !currentDisabled.Disabled || currentDisabled.Status != coreauth.StatusDisabled {
		t.Fatalf("disabled auth status = disabled:%v status:%q, want still disabled", currentDisabled.Disabled, currentDisabled.Status)
	}
	if currentDisabled.Unavailable || !currentDisabled.NextRetryAfter.IsZero() || currentDisabled.CooldownScope != "" {
		t.Fatalf("disabled cooldown = unavailable:%v retry:%v scope:%q, want cooldown cleared only", currentDisabled.Unavailable, currentDisabled.NextRetryAfter, currentDisabled.CooldownScope)
	}
}

func TestClearSelectedAuthCooldowns_ClearsOnlySelectedModels(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	retryA := time.Now().Add(5 * time.Minute).UTC()
	retryB := time.Now().Add(10 * time.Minute).UTC()
	record := &coreauth.Auth{
		ID:             "cooldown-selected.json",
		FileName:       "cooldown-selected.json",
		Provider:       "gemini",
		Status:         coreauth.StatusError,
		Unavailable:    true,
		CooldownScope:  "model",
		StatusMessage:  "quota exhausted",
		LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
		NextRetryAfter: retryA,
		Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryA},
		ModelStates: map[string]*coreauth.ModelState{
			"gemini-2.5-pro": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
				NextRetryAfter: retryA,
				Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryA},
			},
			"gemini-2.5-flash": {
				Status:         coreauth.StatusError,
				StatusMessage:  "quota exhausted",
				Unavailable:    true,
				LastError:      &coreauth.Error{HTTPStatus: 429, Message: "quota exhausted"},
				NextRetryAfter: retryB,
				Quota:          coreauth.QuotaState{Exceeded: true, NextRecoverAt: retryB},
			},
		},
	}
	if _, errRegister := manager.Register(context.Background(), record); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-selected", bytes.NewBufferString(`{"items":[{"name":"cooldown-selected.json","models":["gemini-2.5-pro"]}]}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.ClearSelectedAuthCooldowns(ctx)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusOK, rec.Body.String())
	}
	current, ok := manager.GetByID("cooldown-selected.json")
	if !ok || current == nil {
		t.Fatal("expected auth to remain registered")
	}
	if current.Unavailable {
		t.Fatal("expected aggregate auth unavailable flag to be cleared after one model recovers")
	}
	if !current.NextRetryAfter.IsZero() {
		t.Fatalf("aggregate next retry after = %v, want zero", current.NextRetryAfter)
	}
	selected := current.ModelStates["gemini-2.5-pro"]
	if selected == nil {
		t.Fatal("expected selected model state")
	}
	if selected.Status != coreauth.StatusActive || selected.Unavailable || !selected.NextRetryAfter.IsZero() || selected.LastError != nil {
		t.Fatalf("selected model state = %#v, want cooldown cleared", selected)
	}
	other := current.ModelStates["gemini-2.5-flash"]
	if other == nil {
		t.Fatal("expected other model state")
	}
	if other.Status != coreauth.StatusError || !other.Unavailable || !other.NextRetryAfter.Equal(retryB) || other.LastError == nil {
		t.Fatalf("other model state = %#v, want cooldown preserved", other)
	}
}

func TestClearSelectedAuthCooldowns_RejectsEmptySelection(t *testing.T) {
	t.Setenv("MANAGEMENT_PASSWORD", "")

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/v0/management/auth-files/cooldowns/clear-selected", bytes.NewBufferString(`{}`))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	h.ClearSelectedAuthCooldowns(ctx)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

type managementTestTokenStorage struct {
	metadata map[string]any
}

func (s *managementTestTokenStorage) SetMetadata(meta map[string]any) {
	if meta == nil {
		s.metadata = nil
		return
	}
	cloned := make(map[string]any, len(meta))
	for key, value := range meta {
		cloned[key] = value
	}
	s.metadata = cloned
}

func (s *managementTestTokenStorage) MetadataSnapshot() map[string]any {
	if s == nil {
		return nil
	}
	cloned := make(map[string]any, len(s.metadata))
	for key, value := range s.metadata {
		cloned[key] = value
	}
	return cloned
}

func (s *managementTestTokenStorage) SaveTokenToFile(authFilePath string) error {
	payload := map[string]any{
		"access_token":  "tok-storage",
		"refresh_token": "refresh-storage",
	}
	for key, value := range s.metadata {
		payload[key] = value
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return os.WriteFile(authFilePath, raw, 0o600)
}
