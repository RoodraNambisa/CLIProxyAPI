package management

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	chatgptwebauth "github.com/router-for-me/CLIProxyAPI/v6/internal/auth/chatgptweb"
	"github.com/router-for-me/CLIProxyAPI/v6/internal/config"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v6/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v6/sdk/cliproxy/auth"
)

func TestDeleteAuthFileRetainsLinkedCodexSourceAndRestoreReactivatesIt(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)

	retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	var retainedBody map[string]any
	decodeChatGPTWebDependencyManagementResponse(t, retained, &retainedBody)
	if retainedBody["status"] != "retained" || retainedBody["deleted"] != false || retainedBody["dependent_count"] != float64(1) {
		t.Fatalf("retain response = %#v", retainedBody)
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || current == nil || !current.Disabled || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("retained source = %#v", current)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, source.FileName)); errStat != nil {
		t.Fatalf("retained source file: %v", errStat)
	}
	retainedFile, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
	if errRead != nil {
		t.Fatal(errRead)
	}
	disable := performChatGPTWebDependencyManagementRequest(router, http.MethodPatch, "/auth-files/status", `{"name":"codex-source.json","disabled":true}`)
	if disable.Code != http.StatusConflict {
		t.Fatalf("disable retained source status = %d, body=%s", disable.Code, disable.Body.String())
	}
	afterDisable, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
	if errRead != nil || !bytes.Equal(retainedFile, afterDisable) {
		t.Fatalf("retained source changed after status PATCH: err=%v before=%s after=%s", errRead, retainedFile, afterDisable)
	}

	enable := performChatGPTWebDependencyManagementRequest(router, http.MethodPatch, "/auth-files/status", `{"name":"codex-source.json","disabled":false}`)
	if enable.Code != http.StatusConflict {
		t.Fatalf("enable retained source status = %d, body=%s", enable.Code, enable.Body.String())
	}
	restored := performChatGPTWebDependencyManagementRequest(router, http.MethodPost, "/auth-files/restore", `{"name":"codex-source.json"}`)
	if restored.Code != http.StatusOK {
		t.Fatalf("restore status = %d, body=%s", restored.Code, restored.Body.String())
	}
	current, ok = manager.GetByID(source.ID)
	if !ok || current == nil || current.Disabled || coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("restored source = %#v", current)
	}
	data, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
	if errRead != nil {
		t.Fatal(errRead)
	}
	if strings.Contains(string(data), "retained_for_dependents") {
		t.Fatalf("retention state remained in restored file: %s", data)
	}
}

func TestDeleteAuthFilesWithDependenciesRejectsNilHandler(t *testing.T) {
	var h *Handler
	result := h.deleteAuthFilesWithDependencies(t.Context(), nil, "", "", []string{"auth.json"}, false, false)
	if len(result.failed) != 1 {
		t.Fatalf("failed = %#v, want one failure", result.failed)
	}
	if result.failed[0]["name"] != "auth.json" || result.failed[0]["status"] != http.StatusServiceUnavailable {
		t.Fatalf("failure = %#v", result.failed[0])
	}
}

func TestDeleteAuthFileRejectsInFlightWebConversionReservation(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	_, reservation, errReserve := manager.ReserveChatGPTWebDependent(t.Context(), source, "web-copy.json", "web-copy-uid", time.Now())
	if errReserve != nil {
		t.Fatal(errReserve)
	}

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "in-flight Web conversion") {
		t.Fatalf("delete status = %d, body=%s", response.Code, response.Body.String())
	}
	if _, errStat := os.Stat(filepath.Join(authDir, source.FileName)); errStat != nil {
		t.Fatalf("reserved source file: %v", errStat)
	}
	if errRelease := manager.ReleaseChatGPTWebDependentReservation(t.Context(), source.ID, "uid-a", reservation, time.Now()); errRelease != nil {
		t.Fatal(errRelease)
	}
}

func TestDeleteAuthFileRetainsCodexForPersistedWebMissingFromRuntime(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	diskOnly := managementDependencyWebAuth("disk-only-web.json", source.ID, "uid-a")
	payload, errMarshal := json.Marshal(diskOnly.Metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(filepath.Join(authDir, diskOnly.FileName), payload, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if _, loaded := manager.GetByID(diskOnly.ID); loaded {
		t.Fatal("disk-only Web credential unexpectedly exists in runtime")
	}

	retained := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if retained.Code != http.StatusAccepted || !strings.Contains(retained.Body.String(), `"dependent_count":1`) ||
		!strings.Contains(retained.Body.String(), diskOnly.FileName) {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || current == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("source after retained delete = %#v", current)
	}
}

func TestPatchAuthFileFieldsRejectsRetainedCodexSource(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)
	if retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", ""); retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	before, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
	if errRead != nil {
		t.Fatal(errRead)
	}

	response := performChatGPTWebDependencyManagementRequest(router, http.MethodPatch, "/auth-files/fields", `{"name":"codex-source.json","priority":7}`)
	if response.Code != http.StatusConflict {
		t.Fatalf("fields status = %d, body=%s", response.Code, response.Body.String())
	}
	after, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
	if errRead != nil || !bytes.Equal(before, after) {
		t.Fatalf("retained source changed: err=%v before=%s after=%s", errRead, before, after)
	}
}

func TestDeleteLastLinkedWebCredentialRemovesRetainedCodexSource(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	web := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	reconcileCalls := 0
	h.SetChatGPTWebDependencyReconcileHook(func(ctx context.Context, reason string) ([]string, error) {
		reconcileCalls++
		if reason != "management-delete" {
			t.Fatalf("reconcile reason = %q", reason)
		}
		return manager.ReconcileChatGPTWebDependencies(ctx)
	})
	router := chatGPTWebDependencyManagementRouter(h)

	retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	deleted := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name="+web.FileName, "")
	if deleted.Code != http.StatusOK {
		entries, _ := os.ReadDir(authDir)
		t.Fatalf("delete Web status = %d, body=%s, auths=%#v, files=%#v", deleted.Code, deleted.Body.String(), manager.List(), entries)
	}
	if _, ok := manager.GetByID(source.ID); ok {
		t.Fatal("retained source remained after its last dependent was deleted")
	}
	if _, ok := manager.GetByID(web.ID); ok {
		t.Fatal("Web dependent remained after deletion")
	}
	if reconcileCalls != 1 {
		t.Fatalf("dependency reconcile calls = %d, want 1", reconcileCalls)
	}
	for _, name := range []string{source.FileName, web.FileName} {
		if _, errStat := os.Stat(filepath.Join(authDir, name)); !os.IsNotExist(errStat) {
			t.Fatalf("file %s still exists: %v", name, errStat)
		}
	}
}

func TestCleanupRetainedCodexSourceUsesServiceHook(t *testing.T) {
	h, _, _ := newChatGPTWebDependencyManagementHandler(t)
	type contextKey string
	const key contextKey = "dependency-hook"
	ctx := context.WithValue(t.Context(), key, "present")
	called := 0
	h.SetChatGPTWebDependencyReconcileHook(func(hookCtx context.Context, reason string) ([]string, error) {
		called++
		if got := hookCtx.Value(key); got != "present" {
			t.Fatalf("hook context value = %v, want present", got)
		}
		if reason != "management" {
			t.Fatalf("hook reason = %q, want management", reason)
		}
		return []string{"codex-source.json"}, nil
	})

	h.cleanupRetainedCodexSource(ctx, "uid-a")
	if called != 1 {
		t.Fatalf("dependency reconcile hook calls = %d, want 1", called)
	}
}

func TestDeleteAuthFileCascadeRemovesCodexSourceAndAllLinkedWebCredentials(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	first := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-a.json", source.ID, "uid-a"))
	second := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-b.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)

	deleted := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json&dependency_action=cascade", "")
	if deleted.Code != http.StatusOK {
		t.Fatalf("cascade status = %d, body=%s", deleted.Code, deleted.Body.String())
	}
	for _, auth := range []*coreauth.Auth{source, first, second} {
		if _, ok := manager.GetByID(auth.ID); ok {
			t.Fatalf("auth %s remained after cascade", auth.ID)
		}
		if _, errStat := os.Stat(filepath.Join(authDir, auth.FileName)); !os.IsNotExist(errStat) {
			t.Fatalf("file %s still exists: %v", auth.FileName, errStat)
		}
	}
}

func TestDeleteAuthFileWithoutCredentialUIDUsesSourceGeneration(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("legacy-codex.json", "", false))

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=legacy-codex.json", "")
	if response.Code != http.StatusOK {
		t.Fatalf("delete legacy source status = %d, body=%s", response.Code, response.Body.String())
	}
	if _, ok := manager.GetByID(source.ID); ok {
		t.Fatal("legacy source remained in runtime")
	}
	if _, errStat := os.Stat(filepath.Join(authDir, source.FileName)); !os.IsNotExist(errStat) {
		t.Fatalf("legacy source file remained: %v", errStat)
	}
}

func TestCascadeDeleteRetainsSourceWhenDependentCannotBeDeleted(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	dependent := managementDependencyWebAuth("runtime-web", source.ID, "uid-a")
	dependent.Attributes = map[string]string{"runtime_only": "true"}
	if _, errRegister := manager.Register(coreauth.WithSkipPersist(t.Context()), dependent); errRegister != nil {
		t.Fatal(errRegister)
	}

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json&dependency_action=cascade", "")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("cascade status = %d, body=%s", response.Code, response.Body.String())
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("source after failed cascade = %#v", current)
	}
	if _, errStat := os.Stat(filepath.Join(authDir, source.FileName)); errStat != nil {
		t.Fatalf("retained source file: %v", errStat)
	}
}

func TestDeleteLinkedWebReportsCurrentSourceDeletionBeforeUnrelatedReconcileError(t *testing.T) {
	h, manager, _ := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	web := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)
	if retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", ""); retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	h.SetChatGPTWebDependencyReconcileHook(func(ctx context.Context, _ string) ([]string, error) {
		deleted, errReconcile := manager.ReconcileChatGPTWebDependencies(ctx)
		return deleted, errors.Join(errReconcile, errors.New("unrelated retained source failed"))
	})

	response := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name="+web.FileName, "")
	if response.Code != http.StatusOK {
		t.Fatalf("delete Web status = %d, body=%s", response.Code, response.Body.String())
	}
	if _, ok := manager.GetByID(source.ID); ok {
		t.Fatal("successfully reconciled source remained")
	}
}

func TestUploadAuthFileRejectsRetainedCodexInEveryUploadMode(t *testing.T) {
	tests := []struct {
		name       string
		batch      bool
		multipart  bool
		wantStatus int
	}{
		{name: "raw", wantStatus: http.StatusConflict},
		{name: "single multipart", multipart: true, wantStatus: http.StatusConflict},
		{name: "batch multipart", multipart: true, batch: true, wantStatus: http.StatusMultiStatus},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
			source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
			registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
			if retained := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json", ""); retained.Code != http.StatusAccepted {
				t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
			}
			before, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
			if errRead != nil {
				t.Fatal(errRead)
			}
			replacement := []byte(`{"type":"codex","access_token":"replacement"}`)
			recorder := httptest.NewRecorder()
			ctx, _ := gin.CreateTestContext(recorder)
			if test.multipart {
				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, errPart := writer.CreateFormFile("file", source.FileName)
				if errPart != nil {
					t.Fatal(errPart)
				}
				_, _ = part.Write(replacement)
				if test.batch {
					valid, errValid := writer.CreateFormFile("file", "valid.json")
					if errValid != nil {
						t.Fatal(errValid)
					}
					_, _ = valid.Write([]byte(`{"type":"claude","access_token":"valid"}`))
				}
				if errClose := writer.Close(); errClose != nil {
					t.Fatal(errClose)
				}
				ctx.Request = httptest.NewRequest(http.MethodPost, "/auth-files", &body)
				ctx.Request.Header.Set("Content-Type", writer.FormDataContentType())
			} else {
				ctx.Request = httptest.NewRequest(http.MethodPost, "/auth-files?name="+source.FileName, bytes.NewReader(replacement))
				ctx.Request.Header.Set("Content-Type", "application/json")
			}
			h.UploadAuthFile(ctx)
			if recorder.Code != test.wantStatus {
				t.Fatalf("upload status = %d, want %d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if test.batch && !strings.Contains(recorder.Body.String(), `"status":409`) {
				t.Fatalf("batch response = %s, want retained item status 409", recorder.Body.String())
			}
			after, errRead := os.ReadFile(filepath.Join(authDir, source.FileName))
			if errRead != nil || !bytes.Equal(after, before) {
				t.Fatalf("retained source changed: err=%v before=%s after=%s", errRead, before, after)
			}
			if current, ok := manager.GetByID(source.ID); !ok || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
				t.Fatalf("runtime retained source changed: %#v", current)
			}
		})
	}
}

func TestRestoreAuthFileRejectsExternallyReplacedSource(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)

	retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	path := filepath.Join(authDir, source.FileName)
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	var metadata map[string]any
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
		t.Fatal(errDecode)
	}
	metadata["note"] = "external replacement"
	replaced, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(path, replaced, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}

	restored := performChatGPTWebDependencyManagementRequest(router, http.MethodPost, "/auth-files/restore", `{"name":"codex-source.json"}`)
	if restored.Code != http.StatusConflict {
		t.Fatalf("restore replaced source status = %d, body=%s", restored.Code, restored.Body.String())
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("runtime source changed after rejected restore: %#v", current)
	}
}

func TestRestoreAuthFileRejectsOversizedRequest(t *testing.T) {
	h, _, _ := newChatGPTWebDependencyManagementHandler(t)
	requestBody := `{"name":"` + strings.Repeat("x", (1<<20)+1) + `"}`
	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodPost, "/auth-files/restore", requestBody)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413; body=%s", response.Code, response.Body.String())
	}
}

func TestRestoreAuthFileReturnsNotFoundWhenAuthDirectoryIsMissing(t *testing.T) {
	manager := coreauth.NewManager(&memoryAuthStore{}, nil, nil)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: filepath.Join(t.TempDir(), "missing")}, manager)
	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodPost, "/auth-files/restore", `{"name":"codex-source.json"}`)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", response.Code, response.Body.String())
	}
}

func TestRestoreAuthFileReturnsUnavailableWithoutAuthManager(t *testing.T) {
	h := NewHandler(&config.Config{AuthDir: t.TempDir()}, "", nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	response := performChatGPTWebDependencyManagementRequest(
		chatGPTWebDependencyManagementRouter(h),
		http.MethodPost,
		"/auth-files/restore",
		`{"name":"codex-source.json"}`,
	)
	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "auth manager unavailable") {
		t.Fatalf("restore status = %d, body=%s", response.Code, response.Body.String())
	}
}

func TestRetainAuthFileRejectsReplacementBetweenCheckAndSave(t *testing.T) {
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	path := filepath.Join(authDir, source.FileName)
	store.setBeforeConditionalSave(func() { replaceManagementDependencyAuthFile(t, path, "external retain replacement", "") })

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("retain status = %d, body=%s", response.Code, response.Body.String())
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || current == nil || current.Disabled || coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("runtime source changed after rejected retain: %#v", current)
	}
	assertManagementDependencyAuthNote(t, path, "external retain replacement")
}

func TestRestoreAuthFileRejectsReplacementBetweenCheckAndSave(t *testing.T) {
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)
	retained := performChatGPTWebDependencyManagementRequest(router, http.MethodDelete, "/auth-files?name=codex-source.json", "")
	if retained.Code != http.StatusAccepted {
		t.Fatalf("retain status = %d, body=%s", retained.Code, retained.Body.String())
	}
	path := filepath.Join(authDir, source.FileName)
	store.setBeforeConditionalSave(func() { replaceManagementDependencyAuthFile(t, path, "external restore replacement", "") })

	restored := performChatGPTWebDependencyManagementRequest(router, http.MethodPost, "/auth-files/restore", `{"name":"codex-source.json"}`)
	if restored.Code != http.StatusConflict {
		t.Fatalf("restore status = %d, body=%s", restored.Code, restored.Body.String())
	}
	current, ok := manager.GetByID(source.ID)
	if !ok || current == nil || !coreauth.ChatGPTWebAuthRetainedForDependents(current) {
		t.Fatalf("runtime source changed after rejected restore: %#v", current)
	}
	assertManagementDependencyAuthNote(t, path, "external restore replacement")
}

func TestCascadeDeleteRejectsReplacedCodexSourceBeforeDeletingDependents(t *testing.T) {
	h, manager, authDir := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	web := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	path := filepath.Join(authDir, source.FileName)
	replaceManagementDependencyAuthFile(t, path, "external cascade replacement", "uid-b")

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json&dependency_action=cascade", "")
	if response.Code != http.StatusConflict {
		t.Fatalf("cascade status = %d, body=%s", response.Code, response.Body.String())
	}
	for _, authID := range []string{source.ID, web.ID} {
		if _, ok := manager.GetByID(authID); !ok {
			t.Fatalf("auth %s was removed after rejected cascade", authID)
		}
	}
	if _, errStat := os.Stat(filepath.Join(authDir, web.FileName)); errStat != nil {
		t.Fatalf("Web dependent was deleted before source validation: %v", errStat)
	}
	assertManagementDependencyAuthNote(t, path, "external cascade replacement")
}

func TestCascadeDeleteRejectsExternallyReplacedWebDependent(t *testing.T) {
	h, manager, authDir, store := newChatGPTWebDependencyRaceManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	web := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	webPath := filepath.Join(authDir, web.FileName)
	store.setBeforeConditionalDelete(func(id string) {
		if id == web.ID {
			replaceManagementDependencyAuthFile(t, webPath, "external dependent replacement", "")
		}
	})

	response := performChatGPTWebDependencyManagementRequest(chatGPTWebDependencyManagementRouter(h), http.MethodDelete, "/auth-files?name=codex-source.json&dependency_action=cascade", "")
	if response.Code != http.StatusMultiStatus {
		t.Fatalf("cascade status = %d, body=%s", response.Code, response.Body.String())
	}
	currentSource, sourceExists := manager.GetByID(source.ID)
	if !sourceExists || !coreauth.ChatGPTWebAuthRetainedForDependents(currentSource) {
		t.Fatalf("source after failed dependent delete = %#v", currentSource)
	}
	if _, webExists := manager.GetByID(web.ID); !webExists {
		t.Fatal("externally replaced Web dependent was removed from runtime")
	}
	assertManagementDependencyAuthNote(t, webPath, "external dependent replacement")
	if !strings.Contains(response.Body.String(), `"dependent_count":1`) || !strings.Contains(response.Body.String(), web.FileName) {
		t.Fatalf("cascade response did not report current dependency: %s", response.Body.String())
	}
}

func TestListAuthFilesIncludesChatGPTWebDependencyFields(t *testing.T) {
	h, manager, _ := newChatGPTWebDependencyManagementHandler(t)
	source := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	webAuth := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", source.ID, "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)

	response := performChatGPTWebDependencyManagementRequest(router, http.MethodGet, "/auth-files", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Files []map[string]any `json:"files"`
	}
	decodeChatGPTWebDependencyManagementResponse(t, response, &body)
	entries := make(map[string]map[string]any, len(body.Files))
	for _, entry := range body.Files {
		name, _ := entry["name"].(string)
		entries[name] = entry
	}
	codex := entries[source.FileName]
	if codex["credential_uid"] != "uid-a" || codex["dependent_count"] != float64(1) {
		t.Fatalf("Codex dependency summary = %#v", codex)
	}
	webEntry := entries[webAuth.FileName]
	if webEntry["refresh_strategy"] != string(chatgptwebauth.RefreshStrategyCodexSource) || webEntry["source_auth_id"] != source.ID || webEntry["source_missing"] != false {
		t.Fatalf("Web dependency summary = %#v, entries=%#v", webEntry, entries)
	}
}

func TestListAuthFilesMarksMismatchedLinkedSourceIDMissing(t *testing.T) {
	h, manager, _ := newChatGPTWebDependencyManagementHandler(t)
	registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyCodexAuth("codex-source.json", "uid-a", false))
	webAuth := registerChatGPTWebDependencyManagementAuth(t, manager, managementDependencyWebAuth("web-copy.json", "missing-source.json", "uid-a"))
	router := chatGPTWebDependencyManagementRouter(h)

	response := performChatGPTWebDependencyManagementRequest(router, http.MethodGet, "/auth-files", "")
	if response.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", response.Code, response.Body.String())
	}
	var body struct {
		Files []map[string]any `json:"files"`
	}
	decodeChatGPTWebDependencyManagementResponse(t, response, &body)
	for _, entry := range body.Files {
		if entry["name"] != webAuth.FileName {
			continue
		}
		if entry["source_missing"] != true {
			t.Fatalf("source_missing = %#v, want true; entry=%#v", entry["source_missing"], entry)
		}
		return
	}
	t.Fatalf("linked Web credential %q not found", webAuth.FileName)
}

func TestListAuthFilesDiskFallbackIncludesDependencyFieldsWithoutSecrets(t *testing.T) {
	authDir := t.TempDir()
	h := NewHandler(&config.Config{AuthDir: authDir}, "", nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	sourceName := "codex-source.json"
	sourcePath := filepath.Join(authDir, sourceName)
	sourceID := h.authIDForPath(sourcePath)
	sourceData := map[string]any{
		"type": "codex", "credential_uid": "uid-a", "deletion_state": coreauth.ChatGPTWebDeletionStateRetained,
		"deletion_requested_at": "2026-07-19T00:00:00Z", "disabled": true, "refresh_token": "codex-secret",
	}
	webData := map[string]any{
		"type": chatgptwebauth.Provider, "credential_uid": "web-uid", "refresh_strategy": "codex_source",
		"source_auth_id": sourceID, "source_credential_uid": "uid-a", "access_token": "web-secret", "email": "person@example.com",
	}
	for name, metadata := range map[string]map[string]any{sourceName: sourceData, "web-copy.json": webData} {
		payload, errMarshal := json.Marshal(metadata)
		if errMarshal != nil {
			t.Fatal(errMarshal)
		}
		if errWrite := os.WriteFile(filepath.Join(authDir, name), payload, 0o600); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/auth-files", nil)
	h.ListAuthFiles(ctx)
	if recorder.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "codex-secret") || strings.Contains(recorder.Body.String(), "web-secret") {
		t.Fatalf("list leaked credential secret: %s", recorder.Body.String())
	}
	var body struct {
		Files []map[string]any `json:"files"`
	}
	decodeChatGPTWebDependencyManagementResponse(t, recorder, &body)
	entries := make(map[string]map[string]any, len(body.Files))
	for _, entry := range body.Files {
		name, _ := entry["name"].(string)
		entries[name] = entry
	}
	if source := entries[sourceName]; source["credential_uid"] != "uid-a" || source["dependent_count"] != float64(1) || source["retained_for_dependents"] != true {
		t.Fatalf("fallback Codex summary = %#v", source)
	}
	if web := entries["web-copy.json"]; web["refresh_strategy"] != "codex_source" || web["source_missing"] != false {
		t.Fatalf("fallback Web summary = %#v", web)
	}
}

func newChatGPTWebDependencyManagementHandler(t *testing.T) (*Handler, *coreauth.Manager, string) {
	t.Helper()
	authDir := t.TempDir()
	store := sdkAuth.NewFileTokenStore()
	store.SetBaseDir(authDir)
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	h.tokenStore = store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return h, manager, authDir
}

type dependencyConditionalSaveRaceStore struct {
	*sdkAuth.FileTokenStore
	mu                      sync.Mutex
	beforeConditionalSave   func()
	beforeConditionalDelete func(string)
	afterSave               func(string)
}

func (store *dependencyConditionalSaveRaceStore) Save(ctx context.Context, auth *coreauth.Auth) (string, error) {
	if _, conditional := coreauth.SourceHashSavePrecondition(ctx); conditional {
		store.mu.Lock()
		hook := store.beforeConditionalSave
		store.beforeConditionalSave = nil
		store.mu.Unlock()
		if hook != nil {
			hook()
		}
	}
	name, errSave := store.FileTokenStore.Save(ctx, auth)
	if errSave != nil {
		return name, errSave
	}
	store.runAfterSave(auth)
	return name, nil
}

func (store *dependencyConditionalSaveRaceStore) SaveIfAbsent(ctx context.Context, auth *coreauth.Auth) (string, error) {
	name, errSave := store.FileTokenStore.SaveIfAbsent(ctx, auth)
	if errSave != nil {
		return name, errSave
	}
	store.runAfterSave(auth)
	return name, nil
}

func (store *dependencyConditionalSaveRaceStore) runAfterSave(auth *coreauth.Auth) {
	store.mu.Lock()
	afterSave := store.afterSave
	store.mu.Unlock()
	if afterSave != nil && auth != nil {
		afterSave(auth.ID)
	}
}

func (store *dependencyConditionalSaveRaceStore) setBeforeConditionalSave(hook func()) {
	store.mu.Lock()
	store.beforeConditionalSave = hook
	store.mu.Unlock()
}

func (store *dependencyConditionalSaveRaceStore) setAfterSave(hook func(string)) {
	store.mu.Lock()
	store.afterSave = hook
	store.mu.Unlock()
}

func (store *dependencyConditionalSaveRaceStore) DeleteIfSourceHashMatches(ctx context.Context, id, expectedSourceHash string) error {
	store.mu.Lock()
	hook := store.beforeConditionalDelete
	store.beforeConditionalDelete = nil
	store.mu.Unlock()
	if hook != nil {
		hook(id)
	}
	return store.FileTokenStore.DeleteIfSourceHashMatches(ctx, id, expectedSourceHash)
}

func (store *dependencyConditionalSaveRaceStore) setBeforeConditionalDelete(hook func(string)) {
	store.mu.Lock()
	store.beforeConditionalDelete = hook
	store.mu.Unlock()
}

func newChatGPTWebDependencyRaceManagementHandler(t *testing.T) (*Handler, *coreauth.Manager, string, *dependencyConditionalSaveRaceStore) {
	t.Helper()
	authDir := t.TempDir()
	baseStore := sdkAuth.NewFileTokenStore()
	baseStore.SetBaseDir(authDir)
	store := &dependencyConditionalSaveRaceStore{FileTokenStore: baseStore}
	manager := coreauth.NewManager(store, nil, nil)
	h := NewHandler(&config.Config{AuthDir: authDir}, "", manager)
	h.tokenStore = store
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if errShutdown := h.Shutdown(ctx); errShutdown != nil {
			t.Errorf("shutdown management handler: %v", errShutdown)
		}
	})
	return h, manager, authDir, store
}

func replaceManagementDependencyAuthFile(t *testing.T, path, note, credentialUID string) {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	metadata := make(map[string]any)
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
		t.Fatal(errDecode)
	}
	metadata["note"] = note
	if credentialUID != "" {
		metadata["credential_uid"] = credentialUID
	}
	replacement, errMarshal := json.Marshal(metadata)
	if errMarshal != nil {
		t.Fatal(errMarshal)
	}
	if errWrite := os.WriteFile(path, replacement, 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
}

func assertManagementDependencyAuthNote(t *testing.T, path, want string) {
	t.Helper()
	data, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	metadata := make(map[string]any)
	if errDecode := json.Unmarshal(data, &metadata); errDecode != nil {
		t.Fatal(errDecode)
	}
	if got, _ := metadata["note"].(string); got != want {
		t.Fatalf("auth note = %q, want %q; data=%s", got, want, data)
	}
}

func registerChatGPTWebDependencyManagementAuth(t *testing.T, manager *coreauth.Manager, auth *coreauth.Auth) *coreauth.Auth {
	t.Helper()
	installed, errRegister := manager.Register(t.Context(), auth)
	if errRegister != nil {
		t.Fatal(errRegister)
	}
	return installed
}

func managementDependencyCodexAuth(name, uid string, disabled bool) *coreauth.Auth {
	status := coreauth.StatusActive
	if disabled {
		status = coreauth.StatusDisabled
	}
	return &coreauth.Auth{
		ID:       name,
		Provider: "codex",
		FileName: name,
		Disabled: disabled,
		Status:   status,
		Metadata: map[string]any{
			"type":           "codex",
			"credential_uid": uid,
			"account_id":     "account-" + uid,
			"email":          uid + "@example.com",
			"access_token":   "codex-access",
			"disabled":       disabled,
		},
	}
}

func managementDependencyWebAuth(name, sourceID, sourceUID string) *coreauth.Auth {
	identitySource := managementDependencyCodexAuth(sourceID, sourceUID, false)
	identitySource.Provider = chatgptwebauth.Provider
	return &coreauth.Auth{
		ID:       name,
		Provider: chatgptwebauth.Provider,
		FileName: name,
		Status:   coreauth.StatusActive,
		Metadata: map[string]any{
			"type":                  chatgptwebauth.Provider,
			"credential_uid":        "web-" + name,
			"credential_mode":       chatgptwebauth.CredentialModeLinkedCodex,
			"refresh_strategy":      string(chatgptwebauth.RefreshStrategyCodexSource),
			"source_auth_id":        sourceID,
			"source_credential_uid": sourceUID,
			"source_identity":       coreauth.ChatGPTWebCredentialReferenceValue(identitySource),
			"email":                 strings.TrimSuffix(name, ".json") + "@example.com",
			"access_token":          "web-access",
			"lifecycle_state":       string(chatgptwebauth.LifecycleActive),
		},
	}
}

func chatGPTWebDependencyManagementRouter(h *Handler) http.Handler {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/auth-files", h.ListAuthFiles)
	router.DELETE("/auth-files", h.DeleteAuthFile)
	router.PATCH("/auth-files/status", h.PatchAuthFileStatus)
	router.PATCH("/auth-files/fields", h.PatchAuthFileFields)
	router.POST("/auth-files/restore", h.RestoreAuthFile)
	return router
}

func performChatGPTWebDependencyManagementRequest(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	router.ServeHTTP(recorder, request)
	return recorder
}

func decodeChatGPTWebDependencyManagementResponse(t *testing.T, recorder *httptest.ResponseRecorder, target any) {
	t.Helper()
	if errDecode := json.Unmarshal(recorder.Body.Bytes(), target); errDecode != nil {
		t.Fatalf("decode response: %v; body=%s", errDecode, recorder.Body.String())
	}
}
